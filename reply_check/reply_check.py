import csv
import re
import sys
import threading
import time
from collections import Counter, defaultdict
from concurrent.futures import ThreadPoolExecutor
from datetime import datetime
from pathlib import Path

import requests
from openpyxl import Workbook, load_workbook
from openpyxl.styles import Alignment, Font

HERE = Path(__file__).resolve().parent
ROOT = HERE.parent
sys.path.insert(0, str(ROOT))
from exclusions import is_excluded_sender  # noqa: E402

USERS_FILE = HERE / "Email Summary.xlsx"


def load_users():
    rows = load_workbook(USERS_FILE, read_only=True).active.iter_rows(values_only=True)
    col = next(rows).index("Mail ID")
    # The sheet is typed by hand, e.g. "Shankar@padams,in".
    return [str(r[col]).strip().lower().replace(",", ".") for r in rows if r[col]]


USERS = load_users()
START_DATE = "2026-07-01"
END_DATE = "2026-10-01"  # exclusive, so the report runs through 30 Sept

SUMMARY_XLSX = HERE / "reply_check_summary.xlsx"
DETAILS_CSV = HERE / "reply_check_details.csv"

GRAPH = "https://graph.microsoft.com/v1.0"
AUTO_REPLY_PREFIXES = ("automatic reply:", "out of office:", "autoreply:", "auto reply:", "auto:")
# Same inbound subjects emailReply.py skips: auto-replies, meeting responses, bounces and read receipts.
SKIP_SUBJECT_PREFIXES = AUTO_REPLY_PREFIXES + (
    "accepted:", "declined:", "tentative:", "cancelled:", "cancellation:", "meeting request:",
    "undeliverable:", "delivery status notification", "failure notice", "mail delivery failed",
    "returned mail:", "read:", "read receipt", "delivery failure",
)


# One file per Microsoft 365 account, tried in this order. The secrets stay in these local files.
CREDENTIAL_FILES = [ROOT / "emailReply.py", ROOT / "co-in.txt"]


class Tenant:
    def __init__(self, path):
        src = path.read_text(encoding="utf-8")
        self.name = path.name
        self.creds = {}
        for key in ("TENANT_ID", "CLIENT_ID", "CLIENT_SECRET"):
            m = re.search(rf'^{key}\s*=\s*"([^"]+)"', src, re.M)
            if not m:
                sys.exit(f"{key} not found in {path}")
            self.creds[key] = m.group(1)
        self.token = None
        self.lock = threading.Lock()

    def refresh_token(self):
        r = requests.post(
            f"https://login.microsoftonline.com/{self.creds['TENANT_ID']}/oauth2/v2.0/token",
            data={
                "client_id": self.creds["CLIENT_ID"],
                "client_secret": self.creds["CLIENT_SECRET"],
                "scope": "https://graph.microsoft.com/.default",
                "grant_type": "client_credentials",
            },
            timeout=60,
        )
        r.raise_for_status()
        self.token = r.json()["access_token"]

    def get(self, url, params=None):
        for attempt in range(6):
            if self.token is None:
                with self.lock:
                    if self.token is None:
                        self.refresh_token()
            try:
                r = requests.get(url, headers={"Authorization": f"Bearer {self.token}"}, params=params, timeout=120)
            except (requests.ConnectionError, requests.Timeout):
                time.sleep(2 ** attempt)
                continue
            if r.status_code == 401:
                with self.lock:
                    self.refresh_token()
                continue
            if r.status_code in (429, 500, 502, 503, 504):
                time.sleep(int(r.headers.get("Retry-After", 2 ** attempt)))
                continue
            r.raise_for_status()
            return r.json()
        raise RuntimeError(f"Giving up on {url}")

    def get_all(self, url, params=None):
        while url:
            data = self.get(url, params)
            yield from data.get("value", [])
            url, params = data.get("@odata.nextLink"), None


TENANTS = [Tenant(p) for p in CREDENTIAL_FILES if p.exists()]


def parse_time(ts):
    return datetime.fromisoformat(ts.replace("Z", "+00:00"))


def sender_of(msg):
    return (((msg.get("from") or {}).get("emailAddress") or {}).get("address") or "").lower()


def addresses(recipients):
    return {
        (r.get("emailAddress") or {}).get("address", "").lower()
        for r in recipients or []
        if (r.get("emailAddress") or {}).get("address")
    }


def check_user(email):
    for tenant in TENANTS:
        try:
            rows = check_user_in(tenant, email)
        except requests.HTTPError as e:
            if e.response is not None and e.response.status_code == 404:
                continue  # the account exists here, but its mailbox is in another tenant
            print(f"{email}: failed - {e}")
            return None
        except (requests.RequestException, RuntimeError) as e:
            print(f"{email}: failed - {e}")
            return None
        if rows is not None:
            print(f"{email}: {len(rows)} emails ({tenant.name})")
            return rows
    print(f"{email}: no readable mailbox in {', '.join(t.name for t in TENANTS)}")
    return None


def check_user_in(tenant, email):
    select = "id,mail,proxyAddresses"
    found = list(tenant.get_all(f"{GRAPH}/users", {"$filter": f"mail eq '{email}'", "$select": select}))
    if not found:
        found = list(tenant.get_all(
            f"{GRAPH}/users", {"$filter": f"proxyAddresses/any(p:p eq 'smtp:{email}')", "$select": select}
        ))
    if not found:
        return None
    uid = found[0]["id"]
    own = {email, (found[0].get("mail") or "").lower()} | {
        p[5:].lower() for p in found[0].get("proxyAddresses") or [] if p.lower().startswith("smtp:")
    }
    start, end = f"{START_DATE}T00:00:00Z", f"{END_DATE}T00:00:00Z"

    inbox = list(tenant.get_all(
        f"{GRAPH}/users/{uid}/mailFolders/inbox/messages",
        {
            "$filter": f"receivedDateTime ge {start} and receivedDateTime lt {end}",
            "$select": "receivedDateTime,subject,from,toRecipients,ccRecipients,conversationId",
            "$top": "500",
        },
    ))

    sent_items_id = tenant.get(f"{GRAPH}/users/{uid}/mailFolders/sentitems", {"$select": "id"})["id"]

    # Everything in the mailbox since the start date (all folders, incl. Sent Items),
    # so replies sent after the end date still count.
    threads = defaultdict(list)
    for m in tenant.get_all(
        f"{GRAPH}/users/{uid}/messages",
        {
            "$filter": f"receivedDateTime ge {start}",
            "$select": "from,sentDateTime,subject,conversationId,isDraft,parentFolderId",
            "$top": "500",
        },
    ):
        subject = (m.get("subject") or "").lower().strip()
        if m.get("sentDateTime") and not m.get("isDraft") and not subject.startswith(AUTO_REPLY_PREFIXES):
            # With $select, Graph often returns the user's own sender as an Exchange ID
            # ("/o=exchangelabs/...") instead of their email, so anything in Sent Items counts as theirs.
            mine = sender_of(m) in own or m.get("parentFolderId") == sent_items_id
            threads[m["conversationId"]].append((parse_time(m["sentDateTime"]), sender_of(m), mine))

    rows = []
    for m in inbox:
        sender = sender_of(m)
        if not sender or sender in own or is_excluded_sender(sender):
            continue
        if (m.get("subject") or "").lower().strip().startswith(SKIP_SUBJECT_PREFIXES):
            continue
        to, cc = addresses(m.get("toRecipients")), addresses(m.get("ccRecipients"))
        received = parse_time(m["receivedDateTime"])
        later = sorted(r for r in threads.get(m["conversationId"], []) if r[0] > received)
        mine = next((r for r in later if r[2]), None)
        other = next((r for r in later if not r[2] and r[1] in (to | cc) - own - {sender}), None)
        reply = (mine[0], email) if mine else other
        rows.append({
            "User": email,
            "Type": "To" if own & to else "CC" if own & cc else "BCC / group",
            "Received (UTC)": received.strftime("%Y-%m-%d %H:%M"),
            "From": sender,
            "Subject": m.get("subject") or "",
            "Status": "Replied" if mine else "CC replied" if other else "Not replied",
            "Replied By": reply[1] if reply else "",
            "Reply Time (UTC)": reply[0].strftime("%Y-%m-%d %H:%M") if reply else "",
        })
    rows.sort(key=lambda r: r["Received (UTC)"])
    return rows


def summarize(rows, users):
    summary = []
    for email in users:
        for kind in ("To", "CC", "BCC / group"):
            mails = [r for r in rows if r["User"] == email and r["Type"] == kind]
            status = Counter(r["Status"] for r in mails)
            answered = status["Replied"] + status["CC replied"]
            summary.append({
                "User": email,
                "Type": kind,
                "Total Emails": len(mails),
                "Replied": status["Replied"],
                "CC Replied": status["CC replied"],
                "Not Replied": status["Not replied"],
                "Reply %": round(answered / len(mails) * 100, 1) if mails else 0,
            })
    return summary


def write_csv(path, rows):
    with open(path, "w", newline="", encoding="utf-8-sig") as f:
        writer = csv.DictWriter(f, fieldnames=list(rows[0]))
        writer.writeheader()
        writer.writerows(rows)
    print(f"Wrote {path} ({len(rows)} rows)")


def write_summary_xlsx(path, summary):
    wb = Workbook()
    ws = wb.active
    ws.title = "Summary"
    headers = list(summary[0]) + ["Average Reply % (To & CC)"]
    ws.append(headers)
    for cell in ws[1]:
        cell.font = Font(bold=True)
    avg_col = len(headers)
    row = 2
    for email in dict.fromkeys(s["User"] for s in summary):
        block = [s for s in summary if s["User"] == email]
        for s in block:
            ws.append(list(s.values()))
        # A type with no mails is left out so its 0% doesn't pull the average down.
        pcts = [s["Reply %"] for s in block if s["Type"] in ("To", "CC") and s["Total Emails"]]
        ws.cell(row, avg_col, round(sum(pcts) / len(pcts), 1) if pcts else "No mails")
        ws.merge_cells(start_row=row, start_column=avg_col, end_row=row + len(block) - 1, end_column=avg_col)
        ws.cell(row, avg_col).alignment = Alignment(horizontal="center", vertical="center")
        row += len(block)
    for col in ws.columns:
        ws.column_dimensions[col[0].column_letter].width = max(len(str(c.value or "")) for c in col) + 2
    wb.save(path)
    print(f"Wrote {path} ({len(summary)} rows)")


def main():
    print(f"Checking {len(USERS)} users from {START_DATE} to {END_DATE} (exclusive)")
    with ThreadPoolExecutor(max_workers=8) as pool:
        results = dict(zip(USERS, pool.map(check_user, USERS)))
    users = [u for u in USERS if results[u] is not None]
    skipped = [u for u in USERS if results[u] is None]
    if skipped:
        print(f"Left out of the report (mailbox could not be read): {', '.join(skipped)}")
    rows = [r for u in users for r in results[u]]
    if not rows:
        sys.exit("No emails found")
    write_csv(DETAILS_CSV, rows)
    write_summary_xlsx(SUMMARY_XLSX, summarize(rows, users))


if __name__ == "__main__":
    main()
