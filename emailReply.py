import requests
from datetime import datetime, timedelta, timezone
import time
import csv
import os
import threading
import queue
import random
from concurrent.futures import ThreadPoolExecutor, as_completed
import logging
from tqdm import tqdm

# -------------------
# Config
# -------------------
# TENANT_ID     = os.environ.get("TENANT_ID", "")
# CLIENT_ID     = os.environ.get("CLIENT_ID", "")
# CLIENT_SECRET = os.environ.get("CLIENT_SECRET", "")

# -------------------
# Date range configuration
# Specify the date range to process emails (format: YYYY-MM-DD)
# Leave empty strings to use DAYS_BACK instead (default: 1 day back)
# -------------------
START_DATE = "2026-01-01"  #"2026-03-20"
END_DATE = "2026-03-31"    #"2026-03-23"
DAYS_BACK = 1    # Used only if START_DATE is empty

MAX_EMAILS = 100
OUTPUT_CSV = "EmailReplyReport.csv"
BATCH_SIZE = 50
# -------------------
# SELECTED USERS - leave empty to process all users in the tenant
# -------------------
SELECTED_USERS = set()

EXCLUDED_USERS = {
"careers@padams.in", "erp@padams.in", "ac-payabale@padams.in", "ac-payable2@padams.in", "accounts1@padams.in", "accounts2@padams.in",
"accounts3@padams.in", "accounts4@padams.in", "accounts5@padams.in"
}
EXCLUDED_SENDERS = {
    "airtelupdate@airtel.com", "csm.bot1@kotak.com", "kmb.reports@kotak.com", "noreply_1@rblbank.com", "s2bweb.admin@sc.com", "alerts@hdfcbank.net", "contact@send.houseofekam.com",
    "corporatenetbanking.automailer@hdfcbank.com", "credit_cards@icicibank.com", "custcomm@services.hdfcergo.com", "do_not_reply@apple.com",
    "donotreply@gst.gov.in", "emailstatements@hdfcbank.net", "enterprise@communication.porter.in", "erp@padams.in", "hereforyou@update.flourish.shop",
    "iedpms@rblbank.com", "info@easemytrip.com", "info@web-akasaair.in", "informations@hdfcbank.net", "mailers@marketing.goindigo.in",
    "maccount@microsoft.com", "newsletter@ethrworld.com", "newsletter@event.riseexpo.com", "newsletter@mktg.pepperfry.com",
    "newsletter@store.ferrari.com", "no-reply@otter.ai", "no-reply@sampark.gov.in", "no-reply@sg.newsletter.agoda-emails.com",
    "no-reply+3@bizanalyst.in", "noreply-ai-notification@airindia.com", "noreply@antraweb.com", "noreply@cleartrip.com",
    "noreply@communication.hdfcergo.com", "noreply@cult.fit", "noreply@hdfcbank.net", "noreply@paytm.com", "noreply@rblbank.com",
    "notification@jio.com", "predue@vodafoneideamailer.com", "service@icicisecurities.com", "statements@rblbank.com", "store+70161039648@m.shopifyemail.com",
    "tiplus.prod@kotak.com", "tradequalityunit@hdfcbank.com", "trademumbai@rblbank.com", "update@airtel.com", "careers@padams.in",
    "erp@padams.in", "ac-payabale@padams.in", "ac-payable2@padams.in", "accounts1@padams.in", "accounts2@padams.in",
    "accounts3@padams.in", "accounts4@padams.in", "accounts5@padams.in", "renewals@iciciprulife.com",
    "bankalerts@kotak.bank.in", "service@iciciprulife.com", "statements@rbl.bank.in", "noreply@rbl.bank.in", "info@news.ashampoo.com", "enet.admin@hdfcbank.com",
    "rblalerts@rbl.bank.in", "rblalerts@rbl.bank.com", "information@hdfcbank.net", "rblalerts@rblbank.com", "rbltradeservices@rblbank.com", "service@iciciprulife.com", "noreply@cibilenqalert.transunion.com",
    "statements@axisbank.com", "cc.statements@axisbank.com", "hrms@padams.in", "hr@padams.in", "alerts@axis.bank.in", "hello@mail.grammarly.com",
    "hello@notification.grammarly.com", "ebill@airtel.com", "121@airtel.com", "ext-ticket@google.com", "theteam@spacematrix.com",
    "customercare.india@kcc.com", "hello@ncpmailer.asianpaints.com", "marketing@email.kiaindia.net", "marketing2@fcmlindia.co.in",
    "marketing@nmp.nidoworld.com", "connect@easemytrip.com", "contact@detailsbe.com", "machineries@nmp.nidoworld.com",
    "uniopsservice@unilever.com", "marketing@united-group.in",
    "office365reports@microsoft.com", "teamzoom@e.zoom.us",
    "microsoftexchange329e71ec88ae4615bbc36ab6ce41109e@padams.co.in",
    "info@isomi.com", "marketing@tactileindicators.in",
    "teams@communication.microsoft.com", "invites@microsoft.com",
    "autodesk@autodeskcommunications.com", "support@e.read.ai",
    "m365copilotupdates@microsoft.com", "aec@autodeskcommunications.com",
    "tatiana.fedotova@autodeskcommunications.com", "slee9@c.snap.com",
    "shoutout@rapido.bike", "executiveassistant@e.read.ai",
    "sendsecure.support@bankofamerica.com", "support@e-builder.net",
    "bounces@e-builder.net", "microsoft365@communication.microsoft.com",
    "hello@updates.rapido.bike"
}

# -------------------
# FIX #2: Auto-reply / OOO subject prefixes
# Covers English, German, French OOO formats
# -------------------
AUTO_REPLY_SUBJECT_PREFIXES = (
    "automatic reply:",
    "out of office:",
    "autoreply:",
    "auto reply:",
    "auto:",
)

# -------------------
# FIX #4: Meeting invite / calendar response subject prefixes
# -------------------
MEETING_SUBJECT_PREFIXES = (
    "accepted:",
    "declined:",
    "tentative:",
    "cancelled:",
    "cancellation:",
    "meeting request:",
)

# -------------------
# FIX #5: NDR / delivery failure / read receipt subject prefixes
# -------------------
NDR_SUBJECT_PREFIXES = (
    "undeliverable:",
    "delivery status notification",
    "failure notice",
    "mail delivery failed",
    "returned mail:",
    "read:",
    "read receipt",
    "delivery failure",
)

# -------------------
# FIX #7: Forward subject prefixes (user forwarding instead of replying)
# Covers English, French (tr=transféré), German (wg=weitergeleitet)
# -------------------
FORWARD_SUBJECT_PREFIXES = ("fw:", "fwd:", "tr:", "wg:")


# -------------------
# FIX #2: Detect auto-reply / out-of-office messages
# Checks subject prefix AND the Auto-Submitted internet header (if fetched)
# -------------------
def is_auto_reply(msg):
    subject = (msg.get("subject") or "").lower().strip()
    if any(subject.startswith(p) for p in AUTO_REPLY_SUBJECT_PREFIXES):
        return True
    # Check internet message headers if $select=internetMessageHeaders was requested
    for h in msg.get("internetMessageHeaders", []):
        name = (h.get("name") or "").lower()
        value = (h.get("value") or "").lower()
        if name == "auto-submitted" and value != "no":
            return True
        if name == "x-auto-response-suppress":
            return True
    return False


# -------------------
# Logging Setup
# -------------------
logging.basicConfig(
    filename="run.log",
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s",
)
log = logging.getLogger("booster")

# -------------------
# Auth
# -------------------
def get_token():
    token_url = f"https://login.microsoftonline.com/{TENANT_ID}/oauth2/v2.0/token"
    token_data = {
        "client_id": CLIENT_ID,
        "scope": "https://graph.microsoft.com/.default",
        "client_secret": CLIENT_SECRET,
        "grant_type": "client_credentials"
    }
    r = requests.post(token_url, data=token_data)
    r.raise_for_status()
    return r.json()["access_token"]

token = get_token()
headers = {"Authorization": f"Bearer {token}"}
token_lock = threading.Lock()   # Issue 3: prevents redundant parallel refreshes

sheet = None   # Set to None while Google Sheets is disabled


# -------------------
# CSV + Google Sheets writer thread
# -------------------
output_queue = queue.Queue()
stop_signal = object()

def csv_writer_thread():
    buffer = []
    all_rows = []  # store for one-time Google Sheets upload
    csv_header = ["User","Subject","ReceivedTime","ReplyTime",
                  "ReplyGapHours","ReplyGapDays","SLABucket","CorrespondentEmail",
                  "CCRecipients","ReportDate"]

    if not os.path.exists(OUTPUT_CSV):
        with open(OUTPUT_CSV, "w", newline="", encoding="utf-8-sig") as f:
            writer = csv.DictWriter(f, fieldnames=csv_header)
            writer.writeheader()

    while True:
        item = output_queue.get()
        if item is stop_signal:
            break

        # append ReportDate to each row
        for row in item:
            # Use the date of the received email instead of fixed "today"
            if row.get("ReceivedTime"):
                row["ReportDate"] = row["ReceivedTime"].split(" ")[0]  # YYYY-MM-DD
            else:
                row["ReportDate"] = datetime.now().strftime("%Y-%m-%d")

        buffer.extend(item)
        all_rows.extend(item)

        if len(buffer) >= BATCH_SIZE:
            with open(OUTPUT_CSV, "a", newline="", encoding="utf-8-sig") as f:
                writer = csv.DictWriter(f, fieldnames=csv_header)
                writer.writerows(buffer)
            buffer.clear()

    # Flush remaining CSV
    if buffer:
        with open(OUTPUT_CSV, "a", newline="", encoding="utf-8-sig") as f:
            writer = csv.DictWriter(f, fieldnames=csv_header)
            writer.writerows(buffer)

writer_thread = threading.Thread(target=csv_writer_thread, daemon=True)
writer_thread.start()

# -------------------
# Issue 2: Load already-logged rows for deduplication
# Key = (user, received_time, correspondent_email) — unique per email per user.
# On a crash-resume, rows already written to CSV are skipped at the per-email
# level so partial users continue from where they left off, not from scratch.
# -------------------
existing_keys = set()
if os.path.exists(OUTPUT_CSV):
    with open(OUTPUT_CSV, "r", encoding="utf-8-sig") as f:
        reader = csv.DictReader(f)
        for row in reader:
            key = (
                row.get("User", "").lower(),
                row.get("ReceivedTime", ""),
                row.get("CorrespondentEmail", "").lower(),
            )
            existing_keys.add(key)
    print(f"📋 Loaded {len(existing_keys)} existing entries — duplicates will be skipped")

# -------------------
# Graph GET with retry
# -------------------
def graph_get(url, max_retries=5):
    global token, headers
    for attempt in range(max_retries):
        try:
            r = requests.get(url, headers=headers, timeout=60)

            if r.status_code == 401:
                with token_lock:
                    # Only refresh if this thread is the first to notice expiry;
                    # others waiting on the lock will reuse the new token automatically.
                    log.warning("Token expired, refreshing")
                    token = get_token()
                    headers = {"Authorization": f"Bearer {token}"}
                r = requests.get(url, headers=headers, timeout=60)

            if r.status_code in (429, 503, 504):
                wait = (2 ** attempt) + random.random()
                log.warning(f"Throttled ({r.status_code}) on {url} — retry in {wait:.1f}s")
                time.sleep(wait)
                continue

            return r

        except requests.exceptions.RequestException as e:
            wait = (2 ** attempt) + random.random()
            log.error(f"Network error on {url}: {e} — retrying in {wait:.1f}s")
            time.sleep(wait)

    raise Exception(f"❌ Failed after {max_retries} attempts: {url}")

# -------------------
# Fetch all users and filter mail-enabled members locally
# -------------------
all_users = []
url = "https://graph.microsoft.com/v1.0/users?$select=id,mail,userPrincipalName,userType&$top=100"
while url:
    r = graph_get(url)
    if r.status_code != 200:
        print("❌ ERROR fetching users:", r.status_code, r.text)
        exit()
    data = r.json()
    all_users.extend(data.get("value", []))
    url = data.get("@odata.nextLink")

all_users = [u for u in all_users if u.get("mail") and (u.get("userType") or "").lower() == "member"]
print(f"✅ Found {len(all_users)} member users with mail addresses")

# -------------------
# Filter to SELECTED_USERS if specified
# -------------------
if SELECTED_USERS:
    all_users = [u for u in all_users if (u.get("mail") or u.get("userPrincipalName")).lower() in {e.lower() for e in SELECTED_USERS}]
    print(f"🎯 Filtered to {len(all_users)} selected users")
else:
    print(f"👥 Processing all users ({len(all_users)} total)")

# -------------------
# Date filter - use START_DATE/END_DATE if specified, otherwise use DAYS_BACK
# -------------------
if START_DATE and END_DATE:
    try:
        start_dt = datetime.strptime(START_DATE, "%Y-%m-%d").replace(tzinfo=timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ")
        end_dt = datetime.strptime(END_DATE, "%Y-%m-%d").replace(tzinfo=timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ")
        print(f"📅 Using date range: {START_DATE} to {END_DATE}")
    except ValueError as e:
        print(f"❌ Invalid date format: {e}. Using DAYS_BACK={DAYS_BACK} instead")
        start_dt = (datetime.now(timezone.utc) - timedelta(days=DAYS_BACK)).strftime("%Y-%m-%dT%H:%M:%SZ")
        end_dt   = datetime.now(timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ")
else:
    start_dt = (datetime.now(timezone.utc) - timedelta(days=DAYS_BACK)).strftime("%Y-%m-%dT%H:%M:%SZ")
    end_dt   = datetime.now(timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ")
    print(f"📅 Using DAYS_BACK: {DAYS_BACK} day(s)")

# -------------------
# Process one user
# -------------------
def process_user(user):
    user_id = user.get("id")
    user_mail = user.get("mail") or user.get("userPrincipalName")
    if not user_id or not user_mail:
        return

    if user_mail.lower() in EXCLUDED_USERS:
        print(f"⛔ Skipping excluded user mailbox: {user_mail}")
        return

    log.info(f"Processing user {user_mail}")
    print(f"📩 {user_mail}")

    # FIX #6: Use /mailFolders/inbox/messages instead of /messages
    # This excludes Junk/Spam, Deleted Items, and other non-inbox folders
    messages_url = (
        f"https://graph.microsoft.com/v1.0/users/{user_id}/mailFolders/inbox/messages"
        f"?$filter=receivedDateTime ge {start_dt} and receivedDateTime lt {end_dt}&$top={MAX_EMAILS}"
    )

    messages = []
    while messages_url:
        r = graph_get(messages_url)
        if r.status_code != 200:
            if r.status_code == 404:
                log.warning(f"Mailbox not found or not licensed for {user_mail} (user ID {user_id})")
            else:
                log.error(f"Error {r.status_code} fetching messages for {user_mail}")
            return
        data = r.json()
        for m in data.get("value", []):
            if not m.get("from") or not m["from"].get("emailAddress") or not m["from"]["emailAddress"].get("address"):
                continue
            if m["from"]["emailAddress"]["address"].lower() == user_mail.lower():
                continue
            messages.append(m)
        messages_url = data.get("@odata.nextLink")
        time.sleep(0.1)

    print(f"   → Got {len(messages)} emails")

    thread_cache = {}
    user_rows = []

    for msg in messages:
        received_time = msg["receivedDateTime"]
        conversation_id = msg["conversationId"]
        subject = msg.get("subject") or ""
        sender_email = msg["from"]["emailAddress"]["address"].lower()
        subject_lower = subject.lower().strip()

        # Auto-exclude no-reply style sender addresses.
        # BUG FIX #2: Only check the local part (before @) for keyword matches
        # to avoid false positives on domains like autodesk.com, autonomous-systems.io etc.
        sender_local = sender_email.split("@")[0]
        if (
            sender_email in EXCLUDED_SENDERS
            or "noreply" in sender_local
            or "no-reply" in sender_local
            or "donotreply" in sender_local
            or "do-not-reply" in sender_local
            or "automailer" in sender_local
            or "autoresponder" in sender_local
            or "autoreply" in sender_local
            or "auto-reply" in sender_local
        ):
            continue

        # FIX #2: Skip inbound auto-reply / OOO emails (e.g. client is OOO)
        if is_auto_reply(msg):
            log.info(f"Skipping auto-reply inbound: '{subject}' from {sender_email}")
            continue

        # FIX #4: Skip meeting invite / calendar response emails
        if any(subject_lower.startswith(p) for p in MEETING_SUBJECT_PREFIXES):
            log.info(f"Skipping meeting invite: '{subject}' from {sender_email}")
            continue

        # FIX #5: Skip NDR / delivery failure / read receipt emails
        if any(subject_lower.startswith(p) for p in NDR_SUBJECT_PREFIXES):
            log.info(f"Skipping NDR/read-receipt: '{subject}' from {sender_email}")
            continue

        cc_emails = [
            cc["emailAddress"]["address"].lower()
            for cc in msg.get("ccRecipients", [])
            if cc.get("emailAddress") and cc["emailAddress"].get("address")
        ]
        cc_str = "; ".join(cc_emails)

        to_emails = [
            to["emailAddress"]["address"].lower()
            for to in msg.get("toRecipients", [])
            if to.get("emailAddress") and to["emailAddress"].get("address")
        ]

        user_l = user_mail.lower()

        # If user is ONLY in CC → skip creating report row
        if user_l not in to_emails:
            continue

        if conversation_id not in thread_cache:
            thread_url = (
                f"https://graph.microsoft.com/v1.0/users/{user_id}/messages"
                f"?$filter=conversationId eq '{conversation_id}'&$top={MAX_EMAILS}"
            )
            thread_messages = []
            while thread_url:
                tr = graph_get(thread_url)
                if tr.status_code != 200:
                    break
                t_data = tr.json()
                thread_messages.extend(t_data.get("value", []))
                thread_url = t_data.get("@odata.nextLink")
                time.sleep(0.1)
            # BUG FIX #1: Sort ascending by sentDateTime so the loop always
            # finds the EARLIEST reply first, regardless of Graph API return order.
            thread_messages.sort(key=lambda m: m.get("sentDateTime") or "")
            thread_cache[conversation_id] = thread_messages
        else:
            thread_messages = thread_cache[conversation_id]

        # BUG FIX #3: Compute rcv_time once here, outside the reply loop.
        # Previously it was re-parsed on every iteration even though it never changes.
        rcv_time = datetime.fromisoformat(received_time.replace("Z", "+00:00"))

        # Issue 2: Skip emails already written to CSV in a previous run.
        # Uses (user, received_time, sender) as a unique key per email per user.
        row_key = (user_mail.lower(), rcv_time.strftime("%Y-%m-%d %H:%M:%S"), sender_email)
        if row_key in existing_keys:
            continue

        reply_found = False
        cc_reply_email = None
        to_reply_email = None   # FIX #1: track if another TO recipient replied
        forward_found = False   # FIX #7: track if user forwarded instead of replying

        # NOTE (Point 3): Each inbound message is matched against replies using
        # rpl_time > rcv_time, so replies are always relative to THIS message's
        # received time. A reply to an earlier message in the thread won't be
        # incorrectly credited if it arrived before this message.

        for reply in thread_messages:
            if not reply.get("from") or not reply["from"].get("emailAddress") or not reply["from"]["emailAddress"].get("address"):
                continue
            if not reply.get("sentDateTime"):
                continue

            reply_email = reply["from"]["emailAddress"]["address"].lower()
            reply_subject = (reply.get("subject") or "").lower().strip()

            rpl_time = datetime.fromisoformat(reply["sentDateTime"].replace("Z", "+00:00"))

            if rpl_time <= rcv_time:
                continue  # reply must be after the inbound message

            # FIX #2: Skip auto-replies (OOO) in the thread — don't count as valid reply
            if is_auto_reply(reply):
                log.info(f"Ignoring auto-reply in thread from {reply_email} for '{subject}'")
                continue

            # ✅ If user replied directly
            if reply_email == user_l:
                # FIX #7: Check if user forwarded instead of replying directly
                if any(reply_subject.startswith(p) for p in FORWARD_SUBJECT_PREFIXES):
                    forward_found = True
                    reply_found = True
                    break

                hours = round((rpl_time - rcv_time).total_seconds() / 3600, 2)
                if hours <= 2:   bucket = "<= 2 hours"
                elif hours <= 4: bucket = "<= 4 hours"
                elif hours <= 6: bucket = "<= 6 hours"
                else:            bucket = "> 6 hours"

                user_rows.append({
                    "User": user_mail,
                    "Subject": subject,
                    "ReceivedTime": rcv_time.strftime("%Y-%m-%d %H:%M:%S"),
                    "ReplyTime": rpl_time.strftime("%Y-%m-%d %H:%M:%S"),
                    "ReplyGapHours": hours,
                    "ReplyGapDays": f"{int(hours // 24)} days",
                    "SLABucket": bucket,
                    "CorrespondentEmail": sender_email,
                    "CCRecipients": cc_str,
                })
                reply_found = True
                break

            # FIX #1: Another TO recipient replied → credit all TO members.
            # Capture rpl_time and rcv_time here so SLA can be computed below.
            if reply_email in to_emails and reply_email != user_l:
                to_reply_email = reply_email
                reply_found = True
                break

            # ✅ CC person replied
            if reply_email in cc_emails:
                cc_reply_email = reply_email
                reply_found = True
                break

        # FIX #7: User forwarded the email instead of replying
        if reply_found and forward_found:
            user_rows.append({
                "User": user_mail,
                "Subject": subject,
                "ReceivedTime": rcv_time.strftime("%Y-%m-%d %H:%M:%S"),
                "ReplyTime": rpl_time.strftime("%Y-%m-%d %H:%M:%S"),
                "ReplyGapHours": "",
                "ReplyGapDays": "",
                "SLABucket": "Forwarded",
                "CorrespondentEmail": sender_email,
                "CCRecipients": cc_str,
            })
            continue

        # FIX #1: Another TO recipient replied → mark as replied for this user too.
        # Compute the same SLA hours/bucket so the graph treats it identically
        # to a direct reply. ReplyTime shows when the team member replied.
        if reply_found and to_reply_email:
            hours = round((rpl_time - rcv_time).total_seconds() / 3600, 2)
            if hours <= 2:   bucket = "<= 2 hours"
            elif hours <= 4: bucket = "<= 4 hours"
            elif hours <= 6: bucket = "<= 6 hours"
            else:            bucket = "> 6 hours"
            user_rows.append({
                "User": user_mail,
                "Subject": subject,
                "ReceivedTime": rcv_time.strftime("%Y-%m-%d %H:%M:%S"),
                "ReplyTime": rpl_time.strftime("%Y-%m-%d %H:%M:%S"),
                "ReplyGapHours": hours,
                "ReplyGapDays": f"{int(hours // 24)} days",
                "SLABucket": bucket,
                "CorrespondentEmail": sender_email,
                "CCRecipients": cc_str,
            })
            continue

        # CC replied → mark differently
        if reply_found and cc_reply_email:
            user_rows.append({
                "User": user_mail,
                "Subject": subject,
                "ReceivedTime": rcv_time.strftime("%Y-%m-%d %H:%M:%S"),
                "ReplyTime": rpl_time.strftime("%Y-%m-%d %H:%M:%S"),
                "ReplyGapHours": "",
                "ReplyGapDays": "",
                "SLABucket": f"CC Reply ({cc_reply_email})",
                "CorrespondentEmail": sender_email,
                "CCRecipients": cc_str,
            })
            continue

        if not reply_found:
            user_rows.append({
                "User": user_mail,
                "Subject": subject,
                "ReceivedTime": rcv_time.strftime("%Y-%m-%d %H:%M:%S"),
                "ReplyTime": "",
                "ReplyGapHours": "",
                "ReplyGapDays": "",
                "SLABucket": "No Reply",
                "CorrespondentEmail": sender_email,
                "CCRecipients": cc_str,
            })

    if user_rows:
        output_queue.put(user_rows)

# -------------------
# Parallel execution with Progress Bar
# -------------------
start_time = time.time()

with ThreadPoolExecutor(max_workers=20) as executor:
    futures = {executor.submit(process_user, u): u for u in all_users}

    bar_format = "{desc} |{bar}| {n_fmt}/{total_fmt} [{elapsed}]"
    with tqdm(total=len(futures), desc="Processing Users", bar_format=bar_format) as pbar:
        for future in as_completed(futures):
            user = futures[future]
            user_mail = user.get("mail") or user.get("userPrincipalName")
            if user_mail:
                pbar.set_description(f"Processing: {user_mail}")
            try:
                future.result()
            except Exception as e:
                log.error(f"Error in thread ({user_mail}): {e}", exc_info=True)
            finally:
                pbar.update(1)

output_queue.put(stop_signal)
writer_thread.join()

elapsed = time.time() - start_time
print(f"⏱️ Script finished in {time.strftime('%H:%M:%S', time.gmtime(elapsed))}")
