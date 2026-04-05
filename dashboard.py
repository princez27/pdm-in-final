import streamlit as st
import pandas as pd
import plotly.express as px
import plotly.graph_objects as go
from io import BytesIO
import os
import hashlib
import time

# ─────────────────────────────────────────────
# Page config  (must be first Streamlit call)
# ─────────────────────────────────────────────
st.set_page_config(
    page_title="Email Reply Analytics",
    page_icon="📧",
    layout="wide",
    initial_sidebar_state="expanded"
)

# ─────────────────────────────────────────────
# USER CREDENTIALS
# Add / remove users here.
# To generate a hash for a new password run:
#   python -c "import hashlib; print(hashlib.sha256('yourpassword'.encode()).hexdigest())"
# ─────────────────────────────────────────────
USERS = {
    "admin": {
        "name":          "Administrator",
        "password_hash": hashlib.sha256("Pdms@#2050$".encode()).hexdigest(),
        "role":          "admin",   # admin sees everything
    },
    "manager": {
        "name":          "Manager",
        "password_hash": hashlib.sha256("manager@123".encode()).hexdigest(),
        "role":          "viewer",  # read-only
    },
}
# ─────────────────────────────────────────────


# ─────────────────────────────────────────────
# Global CSS
# ─────────────────────────────────────────────
st.markdown("""
<style>
    .main { background-color: #f5f7fa; }
    #MainMenu, footer, header { visibility: hidden; }

    /* ── KPI cards ── */
    .kpi-card {
        background: white;
        border-radius: 12px;
        padding: 20px 24px;
        box-shadow: 0 2px 8px rgba(0,0,0,0.07);
        text-align: center;
    }
    .kpi-label {
        font-size: 12px;
        color: #6b7280;
        font-weight: 600;
        text-transform: uppercase;
        letter-spacing: 0.6px;
        margin-bottom: 6px;
    }
    .kpi-value { font-size: 32px; font-weight: 700; color: #1e293b; }
    .kpi-sub   { font-size: 12px; color: #9ca3af; margin-top: 4px; }

    /* ── Section titles ── */
    .section-title {
        font-size: 15px;
        font-weight: 600;
        color: #1e293b;
        margin: 4px 0 12px 0;
        padding-bottom: 6px;
        border-bottom: 2px solid #e2e8f0;
    }

    /* ── Login card ── */
    .login-title    { font-size: 26px; font-weight: 700; color: #1e293b; margin-bottom: 4px; }
    .login-subtitle { font-size: 14px; color: #6b7280; margin-bottom: 28px; }

    /* ── Sidebar ── */
    [data-testid="stSidebar"] { background-color: #1e293b; }
    [data-testid="stSidebar"] * { color: #e2e8f0 !important; }

    /* Filter labels */
    [data-testid="stSidebar"] .stSelectbox label,
    [data-testid="stSidebar"] .stMultiSelect label,
    [data-testid="stSidebar"] .stDateInput label {
        color: #94a3b8 !important;
        font-size: 12px;
    }

    /* ── All sidebar inputs — unified style ── */

    /* Date inputs */
    [data-testid="stSidebar"] .stDateInput input {
        color: #f1f5f9 !important;
        background-color: #334155 !important;
        border: 1px solid #475569 !important;
        border-radius: 6px !important;
    }

    /* Multiselect input area */
    [data-testid="stSidebar"] .stMultiSelect > div > div,
    [data-testid="stSidebar"] .stMultiSelect > div {
        background-color: #334155 !important;
        border: 1px solid #475569 !important;
        border-radius: 6px !important;
        color: #f1f5f9 !important;
    }

    /* Selectbox */
    [data-testid="stSidebar"] .stSelectbox > div > div,
    [data-testid="stSidebar"] .stSelectbox > div {
        background-color: #334155 !important;
        border: 1px solid #475569 !important;
        border-radius: 6px !important;
        color: #f1f5f9 !important;
    }

    /* Multiselect tags (selected items) */
    [data-testid="stSidebar"] .stMultiSelect span[data-baseweb="tag"] {
        background-color: #475569 !important;
        color: #f1f5f9 !important;
        border-radius: 4px !important;
    }

    /* Placeholder text inside inputs */
    [data-testid="stSidebar"] input::placeholder,
    [data-testid="stSidebar"] .stMultiSelect input::placeholder {
        color: #94a3b8 !important;
    }

    /* Logout + Refresh buttons */
    [data-testid="stSidebar"] .stButton > button {
        background-color: #334155 !important;
        color: #f1f5f9 !important;
        border: 1px solid #475569 !important;
        border-radius: 6px !important;
        width: 100%;
    }
    [data-testid="stSidebar"] .stButton > button:hover {
        background-color: #475569 !important;
        border-color: #64748b !important;
    }

    /* Caption / small text */
    [data-testid="stSidebar"] small,
    [data-testid="stSidebar"] .stCaption { color: #94a3b8 !important; }

    /* ── Hide sidebar collapse button to prevent accidental close ── */
    [data-testid="stSidebarCollapseButton"],
    [data-testid="collapsedControl"] button {
        display: none !important;
    }
</style>
""", unsafe_allow_html=True)


# ─────────────────────────────────────────────
# Session-state defaults
# ─────────────────────────────────────────────
for key, default in {
    "authenticated": False,
    "username":      "",
    "user_name":     "",
    "role":          "",
    "login_error":   "",
    "login_attempts": 0,
    "locked_until":  0.0,
}.items():
    if key not in st.session_state:
        st.session_state[key] = default


# ─────────────────────────────────────────────
# Login page
# ─────────────────────────────────────────────
def show_login():
    _, centre, _ = st.columns([1, 1.1, 1])
    with centre:
        st.markdown("<div style='padding-top:60px;'>", unsafe_allow_html=True)
        st.markdown("## 📧 Email Reply Analytics")
        st.markdown(
            "<div class='login-subtitle'>Sign in to access the dashboard</div>",
            unsafe_allow_html=True
        )

        # Lockout check
        if time.time() < st.session_state.locked_until:
            remaining = int(st.session_state.locked_until - time.time())
            st.error(f"Too many failed attempts. Try again in {remaining}s.")
            st.stop()

        if st.session_state.login_error:
            st.error(st.session_state.login_error)

        with st.form("login_form", clear_on_submit=False):
            username  = st.text_input("Username", placeholder="Enter username")
            password  = st.text_input("Password", type="password", placeholder="Enter password")
            submitted = st.form_submit_button("Sign In", width='stretch')

        if submitted:
            pw_hash = hashlib.sha256(password.encode()).hexdigest()
            user    = USERS.get(username.strip().lower())

            if user and user["password_hash"] == pw_hash:
                st.session_state.authenticated   = True
                st.session_state.username        = username.strip().lower()
                st.session_state.user_name       = user["name"]
                st.session_state.role            = user["role"]
                st.session_state.login_error     = ""
                st.session_state.login_attempts  = 0
                st.rerun()
            else:
                st.session_state.login_attempts += 1
                left = 5 - st.session_state.login_attempts
                if st.session_state.login_attempts >= 5:
                    st.session_state.locked_until   = time.time() + 120
                    st.session_state.login_attempts = 0
                    st.session_state.login_error    = "Account locked for 2 minutes."
                else:
                    st.session_state.login_error = (
                        f"Invalid username or password. {left} attempt(s) remaining."
                    )
                st.rerun()

        st.markdown("</div>", unsafe_allow_html=True)


# ─────────────────────────────────────────────
# Guard — stop here if not logged in
# ─────────────────────────────────────────────
if not st.session_state.authenticated:
    show_login()
    st.stop()


# ══════════════════════════════════════════════
# DASHBOARD  (only reached when authenticated)
# ══════════════════════════════════════════════

CSV_PATH = os.path.join(os.path.dirname(__file__), "EmailReplyReport.csv")

SLA_ORDER  = ["<= 2 hours", "<= 4 hours", "<= 6 hours", "> 6 hours", "No Reply"]
SLA_COLORS = {
    "<= 2 hours": "#16a34a",
    "<= 4 hours": "#65a30d",
    "<= 6 hours": "#f59e0b",
    "> 6 hours":  "#ef4444",
    "No Reply":   "#6b7280",
}


# ─────────────────────────────────────────────
# Load + cache data  (refreshes every 5 min)
# ─────────────────────────────────────────────
@st.cache_data(ttl=300)
def load_data():
    if not os.path.exists(CSV_PATH):
        return pd.DataFrame()
    df = pd.read_csv(CSV_PATH, encoding="utf-8-sig")
    df["ReceivedTime"]       = pd.to_datetime(df.get("ReceivedTime"),       errors="coerce")
    df["ReplyTime"]          = pd.to_datetime(df.get("ReplyTime"),          errors="coerce")
    df["ReportDate"]         = pd.to_datetime(df.get("ReportDate"),         errors="coerce")
    df["ReplyGapHours"]      = pd.to_numeric(df.get("ReplyGapHours"),       errors="coerce")
    df["ReplyGapDays"]       = pd.to_numeric(df.get("ReplyGapDays").astype(str).str.replace(r"\s*days?$", "", regex=True), errors="coerce")
    # CC-Count removed
    df["Date"]               = df["ReceivedTime"].dt.date
    df["DayOfWeek"]          = df["ReceivedTime"].dt.day_name()
    df["Replied"]            = df["SLABucket"].notna() & (df["SLABucket"] != "No Reply")
    df["User"]               = df["User"].str.strip().str.lower()
    df["CorrespondentEmail"] = df["CorrespondentEmail"].str.strip().str.lower()
    return df


df_raw = load_data()

if df_raw.empty:
    st.error("No data found — make sure `EmailReplyReport.csv` is in the same folder as dashboard.py.")
    st.stop()


# ─────────────────────────────────────────────
# Sidebar — user info + all filters
# ─────────────────────────────────────────────
with st.sidebar:
    st.markdown("## 📧 Email Analytics")
    st.markdown(
        f"<div style='font-size:13px;color:#94a3b8;margin-bottom:2px'>Logged in as</div>"
        f"<div style='font-size:15px;font-weight:600;color:#f1f5f9'>{st.session_state.user_name}</div>"
        f"<div style='font-size:11px;color:#94a3b8'>{st.session_state.role.capitalize()}</div>",
        unsafe_allow_html=True
    )
    st.markdown("---")
    st.markdown("### Filters")

    min_date = df_raw["Date"].min()
    max_date = df_raw["Date"].max()

    date_from = st.date_input("From Date", value=min_date, min_value=min_date, max_value=max_date)
    date_to   = st.date_input("To Date",   value=max_date, min_value=min_date, max_value=max_date)
    st.markdown("---")

    all_users = sorted(df_raw["User"].dropna().unique())
    sel_users = st.multiselect("User (Email)", options=all_users, placeholder="All users")

    all_senders = sorted(df_raw["CorrespondentEmail"].dropna().unique())
    sel_senders = st.multiselect("Client / Sender Email", options=all_senders, placeholder="All senders")

    all_buckets = [b for b in SLA_ORDER if b in df_raw["SLABucket"].values]
    sel_buckets = st.multiselect("SLA Bucket", options=all_buckets, placeholder="All buckets")

    all_days = ["Monday","Tuesday","Wednesday","Thursday","Friday","Saturday","Sunday"]
    sel_days = st.multiselect("Day of Week", options=all_days, placeholder="All days")

    st.markdown("---")
    st.caption(f"Auto-refreshes every 5 min\nLast load: {pd.Timestamp.now().strftime('%H:%M:%S')}")
    if st.button("Refresh Now", width='stretch'):
        st.cache_data.clear()
        st.rerun()
    st.markdown("---")
    if st.button("Logout", width='stretch'):
        st.session_state.authenticated = False
        st.session_state.username      = ""
        st.session_state.user_name     = ""
        st.session_state.role          = ""
        st.rerun()


# ─────────────────────────────────────────────
# Apply filters
# ─────────────────────────────────────────────
df = df_raw.copy()
df = df[(df["Date"] >= date_from) & (df["Date"] <= date_to)]
if sel_users:   df = df[df["User"].isin(sel_users)]
if sel_senders: df = df[df["CorrespondentEmail"].isin(sel_senders)]
if sel_buckets: df = df[df["SLABucket"].isin(sel_buckets)]
if sel_days:    df = df[df["DayOfWeek"].isin(sel_days)]


# ─────────────────────────────────────────────
# Page header
# ─────────────────────────────────────────────
st.markdown("## 📧 Email Reply Analytics Dashboard")
st.markdown(
    f"Showing **{len(df):,}** records &nbsp;|&nbsp; "
    f"`{date_from}` → `{date_to}`"
)
st.markdown("---")

if df.empty:
    st.warning("No records match the selected filters.")
    st.stop()


# ─────────────────────────────────────────────
# KPI cards — row 1
# ─────────────────────────────────────────────
total        = len(df)
replied      = int(df["Replied"].sum())
not_replied  = total - replied
reply_pct    = replied / total * 100 if total else 0
avg_hrs      = df.loc[df["ReplyGapHours"].notna(), "ReplyGapHours"].mean()
avg_hrs_str  = f"{avg_hrs:.1f} hrs" if pd.notna(avg_hrs) else "N/A"
total_users  = df["User"].nunique()
k1, k2, k3, k4, k5 = st.columns(5)
kpis = [
    (k1, "Total Emails",   f"{total:,}",         "In selected range",                         "#1e293b"),
    (k2, "Reply Rate",     f"{reply_pct:.1f}%",  f"{replied:,} replied",                      "#16a34a" if reply_pct >= 70 else "#dc2626"),
    (k3, "No Reply",       f"{not_replied:,}",   f"{not_replied/total*100:.1f}% of total",    "#dc2626"),
    (k4, "Avg Reply Time", avg_hrs_str,           "Among replied emails",                      "#1e293b"),
    (k5, "Users Tracked",  str(total_users),      "Unique users",                              "#1e293b"),
]
for col, label, value, sub, color in kpis:
    col.markdown(f"""
    <div class="kpi-card">
        <div class="kpi-label">{label}</div>
        <div class="kpi-value" style="color:{color}">{value}</div>
        <div class="kpi-sub">{sub}</div>
    </div>""", unsafe_allow_html=True)

st.markdown("<br>", unsafe_allow_html=True)


# ─────────────────────────────────────────────
# SLA breakdown df (reused in multiple charts)
# ─────────────────────────────────────────────
sla_df = (
    df["SLABucket"].fillna("No Reply")
    .value_counts()
    .reindex(SLA_ORDER, fill_value=0)
    .reset_index()
)
sla_df.columns      = ["SLA Bucket", "Count"]
sla_df["Percentage"] = (sla_df["Count"] / total * 100).round(1) if total else 0


# ─────────────────────────────────────────────
# Row 2 — SLA Pie  |  Daily Trend
# ─────────────────────────────────────────────
col_l, col_r = st.columns([1, 2])

with col_l:
    st.markdown('<div class="section-title">SLA Bucket Breakdown</div>', unsafe_allow_html=True)
    fig_pie = px.pie(
        sla_df, names="SLA Bucket", values="Count",
        color="SLA Bucket", color_discrete_map=SLA_COLORS, hole=0.55
    )
    fig_pie.update_traces(textinfo="percent+label", pull=[0.03] * len(sla_df))
    fig_pie.update_layout(
        showlegend=False, height=300,
        margin=dict(t=10, b=10, l=10, r=10),
        paper_bgcolor="rgba(0,0,0,0)"
    )
    st.plotly_chart(fig_pie, width='stretch')

with col_r:
    st.markdown('<div class="section-title">Daily Reply Trend</div>', unsafe_allow_html=True)
    daily = (
        df.groupby("Date")
        .apply(lambda x: pd.Series({
            "Replied":  int(x["Replied"].sum()),
            "No Reply": int((~x["Replied"]).sum()),
            "Reply %":  round(x["Replied"].sum() / len(x) * 100, 1) if len(x) else 0,
        }))
        .reset_index()
    )
    fig_trend = go.Figure()
    fig_trend.add_bar(x=daily["Date"], y=daily["Replied"],  name="Replied",  marker_color="#16a34a")
    fig_trend.add_bar(x=daily["Date"], y=daily["No Reply"], name="No Reply", marker_color="#ef4444")
    fig_trend.add_scatter(
        x=daily["Date"], y=daily["Reply %"], name="Reply %",
        yaxis="y2", mode="lines+markers",
        line=dict(color="#3b82f6", width=2.5), marker=dict(size=5)
    )
    fig_trend.update_layout(
        barmode="stack",
        yaxis=dict(title="Email Count"),
        yaxis2=dict(title="Reply %", overlaying="y", side="right",
                    range=[0, 115], ticksuffix="%"),
        legend=dict(orientation="h", y=1.12),
        height=300, margin=dict(t=30, b=20, l=0, r=0),
        paper_bgcolor="rgba(0,0,0,0)"
    )
    st.plotly_chart(fig_trend, width='stretch')


# ─────────────────────────────────────────────
# Row 3 — Per-user bar  |  SLA bar + Top senders
# ─────────────────────────────────────────────
col_l2, col_r2 = st.columns([2, 1])

with col_l2:
    st.markdown('<div class="section-title">Per-User Reply Performance</div>', unsafe_allow_html=True)
    user_perf = (
        df.groupby("User")
        .apply(lambda x: pd.Series({
            "Total":           len(x),
            "Replied":         int(x["Replied"].sum()),
            "No Reply":        int((~x["Replied"]).sum()),
            "Reply %":         round(x["Replied"].sum() / len(x) * 100, 1) if len(x) else 0,
            "Avg Reply (hrs)": round(x["ReplyGapHours"].mean(), 2) if x["ReplyGapHours"].notna().any() else None,
        }))
        .reset_index()
        .sort_values("Reply %", ascending=True)
    )
    fig_user = go.Figure()
    fig_user.add_bar(
        y=user_perf["User"], x=user_perf["Reply %"],
        orientation="h",
        marker=dict(
            color=user_perf["Reply %"],
            colorscale=[[0, "#ef4444"], [0.5, "#f59e0b"], [1, "#16a34a"]],
            cmin=0, cmax=100
        ),
        text=user_perf["Reply %"].astype(str) + "%",
        textposition="outside"
    )
    fig_user.update_layout(
        xaxis=dict(range=[0, 115], title="Reply %", ticksuffix="%"),
        yaxis=dict(title=""),
        height=max(300, len(user_perf) * 28),
        margin=dict(t=10, b=20, l=0, r=30),
        paper_bgcolor="rgba(0,0,0,0)"
    )
    st.plotly_chart(fig_user, width='stretch')

with col_r2:
    st.markdown('<div class="section-title">Top Senders by Volume</div>', unsafe_allow_html=True)
    top_senders = (
        df["CorrespondentEmail"].value_counts().head(8)
        .reset_index()
        .rename(columns={"CorrespondentEmail": "Sender", "count": "Emails"})
    )
    st.dataframe(top_senders, width='stretch', hide_index=True, height=220)


# ─────────────────────────────────────────────
# Row 4 — Day-of-week heatmap
# ─────────────────────────────────────────────
st.markdown("---")
st.markdown('<div class="section-title">Reply Rate by Day of Week</div>', unsafe_allow_html=True)

dow_df = (
    df.groupby("DayOfWeek")
    .apply(lambda x: pd.Series({
        "Total":   len(x),
        "Replied": int(x["Replied"].sum()),
        "Reply %": round(x["Replied"].sum() / len(x) * 100, 1) if len(x) else 0,
    }))
    .reindex(["Monday","Tuesday","Wednesday","Thursday","Friday","Saturday","Sunday"])
    .reset_index()
)
fig_dow = px.bar(
    dow_df, x="DayOfWeek", y="Reply %",
    color="Reply %", color_continuous_scale=["#ef4444","#f59e0b","#16a34a"],
    range_color=[0, 100], text="Reply %"
)
fig_dow.update_traces(texttemplate="%{text}%", textposition="outside")
fig_dow.update_layout(
    yaxis=dict(range=[0, 115], ticksuffix="%"),
    coloraxis_showscale=False,
    height=260, margin=dict(t=10, b=20, l=0, r=0),
    paper_bgcolor="rgba(0,0,0,0)"
)
st.plotly_chart(fig_dow, width='stretch')


# ─────────────────────────────────────────────
# Row 5 — User summary table
# ─────────────────────────────────────────────
st.markdown("---")
st.markdown('<div class="section-title">User Summary Table</div>', unsafe_allow_html=True)

user_summary = (
    df.groupby("User")
    .apply(lambda x: pd.Series({
        "Total Emails":    len(x),
        "Replied":         int(x["Replied"].sum()),
        "No Reply":        int((~x["Replied"]).sum()),
        "Reply %":         f"{round(x['Replied'].sum()/len(x)*100,1) if len(x) else 0}%",
        "Avg Reply Time":  f"{round(x['ReplyGapHours'].mean(),2)} hrs" if x["ReplyGapHours"].notna().any() else "—",
        "Min Reply (hrs)": round(x["ReplyGapHours"].min(), 2) if x["ReplyGapHours"].notna().any() else None,
        "Max Reply (hrs)": round(x["ReplyGapHours"].max(), 2) if x["ReplyGapHours"].notna().any() else None,        "Avg Reply Days":  f"{round(x['ReplyGapDays'].mean(),2)} days" if x["ReplyGapDays"].notna().any() else "—",    }))
    .reset_index()
    .sort_values("Total Emails", ascending=False)
)
st.dataframe(user_summary, width='stretch', hide_index=True)


# ─────────────────────────────────────────────
# Row 6 — Full email log
# ─────────────────────────────────────────────
st.markdown("---")
st.markdown('<div class="section-title">Detailed Email Log</div>', unsafe_allow_html=True)

display_cols = [c for c in [
    "User", "CorrespondentEmail", "Subject",
    "ReceivedTime", "ReplyTime", "ReplyGapHours", "ReplyGapDays",
    "SLABucket", "CCRecipients", "ReportDate"
] if c in df.columns]

st.dataframe(
    df[display_cols].sort_values("ReceivedTime", ascending=False),
    width='stretch', hide_index=True, height=420
)


# ─────────────────────────────────────────────
# Download
# ─────────────────────────────────────────────
st.markdown("---")
st.markdown('<div class="section-title">Download Report</div>', unsafe_allow_html=True)

dl1, dl2, _ = st.columns([1, 1, 4])

with dl1:
    st.download_button(
        label="⬇ Download CSV",
        data=df[display_cols].to_csv(index=False).encode("utf-8-sig"),
        file_name=f"EmailReply_{date_from}_to_{date_to}.csv",
        mime="text/csv",
        width='stretch'
    )

with dl2:
    buf = BytesIO()
    with pd.ExcelWriter(buf, engine="openpyxl") as writer:
        df[display_cols].to_excel(writer,  index=False, sheet_name="Email Log")
        user_summary.to_excel(writer,      index=False, sheet_name="User Summary")
        sla_df.to_excel(writer,            index=False, sheet_name="SLA Breakdown")
        dow_df.to_excel(writer,            index=False, sheet_name="Day of Week")
        top_senders.to_excel(writer,       index=False, sheet_name="Top Senders")
    st.download_button(
        label="⬇ Download Excel",
        data=buf.getvalue(),
        file_name=f"EmailReply_{date_from}_to_{date_to}.xlsx",
        mime="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet",
        width='stretch'
    )
