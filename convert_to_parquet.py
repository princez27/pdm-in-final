"""
Rebuilds EmailReplyReport.parquet from EmailReplyReport.csv.

Run this after emailReply.py appends new rows to the CSV, then commit the
regenerated .parquet file. dashboard.py reads the parquet file at runtime —
it never parses the raw CSV — because the CSV has grown large enough (~100MB,
360k+ rows) that reading and copying it as text on every deploy was pushing
the Streamlit Cloud container past its memory limit and getting it OOM-killed.

Usage:
    python convert_to_parquet.py
"""
import os
import re
import pandas as pd

CSV_PATH     = os.path.join(os.path.dirname(__file__), "EmailReplyReport.csv")
PARQUET_PATH = os.path.join(os.path.dirname(__file__), "EmailReplyReport.parquet")

_ISO_RE = re.compile(r"^\d{4}-\d{2}-\d{2}")


def parse_mixed_datetime(series, iso_format, dmy_format):
    """
    A prior Excel open/save silently reformatted every ReceivedTime/ReplyTime/
    ReportDate value written up to that point from emailReply.py's native
    "%Y-%m-%d %H:%M:%S" into locale-style "DD-MM-YYYY HH:MM" (seconds lost),
    while rows appended afterward stayed ISO. The two formats are ambiguous
    to a single dayfirst guess (e.g. 05-09-2025), so each value is parsed
    with an explicit format picked by matching the leading YYYY- prefix
    rather than relying on pandas' mixed-format auto-detection.
    """
    s = series.astype("string")
    is_iso = s.str.match(_ISO_RE).fillna(False)
    out = pd.Series(pd.NaT, index=series.index, dtype="datetime64[ns]")
    out[is_iso]  = pd.to_datetime(s[is_iso],  format=iso_format, errors="coerce")
    out[~is_iso] = pd.to_datetime(s[~is_iso], format=dmy_format, errors="coerce")
    return out


def build():
    df = pd.read_csv(CSV_PATH, encoding="utf-8-sig")

    df["ReceivedTime"] = parse_mixed_datetime(df.get("ReceivedTime"), "%Y-%m-%d %H:%M:%S", "%d-%m-%Y %H:%M")
    df["ReplyTime"]    = parse_mixed_datetime(df.get("ReplyTime"),    "%Y-%m-%d %H:%M:%S", "%d-%m-%Y %H:%M")
    df["ReportDate"]   = parse_mixed_datetime(df.get("ReportDate"),   "%Y-%m-%d",          "%d-%m-%Y")
    df["ReplyGapHours"] = pd.to_numeric(df.get("ReplyGapHours"), errors="coerce").astype("float32")
    df["ReplyGapDays"]  = pd.to_numeric(
        df.get("ReplyGapDays").astype(str).str.replace(r"\s*days?$", "", regex=True),
        errors="coerce",
    ).astype("float32")
    df["Date"]      = df["ReceivedTime"].dt.normalize()
    df["DayOfWeek"] = df["ReceivedTime"].dt.day_name()
    df["Replied"]   = df["SLABucket"].notna() & (df["SLABucket"] != "No Reply")
    df["User"]               = df["User"].str.strip().str.lower()
    df["CorrespondentEmail"] = df["CorrespondentEmail"].str.strip().str.lower()

    for col in ["User", "CorrespondentEmail", "SLABucket", "DayOfWeek"]:
        df[col] = df[col].astype("category")

    df.to_parquet(PARQUET_PATH, index=False)
    print(f"Wrote {PARQUET_PATH} ({os.path.getsize(PARQUET_PATH) / 1_048_576:.1f} MB, {len(df):,} rows)")


if __name__ == "__main__":
    build()
