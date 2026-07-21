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
import pandas as pd

CSV_PATH     = os.path.join(os.path.dirname(__file__), "EmailReplyReport.csv")
PARQUET_PATH = os.path.join(os.path.dirname(__file__), "EmailReplyReport.parquet")


def build():
    df = pd.read_csv(CSV_PATH, encoding="utf-8-sig")

    df["ReceivedTime"]  = pd.to_datetime(df.get("ReceivedTime"), errors="coerce")
    df["ReplyTime"]     = pd.to_datetime(df.get("ReplyTime"),    errors="coerce")
    df["ReportDate"]    = pd.to_datetime(df.get("ReportDate"),   errors="coerce")
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
