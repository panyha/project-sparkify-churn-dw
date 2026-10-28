"""
EXTRACT + LOAD: read Sparkify JSON-lines event logs with pandas and bulk-load
them into PostgreSQL table raw.event_logs (full refresh).

Accepted inputs in raw_data/ : *.json, *.json.gz, *.jsonl  (JSON lines)
                               *.csv                         (same columns)

The raw layer keeps the source as-is (only column names are snake_cased) —
cleaning and typing happen later in dbt (models/staging).

Used two ways:
  1. imported by the Airflow DAG  (dags/sparkify_churn_pipeline.py)
  2. run by hand:  python scripts/load_raw_events.py --host localhost --port 35432
"""
from __future__ import annotations

import argparse
import io
import os
from pathlib import Path

import pandas as pd
import psycopg2

RAW_DIR = Path(os.getenv("SPARKIFY_RAW_DIR", "/home/raw_data"))

# source key → raw column, in table order
COLUMN_MAP = {
    "ts": "ts",
    "userId": "user_id",
    "sessionId": "session_id",
    "itemInSession": "item_in_session",
    "page": "page",
    "auth": "auth",
    "method": "method",
    "status": "status",
    "level": "level",
    "firstName": "first_name",
    "lastName": "last_name",
    "gender": "gender",
    "location": "location",
    "userAgent": "user_agent",
    "registration": "registration",
    "artist": "artist",
    "song": "song",
    "length": "length",
}

DDL = """
CREATE SCHEMA IF NOT EXISTS raw;
CREATE TABLE IF NOT EXISTS raw.event_logs (
    ts               BIGINT,          -- epoch milliseconds
    user_id          TEXT,            -- '' for logged-out visitors
    session_id       BIGINT,
    item_in_session  INTEGER,
    page             TEXT,
    auth             TEXT,
    method           TEXT,
    status           INTEGER,
    level            TEXT,
    first_name       TEXT,
    last_name        TEXT,
    gender           TEXT,
    location         TEXT,
    user_agent       TEXT,
    registration     BIGINT,          -- epoch milliseconds
    artist           TEXT,
    song             TEXT,
    length           DOUBLE PRECISION,-- seconds
    source_file      TEXT,
    loaded_at        TIMESTAMPTZ DEFAULT now()
);
"""


def find_source_files(raw_dir: Path = RAW_DIR) -> list[Path]:
    patterns = ("*.json", "*.json.gz", "*.jsonl", "*.csv")
    files = sorted({p for pat in patterns for p in raw_dir.glob(pat)})
    if not files:
        raise FileNotFoundError(
            f"No event log found in {raw_dir}. Run scripts/generate_sample_data.py "
            "or put mini_sparkify_event_data.json there."
        )
    return files


def read_events(path: Path) -> pd.DataFrame:
    if path.suffix == ".csv":
        df = pd.read_csv(path)
    else:
        df = pd.read_json(path, lines=True, dtype=False)
    missing = set(COLUMN_MAP) - set(df.columns)
    if missing:
        raise ValueError(f"{path.name} is missing columns: {sorted(missing)}")
    df = df[list(COLUMN_MAP)].rename(columns=COLUMN_MAP)
    df["user_id"] = df["user_id"].fillna("").astype(str).str.replace(r"\.0$", "", regex=True)
    for col in ("ts", "session_id", "item_in_session", "status", "registration"):
        df[col] = pd.to_numeric(df[col], errors="coerce").astype("Int64")
    df["source_file"] = path.name
    return df


def get_connection(host: str, port: int, dbname: str, user: str, password: str):
    """Connect to the warehouse DB, creating it first if it doesn't exist yet."""
    try:
        return psycopg2.connect(host=host, port=port, dbname=dbname, user=user, password=password)
    except psycopg2.OperationalError as err:
        if "does not exist" not in str(err):
            raise
        admin = psycopg2.connect(host=host, port=port, dbname="postgres", user=user, password=password)
        admin.autocommit = True
        with admin.cursor() as cur:
            cur.execute(f'CREATE DATABASE "{dbname}"')
        admin.close()
        return psycopg2.connect(host=host, port=port, dbname=dbname, user=user, password=password)


def load(host="postgres", port=5432, dbname="sparkify_dw", user="dw_user", password="dw_pass",
         raw_dir: Path = RAW_DIR) -> int:
    files = find_source_files(raw_dir)
    conn = get_connection(host, port, dbname, user, password)
    total = 0
    try:
        with conn, conn.cursor() as cur:
            cur.execute(DDL)
            cur.execute("TRUNCATE raw.event_logs")
            cols = list(COLUMN_MAP.values()) + ["source_file"]
            for path in files:
                df = read_events(path)
                buf = io.StringIO()
                df.to_csv(buf, index=False, header=False, na_rep="\\N")
                buf.seek(0)
                cur.copy_expert(
                    f"COPY raw.event_logs ({', '.join(cols)}) FROM STDIN "
                    "WITH (FORMAT csv, NULL '\\N')",
                    buf,
                )
                print(f"Loaded {len(df):,} rows from {path.name}")
                total += len(df)
        print(f"raw.event_logs now holds {total:,} rows")
        return total
    finally:
        conn.close()


if __name__ == "__main__":
    ap = argparse.ArgumentParser(description="Load Sparkify event logs into raw.event_logs")
    ap.add_argument("--host", default="localhost")
    ap.add_argument("--port", type=int, default=35432)
    ap.add_argument("--dbname", default="sparkify_dw")
    ap.add_argument("--user", default="dw_user")
    ap.add_argument("--password", default="dw_pass")
    ap.add_argument("--raw-dir", default=str(Path(__file__).resolve().parent.parent / "raw_data"))
    a = ap.parse_args()
    load(a.host, a.port, a.dbname, a.user, a.password, Path(a.raw_dir))
