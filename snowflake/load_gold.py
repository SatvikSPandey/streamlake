"""
StreamLake - Snowflake Gold loader.

Reads Gold Delta tables from S3 and loads them into Snowflake STREAMLAKE_DB.GOLD.
Uses the deltalake Python library (Rust-based, no Spark needed) to read Delta,
then snowflake-connector's write_pandas for efficient bulk insert via PUT+COPY.

All 5 Gold tables are refreshed (full replace) on each run - idempotent.

Usage:
    python snowflake/load_gold.py
"""
from __future__ import annotations

import os
from pathlib import Path
from dotenv import load_dotenv
from deltalake import DeltaTable
import snowflake.connector
from snowflake.connector.pandas_tools import write_pandas

ROOT = Path(__file__).resolve().parent.parent
load_dotenv(ROOT / "imp" / ".env")

GOLD_TABLES = [
    "daily_watch_time_by_country",
    "top_content_by_watch_time",
    "hourly_event_distribution",
    "daily_device_mix",
    "user_session_summary",
]


def read_delta_from_s3(table_name: str) -> "pd.DataFrame":
    """Read a Delta table from S3 into a pandas DataFrame."""
    bucket = os.environ["AWS_S3_BUCKET"]
    path = f"s3://{bucket}/gold/{table_name}/"
    storage_opts = {
        "AWS_ACCESS_KEY_ID": os.environ["AWS_ACCESS_KEY_ID"],
        "AWS_SECRET_ACCESS_KEY": os.environ["AWS_SECRET_ACCESS_KEY"],
        "AWS_REGION": os.environ["AWS_REGION"],
        "AWS_S3_ALLOW_UNSAFE_RENAME": "true",
    }
    dt = DeltaTable(path, storage_options=storage_opts)
    return dt.to_pandas()


def load_table_to_snowflake(conn, table_name: str, df) -> int:
    """Load a DataFrame to Snowflake, replacing any existing table."""
    # write_pandas auto-creates the table if it doesn't exist, or we can
    # explicitly drop+create for idempotency. Drop + auto-create is cleanest.
    snowflake_table = table_name.upper()
    cur = conn.cursor()
    cur.execute(f"DROP TABLE IF EXISTS {snowflake_table}")
    cur.close()

    # Snowflake expects uppercase column names by default
    df.columns = [c.upper() for c in df.columns]

    success, nchunks, nrows, _ = write_pandas(
        conn=conn,
        df=df,
        table_name=snowflake_table,
        auto_create_table=True,
        overwrite=False,  # we already dropped
        quote_identifiers=False,
    )
    if not success:
        raise RuntimeError(f"write_pandas failed for {table_name}")
    return nrows


def main() -> None:
    print("=" * 70)
    print("StreamLake - Snowflake Gold loader")
    print("=" * 70)

    conn = snowflake.connector.connect(
        account=os.environ["SNOWFLAKE_ACCOUNT"],
        user=os.environ["SNOWFLAKE_USER"],
        password=os.environ["SNOWFLAKE_PASSWORD"],
        role=os.environ["SNOWFLAKE_ROLE"],
        warehouse=os.environ["SNOWFLAKE_WAREHOUSE"],
        database=os.environ["SNOWFLAKE_DATABASE"],
        schema=os.environ["SNOWFLAKE_SCHEMA"],
    )
    print(f"[INFO] Connected to Snowflake as {os.environ['SNOWFLAKE_USER']}")
    print("-" * 70)

    try:
        for table in GOLD_TABLES:
            print(f"[READ]  s3://{os.environ['AWS_S3_BUCKET']}/gold/{table}/")
            df = read_delta_from_s3(table)
            rows = len(df)
            print(f"        -> {rows:,} rows, {len(df.columns)} cols")

            print(f"[WRITE] STREAMLAKE_DB.GOLD.{table.upper()}")
            loaded = load_table_to_snowflake(conn, table, df)
            print(f"        -> {loaded:,} rows loaded")
            print("-" * 70)
        print("[OK] All Gold tables loaded to Snowflake.")
    finally:
        conn.close()


if __name__ == "__main__":
    main()