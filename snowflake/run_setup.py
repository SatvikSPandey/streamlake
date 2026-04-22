"""
Run the Snowflake setup DDL.
Creates warehouse, database, schema for StreamLake Gold layer.
Idempotent - safe to re-run.

Usage:
    python snowflake/run_setup.py
"""
import os
from pathlib import Path
from dotenv import load_dotenv
import snowflake.connector

ROOT = Path(__file__).resolve().parent.parent
load_dotenv(ROOT / "imp" / ".env")

SQL_FILE = ROOT / "snowflake" / "setup_ddl.sql"

conn = snowflake.connector.connect(
    account=os.environ["SNOWFLAKE_ACCOUNT"],
    user=os.environ["SNOWFLAKE_USER"],
    password=os.environ["SNOWFLAKE_PASSWORD"],
    role=os.environ["SNOWFLAKE_ROLE"],
)

try:
    with SQL_FILE.open("r", encoding="utf-8") as f:
        sql_text = f.read()

    # Split on semicolons, skip blanks and comment-only statements
    statements = [
        s.strip() for s in sql_text.split(";")
        if s.strip() and not all(
            line.strip().startswith("--") or not line.strip()
            for line in s.strip().splitlines()
        )
    ]

    cur = conn.cursor()
    for stmt in statements:
        first_line = stmt.splitlines()[0][:70]
        print(f"[SQL] {first_line}...")
        cur.execute(stmt)
        # For SHOW statements, print first row
        if stmt.upper().startswith("SHOW"):
            rows = cur.fetchall()
            print(f"      -> {len(rows)} result(s)")
    cur.close()
    print("[OK] Setup DDL complete.")
finally:
    conn.close()