"""
PySpark sanity test for StreamLake — Real-Time Streaming Data Lakehouse.

Validates that the local Spark environment is correctly configured:
  - PySpark can find the JDK
  - Py4J bridge (Python <-> JVM) works
  - Spark's JVM runtime starts without errors
  - Python worker processes can be spawned and communicate back
  - Basic DataFrame operations execute correctly

The test data mirrors the schema of events we will stream from Kafka
in Phase 2 (user_id, event_type, watch_seconds) so the test also
serves as a preview of the production ingestion model.

IMPORTANT — Windows-specific configuration:
PYSPARK_PYTHON and PYSPARK_DRIVER_PYTHON are set to sys.executable
BEFORE importing pyspark. This forces Spark workers to use the same
Python interpreter as the driver (our venv Python), rather than
resolving 'python' via Windows PATH. On Windows, PATH routing would
hit the Microsoft Store App Execution Alias stub, causing worker
processes to exit immediately and jobs to fail with
'Python worker failed to connect back' socket timeouts.
"""

import os
import sys

# Pin Spark workers AND driver to the current Python interpreter.
# Must be set BEFORE `import pyspark` — env vars are captured on import.
os.environ["PYSPARK_PYTHON"] = sys.executable
os.environ["PYSPARK_DRIVER_PYTHON"] = sys.executable

from pyspark.sql import SparkSession
from pyspark.sql.functions import col, sum as spark_sum, count


def main() -> None:
    print("=" * 60)
    print("StreamLake — PySpark Sanity Test")
    print("=" * 60)
    print(f"[INFO] Using Python: {sys.executable}")

    # Build a local Spark session. .master("local[*]") tells Spark to
    # run in-process using all available CPU cores. No cluster needed.
    spark = (
        SparkSession.builder
        .appName("Project16-SanityTest")
        .master("local[*]")
        .config("spark.sql.shuffle.partitions", "4")  # small for local test
        .getOrCreate()
    )

    # Quiet Spark's default INFO-level logging so we can see OUR output.
    spark.sparkContext.setLogLevel("WARN")

    print(f"\n[OK] Spark session started.")
    print(f"     Spark version : {spark.version}")
    print(f"     Master        : {spark.sparkContext.master}")
    print(f"     App name      : {spark.sparkContext.appName}")

    # Simulated user viewing events — same schema we will stream via Kafka
    # in Phase 2. Kept tiny here because this is an environment test.
    events = [
        ("u001", "play",  120),
        ("u001", "pause",   0),
        ("u001", "play",   60),
        ("u002", "play",  300),
        ("u002", "skip",    0),
        ("u003", "play",  450),
        ("u003", "play",  200),
    ]
    columns = ["user_id", "event_type", "watch_seconds"]

    df = spark.createDataFrame(events, columns)

    print("\n[OK] Raw events DataFrame:")
    df.show(truncate=False)

    # Aggregation: total watch seconds and event count per user.
    # This is the simplest non-trivial Spark operation — if this works,
    # the runtime is fully functional.
    agg = (
        df.groupBy("user_id")
          .agg(
              spark_sum(col("watch_seconds")).alias("total_watch_seconds"),
              count("*").alias("event_count"),
          )
          .orderBy("user_id")
    )

    print("[OK] Per-user aggregation:")
    agg.show(truncate=False)

    # Clean shutdown of the Spark session.
    spark.stop()
    print("[OK] Spark session stopped cleanly.")
    print("=" * 60)
    print("PySpark environment is WORKING.")
    print("=" * 60)


if __name__ == "__main__":
    main()
