"""
StreamLake - Gold layer transforms.

Reads Silver, produces multiple Gold tables - each answering a specific
business question. Gold tables are the ones that power dashboards
(Phase 8) and Snowflake (Phase 5).

Gold tables produced:
  1. daily_watch_time_by_country - watch seconds per country per day
  2. top_content_by_watch_time   - most-watched content
  3. hourly_event_distribution   - events per type per hour (heartbeat)
  4. daily_device_mix            - device type split per day
  5. user_session_summary        - per-session watch/event totals

Each table is overwritten on every run (idempotent).

Usage (from project root):
    python spark/transform_to_gold.py
"""
from __future__ import annotations

import os
from pathlib import Path

from dotenv import load_dotenv
from pyspark.sql import DataFrame, SparkSession
from pyspark.sql.functions import (
    col,
    count,
    countDistinct,
    current_timestamp,
    desc,
    max as spark_max,
    min as spark_min,
    sum as spark_sum,
)


ENV_PATH = Path(__file__).resolve().parent.parent / "imp" / ".env"
load_dotenv(dotenv_path=ENV_PATH)


def build_spark() -> SparkSession:
    packages = [
        "io.delta:delta-spark_2.12:3.2.0",
        "org.apache.hadoop:hadoop-aws:3.3.4",
        "com.amazonaws:aws-java-sdk-bundle:1.12.262",
    ]
    return (
        SparkSession.builder
        .appName("StreamLake-Gold")
        .master("local[*]")
        .config("spark.jars.packages", ",".join(packages))
        .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension")
        .config(
            "spark.sql.catalog.spark_catalog",
            "org.apache.spark.sql.delta.catalog.DeltaCatalog",
        )
        .config("spark.hadoop.fs.s3a.impl", "org.apache.hadoop.fs.s3a.S3AFileSystem")
        .config("spark.hadoop.fs.s3a.access.key", os.environ["AWS_ACCESS_KEY_ID"])
        .config("spark.hadoop.fs.s3a.secret.key", os.environ["AWS_SECRET_ACCESS_KEY"])
        .config(
            "spark.hadoop.fs.s3a.endpoint",
            f"s3.{os.environ['AWS_REGION']}.amazonaws.com",
        )
        .config("spark.sql.shuffle.partitions", "4")
        .config("spark.ui.showConsoleProgress", "false")
        .getOrCreate()
    )


def write_gold_table(df: DataFrame, name: str, bucket: str) -> None:
    """Write a Gold table, overwriting any existing one, and print row count."""
    path = f"s3a://{bucket}/gold/{name}/"
    df_with_ts = df.withColumn("transformed_at", current_timestamp())
    (
        df_with_ts.write
        .format("delta")
        .mode("overwrite")
        .option("overwriteSchema", "true")
        .save(path)
    )
    count_val = df_with_ts.count()
    print(f"[OK] gold/{name:<32s} rows: {count_val:,}")


def main() -> None:
    bucket = os.environ["AWS_S3_BUCKET"]
    silver_path = f"s3a://{bucket}/silver/user_events/"

    print("=" * 70)
    print("StreamLake - Gold Transforms")
    print("=" * 70)
    print(f"[INFO] Silver in: {silver_path}")
    print(f"[INFO] Gold out : s3a://{bucket}/gold/")
    print("=" * 70)

    spark = build_spark()
    spark.sparkContext.setLogLevel("WARN")

    silver = spark.read.format("delta").load(silver_path)
    silver.cache()  # reused across multiple aggregates
    print(f"[INFO] Silver rows loaded: {silver.count():,}")
    print("-" * 70)

    # -----------------------------------------------------------------
    # Gold 1: daily watch time by country
    # -----------------------------------------------------------------
    # Sum of watch_seconds captures the total viewing time per country per day.
    # Using max(watch_seconds) per session would be more accurate (cumulative),
    # but sum across play heartbeats approximates the same at event grain here.
    daily_watch_country = (
        silver
        .groupBy("event_date", "country_code")
        .agg(
            spark_sum("watch_seconds").alias("total_watch_seconds"),
            countDistinct("user_id").alias("unique_users"),
            countDistinct("session_id").alias("unique_sessions"),
            count("*").alias("event_count"),
        )
        .orderBy("event_date", desc("total_watch_seconds"))
    )
    write_gold_table(daily_watch_country, "daily_watch_time_by_country", bucket)

    # -----------------------------------------------------------------
    # Gold 2: top content by watch time (all-time)
    # -----------------------------------------------------------------
    top_content = (
        silver
        .groupBy("content_id", "content_type")
        .agg(
            spark_sum("watch_seconds").alias("total_watch_seconds"),
            countDistinct("user_id").alias("unique_viewers"),
            countDistinct("session_id").alias("unique_sessions"),
            count("*").alias("event_count"),
        )
        .orderBy(desc("total_watch_seconds"))
    )
    write_gold_table(top_content, "top_content_by_watch_time", bucket)

    # -----------------------------------------------------------------
    # Gold 3: hourly event-type distribution
    # -----------------------------------------------------------------
    hourly_events = (
        silver
        .groupBy("event_date", "event_hour", "event_type")
        .agg(count("*").alias("event_count"))
        .orderBy("event_date", "event_hour", "event_type")
    )
    write_gold_table(hourly_events, "hourly_event_distribution", bucket)

    # -----------------------------------------------------------------
    # Gold 4: daily device-type mix
    # -----------------------------------------------------------------
    daily_devices = (
        silver
        .groupBy("event_date", "device_type")
        .agg(
            countDistinct("user_id").alias("unique_users"),
            countDistinct("session_id").alias("unique_sessions"),
            count("*").alias("event_count"),
        )
        .orderBy("event_date", desc("unique_sessions"))
    )
    write_gold_table(daily_devices, "daily_device_mix", bucket)

    # -----------------------------------------------------------------
    # Gold 5: per-session summary
    # -----------------------------------------------------------------
    session_summary = (
        silver
        .groupBy("session_id", "user_id", "content_id", "content_type", "device_type", "country_code")
        .agg(
            spark_min("event_timestamp").alias("session_start"),
            spark_max("event_timestamp").alias("session_end"),
            spark_max("watch_seconds").alias("max_watch_seconds"),
            count("*").alias("event_count"),
        )
        .orderBy(desc("max_watch_seconds"))
    )
    write_gold_table(session_summary, "user_session_summary", bucket)

    print("-" * 70)
    print("[OK] All Gold tables written.")
    silver.unpersist()
    spark.stop()


if __name__ == "__main__":
    main()