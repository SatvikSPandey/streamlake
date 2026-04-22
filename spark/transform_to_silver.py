"""
StreamLake - Silver layer transform.

Reads Bronze, produces Silver:
  - Deduplicates on event_id (producer retries create duplicates)
  - Drops rows where from_json failed (event_id IS NULL)
  - Enriches with event_date and event_hour for partitioning
  - Partitioned by event_date for efficient time-range queries downstream

Silver is the "cleaned, trustworthy" layer - Gold tables read only from Silver.

Usage (from project root):
    python spark/transform_to_silver.py
"""
from __future__ import annotations

import os
from pathlib import Path

from dotenv import load_dotenv
from pyspark.sql import SparkSession
from pyspark.sql.functions import (
    col,
    current_timestamp,
    date_format,
    hour,
    to_date,
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
        .appName("StreamLake-Silver")
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


def main() -> None:
    bucket = os.environ["AWS_S3_BUCKET"]
    bronze_path = f"s3a://{bucket}/bronze/user_events/"
    silver_path = f"s3a://{bucket}/silver/user_events/"

    print("=" * 70)
    print("StreamLake - Silver Transform")
    print("=" * 70)
    print(f"[INFO] Bronze in : {bronze_path}")
    print(f"[INFO] Silver out: {silver_path}")

    spark = build_spark()
    spark.sparkContext.setLogLevel("WARN")

    bronze = spark.read.format("delta").load(bronze_path)
    bronze_count = bronze.count()
    print(f"[INFO] Bronze rows: {bronze_count:,}")

    silver = (
        bronze
        # Drop rows where JSON parsing failed (event_id is null when from_json fails)
        .filter(col("event_id").isNotNull())
        # Deduplicate on event_id - producer retries can create duplicates
        .dropDuplicates(["event_id"])
        # Enrich for downstream partitioning and aggregation
        .withColumn("event_date", to_date(col("event_timestamp")))
        .withColumn("event_hour", hour(col("event_timestamp")))
        .withColumn("transformed_at", current_timestamp())
    )

    (
        silver.write
        .format("delta")
        .mode("overwrite")
        .partitionBy("event_date")
        .save(silver_path)
    )

    silver_count = spark.read.format("delta").load(silver_path).count()
    dropped = bronze_count - silver_count
    print(f"[INFO] Silver rows: {silver_count:,}")
    print(f"[INFO] Dropped (nulls + dupes): {dropped:,}")
    print("[OK] Silver transform complete.")

    spark.stop()


if __name__ == "__main__":
    main()