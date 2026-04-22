"""
StreamLake — Bronze layer ingestion.

Reads events from Confluent Cloud Kafka (user-events topic),
parses each event's JSON payload, and writes to a Delta Lake table
at s3://streamlake-data-lake/bronze/user_events/.

This is the Bronze layer in Medallion Architecture: raw events
persisted exactly as received, append-only, the immutable source
of truth for all downstream processing.

Usage (from project root, with venv activated):
    python spark/ingest_to_bronze.py
"""
from __future__ import annotations

import os
import sys
from pathlib import Path

from dotenv import load_dotenv
from pyspark.sql import SparkSession
from pyspark.sql.functions import col, current_timestamp, from_json
from pyspark.sql.types import (
    IntegerType,
    StringType,
    StructField,
    StructType,
    TimestampType,
)


# ---------------------------------------------------------------------
# Configuration
# ---------------------------------------------------------------------

# Load credentials from imp/.env
ENV_PATH = Path(__file__).resolve().parent.parent / "imp" / ".env"
if not ENV_PATH.exists():
    raise FileNotFoundError(f"Expected credentials file at {ENV_PATH}")
load_dotenv(dotenv_path=ENV_PATH)


def require_env(*keys: str) -> dict:
    """Fail fast if required env vars are missing — better than cryptic JVM errors."""
    missing = [k for k in keys if not os.getenv(k)]
    if missing:
        raise RuntimeError(f"Missing required env vars in imp/.env: {missing}")
    return {k: os.environ[k] for k in keys}


env = require_env(
    "CONFLUENT_BOOTSTRAP_SERVERS",
    "CONFLUENT_API_KEY",
    "CONFLUENT_API_SECRET",
    "KAFKA_TOPIC_USER_EVENTS",
    "AWS_ACCESS_KEY_ID",
    "AWS_SECRET_ACCESS_KEY",
    "AWS_REGION",
    "AWS_S3_BUCKET",
)

KAFKA_BOOTSTRAP = env["CONFLUENT_BOOTSTRAP_SERVERS"]
KAFKA_API_KEY = env["CONFLUENT_API_KEY"]
KAFKA_API_SECRET = env["CONFLUENT_API_SECRET"]
KAFKA_TOPIC = env["KAFKA_TOPIC_USER_EVENTS"]

AWS_ACCESS_KEY = env["AWS_ACCESS_KEY_ID"]
AWS_SECRET_KEY = env["AWS_SECRET_ACCESS_KEY"]
AWS_REGION = env["AWS_REGION"]
S3_BUCKET = env["AWS_S3_BUCKET"]

BRONZE_PATH = f"s3a://{S3_BUCKET}/bronze/user_events/"
CHECKPOINT_PATH = f"s3a://{S3_BUCKET}/_checkpoints/bronze/user_events/"


# ---------------------------------------------------------------------
# Event schema — must match producer/event_schema.py exactly
# ---------------------------------------------------------------------

event_schema = StructType([
    StructField("event_id", StringType(), nullable=False),
    StructField("user_id", StringType(), nullable=False),
    StructField("session_id", StringType(), nullable=False),
    StructField("content_id", StringType(), nullable=False),
    StructField("content_type", StringType(), nullable=False),
    StructField("event_type", StringType(), nullable=False),
    StructField("watch_seconds", IntegerType(), nullable=False),
    StructField("playback_position_seconds", IntegerType(), nullable=True),
    StructField("device_type", StringType(), nullable=False),
    StructField("app_version", StringType(), nullable=False),
    StructField("country_code", StringType(), nullable=False),
    StructField("event_timestamp", TimestampType(), nullable=False),
])


# ---------------------------------------------------------------------
# Spark session
# ---------------------------------------------------------------------

def build_spark_session() -> SparkSession:
    """
    Build Spark session with all necessary connectors declared as packages.
    Maven coordinates auto-download on first run to ~/.ivy2/jars/.
    """
    packages = [
        # Kafka connector — Scala 2.12 build, matching Spark 3.5.3
        "org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.3",
        # Delta Lake — matches installed delta-spark Python package version 3.2.0
        "io.delta:delta-spark_2.12:3.2.0",
        # AWS S3 connector for Hadoop
        "org.apache.hadoop:hadoop-aws:3.3.4",
        "com.amazonaws:aws-java-sdk-bundle:1.12.262",
    ]

    return (
        SparkSession.builder
        .appName("StreamLake-BronzeIngestion")
        .master("local[*]")
        .config("spark.jars.packages", ",".join(packages))
        # Delta Lake extensions
        .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension")
        .config(
            "spark.sql.catalog.spark_catalog",
            "org.apache.spark.sql.delta.catalog.DeltaCatalog",
        )
        # S3A connector config
        .config("spark.hadoop.fs.s3a.impl", "org.apache.hadoop.fs.s3a.S3AFileSystem")
        .config("spark.hadoop.fs.s3a.access.key", AWS_ACCESS_KEY)
        .config("spark.hadoop.fs.s3a.secret.key", AWS_SECRET_KEY)
        .config("spark.hadoop.fs.s3a.endpoint", f"s3.{AWS_REGION}.amazonaws.com")
        .config("spark.hadoop.fs.s3a.path.style.access", "true")
        # Tuning for local dev — fewer shuffle partitions than default 200
        .config("spark.sql.shuffle.partitions", "4")
        # Reduce log noise
        .config("spark.ui.showConsoleProgress", "false")
        .getOrCreate()
    )


# ---------------------------------------------------------------------
# Streaming pipeline
# ---------------------------------------------------------------------

def main() -> None:
    print("=" * 70)
    print("StreamLake — Bronze Layer Ingestion")
    print("=" * 70)
    print(f"[INFO] Kafka bootstrap : {KAFKA_BOOTSTRAP}")
    print(f"[INFO] Kafka topic     : {KAFKA_TOPIC}")
    print(f"[INFO] Bronze path     : {BRONZE_PATH}")
    print(f"[INFO] Checkpoint path : {CHECKPOINT_PATH}")
    print("=" * 70)

    spark = build_spark_session()
    spark.sparkContext.setLogLevel("WARN")
    print("[INFO] Spark session ready.")

    # Read stream from Kafka
    # SASL_SSL + PLAIN authentication for Confluent Cloud
    sasl_jaas = (
        "org.apache.kafka.common.security.plain.PlainLoginModule required "
        f'username="{KAFKA_API_KEY}" '
        f'password="{KAFKA_API_SECRET}";'
    )

    kafka_df = (
        spark.readStream
        .format("kafka")
        .option("kafka.bootstrap.servers", KAFKA_BOOTSTRAP)
        .option("subscribe", KAFKA_TOPIC)
        .option("startingOffsets", "earliest")
        .option("kafka.security.protocol", "SASL_SSL")
        .option("kafka.sasl.mechanism", "PLAIN")
        .option("kafka.sasl.jaas.config", sasl_jaas)
        .option("failOnDataLoss", "false")
        .load()
    )
    print("[INFO] Kafka source configured.")

    # Parse the JSON value column
    # Also preserve Kafka-level metadata for traceability
    parsed_df = (
        kafka_df
        .select(
            col("key").cast("string").alias("kafka_key"),
            col("partition").alias("kafka_partition"),
            col("offset").alias("kafka_offset"),
            col("timestamp").alias("kafka_timestamp"),
            from_json(col("value").cast("string"), event_schema).alias("event"),
        )
        .select(
            "kafka_key",
            "kafka_partition",
            "kafka_offset",
            "kafka_timestamp",
            "event.*",
        )
        # Audit column — when this row landed in Bronze
        .withColumn("ingested_at", current_timestamp())
    )
    print("[INFO] Schema parser configured.")

    # Write stream to Delta Lake on S3
    # Trigger every 30 seconds (micro-batch), append mode, checkpoint on S3
    query = (
        parsed_df.writeStream
        .format("delta")
        .outputMode("append")
        .option("checkpointLocation", CHECKPOINT_PATH)
        .trigger(processingTime="30 seconds")
        .start(BRONZE_PATH)
    )
    print(f"[INFO] Streaming query started. ID: {query.id}")
    print("[INFO] Ctrl+C to stop.")
    print("=" * 70)

    try:
        query.awaitTermination()
    except KeyboardInterrupt:
        print("\n[INFO] Stopping gracefully…")
        query.stop()
        spark.stop()
        print("[INFO] Done.")


if __name__ == "__main__":
    main()