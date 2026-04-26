#!/bin/bash
# =============================================================================
# StreamLake EMR Job Submission Script
# Shows how StreamLake's Spark jobs would be submitted to the EMR cluster
# defined in cluster_config.json.
#
# NOTE: This script is documented infrastructure. It reflects the exact JAR
# versions, Spark configs, and S3 paths used in local development, translated
# to production EMR spark-submit syntax.
#
# Prerequisites:
#   - AWS CLI configured with appropriate IAM permissions
#   - EMR cluster running (launched via cluster_config.json)
#   - StreamLake scripts uploaded to S3 (see upload commands below)
#   - Confluent credentials in AWS Secrets Manager (see Step 0)
# =============================================================================

set -e
set -x

# =============================================================================
# CONFIGURATION — set these before running
# =============================================================================
CLUSTER_ID="j-XXXXXXXXXXXX"          # Replace with actual EMR cluster ID
S3_BUCKET="streamlake-data-lake"     # Your S3 bucket (already exists)
AWS_REGION="ap-south-1"             # Mumbai — matches local dev
LOG_URI="s3://${S3_BUCKET}/emr-logs/"

# =============================================================================
# Step 0: Upload StreamLake scripts to S3
# EMR nodes pull job scripts from S3, not from local disk.
# Run this once before submitting jobs, or as part of CI/CD.
# =============================================================================
echo "=== Uploading StreamLake scripts to S3 ==="

aws s3 cp spark/ingest_to_bronze.py \
    s3://${S3_BUCKET}/scripts/ingest_to_bronze.py \
    --region ${AWS_REGION}

aws s3 cp spark/transform_to_silver.py \
    s3://${S3_BUCKET}/scripts/transform_to_silver.py \
    --region ${AWS_REGION}

aws s3 cp spark/transform_to_gold.py \
    s3://${S3_BUCKET}/scripts/transform_to_gold.py \
    --region ${AWS_REGION}

echo "Scripts uploaded to s3://${S3_BUCKET}/scripts/"

# =============================================================================
# Step 1: Submit Bronze ingestion job
# Reads from Confluent Kafka topic user-events → writes Bronze Delta on S3
# partitioned by user_id.
#
# Key differences from local dev:
#   - Script path is S3 URI, not local path
#   - No --master local[*] — EMR provides YARN cluster manager automatically
#   - executor-memory and executor-cores sized for m5.2xlarge (32GB, 8 vCPU)
#   - Two executors per node (4 cores each) = full utilization
# =============================================================================
echo "=== Submitting Bronze ingestion job ==="

aws emr add-steps \
    --cluster-id ${CLUSTER_ID} \
    --region ${AWS_REGION} \
    --steps Type=Spark,Name="StreamLake-Bronze-Ingestion",ActionOnFailure=CONTINUE,Args=[
        --deploy-mode,cluster,
        --master,yarn,
        --executor-memory,20g,
        --executor-cores,4,
        --num-executors,4,
        --driver-memory,8g,
        --conf,spark.sql.extensions=io.delta.sql.DeltaSparkSessionExtension,
        --conf,spark.sql.catalog.spark_catalog=org.apache.spark.sql.delta.catalog.DeltaCatalog,
        --conf,spark.sql.shuffle.partitions=200,
        --conf,spark.serializer=org.apache.spark.serializer.KryoSerializer,
        --packages,io.delta:delta-spark_2.12:3.2.0,org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.3,org.apache.hadoop:hadoop-aws:3.3.4,com.amazonaws:aws-java-sdk-bundle:1.12.262,
        s3://${S3_BUCKET}/scripts/ingest_to_bronze.py
    ]

echo "Bronze job submitted. Monitor at:"
echo "https://${AWS_REGION}.console.aws.amazon.com/emr/home?region=${AWS_REGION}#/clusterDetails/${CLUSTER_ID}"

# =============================================================================
# Step 2: Submit Silver transformation job
# Reads Bronze Delta from S3 → dedupes on event_id → validates schema →
# writes Silver Delta partitioned by event_date.
#
# Depends on Bronze completing successfully. In production this dependency
# would be managed by Prefect (orchestration/flow.py) rather than sequential
# shell commands — Prefect's task graph handles retries and ordering.
# =============================================================================
echo "=== Submitting Silver transformation job ==="

aws emr add-steps \
    --cluster-id ${CLUSTER_ID} \
    --region ${AWS_REGION} \
    --steps Type=Spark,Name="StreamLake-Silver-Transform",ActionOnFailure=CONTINUE,Args=[
        --deploy-mode,cluster,
        --master,yarn,
        --executor-memory,20g,
        --executor-cores,4,
        --num-executors,4,
        --driver-memory,8g,
        --conf,spark.sql.extensions=io.delta.sql.DeltaSparkSessionExtension,
        --conf,spark.sql.catalog.spark_catalog=org.apache.spark.sql.delta.catalog.DeltaCatalog,
        --conf,spark.sql.shuffle.partitions=200,
        --conf,spark.serializer=org.apache.spark.serializer.KryoSerializer,
        --packages,io.delta:delta-spark_2.12:3.2.0,org.apache.hadoop:hadoop-aws:3.3.4,com.amazonaws:aws-java-sdk-bundle:1.12.262,
        s3://${S3_BUCKET}/scripts/transform_to_silver.py
    ]

# =============================================================================
# Step 3: Submit Gold transformation job
# Reads Silver Delta → computes 5 Gold aggregate tables → writes Gold Delta.
# =============================================================================
echo "=== Submitting Gold transformation job ==="

aws emr add-steps \
    --cluster-id ${CLUSTER_ID} \
    --region ${AWS_REGION} \
    --steps Type=Spark,Name="StreamLake-Gold-Transform",ActionOnFailure=CONTINUE,Args=[
        --deploy-mode,cluster,
        --master,yarn,
        --executor-memory,20g,
        --executor-cores,4,
        --num-executors,4,
        --driver-memory,8g,
        --conf,spark.sql.extensions=io.delta.sql.DeltaSparkSessionExtension,
        --conf,spark.sql.catalog.spark_catalog=org.apache.spark.sql.delta.catalog.DeltaCatalog,
        --conf,spark.sql.shuffle.partitions=200,
        --conf,spark.serializer=org.apache.spark.serializer.KryoSerializer,
        --packages,io.delta:delta-spark_2.12:3.2.0,org.apache.hadoop:hadoop-aws:3.3.4,com.amazonaws:aws-java-sdk-bundle:1.12.262,
        s3://${S3_BUCKET}/scripts/transform_to_gold.py
    ]

echo "=== All StreamLake EMR steps submitted ==="
echo "Full pipeline: Bronze ingestion → Silver transform → Gold transform"
echo "Monitor progress in EMR console or via:"
echo "aws emr describe-cluster --cluster-id ${CLUSTER_ID} --region ${AWS_REGION}"