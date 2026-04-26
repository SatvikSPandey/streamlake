#!/bin/bash
# =============================================================================
# StreamLake EMR Bootstrap Script
# Runs on every node (master + core) at cluster startup, before Spark launches.
# Purpose: install Python 3.11 and all StreamLake Python dependencies so that
# PYSPARK_PYTHON=/usr/local/bin/python3.11 resolves correctly on all nodes.
#
# NOTE: This script is documented infrastructure — it reflects the exact
# dependency versions used in local development (requirements.txt) and is
# designed to run on EMR 6.15.0 (Amazon Linux 2, Python 3.9 default).
# =============================================================================

set -e  # Exit immediately on any error
set -x  # Print each command before executing (visible in EMR bootstrap logs)

echo "=== StreamLake Bootstrap: Starting ==="
echo "Node hostname: $(hostname)"
echo "Timestamp: $(date -u)"

# -----------------------------------------------------------------------------
# Step 1: Install Python 3.11
# EMR 6.15 ships Python 3.9 as default. We need 3.11 to match local dev and
# avoid pyarrow/py4j compatibility issues that surface on 3.12+.
# -----------------------------------------------------------------------------
echo "=== Installing Python 3.11 ==="
sudo amazon-linux-extras enable python3.11 || true
sudo yum install -y python3.11 python3.11-pip python3.11-devel

# Verify 3.11 installed correctly
python3.11 --version

# -----------------------------------------------------------------------------
# Step 2: Upgrade pip on Python 3.11
# -----------------------------------------------------------------------------
echo "=== Upgrading pip ==="
sudo python3.11 -m pip install --upgrade pip

# -----------------------------------------------------------------------------
# Step 3: Install all StreamLake Python dependencies
# Versions pinned to match requirements.txt exactly — same as local dev.
# Critical pins:
#   numpy==1.26.4        — must be <2.0 (PySpark 3.5 C API compatibility)
#   pyopenssl>=24.0.0    — must be >=24.0.0 (cryptography 46 compatibility)
#   delta-spark==3.2.0   — must match spark.jars.packages version exactly
# -----------------------------------------------------------------------------
echo "=== Installing StreamLake Python dependencies ==="
sudo python3.11 -m pip install \
    confluent-kafka==2.6.1 \
    pyspark==3.5.3 \
    delta-spark==3.2.0 \
    deltalake==0.22.3 \
    snowflake-connector-python==3.12.3 \
    snowflake-sqlalchemy==1.6.1 \
    prefect==3.6.27 \
    streamlit==1.39.0 \
    plotly==5.24.1 \
    altair==5.4.1 \
    boto3==1.35.60 \
    pandas==2.2.3 \
    "numpy==1.26.4" \
    python-dotenv==1.0.1 \
    pyyaml==6.0.2 \
    requests==2.32.3 \
    faker==30.8.2 \
    "pyopenssl>=24.0.0"

# -----------------------------------------------------------------------------
# Step 4: Set PYSPARK_PYTHON system-wide so all Spark workers use Python 3.11
# This mirrors the PYSPARK_PYTHON = sys.executable pattern used in local dev
# to prevent workers from picking up the wrong Python interpreter.
# On EMR, the equivalent is setting it in /etc/environment.
# cluster_config.json sets it via spark-env Classification as well (belt +
# suspenders approach).
# -----------------------------------------------------------------------------
echo "=== Setting PYSPARK_PYTHON system-wide ==="
echo "export PYSPARK_PYTHON=/usr/local/bin/python3.11" | sudo tee -a /etc/environment
echo "export PYSPARK_DRIVER_PYTHON=/usr/local/bin/python3.11" | sudo tee -a /etc/environment

# Verify the python3.11 binary is at the expected path
which python3.11
python3.11 -c "import pyspark; print('PySpark version:', pyspark.__version__)"
python3.11 -c "import delta; print('delta-spark installed correctly')"
python3.11 -c "import numpy; print('NumPy version:', numpy.__version__)"

echo "=== StreamLake Bootstrap: Complete ==="
echo "Timestamp: $(date -u)"