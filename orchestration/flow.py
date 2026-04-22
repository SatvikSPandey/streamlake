"""
StreamLake - Prefect orchestration.

Wraps the batch pipeline as a scheduled Prefect flow:
  Silver transform (Bronze -> Silver)
    |
    +--> Gold transforms (Silver -> 5 Gold tables)
           |
           +--> Snowflake load (Gold -> Snowflake)

The Kafka -> Bronze streaming job is NOT orchestrated here because streaming
is continuous, not batch. This flow handles the downstream batch refresh.

Each task is a thin wrapper around the existing script in spark/ or
snowflake/ so the orchestration layer adds observability without
duplicating logic.

Run locally:
    python orchestration/flow.py
"""
from __future__ import annotations

import subprocess
import sys
from pathlib import Path

from prefect import flow, get_run_logger, task

ROOT = Path(__file__).resolve().parent.parent


def _run_script(script: str) -> None:
    """
    Run a Python script as a subprocess.
    Raises CalledProcessError on non-zero exit so Prefect marks the task failed.
    """
    logger = get_run_logger()
    logger.info(f"Running {script}")
    result = subprocess.run(
        [sys.executable, script],
        cwd=str(ROOT),
        capture_output=True,
        text=True,
        check=False,
    )
    if result.stdout:
        logger.info(result.stdout)
    if result.stderr:
        logger.info(result.stderr)  # Spark prints INFO/WARN to stderr by default
    if result.returncode != 0:
        raise RuntimeError(
            f"{script} exited with code {result.returncode}"
        )


@task(retries=2, retry_delay_seconds=30, log_prints=True)
def silver_transform() -> None:
    """Bronze -> Silver via Spark."""
    _run_script("spark/transform_to_silver.py")


@task(retries=2, retry_delay_seconds=30, log_prints=True)
def gold_transforms() -> None:
    """Silver -> 5 Gold tables via Spark."""
    _run_script("spark/transform_to_gold.py")


@task(retries=3, retry_delay_seconds=15, log_prints=True)
def load_gold_to_snowflake() -> None:
    """S3 Gold Delta -> Snowflake via pandas + write_pandas."""
    _run_script("snowflake/load_gold.py")


@flow(name="streamlake-batch-refresh", log_prints=True)
def streamlake_batch_refresh() -> None:
    """
    Full downstream pipeline: Silver -> Gold -> Snowflake.

    Kafka ingestion runs separately (spark/ingest_to_bronze.py) as a
    long-running streaming job.
    """
    logger = get_run_logger()
    logger.info("StreamLake batch refresh starting.")

    silver_future = silver_transform.submit()
    gold_future = gold_transforms.submit(wait_for=[silver_future])
    snowflake_future = load_gold_to_snowflake.submit(wait_for=[gold_future])

    # Block until snowflake load completes (implicitly waits for Silver + Gold too)
    snowflake_future.result()

    logger.info("StreamLake batch refresh complete.")


if __name__ == "__main__":
    streamlake_batch_refresh()