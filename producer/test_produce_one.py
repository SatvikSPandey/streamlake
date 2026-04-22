"""
Minimal Kafka producer — sends ONE event to the user-events topic.

This is a connectivity test. It proves:
  - imp/.env loads correctly
  - Confluent Cloud credentials authenticate over SASL/SSL
  - The bootstrap server is reachable
  - The Kafka client library is working
  - Our Pydantic event schema serializes to valid JSON

If this succeeds, everything downstream (high-volume producer, Streamlit
UI, Spark streaming consumer) can assume the pipeline foundation works.

Usage (from project root):
    python producer/test_produce_one.py
"""
from __future__ import annotations

import os
import sys
import uuid
from datetime import datetime, timezone
from pathlib import Path

from confluent_kafka import Producer
from dotenv import load_dotenv

# Import the schema we wrote in Step 7
sys.path.insert(0, str(Path(__file__).resolve().parent.parent))
from producer.event_schema import DeviceType, EventType, UserEvent


def load_config() -> dict:
    """
    Load Confluent Cloud credentials from imp/.env and return the config
    dict that confluent-kafka expects. Fails loudly if any required env var
    is missing — better to crash clearly here than to get a cryptic
    SASL error ten seconds into producing.
    """
    project_root = Path(__file__).resolve().parent.parent
    env_path = project_root / "imp" / ".env"
    if not env_path.exists():
        raise FileNotFoundError(
            f"Expected credentials file at {env_path} — see imp/.env.example."
        )
    load_dotenv(dotenv_path=env_path)

    required = [
        "CONFLUENT_BOOTSTRAP_SERVERS",
        "CONFLUENT_API_KEY",
        "CONFLUENT_API_SECRET",
    ]
    missing = [k for k in required if not os.getenv(k)]
    if missing:
        raise RuntimeError(f"Missing required env vars in imp/.env: {missing}")

    return {
        "bootstrap.servers": os.environ["CONFLUENT_BOOTSTRAP_SERVERS"],
        "security.protocol": "SASL_SSL",
        "sasl.mechanisms": "PLAIN",
        "sasl.username": os.environ["CONFLUENT_API_KEY"],
        "sasl.password": os.environ["CONFLUENT_API_SECRET"],
        "client.id": "streamlake-producer-proof-of-life",
        # Reasonable defaults for a test producer
        "acks": "all",       # broker acknowledges after replication (strongest)
        "linger.ms": 10,     # small batching window for efficiency
    }


def delivery_callback(err, msg) -> None:
    """
    Called once per message, asynchronously, after Kafka confirms delivery
    (or rejection). We log either outcome clearly so the test is obviously
    pass/fail.
    """
    if err is not None:
        print(f"[FAIL] Delivery failed: {err}")
        return
    print(
        f"[OK] Delivered to topic={msg.topic()} "
        f"partition={msg.partition()} offset={msg.offset()}"
    )


def build_sample_event() -> UserEvent:
    """Build one realistic-looking event for the connectivity test."""
    return UserEvent(
        event_id=str(uuid.uuid4()),
        user_id="u-test-0001",
        session_id="s-test-0001",
        content_id="c-movie-42",
        content_type="movie",
        event_type=EventType.PLAY,
        watch_seconds=0,
        playback_position_seconds=0,
        device_type=DeviceType.WEB,
        app_version="1.0.0",
        country_code="IN",
        event_timestamp=datetime.now(timezone.utc),
    )


def main() -> None:
    print("=" * 60)
    print("StreamLake — Producer Proof of Life")
    print("=" * 60)

    config = load_config()
    topic = os.environ.get("KAFKA_TOPIC_USER_EVENTS", "user-events")
    print(f"[INFO] Bootstrap: {config['bootstrap.servers']}")
    print(f"[INFO] Topic    : {topic}")

    producer = Producer(config)

    event = build_sample_event()
    print(f"[INFO] Sending  : event_id={event.event_id} "
          f"user_id={event.user_id} type={event.event_type.value}")

    producer.produce(
        topic=topic,
        key=event.to_kafka_key().encode("utf-8"),
        value=event.to_kafka_value_json().encode("utf-8"),
        callback=delivery_callback,
    )

    # Block until all queued messages are delivered or timeout expires.
    # The return value is the number of messages STILL pending — should be 0.
    remaining = producer.flush(timeout=15)
    if remaining > 0:
        print(f"[FAIL] {remaining} message(s) did not deliver in time.")
        sys.exit(1)

    print("=" * 60)
    print("Producer proof of life PASSED.")
    print("Go to Confluent Cloud UI -> streamlake-cluster -> Topics ->")
    print("user-events -> Messages tab to see the event.")
    print("=" * 60)


if __name__ == "__main__":
    main()