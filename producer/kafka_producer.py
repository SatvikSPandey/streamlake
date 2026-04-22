"""
Kafka producer for StreamLake.

Wraps confluent-kafka's Producer with:
  - Credential loading from imp/.env
  - A send() method that takes UserEvent objects
  - Rate-limited bulk publishing
  - Delivery tracking (counts, errors, latency)
  - Graceful shutdown

Separate from event_generator.py — this module only publishes events
it receives; it does not generate them. Composition of generator +
producer happens in the Streamlit control panel (app.py).
"""
from __future__ import annotations

import os
import threading
import time
from dataclasses import dataclass, field
from pathlib import Path
from typing import Iterable

from confluent_kafka import Producer
from dotenv import load_dotenv

from producer.event_schema import UserEvent


@dataclass
class ProducerStats:
    """Thread-safe counters for delivery metrics."""
    sent: int = 0
    delivered: int = 0
    failed: int = 0
    bytes_sent: int = 0
    last_error: str = ""
    _lock: threading.Lock = field(default_factory=threading.Lock)

    def mark_sent(self, nbytes: int) -> None:
        with self._lock:
            self.sent += 1
            self.bytes_sent += nbytes

    def mark_delivered(self) -> None:
        with self._lock:
            self.delivered += 1

    def mark_failed(self, err_msg: str) -> None:
        with self._lock:
            self.failed += 1
            self.last_error = err_msg

    def snapshot(self) -> dict:
        with self._lock:
            return {
                "sent": self.sent,
                "delivered": self.delivered,
                "failed": self.failed,
                "bytes_sent": self.bytes_sent,
                "last_error": self.last_error,
                "in_flight": self.sent - self.delivered - self.failed,
            }


class KafkaEventProducer:
    """Publishes UserEvent objects to Confluent Cloud Kafka."""

    def __init__(self, env_path: Path | None = None):
        self.stats = ProducerStats()
        self._producer = self._build_producer(env_path)
        self._topic = os.environ.get("KAFKA_TOPIC_USER_EVENTS", "user-events")

    def _build_producer(self, env_path: Path | None) -> Producer:
        if env_path is None:
            env_path = Path(__file__).resolve().parent.parent / "imp" / ".env"
        if not env_path.exists():
            raise FileNotFoundError(f"Expected credentials file at {env_path}")
        load_dotenv(dotenv_path=env_path)

        required = [
            "CONFLUENT_BOOTSTRAP_SERVERS",
            "CONFLUENT_API_KEY",
            "CONFLUENT_API_SECRET",
        ]
        missing = [k for k in required if not os.getenv(k)]
        if missing:
            raise RuntimeError(f"Missing env vars: {missing}")

        config = {
            "bootstrap.servers": os.environ["CONFLUENT_BOOTSTRAP_SERVERS"],
            "security.protocol": "SASL_SSL",
            "sasl.mechanisms": "PLAIN",
            "sasl.username": os.environ["CONFLUENT_API_KEY"],
            "sasl.password": os.environ["CONFLUENT_API_SECRET"],
            "client.id": "streamlake-producer",
            "acks": "all",
            "linger.ms": 20,              # batch window for throughput
            "compression.type": "snappy", # smaller payloads on wire
            "queue.buffering.max.messages": 100_000,
        }
        return Producer(config)

    def _delivery_callback(self, err, msg) -> None:
        """Invoked once per message after broker acknowledges or rejects."""
        if err is not None:
            self.stats.mark_failed(str(err))
        else:
            self.stats.mark_delivered()

    def send(self, event: UserEvent) -> None:
        """Queue a single event for delivery (async)."""
        value = event.to_kafka_value_json().encode("utf-8")
        key = event.to_kafka_key().encode("utf-8")
        try:
            self._producer.produce(
                topic=self._topic,
                key=key,
                value=value,
                callback=self._delivery_callback,
            )
            self.stats.mark_sent(len(value))
        except BufferError as e:
            # Internal queue is full; serve outstanding callbacks and retry once.
            self._producer.poll(0.1)
            self._producer.produce(
                topic=self._topic,
                key=key,
                value=value,
                callback=self._delivery_callback,
            )
            self.stats.mark_sent(len(value))

        # poll() serves delivery callbacks — MUST be called regularly
        # or they never fire. 0 = non-blocking.
        self._producer.poll(0)

    def send_many(
        self,
        events: Iterable[UserEvent],
        rate_per_second: float | None = None,
    ) -> None:
        """
        Publish a batch of events, optionally rate-limited.

        If rate_per_second is None, publishes as fast as the client allows.
        Otherwise, spaces events with a target rate (sleep between sends).
        """
        if rate_per_second is None or rate_per_second <= 0:
            for event in events:
                self.send(event)
            return

        interval = 1.0 / rate_per_second
        next_send_at = time.monotonic()
        for event in events:
            now = time.monotonic()
            if now < next_send_at:
                time.sleep(next_send_at - now)
            self.send(event)
            next_send_at += interval

    def flush(self, timeout: float = 15.0) -> int:
        """Wait until all queued messages are delivered. Returns pending count."""
        return self._producer.flush(timeout)

    def stats_snapshot(self) -> dict:
        """Current producer metrics."""
        self._producer.poll(0)  # drain callbacks before snapshot
        return self.stats.snapshot()


__all__ = ["KafkaEventProducer", "ProducerStats"]