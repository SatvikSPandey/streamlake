"""
StreamLake Producer Control Panel.

Streamlit UI for starting/stopping event production to Kafka at configurable
rates, with live stats. Intended for demos and development — not a
production dashboard.

Run from project root:
    streamlit run dashboard/producer_control.py
"""
from __future__ import annotations

import sys
import threading
import time
from pathlib import Path

# Make sibling packages importable when Streamlit runs the file directly.
sys.path.insert(0, str(Path(__file__).resolve().parent.parent))

import streamlit as st

from producer.event_generator import EventGenerator
from producer.kafka_producer import KafkaEventProducer


# ---------------------------------------------------------------------
# Background worker
# ---------------------------------------------------------------------

class ProducerWorker:
    """
    Runs the generator+producer loop on a background thread so the
    Streamlit UI stays responsive.
    """

    def __init__(self, rate_per_second: float, seed: int | None = None):
        self.rate = rate_per_second
        self.seed = seed
        self._stop_flag = threading.Event()
        self._thread: threading.Thread | None = None
        self.producer: KafkaEventProducer | None = None
        self.error: str | None = None

    def start(self) -> None:
        self._thread = threading.Thread(target=self._run, daemon=True)
        self._thread.start()

    def stop(self) -> None:
        self._stop_flag.set()
        if self._thread is not None:
            self._thread.join(timeout=5)
        if self.producer is not None:
            self.producer.flush(timeout=10)

    def is_running(self) -> bool:
        return self._thread is not None and self._thread.is_alive()

    def _run(self) -> None:
        try:
            self.producer = KafkaEventProducer()
            gen = EventGenerator(seed=self.seed)
            interval = 1.0 / self.rate if self.rate > 0 else 0
            next_send = time.monotonic()
            while not self._stop_flag.is_set():
                event = gen.next_event()
                self.producer.send(event)
                if interval > 0:
                    next_send += interval
                    sleep_for = next_send - time.monotonic()
                    if sleep_for > 0:
                        time.sleep(sleep_for)
        except Exception as e:  # noqa: BLE001 — display any error in the UI
            self.error = f"{type(e).__name__}: {e}"


# ---------------------------------------------------------------------
# Streamlit UI
# ---------------------------------------------------------------------

st.set_page_config(
    page_title="StreamLake — Producer Control",
    page_icon="📡",
    layout="wide",
)

st.title("📡 StreamLake — Producer Control Panel")
st.caption(
    "Generate realistic streaming-platform events and publish them to Kafka. "
    "Intended for demos and load testing."
)

# Initialize session state
if "worker" not in st.session_state:
    st.session_state.worker = None

# --- Controls ---------------------------------------------------------
col_controls, col_stats = st.columns([1, 2])

with col_controls:
    st.subheader("Controls")

    rate = st.slider(
        "Events per second",
        min_value=1,
        max_value=500,
        value=20,
        step=1,
        help="Target publishing rate. Start small for demos.",
        disabled=st.session_state.worker is not None,
    )

    seed_input = st.text_input(
        "Seed (optional, for reproducibility)",
        value="",
        help="Integer seed for deterministic event generation. Leave blank for random.",
        disabled=st.session_state.worker is not None,
    )
    seed = int(seed_input) if seed_input.strip().isdigit() else None

    start_clicked = st.button(
        "▶ Start producing",
        type="primary",
        disabled=st.session_state.worker is not None,
        use_container_width=True,
    )
    stop_clicked = st.button(
        "⏹ Stop",
        disabled=st.session_state.worker is None,
        use_container_width=True,
    )

    if start_clicked:
        worker = ProducerWorker(rate_per_second=rate, seed=seed)
        worker.start()
        st.session_state.worker = worker
        st.rerun()

    if stop_clicked and st.session_state.worker is not None:
        st.session_state.worker.stop()
        st.session_state.worker = None
        st.rerun()


# --- Live stats -------------------------------------------------------
with col_stats:
    st.subheader("Live Stats")

    worker = st.session_state.worker

    if worker is None:
        st.info("Producer is stopped. Configure settings and click **Start producing**.")
    elif worker.error is not None:
        st.error(f"Producer crashed: {worker.error}")
    else:
        # Poll producer stats
        stats = worker.producer.stats_snapshot() if worker.producer else None

        if stats is None:
            st.warning("Producer starting up…")
        else:
            m1, m2, m3, m4 = st.columns(4)
            m1.metric("Sent", f"{stats['sent']:,}")
            m2.metric("Delivered", f"{stats['delivered']:,}")
            m3.metric("In-flight", f"{stats['in_flight']:,}")
            m4.metric("Failed", f"{stats['failed']:,}")

            kb = stats["bytes_sent"] / 1024
            st.caption(
                f"Total payload: {kb:,.1f} KB · "
                f"Target rate: {worker.rate}/s · "
                f"Running: {worker.is_running()}"
            )
            if stats["failed"] > 0 and stats["last_error"]:
                st.warning(f"Last error: {stats['last_error']}")

        # Auto-refresh every second while running
        time.sleep(1)
        st.rerun()

st.divider()
st.caption(
    "Events are published to the **user-events** topic on your Confluent Cloud "
    "cluster. Verify them in the Confluent UI → Topics → user-events → Messages."
)