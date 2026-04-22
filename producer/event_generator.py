"""
Realistic event generator for StreamLake.

Models a pool of viewers watching a finite content catalog on various
devices. Each viewer runs independent sessions with state-machine-driven
event sequences: PLAY -> (PAUSE/RESUME/SEEK)* -> (SKIP/COMPLETE/ERROR).

The generator is deterministic when given a seed, so tests and demos
are reproducible.

This module produces UserEvent objects — it does NOT talk to Kafka.
The actual Kafka publishing happens in producer/kafka_producer.py
(Step 10). Keeping generation and publishing separate makes the
generator unit-testable without a live cluster.
"""
from __future__ import annotations

import random
import uuid
from dataclasses import dataclass, field
from datetime import datetime, timezone
from typing import Iterator

from faker import Faker

from producer.event_schema import DeviceType, EventType, UserEvent


# ---------------------------------------------------------------------
# Static catalog / population configuration
# ---------------------------------------------------------------------

# A realistic ratio of device types. Mobile dominates on OTT platforms;
# these weights roughly match public industry reports for Indian OTT
# markets (mobile-heavy) rather than US (TV-heavy).
DEVICE_WEIGHTS = {
    DeviceType.MOBILE_ANDROID: 0.45,
    DeviceType.MOBILE_IOS: 0.15,
    DeviceType.SMART_TV: 0.20,
    DeviceType.WEB: 0.12,
    DeviceType.TABLET: 0.08,
}

# Content-type mix: roughly what you'd see on a major streaming service.
CONTENT_TYPE_WEIGHTS = {
    "series_episode": 0.55,
    "movie": 0.30,
    "short": 0.08,
    "trailer": 0.04,
    "live": 0.03,
}

# Country mix skewed to India (our hypothetical primary market)
# with a long tail of international viewers.
COUNTRY_WEIGHTS = {
    "IN": 0.55,
    "US": 0.10,
    "GB": 0.05,
    "AE": 0.05,
    "SG": 0.04,
    "CA": 0.03,
    "AU": 0.03,
    "DE": 0.03,
    "BD": 0.03,
    "LK": 0.03,
    "NP": 0.03,
    "MY": 0.03,
}

APP_VERSIONS = ["1.8.0", "1.8.1", "1.9.0", "2.0.0", "2.0.1"]


# ---------------------------------------------------------------------
# Session state
# ---------------------------------------------------------------------

@dataclass
class ViewingSession:
    """
    A single viewer watching a single piece of content.
    Tracks position so successive events are consistent.
    """
    user_id: str
    session_id: str
    content_id: str
    content_type: str
    device_type: DeviceType
    app_version: str
    country_code: str
    content_duration_seconds: int   # total length of the content
    position_seconds: int = 0       # current playback position
    watch_seconds: int = 0          # cumulative time actually watched
    is_playing: bool = True         # paused vs playing
    event_count: int = 0
    # Probability-of-termination grows as position approaches duration,
    # so sessions naturally end near the end of the content.
    last_event_type: EventType = EventType.PLAY


# ---------------------------------------------------------------------
# Weighted random helpers
# ---------------------------------------------------------------------

def _weighted_choice(rng: random.Random, weights: dict) -> object:
    """Pick a key from a dict of {value: weight} using the given RNG."""
    keys = list(weights.keys())
    probs = list(weights.values())
    return rng.choices(keys, weights=probs, k=1)[0]


# ---------------------------------------------------------------------
# The generator
# ---------------------------------------------------------------------

class EventGenerator:
    """
    Produces a stream of UserEvent objects modeling a population of
    viewers running concurrent sessions.
    """

    def __init__(
        self,
        num_users: int = 500,
        num_content_items: int = 200,
        seed: int | None = None,
    ):
        self.rng = random.Random(seed)
        self.faker = Faker()
        if seed is not None:
            Faker.seed(seed)

        # Pre-build the stable user pool and content catalog.
        self.user_ids = [f"u-{self.faker.uuid4()[:8]}" for _ in range(num_users)]
        self.content_items = [
            {
                "content_id": f"c-{self.faker.uuid4()[:8]}",
                "content_type": _weighted_choice(self.rng, CONTENT_TYPE_WEIGHTS),
                # Duration ranges depend on content_type
                "duration_seconds": self._random_duration(),
            }
            for _ in range(num_content_items)
        ]

        # Active sessions keyed by session_id.
        self.active_sessions: dict[str, ViewingSession] = {}

    def _random_duration(self) -> int:
        """Random content length in seconds, with a realistic distribution."""
        # Most content is 20-60 minutes; some shorter, some longer.
        # Weighted so 30 min is the most common.
        choice = self.rng.random()
        if choice < 0.1:
            return self.rng.randint(60, 300)          # 1-5 min short
        elif choice < 0.5:
            return self.rng.randint(1_200, 1_800)     # 20-30 min episode
        elif choice < 0.85:
            return self.rng.randint(1_800, 3_600)     # 30-60 min episode
        else:
            return self.rng.randint(5_400, 9_000)     # 90-150 min movie

    def _start_new_session(self) -> ViewingSession:
        """Start a new viewing session for a random user on random content."""
        user_id = self.rng.choice(self.user_ids)
        content = self.rng.choice(self.content_items)
        session = ViewingSession(
            user_id=user_id,
            session_id=f"s-{uuid.uuid4().hex[:12]}",
            content_id=content["content_id"],
            content_type=content["content_type"],
            device_type=_weighted_choice(self.rng, DEVICE_WEIGHTS),
            app_version=self.rng.choice(APP_VERSIONS),
            country_code=_weighted_choice(self.rng, COUNTRY_WEIGHTS),
            content_duration_seconds=content["duration_seconds"],
        )
        self.active_sessions[session.session_id] = session
        return session

    def _advance_session(self, session: ViewingSession) -> EventType:
        """
        Advance one step of the session's state machine and return the
        event type that just occurred.
        """
        # First event in a session is always PLAY.
        if session.event_count == 0:
            session.last_event_type = EventType.PLAY
            return EventType.PLAY

        # If currently paused, high chance of RESUME; otherwise could pause.
        if not session.is_playing:
            if self.rng.random() < 0.7:
                session.is_playing = True
                session.last_event_type = EventType.RESUME
                return EventType.RESUME
            # Otherwise, stay paused but emit another pause event (rare).

        # Currently playing — weighted choice of next action.
        # Most of the time, just progress (no explicit event — handled by PLAY heartbeat).
        # Occasionally pause / seek / skip / complete / error.
        roll = self.rng.random()
        progress = session.position_seconds / max(session.content_duration_seconds, 1)

        # As progress -> 1.0, chance of COMPLETE rises sharply.
        if progress > 0.9 and roll < 0.6:
            session.last_event_type = EventType.COMPLETE
            return EventType.COMPLETE

        if roll < 0.05:
            session.last_event_type = EventType.ERROR
            return EventType.ERROR
        if roll < 0.12:
            session.last_event_type = EventType.SKIP
            return EventType.SKIP
        if roll < 0.22:
            # Seek forward or back up to 60 seconds
            delta = self.rng.randint(-60, 180)
            session.position_seconds = max(0, session.position_seconds + delta)
            session.last_event_type = EventType.SEEK
            return EventType.SEEK
        if roll < 0.35:
            session.is_playing = False
            session.last_event_type = EventType.PAUSE
            return EventType.PAUSE

        # Default: continuing playback — advance position, emit PLAY heartbeat
        jump = self.rng.randint(5, 30)
        session.position_seconds = min(
            session.content_duration_seconds,
            session.position_seconds + jump,
        )
        session.watch_seconds += jump
        session.last_event_type = EventType.PLAY
        return EventType.PLAY

    def _build_event(self, session: ViewingSession, event_type: EventType) -> UserEvent:
        """Construct a validated UserEvent from the current session state."""
        session.event_count += 1
        return UserEvent(
            event_id=str(uuid.uuid4()),
            user_id=session.user_id,
            session_id=session.session_id,
            content_id=session.content_id,
            content_type=session.content_type,
            event_type=event_type,
            watch_seconds=session.watch_seconds,
            playback_position_seconds=session.position_seconds,
            device_type=session.device_type,
            app_version=session.app_version,
            country_code=session.country_code,
            event_timestamp=datetime.now(timezone.utc),
        )

    def next_event(self) -> UserEvent:
        """
        Return the next realistic event.

        Strategy:
          - With probability 0.15, start a brand new session (a user picks
            up a new piece of content).
          - Otherwise, advance an existing active session.
          - Sessions that reach COMPLETE, SKIP to the end, or ERROR are
            removed from the active pool.
        """
        if not self.active_sessions or self.rng.random() < 0.15:
            session = self._start_new_session()
        else:
            session = self.rng.choice(list(self.active_sessions.values()))

        event_type = self._advance_session(session)
        event = self._build_event(session, event_type)

        # Terminal events end the session.
        if event_type in (EventType.COMPLETE, EventType.ERROR):
            self.active_sessions.pop(session.session_id, None)

        return event

    def stream(self, n: int | None = None) -> Iterator[UserEvent]:
        """Yield n events (or forever, if n is None)."""
        if n is None:
            while True:
                yield self.next_event()
        else:
            for _ in range(n):
                yield self.next_event()


__all__ = ["EventGenerator", "ViewingSession"]