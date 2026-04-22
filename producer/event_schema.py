"""
Event schema for StreamLake streaming events.

Defines the shape of every message our producer publishes to Kafka.
Using Pydantic gives us runtime validation — malformed events are
rejected in Python before they ever hit the network, and the schema
doubles as self-documenting code for anyone reading the producer.
"""
from __future__ import annotations

from datetime import datetime, timezone
from enum import Enum
from typing import Optional

from pydantic import BaseModel, Field, field_validator


class EventType(str, Enum):
    """
    Every user-interaction event type our platform emits.

    The string values are what go on the wire — these names must stay stable
    because downstream (Spark, Snowflake, dashboards) partition, filter, and
    aggregate on them.
    """
    PLAY = "play"
    PAUSE = "pause"
    RESUME = "resume"
    SKIP = "skip"
    SEEK = "seek"
    COMPLETE = "complete"
    ERROR = "error"


class DeviceType(str, Enum):
    """Client device category — useful for analytics segmentation."""
    MOBILE_IOS = "mobile_ios"
    MOBILE_ANDROID = "mobile_android"
    WEB = "web"
    SMART_TV = "smart_tv"
    TABLET = "tablet"


class UserEvent(BaseModel):
    """
    A single user-interaction event on the streaming platform.

    This is the contract between the producer and everything downstream
    (Bronze layer, Silver transformations, Gold aggregates). Every field
    has a specific reason to exist; do not add fields here without
    considering the downstream impact.
    """

    # -- Identity --------------------------------------------------------
    event_id: str = Field(
        ...,
        description="Unique ID for this event. UUID4 generated at produce-time.",
        min_length=36,
        max_length=36,
    )
    user_id: str = Field(
        ...,
        description="Stable identifier for the viewer. Same across sessions.",
        min_length=1,
        max_length=64,
    )
    session_id: str = Field(
        ...,
        description="Identifier for the viewer's current viewing session.",
        min_length=1,
        max_length=64,
    )

    # -- Content referenced ---------------------------------------------
    content_id: str = Field(
        ...,
        description="The piece of content the event is about (movie/episode/clip).",
        min_length=1,
        max_length=64,
    )
    content_type: str = Field(
        ...,
        description="Category of content: movie, series_episode, live, short, trailer, etc.",
        min_length=1,
        max_length=32,
    )

    # -- Event semantics -------------------------------------------------
    event_type: EventType = Field(
        ...,
        description="What the user did. One of the EventType enum values.",
    )
    watch_seconds: int = Field(
        0,
        description=(
            "Seconds of content watched cumulatively in this session up to this event. "
            "For instantaneous events (pause/skip/error) this is the position at the moment."
        ),
        ge=0,
        le=86_400,  # one day's worth of seconds; anything beyond is obviously a bug
    )
    playback_position_seconds: Optional[int] = Field(
        None,
        description="Position inside the content when the event occurred.",
        ge=0,
        le=86_400,
    )

    # -- Device / context ------------------------------------------------
    device_type: DeviceType = Field(
        ...,
        description="Device category the viewer is using.",
    )
    app_version: str = Field(
        ...,
        description="Client app version — used to correlate errors with releases.",
        min_length=1,
        max_length=32,
    )
    country_code: str = Field(
        ...,
        description="ISO 3166-1 alpha-2 country code (e.g., IN, US, GB).",
        min_length=2,
        max_length=2,
    )

    # -- Timing ----------------------------------------------------------
    event_timestamp: datetime = Field(
        ...,
        description="Moment the event occurred on the client, in UTC.",
    )

    # -- Validators ------------------------------------------------------
    @field_validator("event_timestamp")
    @classmethod
    def _timestamp_must_be_utc(cls, v: datetime) -> datetime:
        """Reject naive datetimes; coerce everything to UTC."""
        if v.tzinfo is None:
            raise ValueError("event_timestamp must be timezone-aware (UTC)")
        return v.astimezone(timezone.utc)

    @field_validator("country_code")
    @classmethod
    def _country_code_must_be_upper(cls, v: str) -> str:
        """Normalize to uppercase ISO-2."""
        return v.upper()

    # -- Serialization ---------------------------------------------------
    def to_kafka_key(self) -> str:
        """
        Kafka partition key. Using user_id keeps all events for a given user
        on the same partition — preserves per-user ordering across pause/resume/skip
        without cross-partition reordering.
        """
        return self.user_id

    def to_kafka_value_json(self) -> str:
        """Serialize to the JSON string that goes on the wire."""
        return self.model_dump_json()


__all__ = ["UserEvent", "EventType", "DeviceType"]