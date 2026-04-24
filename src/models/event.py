"""
Event schema definitions using Pydantic v2.

Defines the data contracts for the Pub-Sub Log Aggregator system:
- EventSchema: core event structure with validation
- PublishRequest: accepts single or batch events
- PublishResponse: publish operation result
- EventResponse: processed event for API responses
- StatsResponse: system statistics
"""

from __future__ import annotations

from datetime import datetime
from typing import Any, Union

from pydantic import BaseModel, Field, field_validator


class EventSchema(BaseModel):
    """
    Core event model for the Pub-Sub system.

    Uses a composite key of (topic, event_id) for deduplication.
    Timestamp follows ISO 8601 format for ordering support.
    """

    topic: str = Field(
        ...,
        min_length=1,
        max_length=255,
        description="Event topic/channel for routing",
        examples=["auth", "payment", "user.signup"],
    )
    event_id: str = Field(
        ...,
        min_length=1,
        max_length=255,
        description="Unique event identifier within a topic",
        examples=["evt-001", "a1b2c3d4"],
    )
    timestamp: str = Field(
        ...,
        description="ISO 8601 timestamp of when the event occurred",
        examples=["2026-01-15T10:30:00Z"],
    )
    source: str = Field(
        ...,
        min_length=1,
        max_length=255,
        description="Origin system or service that produced the event",
        examples=["auth-service", "payment-gateway"],
    )
    payload: dict[str, Any] = Field(
        default_factory=dict,
        description="Arbitrary event data payload",
    )

    @field_validator("timestamp")
    @classmethod
    def validate_timestamp(cls, v: str) -> str:
        """Validate that timestamp is a valid ISO 8601 string."""
        try:
            datetime.fromisoformat(v.replace("Z", "+00:00"))
        except (ValueError, AttributeError) as e:
            raise ValueError(
                f"Invalid ISO 8601 timestamp: '{v}'. "
                "Expected format like '2026-01-15T10:30:00Z'"
            ) from e
        return v

    @property
    def dedup_key(self) -> tuple[str, str]:
        """Composite deduplication key: (topic, event_id)."""
        return (self.topic, self.event_id)


class PublishRequest(BaseModel):
    """
    Request model for POST /publish.

    Accepts either a single event or a batch of events.
    This flexibility supports both simple and high-throughput publishing.
    """

    events: Union[EventSchema, list[EventSchema]] = Field(
        ...,
        description="Single event or list of events to publish",
    )

    def get_event_list(self) -> list[EventSchema]:
        """Normalize input to always return a list of events."""
        if isinstance(self.events, list):
            return self.events
        return [self.events]


class PublishResponse(BaseModel):
    """Response model for POST /publish."""

    status: str = Field(default="accepted", description="Operation status")
    received: int = Field(description="Number of events received")
    message: str = Field(description="Human-readable result message")


class EventResponse(BaseModel):
    """Response model for GET /events."""

    topic: str
    event_id: str
    timestamp: str
    source: str
    payload: dict[str, Any]
    processed_at: str = Field(description="When the event was processed by consumer")


class StatsResponse(BaseModel):
    """Response model for GET /stats."""

    received: int = Field(description="Total events received via publish endpoint")
    unique_processed: int = Field(description="Events successfully processed (unique)")
    duplicate_dropped: int = Field(description="Duplicate events that were skipped")
    topics: list[str] = Field(description="List of active topics")
    uptime: str = Field(description="System uptime in human-readable format")
    uptime_seconds: float = Field(description="System uptime in seconds")
