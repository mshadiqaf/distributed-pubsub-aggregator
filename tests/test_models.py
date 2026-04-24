"""
Test 4: Schema Validation Tests

Covers:
- Valid event schema parsing
- Invalid events (missing fields, bad timestamp, empty strings)
- PublishRequest normalization (single vs batch)
"""

from __future__ import annotations

import pytest
from pydantic import ValidationError

from src.models.event import EventSchema, PublishRequest


@pytest.mark.asyncio
async def test_event_schema_validation():
    """
    Test 4: Comprehensive schema validation for EventSchema.

    Tests valid events, missing fields, invalid timestamps, and
    empty string rejections.
    """
    # ── Valid event should parse correctly ──
    valid_event = EventSchema(
        topic="auth.login",
        event_id="evt-001",
        timestamp="2026-01-15T10:30:00Z",
        source="auth-service",
        payload={"user_id": "u123"},
    )
    assert valid_event.topic == "auth.login"
    assert valid_event.event_id == "evt-001"
    assert valid_event.dedup_key == ("auth.login", "evt-001")

    # ── Valid event with empty payload ──
    valid_minimal = EventSchema(
        topic="test",
        event_id="e1",
        timestamp="2026-01-15T10:30:00+00:00",
        source="test-src",
        payload={},
    )
    assert valid_minimal.payload == {}

    # ── Missing required field: topic ──
    with pytest.raises(ValidationError) as exc_info:
        EventSchema(
            event_id="evt-001",
            timestamp="2026-01-15T10:30:00Z",
            source="auth-service",
            payload={},
        )
    assert "topic" in str(exc_info.value)

    # ── Missing required field: event_id ──
    with pytest.raises(ValidationError):
        EventSchema(
            topic="auth.login",
            timestamp="2026-01-15T10:30:00Z",
            source="auth-service",
            payload={},
        )

    # ── Invalid timestamp format ──
    with pytest.raises(ValidationError) as exc_info:
        EventSchema(
            topic="auth.login",
            event_id="evt-001",
            timestamp="not-a-timestamp",
            source="auth-service",
            payload={},
        )
    assert "timestamp" in str(exc_info.value).lower() or "ISO 8601" in str(exc_info.value)

    # ── Empty topic string (min_length=1) ──
    with pytest.raises(ValidationError):
        EventSchema(
            topic="",
            event_id="evt-001",
            timestamp="2026-01-15T10:30:00Z",
            source="auth-service",
            payload={},
        )

    # ── Empty event_id string (min_length=1) ──
    with pytest.raises(ValidationError):
        EventSchema(
            topic="auth.login",
            event_id="",
            timestamp="2026-01-15T10:30:00Z",
            source="auth-service",
            payload={},
        )

    # ── PublishRequest: single event normalization ──
    single_req = PublishRequest(events=valid_event)
    events_list = single_req.get_event_list()
    assert len(events_list) == 1
    assert events_list[0].event_id == "evt-001"

    # ── PublishRequest: batch normalization ──
    batch_req = PublishRequest(events=[valid_event, valid_minimal])
    events_list = batch_req.get_event_list()
    assert len(events_list) == 2
