"""
Test 9: Queue Processing Test
"""

from __future__ import annotations

import asyncio

import pytest

from src.models.event import EventSchema


@pytest.mark.asyncio
async def test_queue_processing(app_services: dict):
    """
    Test 9: Verify that events put on the queue are consumed
    and stored by the consumer background task.
    """
    publisher = app_services["publisher"]
    consumer = app_services["consumer"]
    stats = app_services["stats"]
    queue = app_services["queue"]

    assert consumer.is_running is True

    events = [
        EventSchema(
            topic="queue.test",
            event_id=f"q-{i:03d}",
            timestamp="2026-01-15T10:30:00Z",
            source="queue-tester",
            payload={"step": i},
        )
        for i in range(10)
    ]

    for event in events:
        await publisher.publish(event)

    await asyncio.sleep(0.5)

    assert stats.unique_processed == 10
    assert stats.received == 10
    assert stats.duplicate_dropped == 0

    processed = consumer.get_events(topic="queue.test")
    assert len(processed) == 10

    event_ids = {e["event_id"] for e in processed}
    expected_ids = {f"q-{i:03d}" for i in range(10)}
    assert event_ids == expected_ids
    assert queue.empty()
