"""
Tests 1-3: Deduplication Store Tests

Covers:
1. Dedup marks and detects duplicates correctly
2. Duplicate events are not reprocessed through the full pipeline
3. Dedup persistence survives store restart (simulated crash recovery)
"""

from __future__ import annotations

import asyncio

import pytest
import pytest_asyncio

from src.models.event import EventSchema
from src.services.consumer import ConsumerService
from src.services.stats import StatsTracker
from src.storage.dedup_store import DedupStore


@pytest.mark.asyncio
async def test_dedup_marks_and_detects_duplicate(dedup_store: DedupStore):
    """
    Test 1: DedupStore correctly marks events as processed and
    detects them as duplicates on subsequent checks.
    """
    topic = "auth.login"
    event_id = "evt-001"

    # Initially should not be a duplicate
    assert await dedup_store.is_duplicate(topic, event_id) is False

    # Mark as processed
    was_new = await dedup_store.mark_processed(topic, event_id)
    assert was_new is True

    # Now should be detected as duplicate
    assert await dedup_store.is_duplicate(topic, event_id) is True

    # Marking again should return False (already exists)
    was_new_again = await dedup_store.mark_processed(topic, event_id)
    assert was_new_again is False

    # Count should be 1 (not 2)
    count = await dedup_store.get_processed_count()
    assert count == 1


@pytest.mark.asyncio
async def test_duplicate_event_not_reprocessed(app_services: dict):
    """
    Test 2: When the same event is published multiple times through
    the full pipeline, it should only be processed once.
    """
    publisher = app_services["publisher"]
    consumer = app_services["consumer"]
    stats = app_services["stats"]

    event = EventSchema(
        topic="payment.processed",
        event_id="pay-001",
        timestamp="2026-01-15T10:30:00Z",
        source="payment-gateway",
        payload={"amount": 99.99},
    )

    # Publish the same event 5 times (simulating at-least-once delivery)
    for _ in range(5):
        await publisher.publish(event)

    # Give the consumer time to process
    await asyncio.sleep(0.5)

    # Stats should show 5 received but only 1 unique processed
    assert stats.received == 5
    assert stats.unique_processed == 1
    assert stats.duplicate_dropped == 4

    # Only 1 event should be in the processed list
    events = consumer.get_events(topic="payment.processed")
    assert len(events) == 1
    assert events[0]["event_id"] == "pay-001"


@pytest.mark.asyncio
async def test_dedup_persistence_after_restart(tmp_path):
    """
    Test 3: After closing and reopening the DedupStore (simulating
    a container restart), previously processed events are still
    detected as duplicates.
    """
    db_path = str(tmp_path / "persist_test.db")

    # Session 1: Create store and mark events
    store1 = DedupStore(db_path)
    await store1.initialize()

    await store1.mark_processed("auth.login", "evt-001")
    await store1.mark_processed("auth.login", "evt-002")
    await store1.mark_processed("payment.processed", "pay-001")

    count_before = await store1.get_processed_count()
    assert count_before == 3

    # Close the store (simulate crash/restart)
    await store1.close()

    # Session 2: Reopen and verify persistence
    store2 = DedupStore(db_path)
    await store2.initialize()

    # All previously processed events should still be detected
    assert await store2.is_duplicate("auth.login", "evt-001") is True
    assert await store2.is_duplicate("auth.login", "evt-002") is True
    assert await store2.is_duplicate("payment.processed", "pay-001") is True

    # New events should not be duplicates
    assert await store2.is_duplicate("auth.login", "evt-999") is False

    count_after = await store2.get_processed_count()
    assert count_after == 3

    await store2.close()
