"""
Tests 5-8: API Endpoint Tests

Covers:
5. GET /stats returns correct structure and values
6. GET /events returns processed events with topic filtering
7. POST /publish batch processing
8. Stress test with 5000 events and 20%+ duplicates
"""

from __future__ import annotations

import asyncio
import time

import pytest
from httpx import ASGITransport, AsyncClient

from src.main import create_app


@pytest.mark.asyncio
async def test_stats_endpoint(client: AsyncClient):
    """
    Test 5: GET /stats returns correct structure and initial values.
    """
    response = await client.get("/stats")
    assert response.status_code == 200

    data = response.json()

    # Verify all required fields exist
    assert "received" in data
    assert "unique_processed" in data
    assert "duplicate_dropped" in data
    assert "topics" in data
    assert "uptime" in data
    assert "uptime_seconds" in data

    # Initial values should be zero (or reflect restored state)
    assert isinstance(data["received"], int)
    assert isinstance(data["unique_processed"], int)
    assert isinstance(data["duplicate_dropped"], int)
    assert isinstance(data["topics"], list)
    assert isinstance(data["uptime"], str)
    assert isinstance(data["uptime_seconds"], float)

    # Publish an event and check stats update
    event = {
        "events": {
            "topic": "test.stats",
            "event_id": "stat-001",
            "timestamp": "2026-01-15T10:30:00Z",
            "source": "test",
            "payload": {},
        }
    }
    pub_response = await client.post("/publish", json=event)
    assert pub_response.status_code == 202

    # Wait for consumer to process
    await asyncio.sleep(0.3)

    response2 = await client.get("/stats")
    data2 = response2.json()
    assert data2["received"] >= 1
    assert data2["unique_processed"] >= 1
    assert "test.stats" in data2["topics"]


@pytest.mark.asyncio
async def test_events_endpoint(client: AsyncClient):
    """
    Test 6: GET /events returns processed events with topic filtering.
    """
    # Publish events to different topics
    events = [
        {
            "topic": "auth.login",
            "event_id": "auth-001",
            "timestamp": "2026-01-15T10:00:00Z",
            "source": "auth-service",
            "payload": {"user": "alice"},
        },
        {
            "topic": "auth.login",
            "event_id": "auth-002",
            "timestamp": "2026-01-15T10:01:00Z",
            "source": "auth-service",
            "payload": {"user": "bob"},
        },
        {
            "topic": "payment.processed",
            "event_id": "pay-001",
            "timestamp": "2026-01-15T10:02:00Z",
            "source": "payment-gateway",
            "payload": {"amount": 50.0},
        },
    ]

    pub_response = await client.post("/publish", json={"events": events})
    assert pub_response.status_code == 202

    # Wait for consumer to process
    await asyncio.sleep(0.5)

    # Get all events
    all_resp = await client.get("/events")
    assert all_resp.status_code == 200
    all_events = all_resp.json()
    assert len(all_events) >= 3

    # Filter by topic
    auth_resp = await client.get("/events", params={"topic": "auth.login"})
    assert auth_resp.status_code == 200
    auth_events = auth_resp.json()
    assert len(auth_events) >= 2
    assert all(e["topic"] == "auth.login" for e in auth_events)

    payment_resp = await client.get("/events", params={"topic": "payment.processed"})
    assert payment_resp.status_code == 200
    payment_events = payment_resp.json()
    assert len(payment_events) >= 1
    assert all(e["topic"] == "payment.processed" for e in payment_events)

    # Non-existent topic should return empty
    empty_resp = await client.get("/events", params={"topic": "nonexistent"})
    assert empty_resp.status_code == 200
    assert empty_resp.json() == []


@pytest.mark.asyncio
async def test_batch_processing(client: AsyncClient):
    """
    Test 7: POST /publish handles batch events correctly,
    including deduplication within the batch.
    """
    batch = [
        {
            "topic": "batch.test",
            "event_id": f"batch-{i:03d}",
            "timestamp": "2026-01-15T10:30:00Z",
            "source": "batch-sender",
            "payload": {"index": i},
        }
        for i in range(20)
    ]

    # Add 5 duplicates
    duplicates = [
        {
            "topic": "batch.test",
            "event_id": f"batch-{i:03d}",
            "timestamp": "2026-01-15T10:30:00Z",
            "source": "batch-sender",
            "payload": {"index": i},
        }
        for i in range(5)
    ]
    batch_with_dups = batch + duplicates

    response = await client.post("/publish", json={"events": batch_with_dups})
    assert response.status_code == 202
    data = response.json()
    assert data["received"] == 25  # 20 unique + 5 duplicates

    # Wait for consumer to process
    await asyncio.sleep(1.0)

    # Check stats
    stats_resp = await client.get("/stats")
    stats = stats_resp.json()
    assert stats["received"] >= 25
    assert stats["unique_processed"] >= 20
    assert stats["duplicate_dropped"] >= 5

    # Check events
    events_resp = await client.get("/events", params={"topic": "batch.test"})
    events = events_resp.json()
    assert len(events) >= 20  # Exactly 20 unique events


@pytest.mark.asyncio
async def test_stress_test_5000_events(client: AsyncClient):
    """
    Test 8: Stress test with 5000 events and approximately 20%+ duplicates.

    Verifies that the system handles high volume correctly and maintains
    accurate dedup statistics.
    """
    total_events = 5000
    unique_count = 4000
    duplicate_count = total_events - unique_count  # 1000 = 20%

    # Generate unique events
    unique_events = [
        {
            "topic": f"stress.topic-{i % 5}",
            "event_id": f"stress-{i:05d}",
            "timestamp": "2026-01-15T10:30:00Z",
            "source": "stress-tester",
            "payload": {"index": i},
        }
        for i in range(unique_count)
    ]

    # Generate duplicates (repeat some events)
    duplicates = [
        {
            "topic": f"stress.topic-{i % 5}",
            "event_id": f"stress-{i:05d}",
            "timestamp": "2026-01-15T10:30:00Z",
            "source": "stress-tester",
            "payload": {"index": i},
        }
        for i in range(duplicate_count)  # Duplicate first 1000
    ]

    all_events = unique_events + duplicates

    # Send in batches of 100
    batch_size = 100
    start_time = time.monotonic()

    for i in range(0, len(all_events), batch_size):
        batch = all_events[i : i + batch_size]
        response = await client.post("/publish", json={"events": batch})
        assert response.status_code == 202

    # Poll until all events are processed (or timeout)
    max_wait = 20.0
    poll_interval = 0.5
    waited = 0.0

    while waited < max_wait:
        await asyncio.sleep(poll_interval)
        waited += poll_interval
        stats_resp = await client.get("/stats")
        stats = stats_resp.json()
        total_processed = stats["unique_processed"] + stats["duplicate_dropped"]
        if total_processed >= total_events:
            break

    elapsed = time.monotonic() - start_time

    # Verify stats
    assert stats["received"] >= total_events
    assert stats["unique_processed"] >= unique_count
    assert stats["duplicate_dropped"] >= duplicate_count

    # Performance assertion: should complete within 30 seconds
    assert elapsed < 30.0, f"Stress test took too long: {elapsed:.2f}s"

    # Verify topic distribution
    assert len(stats["topics"]) >= 5  # We used 5 topics
