"""
Test 10: Edge Cases
"""

from __future__ import annotations

import pytest
from httpx import AsyncClient


@pytest.mark.asyncio
async def test_edge_cases_empty_and_invalid(client: AsyncClient):
    """
    Test 10: Edge cases — invalid payloads, missing fields, empty topic.
    """
    # Missing required fields
    resp = await client.post("/publish", json={"events": {"topic": "test"}})
    assert resp.status_code == 422  # Validation error

    # Empty topic string
    resp = await client.post(
        "/publish",
        json={
            "events": {
                "topic": "",
                "event_id": "e1",
                "timestamp": "2026-01-15T10:30:00Z",
                "source": "test",
                "payload": {},
            }
        },
    )
    assert resp.status_code == 422

    # Invalid timestamp
    resp = await client.post(
        "/publish",
        json={
            "events": {
                "topic": "test",
                "event_id": "e1",
                "timestamp": "not-valid",
                "source": "test",
                "payload": {},
            }
        },
    )
    assert resp.status_code == 422

    # Completely empty body
    resp = await client.post("/publish", json={})
    assert resp.status_code == 422

    # Valid single event (should succeed)
    resp = await client.post(
        "/publish",
        json={
            "events": {
                "topic": "edge.test",
                "event_id": "edge-001",
                "timestamp": "2026-01-15T10:30:00Z",
                "source": "edge-tester",
                "payload": {},
            }
        },
    )
    assert resp.status_code == 202

    # Health endpoint
    resp = await client.get("/health")
    assert resp.status_code == 200
    assert resp.json()["status"] == "healthy"
