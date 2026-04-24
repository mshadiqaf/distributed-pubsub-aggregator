"""
Shared test fixtures for the Pub-Sub Log Aggregator test suite.

Provides reusable fixtures for:
- DedupStore instances with temporary databases
- FastAPI test clients with full app lifecycle
- Sample event data (single and batch)
"""

from __future__ import annotations

import asyncio
import os
import tempfile
from typing import AsyncGenerator, Generator

import pytest
import pytest_asyncio
from httpx import ASGITransport, AsyncClient

from src.api.routes import configure_routes
from src.main import create_app
from src.models.event import EventSchema
from src.services.consumer import ConsumerService
from src.services.publisher import PublisherService
from src.services.stats import StatsTracker
from src.storage.dedup_store import DedupStore


@pytest.fixture
def sample_event_dict() -> dict:
    """A valid event dictionary for testing."""
    return {
        "topic": "auth.login",
        "event_id": "evt-001",
        "timestamp": "2026-01-15T10:30:00Z",
        "source": "auth-service",
        "payload": {"user_id": "u123", "action": "login"},
    }


@pytest.fixture
def sample_event(sample_event_dict: dict) -> EventSchema:
    """A valid EventSchema instance for testing."""
    return EventSchema(**sample_event_dict)


@pytest.fixture
def sample_batch_with_duplicates() -> list[dict]:
    """A batch of events that includes duplicates for dedup testing."""
    events = [
        {
            "topic": "auth.login",
            "event_id": f"evt-{i:03d}",
            "timestamp": "2026-01-15T10:30:00Z",
            "source": "auth-service",
            "payload": {"index": i},
        }
        for i in range(10)
    ]
    # Add duplicates (events 0, 1, 2 are duplicated)
    duplicates = [
        {
            "topic": "auth.login",
            "event_id": "evt-000",
            "timestamp": "2026-01-15T10:30:00Z",
            "source": "auth-service",
            "payload": {"index": 0},
        },
        {
            "topic": "auth.login",
            "event_id": "evt-001",
            "timestamp": "2026-01-15T10:30:00Z",
            "source": "auth-service",
            "payload": {"index": 1},
        },
        {
            "topic": "auth.login",
            "event_id": "evt-002",
            "timestamp": "2026-01-15T10:30:00Z",
            "source": "auth-service",
            "payload": {"index": 2},
        },
    ]
    return events + duplicates


@pytest_asyncio.fixture
async def dedup_store(tmp_path) -> AsyncGenerator[DedupStore, None]:
    """Create a fresh DedupStore with a temporary SQLite database."""
    db_path = str(tmp_path / "test_dedup.db")
    store = DedupStore(db_path)
    await store.initialize()
    yield store
    await store.close()


@pytest_asyncio.fixture
async def app_services(tmp_path) -> AsyncGenerator[dict, None]:
    """Create a full set of services for integration testing."""
    db_path = str(tmp_path / "test_dedup.db")

    dedup_store = DedupStore(db_path)
    await dedup_store.initialize()

    queue: asyncio.Queue = asyncio.Queue(maxsize=10000)
    stats = StatsTracker()
    stats.set_start_time()

    publisher = PublisherService(queue=queue, stats=stats)
    consumer = ConsumerService(queue=queue, dedup_store=dedup_store, stats=stats)
    await consumer.start()

    yield {
        "dedup_store": dedup_store,
        "queue": queue,
        "stats": stats,
        "publisher": publisher,
        "consumer": consumer,
    }

    await consumer.stop()
    await dedup_store.close()


@pytest_asyncio.fixture
async def client(tmp_path) -> AsyncGenerator[AsyncClient, None]:
    """
    Create an async HTTP test client with a fully wired FastAPI app.

    Uses a temporary SQLite database so tests are isolated.
    """
    # Override the SQLite path for testing
    os.environ["SQLITE_DB_PATH"] = str(tmp_path / "test_dedup.db")

    app = create_app()

    transport = ASGITransport(app=app)
    async with AsyncClient(transport=transport, base_url="http://testserver") as ac:
        # Trigger the lifespan startup
        async with app.router.lifespan_context(app):
            yield ac


@pytest.fixture
def anyio_backend():
    return "asyncio"
