"""
FastAPI route handlers.

Defines the REST API endpoints for the Pub-Sub Log Aggregator:
- POST /publish  — Accept and enqueue events (single or batch)
- GET  /events   — Retrieve processed events (optionally by topic)
- GET  /stats    — System statistics and health metrics
- GET  /health   — Simple health check endpoint
"""

from __future__ import annotations

import logging
from typing import TYPE_CHECKING

from fastapi import APIRouter, HTTPException, Query

from src.models.event import (
    EventResponse,
    EventSchema,
    PublishRequest,
    PublishResponse,
    StatsResponse,
)

if TYPE_CHECKING:
    from src.services.consumer import ConsumerService
    from src.services.publisher import PublisherService
    from src.services.stats import StatsTracker

logger = logging.getLogger(__name__)

router = APIRouter()

# Service references — injected during app startup
_publisher: PublisherService | None = None
_consumer: ConsumerService | None = None
_stats: StatsTracker | None = None


def configure_routes(
    publisher: PublisherService,
    consumer: ConsumerService,
    stats: StatsTracker,
) -> None:
    """
    Inject service dependencies into the route module.

    Called during application startup to wire services into route handlers.
    """
    global _publisher, _consumer, _stats
    _publisher = publisher
    _consumer = consumer
    _stats = stats
    logger.info("API routes configured with service dependencies")


@router.post(
    "/publish",
    response_model=PublishResponse,
    status_code=202,
    summary="Publish events",
    description="Accept a single event or batch of events for processing.",
)
async def publish_events(request: PublishRequest) -> PublishResponse:
    """
    Publish one or more events to the aggregator.

    Events are validated, enqueued on the internal queue, and processed
    asynchronously by the consumer. Duplicate detection happens at the
    consumer level — all events are accepted here (at-least-once semantics).
    """
    assert _publisher is not None, "Publisher service not initialized"

    events = request.get_event_list()
    count = len(events)

    if count == 0:
        raise HTTPException(status_code=400, detail="No events provided")

    if count == 1:
        await _publisher.publish(events[0])
        message = f"Event accepted: topic={events[0].topic}, event_id={events[0].event_id}"
    else:
        await _publisher.publish_batch(events)
        message = f"Batch of {count} events accepted"

    logger.info("API /publish: %s", message)

    return PublishResponse(
        status="accepted",
        received=count,
        message=message,
    )


@router.get(
    "/events",
    response_model=list[EventResponse],
    summary="Get processed events",
    description="Retrieve uniquely processed events, optionally filtered by topic.",
)
async def get_events(
    topic: str | None = Query(
        default=None,
        description="Filter events by topic name",
        examples=["auth", "payment"],
    ),
) -> list[EventResponse]:
    """
    Return all uniquely processed events.

    Only events that passed deduplication are returned.
    Optional topic filter narrows results to a specific topic.
    """
    assert _consumer is not None, "Consumer service not initialized"

    events = _consumer.get_events(topic=topic)

    return [
        EventResponse(
            topic=e["topic"],
            event_id=e["event_id"],
            timestamp=e["timestamp"],
            source=e["source"],
            payload=e["payload"],
            processed_at=e["processed_at"],
        )
        for e in events
    ]


@router.get(
    "/stats",
    response_model=StatsResponse,
    summary="System statistics",
    description="Return aggregated system metrics including event counts and uptime.",
)
async def get_stats() -> StatsResponse:
    """
    Return current system statistics.

    Includes:
    - received: total events received via /publish
    - unique_processed: events that passed dedup and were processed
    - duplicate_dropped: events detected as duplicates and skipped
    - topics: list of active topic names
    - uptime: human-readable uptime string
    """
    assert _stats is not None, "Stats service not initialized"

    stats_data = _stats.get_stats()
    return StatsResponse(**stats_data)


@router.get(
    "/health",
    summary="Health check",
    description="Simple health check endpoint.",
)
async def health_check() -> dict:
    """Return basic health status."""
    return {
        "status": "healthy",
        "service": "pub-sub-log-aggregator",
    }
