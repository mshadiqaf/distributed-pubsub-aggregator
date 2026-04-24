"""
Publisher service.

Responsible for accepting validated events and pushing them onto the
internal asyncio.Queue for asynchronous consumer processing.

This simulates the publisher side of the Pub-Sub pattern using
an in-process queue as the communication channel.
"""

from __future__ import annotations

import asyncio
import logging
from typing import TYPE_CHECKING

if TYPE_CHECKING:
    from src.models.event import EventSchema
    from src.services.stats import StatsTracker

logger = logging.getLogger(__name__)


class PublisherService:
    """
    Publishes events to the internal message queue.

    Acts as the bridge between the API layer and the consumer.
    Events placed on the queue follow at-least-once semantics —
    the consumer is responsible for deduplication.
    """

    def __init__(self, queue: asyncio.Queue, stats: StatsTracker) -> None:
        self._queue = queue
        self._stats = stats

    async def publish(self, event: EventSchema) -> bool:
        """
        Publish a single event to the internal queue.

        Returns True if the event was successfully enqueued.
        """
        await self._queue.put(event)
        self._stats.record_received()
        logger.info(
            "Published event: topic=%s, event_id=%s, queue_size=%d",
            event.topic,
            event.event_id,
            self._queue.qsize(),
        )
        return True

    async def publish_batch(self, events: list[EventSchema]) -> dict:
        """
        Publish a batch of events to the internal queue.

        Returns a summary dict with the count of events received.
        """
        for event in events:
            await self._queue.put(event)

        count = len(events)
        self._stats.record_received(count)
        logger.info(
            "Published batch of %d events, queue_size=%d",
            count,
            self._queue.qsize(),
        )
        return {
            "received": count,
            "message": f"Batch of {count} events accepted",
        }
