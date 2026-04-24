"""
Idempotent consumer service.

Reads events from the internal asyncio.Queue and processes them exactly once
by checking the SQLite dedup store before processing. This implements the
Idempotent Consumer pattern — even if the same event arrives multiple times
(at-least-once delivery), it is only processed once.

Architecture:
  Queue → Consumer → DedupStore check → Process or Skip
"""

from __future__ import annotations

import asyncio
import logging
from collections import defaultdict
from datetime import datetime, timezone
from typing import TYPE_CHECKING

if TYPE_CHECKING:
    from src.models.event import EventSchema
    from src.services.stats import StatsTracker
    from src.storage.dedup_store import DedupStore

logger = logging.getLogger(__name__)


class ConsumerService:
    """
    Idempotent event consumer that reads from the internal queue.

    For each event:
    1. Check DedupStore → if duplicate, skip and log
    2. If new → mark as processed, store in memory, update stats

    Maintains an in-memory store of processed events for fast GET /events queries.
    The DedupStore (SQLite) provides persistence across restarts.
    """

    def __init__(
        self,
        queue: asyncio.Queue,
        dedup_store: DedupStore,
        stats: StatsTracker,
    ) -> None:
        self._queue = queue
        self._dedup_store = dedup_store
        self._stats = stats
        self._running = False
        self._task: asyncio.Task | None = None

        # In-memory event store: topic -> list of processed event dicts
        # This provides fast lookups for GET /events without SQLite queries
        self._processed_events: dict[str, list[dict]] = defaultdict(list)

        # Counter for ordering within the same timestamp
        self._sequence_counter: int = 0

    async def start(self) -> None:
        """Start the consumer background task."""
        if self._running:
            logger.warning("Consumer is already running")
            return

        self._running = True

        # Restore in-memory state from SQLite on startup
        await self._restore_from_store()

        self._task = asyncio.create_task(self._consume_loop())
        logger.info("Consumer started — listening for events on queue")

    async def stop(self) -> None:
        """Gracefully stop the consumer background task."""
        self._running = False
        if self._task:
            # Allow pending items to drain (with timeout)
            try:
                await asyncio.wait_for(self._drain_remaining(), timeout=5.0)
            except asyncio.TimeoutError:
                logger.warning("Consumer drain timed out, some events may be lost")
            self._task.cancel()
            try:
                await self._task
            except asyncio.CancelledError:
                pass
            self._task = None
        logger.info("Consumer stopped")

    async def _consume_loop(self) -> None:
        """
        Main consumer loop — continuously reads from the queue.

        Uses a short timeout on queue.get() to allow graceful shutdown
        checks between iterations.
        """
        logger.info("Consumer loop started")
        while self._running:
            try:
                event: EventSchema = await asyncio.wait_for(
                    self._queue.get(), timeout=0.1
                )
                await self._process_event(event)
                self._queue.task_done()
            except asyncio.TimeoutError:
                # No events in queue — continue polling
                continue
            except asyncio.CancelledError:
                logger.info("Consumer loop cancelled")
                break
            except Exception:
                logger.exception("Unexpected error in consumer loop")
                await asyncio.sleep(0.1)

    async def _process_event(self, event: EventSchema) -> None:
        """
        Process a single event with idempotency check.

        This is the core of the Idempotent Consumer pattern:
        - Check if already processed → skip if duplicate
        - Otherwise → mark processed and store
        """
        topic, event_id = event.dedup_key

        # Check dedup store (persistent)
        is_dup = await self._dedup_store.is_duplicate(topic, event_id)

        if is_dup:
            self._stats.record_duplicate()
            logger.info(
                "DUPLICATE DROPPED: topic=%s, event_id=%s (already processed)",
                topic,
                event_id,
            )
            return

        # Mark as processed in persistent store
        await self._dedup_store.mark_processed(topic, event_id)

        # Store in memory for fast queries
        self._sequence_counter += 1
        processed_at = datetime.now(timezone.utc).isoformat()
        event_record = {
            "topic": event.topic,
            "event_id": event.event_id,
            "timestamp": event.timestamp,
            "source": event.source,
            "payload": event.payload,
            "processed_at": processed_at,
            "sequence": self._sequence_counter,
        }
        self._processed_events[event.topic].append(event_record)

        # Update stats
        self._stats.record_processed(event.topic)

        logger.info(
            "PROCESSED: topic=%s, event_id=%s, seq=%d",
            topic,
            event_id,
            self._sequence_counter,
        )

    async def _drain_remaining(self) -> None:
        """Process any remaining events in the queue before shutdown."""
        while not self._queue.empty():
            try:
                event = self._queue.get_nowait()
                await self._process_event(event)
                self._queue.task_done()
            except asyncio.QueueEmpty:
                break

    async def _restore_from_store(self) -> None:
        """
        Restore in-memory event tracking from SQLite on startup.

        This ensures the consumer knows about previously processed events
        even after a container restart. Note: full event payloads are not
        stored in SQLite (only dedup keys), so we create minimal records.
        """
        records = await self._dedup_store.get_processed_event_ids()
        restored_count = 0
        for topic, event_id, processed_at in records:
            self._processed_events[topic].append({
                "topic": topic,
                "event_id": event_id,
                "timestamp": processed_at,  # Use processed_at as fallback
                "source": "restored",
                "payload": {},
                "processed_at": processed_at,
                "sequence": restored_count,
            })
            restored_count += 1

        if restored_count > 0:
            logger.info(
                "Restored %d event records from persistent store",
                restored_count,
            )

    def get_events(self, topic: str | None = None) -> list[dict]:
        """
        Get processed events, optionally filtered by topic.

        Returns events sorted by processing sequence.
        """
        if topic:
            events = self._processed_events.get(topic, [])
        else:
            events = []
            for topic_events in self._processed_events.values():
                events.extend(topic_events)

        # Sort by sequence number for consistent ordering
        return sorted(events, key=lambda e: e.get("sequence", 0))

    @property
    def is_running(self) -> bool:
        return self._running
