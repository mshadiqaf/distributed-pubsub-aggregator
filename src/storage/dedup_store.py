"""
SQLite-based deduplication store.

Provides persistent storage for tracking processed events using a composite
key of (topic, event_id). This ensures idempotency survives container restarts.

Design decisions:
- WAL journal mode for concurrent read performance
- Async operations via aiosqlite to avoid blocking the event loop
- Atomic writes with proper transaction handling
"""

from __future__ import annotations

import logging
import os
from datetime import datetime, timezone

import aiosqlite

logger = logging.getLogger(__name__)


class DedupStore:
    """
    Persistent deduplication store backed by SQLite.

    Tracks which (topic, event_id) combinations have been processed
    to prevent duplicate processing (idempotent consumer pattern).
    """

    def __init__(self, db_path: str) -> None:
        self._db_path = db_path
        self._db: aiosqlite.Connection | None = None

    async def initialize(self) -> None:
        """
        Initialize the database connection and create schema.

        Creates the data directory if it doesn't exist, opens the SQLite
        connection with WAL mode, and ensures the schema is ready.
        """
        # Ensure the directory exists
        db_dir = os.path.dirname(self._db_path)
        if db_dir:
            os.makedirs(db_dir, exist_ok=True)

        self._db = await aiosqlite.connect(self._db_path)

        # Enable WAL mode for better concurrent read performance
        await self._db.execute("PRAGMA journal_mode=WAL")
        # Ensure writes are durable (important for crash tolerance)
        await self._db.execute("PRAGMA synchronous=NORMAL")

        await self._db.execute(
            """
            CREATE TABLE IF NOT EXISTS processed_events (
                topic       TEXT    NOT NULL,
                event_id    TEXT    NOT NULL,
                processed_at TEXT   NOT NULL,
                PRIMARY KEY (topic, event_id)
            )
            """
        )
        # Index for topic-based queries (GET /events?topic=...)
        await self._db.execute(
            """
            CREATE INDEX IF NOT EXISTS idx_processed_events_topic
            ON processed_events (topic)
            """
        )
        await self._db.commit()

        count = await self.get_processed_count()
        logger.info(
            "DedupStore initialized at '%s' with %d existing records",
            self._db_path,
            count,
        )

    async def is_duplicate(self, topic: str, event_id: str) -> bool:
        """
        Check if an event with the given (topic, event_id) has been processed.

        Returns True if the event is a duplicate, False otherwise.
        """
        assert self._db is not None, "DedupStore not initialized"

        cursor = await self._db.execute(
            "SELECT 1 FROM processed_events WHERE topic = ? AND event_id = ?",
            (topic, event_id),
        )
        row = await cursor.fetchone()
        return row is not None

    async def mark_processed(self, topic: str, event_id: str) -> bool:
        """
        Mark an event as processed in the dedup store.

        Uses INSERT OR IGNORE for atomic, idempotent inserts.
        Returns True if the event was newly inserted, False if it already existed.
        """
        assert self._db is not None, "DedupStore not initialized"

        processed_at = datetime.now(timezone.utc).isoformat()
        cursor = await self._db.execute(
            """
            INSERT OR IGNORE INTO processed_events (topic, event_id, processed_at)
            VALUES (?, ?, ?)
            """,
            (topic, event_id, processed_at),
        )
        await self._db.commit()

        was_new = cursor.rowcount > 0
        if was_new:
            logger.debug("Marked event as processed: topic=%s, event_id=%s", topic, event_id)
        return was_new

    async def get_processed_count(self) -> int:
        """Return the total number of uniquely processed events."""
        assert self._db is not None, "DedupStore not initialized"

        cursor = await self._db.execute("SELECT COUNT(*) FROM processed_events")
        row = await cursor.fetchone()
        return row[0] if row else 0

    async def get_processed_event_ids(self, topic: str | None = None) -> list[tuple[str, str, str]]:
        """
        Get all processed event records, optionally filtered by topic.

        Returns list of (topic, event_id, processed_at) tuples.
        """
        assert self._db is not None, "DedupStore not initialized"

        if topic:
            cursor = await self._db.execute(
                "SELECT topic, event_id, processed_at FROM processed_events WHERE topic = ? ORDER BY processed_at",
                (topic,),
            )
        else:
            cursor = await self._db.execute(
                "SELECT topic, event_id, processed_at FROM processed_events ORDER BY processed_at"
            )
        return await cursor.fetchall()

    async def close(self) -> None:
        """Close the database connection gracefully."""
        if self._db:
            await self._db.close()
            self._db = None
            logger.info("DedupStore connection closed")
