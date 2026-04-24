"""
Statistics tracking service.

Provides real-time system metrics including event counts, duplicate rates,
active topics, and uptime. Thread-safe via asyncio's single-threaded model.
"""

from __future__ import annotations

import logging
import time

logger = logging.getLogger(__name__)


class StatsTracker:
    """
    Tracks system-wide statistics for the Pub-Sub aggregator.

    All counters are maintained in-memory for fast access.
    Safe in asyncio's single-threaded event loop (no threading issues).
    """

    def __init__(self) -> None:
        self._received: int = 0
        self._unique_processed: int = 0
        self._duplicate_dropped: int = 0
        self._topics: set[str] = set()
        self._start_time: float = time.monotonic()
        self._start_datetime: str = ""

    def set_start_time(self) -> None:
        """Record the application start time."""
        self._start_time = time.monotonic()
        from datetime import datetime, timezone

        self._start_datetime = datetime.now(timezone.utc).isoformat()

    def record_received(self, count: int = 1) -> None:
        """Increment the received event counter."""
        self._received += count

    def record_processed(self, topic: str) -> None:
        """Record a successfully processed (unique) event."""
        self._unique_processed += 1
        self._topics.add(topic)
        logger.debug(
            "Stats: unique_processed=%d, topics=%d",
            self._unique_processed,
            len(self._topics),
        )

    def record_duplicate(self) -> None:
        """Record a detected duplicate event."""
        self._duplicate_dropped += 1

    @property
    def received(self) -> int:
        return self._received

    @property
    def unique_processed(self) -> int:
        return self._unique_processed

    @property
    def duplicate_dropped(self) -> int:
        return self._duplicate_dropped

    def get_uptime_seconds(self) -> float:
        """Calculate uptime in seconds."""
        return time.monotonic() - self._start_time

    def _format_uptime(self, seconds: float) -> str:
        """Format uptime as a human-readable string."""
        hours, remainder = divmod(int(seconds), 3600)
        minutes, secs = divmod(remainder, 60)
        return f"{hours}h {minutes}m {secs}s"

    def get_stats(self) -> dict:
        """
        Return a snapshot of current system statistics.

        Returns a dict matching the StatsResponse schema.
        """
        uptime_seconds = self.get_uptime_seconds()
        return {
            "received": self._received,
            "unique_processed": self._unique_processed,
            "duplicate_dropped": self._duplicate_dropped,
            "topics": sorted(self._topics),
            "uptime": self._format_uptime(uptime_seconds),
            "uptime_seconds": round(uptime_seconds, 2),
        }
