"""
Publisher simulator for stress testing and demo.

Sends a configurable number of events to the aggregator with a controlled
duplicate rate. Simulates at-least-once delivery by intentionally re-sending
some events (duplicate injection).

Usage:
    python -m src.simulator

Environment variables:
    AGGREGATOR_URL  — Target aggregator URL (default: http://localhost:8080)
    TOTAL_EVENTS    — Total events to generate (default: 5000)
    DUPLICATE_RATE  — Fraction of events that are duplicates (default: 0.25)
    BATCH_SIZE      — Events per HTTP batch request (default: 50)
"""

from __future__ import annotations

import asyncio
import json
import logging
import os
import random
import sys
import time
from datetime import datetime, timezone
from urllib.request import Request, urlopen
from urllib.error import URLError

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s | %(levelname)-8s | SIMULATOR | %(message)s",
    datefmt="%Y-%m-%d %H:%M:%S",
    stream=sys.stdout,
)
logger = logging.getLogger(__name__)

# Configuration from environment
AGGREGATOR_URL = os.getenv("AGGREGATOR_URL", "http://localhost:8080")
TOTAL_EVENTS = int(os.getenv("TOTAL_EVENTS", "5000"))
DUPLICATE_RATE = float(os.getenv("DUPLICATE_RATE", "0.25"))
BATCH_SIZE = int(os.getenv("BATCH_SIZE", "50"))

# Available topics for realistic simulation
TOPICS = [
    "auth.login",
    "auth.logout",
    "payment.processed",
    "payment.failed",
    "user.signup",
    "user.profile_update",
    "order.created",
    "order.shipped",
    "inventory.low_stock",
    "system.health_check",
]

SOURCES = [
    "auth-service",
    "payment-gateway",
    "user-service",
    "order-service",
    "inventory-service",
    "monitoring-agent",
]


def wait_for_aggregator(url: str, max_retries: int = 30, delay: float = 2.0) -> bool:
    """Wait for the aggregator service to be healthy."""
    health_url = f"{url}/health"
    for attempt in range(1, max_retries + 1):
        try:
            req = Request(health_url)
            with urlopen(req, timeout=5) as resp:
                if resp.status == 200:
                    logger.info("Aggregator is healthy (attempt %d)", attempt)
                    return True
        except (URLError, OSError):
            logger.info(
                "Waiting for aggregator... (attempt %d/%d)",
                attempt,
                max_retries,
            )
            time.sleep(delay)
    return False


def generate_events() -> tuple[list[dict], int, int]:
    """
    Generate events with controlled duplicate injection.

    Returns:
        (all_events, unique_count, duplicate_count)
    """
    unique_count = int(TOTAL_EVENTS * (1 - DUPLICATE_RATE))
    duplicate_count = TOTAL_EVENTS - unique_count

    logger.info(
        "Generating %d total events: %d unique + %d duplicates (%.0f%% dup rate)",
        TOTAL_EVENTS,
        unique_count,
        duplicate_count,
        DUPLICATE_RATE * 100,
    )

    # Generate unique events
    unique_events = []
    for i in range(unique_count):
        topic = random.choice(TOPICS)
        event = {
            "topic": topic,
            "event_id": f"evt-{i:06d}",
            "timestamp": datetime.now(timezone.utc).isoformat(),
            "source": random.choice(SOURCES),
            "payload": {
                "action": topic.split(".")[-1],
                "index": i,
                "data": f"Simulated event data #{i}",
            },
        }
        unique_events.append(event)

    # Create duplicates by randomly selecting from unique events
    duplicates = random.choices(unique_events, k=duplicate_count)

    # Combine and shuffle
    all_events = unique_events + duplicates
    random.shuffle(all_events)

    return all_events, unique_count, duplicate_count


def send_batch(url: str, events: list[dict]) -> dict | None:
    """Send a batch of events to the aggregator via HTTP POST."""
    publish_url = f"{url}/publish"
    body = json.dumps({"events": events}).encode("utf-8")

    req = Request(
        publish_url,
        data=body,
        headers={"Content-Type": "application/json"},
        method="POST",
    )

    try:
        with urlopen(req, timeout=30) as resp:
            return json.loads(resp.read().decode("utf-8"))
    except (URLError, OSError) as e:
        logger.error("Failed to send batch: %s", e)
        return None


def fetch_stats(url: str) -> dict | None:
    """Fetch current stats from the aggregator."""
    try:
        req = Request(f"{url}/stats")
        with urlopen(req, timeout=10) as resp:
            return json.loads(resp.read().decode("utf-8"))
    except (URLError, OSError) as e:
        logger.error("Failed to fetch stats: %s", e)
        return None


def main() -> None:
    """Run the publisher simulator."""
    logger.info("=" * 60)
    logger.info("  Publisher Simulator")
    logger.info("  Target: %s", AGGREGATOR_URL)
    logger.info("  Events: %d (%.0f%% duplicates)", TOTAL_EVENTS, DUPLICATE_RATE * 100)
    logger.info("  Batch size: %d", BATCH_SIZE)
    logger.info("=" * 60)

    # Wait for aggregator to be ready
    if not wait_for_aggregator(AGGREGATOR_URL):
        logger.error("Aggregator not available after retries. Exiting.")
        sys.exit(1)

    # Get initial stats
    initial_stats = fetch_stats(AGGREGATOR_URL)
    if initial_stats:
        logger.info("Initial stats: %s", json.dumps(initial_stats, indent=2))

    # Generate events
    all_events, unique_count, duplicate_count = generate_events()

    # Send in batches
    start_time = time.monotonic()
    total_sent = 0
    batch_num = 0

    for i in range(0, len(all_events), BATCH_SIZE):
        batch = all_events[i : i + BATCH_SIZE]
        batch_num += 1

        result = send_batch(AGGREGATOR_URL, batch)
        if result:
            total_sent += len(batch)
            if batch_num % 10 == 0:
                logger.info(
                    "Progress: %d/%d events sent (batch %d)",
                    total_sent,
                    len(all_events),
                    batch_num,
                )

    elapsed = time.monotonic() - start_time
    throughput = total_sent / elapsed if elapsed > 0 else 0

    logger.info("=" * 60)
    logger.info("  Simulation Complete")
    logger.info("  Total sent: %d events in %.2fs", total_sent, elapsed)
    logger.info("  Throughput: %.0f events/sec", throughput)
    logger.info("  Expected unique: %d", unique_count)
    logger.info("  Expected duplicates: %d", duplicate_count)
    logger.info("=" * 60)

    # Wait a moment for consumer to process remaining events
    logger.info("Waiting 5s for consumer to finish processing...")
    time.sleep(5)

    # Fetch final stats
    final_stats = fetch_stats(AGGREGATOR_URL)
    if final_stats:
        logger.info("Final stats:")
        logger.info("  Received:          %d", final_stats["received"])
        logger.info("  Unique processed:  %d", final_stats["unique_processed"])
        logger.info("  Duplicates dropped: %d", final_stats["duplicate_dropped"])
        logger.info("  Topics:            %s", final_stats["topics"])
        logger.info("  Uptime:            %s", final_stats["uptime"])

        # Verify correctness
        if final_stats["unique_processed"] <= unique_count:
            logger.info("[OK] Deduplication working correctly!")
        else:
            logger.warning(
                "[WARN] More events processed than expected unique count"
            )

        if final_stats["duplicate_dropped"] >= duplicate_count:
            logger.info("[OK] All duplicates properly detected!")
        else:
            logger.info(
                "[INFO] %d/%d duplicates detected (some may still be processing)",
                final_stats["duplicate_dropped"],
                duplicate_count,
            )

    logger.info("Simulator finished.")


if __name__ == "__main__":
    main()
