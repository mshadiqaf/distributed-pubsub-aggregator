"""
Main application entry point.

Assembles all components of the Pub-Sub Log Aggregator:
1. Initializes configuration and logging
2. Creates the FastAPI application with lifecycle management
3. Wires up services: DedupStore, StatsTracker, PublisherService, ConsumerService
4. Starts the consumer background task on startup
5. Gracefully shuts down on SIGTERM/SIGINT

Architecture:
  ┌──────────┐     ┌──────────────┐     ┌──────────────┐     ┌────────────┐
  │  FastAPI  │────▶│  Publisher   │────▶│ asyncio.Queue│────▶│  Consumer  │
  │  (API)   │     │  Service     │     │  (in-memory) │     │  Service   │
  └──────────┘     └──────────────┘     └──────────────┘     └─────┬──────┘
                                                                    │
                                                              ┌─────▼──────┐
                                                              │ DedupStore │
                                                              │  (SQLite)  │
                                                              └────────────┘
"""

from __future__ import annotations

import asyncio
import logging
import sys
from contextlib import asynccontextmanager

import uvicorn
from fastapi import FastAPI

from src.api.routes import configure_routes, router
from src.core.config import get_settings
from src.services.consumer import ConsumerService
from src.services.publisher import PublisherService
from src.services.stats import StatsTracker
from src.storage.dedup_store import DedupStore

# Module-level references for cross-component access
_dedup_store: DedupStore | None = None
_consumer: ConsumerService | None = None


def setup_logging(level: str) -> None:
    """Configure structured logging for the application."""
    logging.basicConfig(
        level=getattr(logging, level.upper(), logging.INFO),
        format="%(asctime)s | %(levelname)-8s | %(name)-30s | %(message)s",
        datefmt="%Y-%m-%d %H:%M:%S",
        stream=sys.stdout,
    )
    # Reduce noise from third-party libraries
    logging.getLogger("uvicorn.access").setLevel(logging.WARNING)
    logging.getLogger("aiosqlite").setLevel(logging.WARNING)


@asynccontextmanager
async def lifespan(app: FastAPI):
    """
    Application lifecycle manager.

    Startup:
    - Initialize SQLite dedup store
    - Create asyncio.Queue for internal pub-sub
    - Wire up Publisher, Consumer, and Stats services
    - Start consumer background task

    Shutdown:
    - Stop consumer gracefully
    - Close SQLite connection
    """
    global _dedup_store, _consumer

    settings = get_settings()
    logger = logging.getLogger(__name__)

    logger.info("=" * 60)
    logger.info("  %s v%s — Starting up", settings.APP_NAME, settings.APP_VERSION)
    logger.info("=" * 60)

    # 1. Initialize dedup store (SQLite)
    _dedup_store = DedupStore(settings.SQLITE_DB_PATH)
    await _dedup_store.initialize()

    # 2. Create internal message queue
    queue: asyncio.Queue = asyncio.Queue(maxsize=settings.QUEUE_MAX_SIZE)

    # 3. Create services
    stats = StatsTracker()
    stats.set_start_time()

    publisher = PublisherService(queue=queue, stats=stats)
    _consumer = ConsumerService(queue=queue, dedup_store=_dedup_store, stats=stats)

    # 4. Configure API routes with service dependencies
    configure_routes(publisher=publisher, consumer=_consumer, stats=stats)

    # 5. Start consumer background task
    await _consumer.start()

    logger.info("All services initialized — ready to accept requests")
    logger.info("Listening on http://%s:%d", settings.HOST, settings.PORT)

    yield  # Application is running

    # --- Shutdown ---
    logger.info("Shutting down gracefully...")

    if _consumer:
        await _consumer.stop()

    if _dedup_store:
        await _dedup_store.close()

    logger.info("Shutdown complete")


def create_app() -> FastAPI:
    """Create and configure the FastAPI application."""
    settings = get_settings()

    app = FastAPI(
        title=settings.APP_NAME,
        version=settings.APP_VERSION,
        description=(
            "A Pub-Sub Log Aggregator with Idempotent Consumer and Deduplication. "
            "Receives events from publishers, processes them through an internal "
            "queue, and prevents duplicate processing using a persistent SQLite store."
        ),
        lifespan=lifespan,
    )

    app.include_router(router)

    return app


# Create the app instance
app = create_app()


if __name__ == "__main__":
    settings = get_settings()
    setup_logging(settings.LOG_LEVEL)

    uvicorn.run(
        "src.main:app",
        host=settings.HOST,
        port=settings.PORT,
        log_level=settings.LOG_LEVEL.lower(),
        reload=False,
    )
