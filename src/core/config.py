"""
Application configuration module.

Centralizes all configurable parameters with environment variable overrides.
Follows the 12-factor app methodology for configuration management.
"""

import os
from dataclasses import dataclass, field


@dataclass(frozen=True)
class Settings:
    """Immutable application settings loaded from environment variables."""

    # Application metadata
    APP_NAME: str = field(
        default_factory=lambda: os.getenv("APP_NAME", "Pub-Sub Log Aggregator")
    )
    APP_VERSION: str = field(
        default_factory=lambda: os.getenv("APP_VERSION", "1.0.0")
    )

    # Server configuration
    HOST: str = field(default_factory=lambda: os.getenv("HOST", "0.0.0.0"))
    PORT: int = field(default_factory=lambda: int(os.getenv("PORT", "8080")))

    # SQLite deduplication store
    SQLITE_DB_PATH: str = field(
        default_factory=lambda: os.getenv("SQLITE_DB_PATH", "data/dedup.db")
    )

    # Internal queue configuration
    QUEUE_MAX_SIZE: int = field(
        default_factory=lambda: int(os.getenv("QUEUE_MAX_SIZE", "10000"))
    )

    # Consumer configuration
    CONSUMER_BATCH_SIZE: int = field(
        default_factory=lambda: int(os.getenv("CONSUMER_BATCH_SIZE", "100"))
    )
    CONSUMER_POLL_INTERVAL: float = field(
        default_factory=lambda: float(os.getenv("CONSUMER_POLL_INTERVAL", "0.01"))
    )

    # Logging
    LOG_LEVEL: str = field(
        default_factory=lambda: os.getenv("LOG_LEVEL", "INFO")
    )


def get_settings() -> Settings:
    """Factory function to create a Settings instance."""
    return Settings()
