"""Market and news ingestion service."""

from services.ingestion.app import app, build_default_service, create_app
from services.ingestion.normalizer import normalize_market_payload
from services.ingestion.replay import (
    DeterministicReplayFeed,
    ReplayDisconnectError,
    build_default_replay_events,
    build_replay_feed_factory,
)
from services.ingestion.service import IngestionMetrics, IngestionService

__all__ = [
    "create_app",
    "build_default_service",
    "app",
    "normalize_market_payload",
    "DeterministicReplayFeed",
    "ReplayDisconnectError",
    "build_replay_feed_factory",
    "build_default_replay_events",
    "IngestionMetrics",
    "IngestionService",
]
