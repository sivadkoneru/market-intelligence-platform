"""Market and news ingestion service."""

from services.ingestion.app import app, build_default_service, create_app
from services.ingestion.exchanges import (
    BINANCE_WS_URL,
    COINBASE_WS_URL,
    BinanceWebSocketClient,
    CoinbaseWebSocketClient,
    ExchangeStreamClosed,
    ExchangeStreamStale,
    ExchangeStreamState,
    ExchangeWebSocketClient,
)
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
    "ExchangeStreamState",
    "ExchangeStreamClosed",
    "ExchangeStreamStale",
    "ExchangeWebSocketClient",
    "BINANCE_WS_URL",
    "BinanceWebSocketClient",
    "COINBASE_WS_URL",
    "CoinbaseWebSocketClient",
    "normalize_market_payload",
    "DeterministicReplayFeed",
    "ReplayDisconnectError",
    "build_replay_feed_factory",
    "build_default_replay_events",
    "IngestionMetrics",
    "IngestionService",
]
