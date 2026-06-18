"""
libs.common — shared event schema, config, and structured logging.

Re-exports the public API so services can import directly from ``libs.common``.
"""

from libs.common.config import Settings, get_settings
from libs.common.logging import (
    bind_context,
    bind_correlation_id,
    bind_trace_id,
    configure_logging,
    get_logger,
    reset_context,
)
from libs.common.schema import (
    TOPIC_ALERTS,
    TOPIC_INSIGHTS,
    TOPIC_MARKET_RAW,
    TOPIC_NEWS_RAW,
    TOPIC_SIGNALS,
    Alert,
    EventBase,
    Insight,
    MarketEvent,
    NewsEvent,
    Signal,
    market_event_key,
)

__all__ = [
    # Schema models
    "EventBase",
    "MarketEvent",
    "NewsEvent",
    "Signal",
    "Insight",
    "Alert",
    # Topic constants
    "TOPIC_MARKET_RAW",
    "TOPIC_NEWS_RAW",
    "TOPIC_SIGNALS",
    "TOPIC_INSIGHTS",
    "TOPIC_ALERTS",
    # Idempotency helper
    "market_event_key",
    # Config
    "Settings",
    "get_settings",
    # Logging
    "configure_logging",
    "get_logger",
    "bind_correlation_id",
    "bind_trace_id",
    "bind_context",
    "reset_context",
]
