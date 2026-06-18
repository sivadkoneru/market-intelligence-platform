"""
libs.common — shared event schema, config, structured logging, and infra clients.

Re-exports the public API so services can import directly from ``libs.common``.
"""

from libs.common.bus import (
    InMemoryBus,
    MessageBus,
    ReceivedMessage,
    ServiceBusBus,
    get_message_bus,
)
from libs.common.config import Settings, get_settings
from libs.common.druid import (
    DruidClient,
    InMemoryTimeSeriesStore,
    TimeSeriesStore,
    get_timeseries_store,
)
from libs.common.es import (
    ElasticsearchStore,
    InMemorySearchStore,
    SearchStore,
    get_search_store,
)
from libs.common.logging import (
    HTTPMetrics,
    bind_context,
    bind_correlation_id,
    bind_trace_id,
    configure_logging,
    configure_new_relic,
    create_observability_middleware,
    get_logger,
    install_observability,
    reset_context,
)
from libs.common.redis_client import (
    Cache,
    InMemoryCache,
    RedisCache,
    get_cache,
)
from libs.common.resilience import (
    CircuitBreaker,
    CircuitOpenError,
    CircuitState,
    retry_async,
    with_retry,
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
    "configure_new_relic",
    "get_logger",
    "HTTPMetrics",
    "bind_correlation_id",
    "bind_trace_id",
    "bind_context",
    "create_observability_middleware",
    "install_observability",
    "reset_context",
    # Resilience
    "CircuitBreaker",
    "CircuitOpenError",
    "CircuitState",
    "retry_async",
    "with_retry",
    # Message bus (Service Bus port)
    "MessageBus",
    "InMemoryBus",
    "ServiceBusBus",
    "ReceivedMessage",
    "get_message_bus",
    # Cache (Redis port)
    "Cache",
    "InMemoryCache",
    "RedisCache",
    "get_cache",
    # Time-series store (Druid port)
    "TimeSeriesStore",
    "InMemoryTimeSeriesStore",
    "DruidClient",
    "get_timeseries_store",
    # Search / vector store (Elasticsearch port)
    "SearchStore",
    "InMemorySearchStore",
    "ElasticsearchStore",
    "get_search_store",
]
