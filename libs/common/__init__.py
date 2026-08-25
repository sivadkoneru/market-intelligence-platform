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
    ServiceMetrics,
    WorkerMetrics,
    bind_context,
    bind_correlation_id,
    bind_trace_id,
    close_log_sink,
    configure_logging,
    configure_new_relic,
    create_observability_middleware,
    get_logger,
    install_observability,
    render_counters,
    reset_context,
)
from libs.common.redis_client import (
    IDEMPOTENCY_TTL_SECONDS,
    Cache,
    InMemoryCache,
    RedisCache,
    get_cache,
)
from libs.common.resilience import (
    CircuitBreaker,
    CircuitOpenError,
    CircuitState,
    close_backends,
    dead_letter_message,
    retry_async,
    run_poll_loop,
    with_retry,
)
from libs.common.schema import (
    INSIGHT_CACHE_PREFIX,
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
    validation_reason,
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
    "INSIGHT_CACHE_PREFIX",
    # Idempotency helper
    "market_event_key",
    # Dead-letter reason formatting
    "validation_reason",
    # Config
    "Settings",
    "get_settings",
    # Logging
    "close_log_sink",
    "configure_logging",
    "configure_new_relic",
    "get_logger",
    "HTTPMetrics",
    "ServiceMetrics",
    "WorkerMetrics",
    "render_counters",
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
    "run_poll_loop",
    "dead_letter_message",
    "close_backends",
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
    "IDEMPOTENCY_TTL_SECONDS",
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
