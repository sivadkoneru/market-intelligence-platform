# libs/common

Shared event schema, configuration, and structured logging for the market intelligence platform.
All five services (`ingestion`, `stream`, `ai-analysis`, `alerting`, `api`) import from this
package — no per-service duplicate event models.

> **Disclaimer:** This is a portfolio project only. No financial advice, no real trades.

---

## Contents

| Module | Purpose |
|---|---|
| `schema.py` | Pydantic v2 event models + topic constants + idempotency key helper |
| `config.py` | `pydantic-settings` `Settings` with offline-safe defaults and `get_settings()` |
| `logging.py` | structlog JSON logger with correlation/trace ID propagation via `contextvars` |

---

## Event Model Contract

Every model inherits `EventBase` which adds observability fields to all events.

### EventBase (mixin on all events)

| Field | Type | Default |
|---|---|---|
| `event_id` | `str` | `uuid4()` |
| `ts` | `datetime` (UTC, tz-aware) | `utcnow()` |
| `correlation_id` | `str \| None` | `None` |
| `trace_id` | `str \| None` | `None` |

### MarketEvent

| Field | Type | Notes |
|---|---|---|
| `symbol` | `str` | e.g. `"BTCUSDT"` |
| `source` | `str` | e.g. `"binance"` |
| `event_type` | `str` | `"trade"` or `"ticker"` |
| `price` | `float` | required |
| `volume` | `float \| None` | optional |
| `bid` | `float \| None` | optional |
| `ask` | `float \| None` | optional |

### NewsEvent

| Field | Type | Notes |
|---|---|---|
| `source` | `str` | feed name |
| `title` | `str` | required |
| `body` | `str` | required |
| `url` | `str \| None` | optional |
| `symbols` | `list[str]` | related tickers |
| `author` | `str \| None` | optional |

### Signal

| Field | Type | Notes |
|---|---|---|
| `symbol` | `str` | ticker |
| `source` | `str` | default `"stream"` |
| `indicators` | `dict[str, float \| None]` | keys: `sma`, `ema`, `rsi`, `volatility`, `trend`, `anomaly_score` |
| `anomaly` | `bool` | default `False` |

### Insight

| Field | Type | Notes |
|---|---|---|
| `symbol` | `str` | ticker |
| `sentiment_score` | `float` | |
| `sentiment_label` | `str` | e.g. `"positive"` |
| `summary` | `str` | |
| `explanation` | `str` | natural-language rationale |
| `citations` | `list[str]` | default `[]` |
| `confidence` | `float` | 0–1 |
| `grounded` | `bool` | guardrail flag |
| `model` | `str` | LLM model identifier |

### Alert

| Field | Type | Notes |
|---|---|---|
| `symbol` | `str` | ticker |
| `rule` | `str` | rule name |
| `severity` | `str` | e.g. `"high"` |
| `message` | `str` | human-readable description |
| `dedupe_key` | `str` | idempotency key |

---

## Topic Constants

```python
from libs.common import (
    TOPIC_MARKET_RAW,   # "market.raw"
    TOPIC_NEWS_RAW,     # "news.raw"
    TOPIC_SIGNALS,      # "signals"
    TOPIC_INSIGHTS,     # "insights"
    TOPIC_ALERTS,       # "alerts"
)
```

---

## Usage Examples

### Schema

```python
from libs.common import MarketEvent, market_event_key

ev = MarketEvent(symbol="BTCUSDT", source="binance", event_type="trade", price=60_000.0)
key = market_event_key(ev.symbol, ev.ts, ev.source)   # deterministic SHA-256 idempotency key

# JSON round-trip (tz-aware ts preserved)
raw = ev.model_dump_json()
ev2 = MarketEvent.model_validate_json(raw)
```

### Config

```python
from libs.common import get_settings

settings = get_settings()   # cached singleton; reads from env / .env file
print(settings.redis_url)   # "redis://localhost:6379/0" by default
print(settings.mock_llm)    # True by default (no LLM keys required)
```

### Logging

```python
from libs.common import configure_logging, get_logger, bind_correlation_id, bind_trace_id

configure_logging(level="INFO")   # JSON to stdout; idempotent
bind_correlation_id("req-abc")
bind_trace_id("trace-xyz")

log = get_logger(__name__)
log.info("ingestion.start", symbol="BTCUSDT")
# → {"event": "ingestion.start", "symbol": "BTCUSDT",
#    "correlation_id": "req-abc", "trace_id": "trace-xyz",
#    "level": "info", "logger": "...", "timestamp": "..."}
```

---

## Inputs / Outputs

- **Input:** Python constructor kwargs or JSON strings (via `model_validate_json`).
- **Output:** Pydantic model instances with tz-aware `datetime` fields; serialisable via
  `model_dump_json()` (ISO-8601 timestamps with UTC offset).

---

## Dependencies

All pinned in `/requirements-dev.txt`:

| Package | Version | Used for |
|---|---|---|
| `pydantic` | 2.9.2 | Event models |
| `pydantic-settings` | 2.6.1 | `Settings` / env loading |
| `structlog` | 24.4.0 | JSON logging |
| `orjson` | 3.10.12 | Fast JSON serialisation |
| `tenacity` | 9.0.0 | Retry policies |
| `redis` | 5.2.1 | RedisCache real client |
| `elasticsearch` | 8.17.0 | ElasticsearchStore real client |
| `azure-servicebus` | 7.12.3 | ServiceBusBus real client |
| `numpy` | 2.1.3 | Cosine-similarity kNN (optional; pure-Python fallback) |
| `python-dateutil` | 2.9.0.post0 | Timestamp parsing in InMemoryTimeSeriesStore |
| `httpx` | 0.28.1 | DruidClient HTTP calls |

No heavy framework dependencies (LangChain, OpenAI, etc.) are required for `task test` —
those are import-guarded in service packages.

---

## Infra Clients (T3)

Each external dependency is accessed through a `typing.Protocol` defined here.
Every port ships: **(a)** a real client, **(b)** an in-memory fake.
Factories select the fake when the env var is absent/default (fully offline).

### Modules

| Module | Port class | Fake | Real client | Factory |
|---|---|---|---|---|
| `resilience.py` | — | — | — | `retry_async()`, `with_retry()`, `CircuitBreaker` |
| `bus.py` | `MessageBus` | `InMemoryBus` | `ServiceBusBus` | `get_message_bus()` |
| `redis_client.py` | `Cache` | `InMemoryCache` | `RedisCache` | `get_cache()` |
| `druid.py` | `TimeSeriesStore` | `InMemoryTimeSeriesStore` | `DruidClient` | `get_timeseries_store()` |
| `es.py` | `SearchStore` | `InMemorySearchStore` | `ElasticsearchStore` | `get_search_store()` |

### resilience.py

```python
from libs.common import retry_async, with_retry, CircuitBreaker, CircuitOpenError

# Function-level retry (up to 5 attempts, exponential back-off)
result = await retry_async(my_coro_fn, arg1, max_attempts=5, wait_min=1.0, wait_max=30.0)

# Decorator
@with_retry(max_attempts=3, wait_min=0.5, wait_max=10.0)
async def fetch():
    ...

# Circuit breaker
cb = CircuitBreaker(failure_threshold=5, reset_timeout=60.0)
try:
    result = await cb.call(my_coro_fn, *args)
except CircuitOpenError:
    # circuit is open — fail fast
    ...
```

### bus.py — Message Bus (Azure Service Bus)

```python
from libs.common import get_message_bus

bus = get_message_bus()  # InMemoryBus offline, ServiceBusBus with real conn string

await bus.publish("market.raw", {"symbol": "BTCUSDT", "price": 60000}, message_id="dedup-key")
msgs = await bus.receive("market.raw", "stream-sub")
for msg in msgs:
    process(msg.body)
    await bus.complete(msg)          # ack
    # or: await bus.dead_letter(msg, reason="parse error")

dlq = await bus.receive_dead_letter("market.raw", "stream-sub")
peeked = await bus.peek("market.raw", "stream-sub", n=5)
```

**Duplicate detection:** Publishing with the same `message_id` twice is idempotent — the
second publish is silently dropped in both `InMemoryBus` and `ServiceBusBus`.

### redis_client.py — Cache (Redis)

```python
from libs.common import get_cache

cache = get_cache()  # InMemoryCache offline, RedisCache with real REDIS_URL

await cache.set("my-key", {"val": 1}, ttl=300)
data = await cache.get("my-key")
await cache.set_snapshot("BTCUSDT", indicators_dict)
snap = await cache.get_snapshot("BTCUSDT")

# Idempotency
if not await cache.seen("event-id-xyz"):
    process_event()  # first time only
```

### druid.py — Time-Series Store (Apache Druid)

```python
from libs.common import get_timeseries_store

store = get_timeseries_store()  # InMemoryTimeSeriesStore offline

await store.ingest([{"symbol": "BTCUSDT", "ts": datetime.utcnow(), "price": 60000}])
latest = await store.latest("BTCUSDT")
rows = await store.history("BTCUSDT", frm=start, to=end)
n = await store.count()
results = await store.query_sql("SELECT COUNT(*) FROM ticks")
```

### es.py — Search / Vector Store (Elasticsearch)

```python
from libs.common import get_search_store

store = get_search_store()  # InMemorySearchStore offline

await store.index_document("articles", "doc1", {"title": "BTC pump"}, vector=[...])
hits = await store.knn_search("articles", query_vector=[...], k=5)  # cosine-ranked
await store.index_log("app-logs", {"level": "info", "msg": "started"})
results = await store.search("articles", {"query": {"match_all": {}}})
```

**kNN:** `InMemorySearchStore` implements real cosine-similarity ranking (numpy when
available, pure-Python fallback). `ElasticsearchStore` delegates to Elasticsearch `knn`
dense-vector queries.

---

## Factory Selection Logic

| Env var (or default in Settings) | Factory returns |
|---|---|
| `SERVICE_BUS_CONNECTION_STRING` has `SAS_KEY_VALUE_HERE` (default) | `InMemoryBus` |
| Real Azure SB connection string | `ServiceBusBus` |
| `REDIS_URL = redis://localhost:6379/0` (default) | `InMemoryCache` |
| Non-default `REDIS_URL` | `RedisCache` |
| `DRUID_URL = http://localhost:8888` (default) | `InMemoryTimeSeriesStore` |
| Non-default `DRUID_URL` | `DruidClient` |
| `ELASTICSEARCH_URL = http://localhost:9200` (default) | `InMemorySearchStore` |
| Non-default `ELASTICSEARCH_URL` | `ElasticsearchStore` |

Real clients are thin wrappers and should only be exercised by `@pytest.mark.integration`
tests that skip gracefully without live infra.
