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
| `config.py` | `pydantic-settings` `Settings` with offline-safe defaults, `get_settings()`, `resolve_settings()`, `is_default()` |
| `logging.py` | structlog JSON logger, FastAPI request observability middleware, ES log sink, shared metric bases, optional New Relic bootstrap |
| `service_app.py` | Shared FastAPI bootstrap: logging/New Relic setup, background-worker lifespan, and the common `/`, `/health`, `/metrics` routes |

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
from fastapi import FastAPI

from libs.common import (
    configure_logging,
    configure_new_relic,
    get_logger,
    bind_correlation_id,
    bind_trace_id,
    install_observability,
)

configure_logging(level="INFO")   # JSON to stdout; idempotent
bind_correlation_id("req-abc")
bind_trace_id("trace-xyz")

log = get_logger(__name__)
log.info("ingestion.start", symbol="BTCUSDT")
# → {"event": "ingestion.start", "symbol": "BTCUSDT",
#    "correlation_id": "req-abc", "trace_id": "trace-xyz",
#    "level": "info", "logger": "...", "timestamp": "..."}

app = FastAPI()
install_observability(app, service_name="api", metrics=my_metrics)
configure_new_relic(settings, service_name="api")  # no-op without config/module
```

### Service metrics

Each service's metrics dataclass extends `ServiceMetrics` (HTTP counters plus the
`record_http_request` hook the middleware calls) or `WorkerMetrics` (adds
`last_error` for `run_poll_loop`). Counter lines come from `render_counters()`
so a metric costs one dict entry instead of two hand-written strings.

```python
from dataclasses import dataclass

from libs.common import WorkerMetrics, render_counters


@dataclass
class StreamMetrics(WorkerMetrics):
    messages_seen: int = 0
    dead_lettered: int = 0

    def render(self) -> str:
        lines = render_counters(
            "stream",
            {"messages_seen": self.messages_seen, "dead_lettered": self.dead_lettered},
        )
        lines.extend(self.http.render("stream"))
        return "\n".join(lines) + "\n"
```

### service_app.py — shared FastAPI bootstrap

All five services are built from one factory, so the observability contract and
the required disclaimer live in a single place.

```python
from libs.common.service_app import (
    bootstrap_service_logging,
    create_service_app,
    worker_lifespan,
)


def create_app(service=None, *, run_on_startup=True):
    bootstrap_service_logging("stream")          # structlog + New Relic
    resolved = service or build_default_service()

    return create_service_app(
        service_name="stream",
        title="Market Intelligence Stream Service",
        summary="Portfolio service for offline-safe market stream processing.",
        service=resolved,
        state_attr="stream_service",
        render_metrics=resolved.metrics.render,
        lifespan=worker_lifespan(
            resolved.run_forever if run_on_startup else None,
            task_name="stream-worker",
            state_attr="stream_task",
        ),
    )
```

`create_service_app` always registers `/`, `/health`, and `/metrics`, installs
the observability middleware, and appends "No financial advice. No real trades."
to the OpenAPI description — no service can ship without it. `worker_lifespan`
cancels and awaits the background task on shutdown so a worker cannot outlive
its app, and runs the optional `close` callback afterwards even when the worker
crashed. Pass `display_name` when the public name differs from the short name
used for logs and metric prefixes (`ai` publishes `ai-analysis`); pass `routes`
to advertise endpoints on `/`.

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
| `resilience.py` | — | — | — | `retry_async()`, `with_retry()`, `CircuitBreaker`, `run_poll_loop()`, `dead_letter_message()`, `close_backends()` |
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

**Consumer loop.** Every service worker (`stream`, `ai`, `alerting`) drives its
`poll_once` through the one shared loop rather than hand-rolling it. A consumer
that dies on the first Service Bus or Redis blip fails invisibly — the
background task ends while `/health` still reports `ok` — so the loop records
the failure on `metrics.last_error`, logs it, and retries. It returns only via
cancellation.

```python
from libs.common import run_poll_loop

async def run_forever(self, *, poll_interval_seconds=0.25, max_messages=10) -> None:
    await run_poll_loop(
        self.poll_once,
        service_name="stream",
        log=self._log,
        metrics=self.metrics,
        poll_interval_seconds=poll_interval_seconds,
        max_messages=max_messages,
    )
```

**Dead-lettering and shutdown.** `dead_letter_message()` performs the three
steps a consumer must not get partially right — count it, remember the reason,
hand it to the broker — and logs a `<service>.dead_lettered` event.
`close_backends()` closes each backend that exposes `close()`, logging rather
than raising, so one unreachable backend cannot strand the sockets held by the
rest. All five services call it from their lifespan — `RedisCache`,
`ServiceBusBus`, `ElasticsearchStore`, and `DruidClient` all hold persistent
connections, so a service that skips this leaks one set per restart.

```python
from libs.common import close_backends, dead_letter_message

await dead_letter_message(
    message,
    "invalid signal payload",
    bus=self._bus,
    log=self._log,
    metrics=self.metrics,
    service_name="alerting",
)

await close_backends((self._store, self._cache, self._bus), log=self._log, service_name="api")
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
for msg in dlq:
    await bus.publish(msg.topic, msg.body, message_id=f"{msg.message_id}:replay")
    await bus.complete(msg)          # settle original DLQ message after replay
peeked = await bus.peek("market.raw", "stream-sub", n=5)

# Peek reads from the head of the subscription. To read only what has arrived
# since the last peek, resume from the sequence number it stopped at.
cursor = peeked[-1].sequence_number + 1
newer = await bus.peek("market.raw", "stream-sub", n=5, from_sequence_number=cursor)
```

**Sequence numbers:** `ReceivedMessage.sequence_number` is broker-assigned and
monotonic within a subscription. It is optional — an implementation that cannot
supply one leaves it `None`, and readers fall back to peeking a fixed window.

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
symbols = await cache.list_snapshot_symbols()

# Idempotency
if not await cache.seen("event-id-xyz"):
    process_event()  # first time only

# Short-lived processing lock
if await cache.set_if_absent("lock:event-id-xyz", True, ttl=300):
    process_event()
```

**Serialisation: JSON, never pickle.** `RedisCache` encodes with
`encode_cache_value` / `decode_cache_value`. Redis holds data from outside the
process, and `pickle.loads` on those bytes is arbitrary code execution
(CWE-502). Every value the platform caches is already JSON-native, so cached
values must stay JSON-serialisable. A payload that is not valid JSON on read —
corruption, or a value left by an older pickle-based build — is logged once and
reported as a cache *miss*: snapshot keys carry no TTL, so raising made a single
legacy value fatal for as long as it sat in Redis. It is never unpickled.

**Every idempotency marker needs a TTL.** Use `IDEMPOTENCY_TTL_SECONDS` (24 h)
for "already handled this event" keys. Without one, each unique event leaves a
permanent key and the store grows without bound. `seen()` writes the marker and
its TTL in one `SET ... NX EX`, not `SETNX` then `EXPIRE` — the two-command form
is not atomic, and a process that dies between them leaves a key that never
expires.

**Key namespaces are defined once.** `snapshot_key()` and `seen_key()` (with the
`SNAPSHOT_PREFIX` / `SEEN_PREFIX` constants) are the single definition of a
cross-process contract: the stream service writes snapshots and the API reads
them, so the fake and the real client must never drift apart on the format.

**`set_if_absent` keys on presence, not truthiness.** `get()` returns `None`
both for a missing key and for one storing `None`, so testing its result would
hand out a lock another worker already holds. Both implementations match Redis
`SET NX`: any existing key is held, whatever its value.

**Fake/real conformance.** The suite runs against `InMemoryCache` while
production runs against `RedisCache`, so a behavioural gap between them is a bug
the tests cannot see. `libs/common/tests/test_cache.py` runs one parametrised
conformance set against *both*; add to it whenever you touch either class.

### druid.py — Time-Series Store (Apache Druid)

```python
from libs.common import get_timeseries_store

store = get_timeseries_store()  # InMemoryTimeSeriesStore offline

await store.ingest([{"symbol": "BTCUSDT", "ts": datetime.now(UTC), "price": 60000}])
latest = await store.latest("BTCUSDT")
rows = await store.history("BTCUSDT", frm=start, to=end)
n = await store.count()
results = await store.query_sql("SELECT COUNT(*) FROM ticks")
await store.close()   # DruidClient only; no-op-safe if never used
```

**Connection reuse.** `DruidClient` builds one `httpx.AsyncClient` lazily and
keeps it for its lifetime, mirroring how `ServiceBusBus` caches senders and
receivers. Ingest and query are the hottest paths in the platform, so a client
per call meant a new TCP connection (and, over TLS, a full handshake) for every
tick and every query. Services release it through `close_backends()` on
shutdown — all five now do.

### es.py — Search / Vector Store (Elasticsearch)

```python
from libs.common import get_search_store

store = get_search_store()  # InMemorySearchStore offline

await store.ensure_vector_index("articles", dimensions=1536)
await store.index_document("articles", "doc1", {"title": "BTC pump"}, vector=[...])
hits = await store.knn_search("articles", query_vector=[...], k=5)  # cosine-ranked
await store.index_log("app-logs", {"level": "info", "msg": "started"})
results = await store.search("articles", {"query": {"match_all": {}}})
```

**kNN:** `InMemorySearchStore` implements real cosine-similarity ranking (numpy when
available, pure-Python fallback). `ElasticsearchStore` creates or validates an
`embedding` dense-vector mapping through `ensure_vector_index()` before delegating to
Elasticsearch `knn` dense-vector queries.

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

Each factory decides "is this still the placeholder?" via `is_default(field, value)`,
which compares against the declared default on `Settings`. Do not re-spell a default
URL at a call site — a change to the default here would then silently flip that
factory into building a *real* client against an address nobody configured.

Real clients are thin wrappers and should only be exercised by `@pytest.mark.integration`
tests that skip gracefully without live infra.
