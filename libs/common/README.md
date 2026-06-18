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

No heavy framework dependencies (LangChain, OpenAI, etc.) are required for `make test` —
those are import-guarded in service packages.
