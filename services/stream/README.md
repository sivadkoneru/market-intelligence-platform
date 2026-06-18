# Stream Service

Consumes `market.raw`, computes deterministic technical indicators per symbol, ingests
tick and indicator rows into the time-series store, caches the latest snapshot, and
publishes `signals`. The service is offline-safe by default because it uses the shared
`MessageBus`, `Cache`, and `TimeSeriesStore` ports from `libs/common`, which resolve to
in-memory fakes when live infrastructure is not configured.

Portfolio project only. No financial advice and no real trades.

## Purpose

- Subscribe to `market.raw` with the `stream` subscription
- Convert message payloads into common `MarketEvent` objects
- Maintain in-process per-symbol price history
- Compute SMA, EMA, RSI, rolling volatility, trend, z-score anomaly, and EWMA anomaly
- Ingest tick rows and indicator rows into the time-series store
- Cache the latest per-symbol snapshot
- Publish `Signal` events to `signals`
- Suppress duplicate inputs with the common market event idempotency key
- Dead-letter malformed payloads with a useful reason

## Inputs

- `market.raw` topic messages whose bodies validate as `libs.common.MarketEvent`
- Ordered per-symbol prices retained in memory while the worker is running

## Outputs

- `ticks` rows in the `TimeSeriesStore`
- `indicators` rows in the `TimeSeriesStore`
- Redis-style latest snapshots via `Cache.set_snapshot(symbol, data)`
- `signals` topic events built from `libs.common.Signal`
- Dead-letter entries for malformed or poison messages

## Runtime Endpoints

- `GET /health`
- `GET /metrics`

Run locally:

```bash
uvicorn services.stream.app:app --host 0.0.0.0 --port 8002
```

## Dependencies

- Python standard library
- `numpy`
- `fastapi`
- `structlog`
- Shared offline-safe ports from `libs/common`

No secrets, network calls, or live infrastructure are required for tests.

## Usage

```python
from libs.common import InMemoryBus, InMemoryCache, InMemoryTimeSeriesStore
from services.stream.service import StreamService

bus = InMemoryBus()
cache = InMemoryCache()
store = InMemoryTimeSeriesStore()

service = StreamService(bus=bus, cache=cache, store=store)
await bus.receive("market.raw", "stream", max_messages=0)  # prime the subscription in tests
await service.poll_once()
```

## Indicator Helpers

The pure math helpers remain in `services.stream.indicators` and are reused by the
message-handling path. They return `None` until enough price history exists for the
requested calculation.
