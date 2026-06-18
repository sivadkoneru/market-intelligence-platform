# API Service

FastAPI service exposing market data, indicators, signals, alerts, and insights over
REST using the shared Druid/Redis/Service Bus ports from `libs/common`.

Portfolio project only. No financial advice and no real trades.

## Purpose

- Expose `GET /health` and `GET /metrics`
- List tracked symbols with `GET /symbols`
- Serve latest market snapshots and historical market rows
- Serve latest indicators per symbol from the stream cache and time-series store
- Surface latest `signals`, `alerts`, and `insights`
- Keep the default runtime offline-safe by resolving to in-memory fakes when live infra
  is not configured

## Endpoints

- `GET /`
- `GET /health`
- `GET /metrics`
- `GET /symbols`
- `GET /market/{symbol}/latest`
- `GET /market/{symbol}/history?from=...&to=...`
- `GET /indicators/{symbol}`
- `GET /signals`
- `GET /alerts`
- `GET /insights/{symbol}`

Run locally:

```bash
uvicorn services.api.app:app --host 0.0.0.0 --port 8005
```

## Dependencies

- `fastapi`
- `structlog`
- Shared `TimeSeriesStore`, `Cache`, and `MessageBus` ports from `libs/common`

## Usage

```python
from libs.common import InMemoryBus, InMemoryCache, InMemoryTimeSeriesStore
from services.api.service import APIService

service = APIService(
    store=InMemoryTimeSeriesStore(),
    cache=InMemoryCache(),
    bus=InMemoryBus(),
)
await service.prime_subscriptions()
```

## Notes

- Market history uses the time-series store directly.
- Indicator reads prefer the latest Redis-style snapshot and fall back to `indicators`
  rows from the time-series store.
- Insights are served from the cached `insight:{symbol}` payload first, then from the
  message bus peek path as a fallback.
