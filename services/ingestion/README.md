# Ingestion Service

Portfolio-only ingestion service for the market intelligence platform. It
normalizes replay or exchange-shaped market payloads into the shared
`libs.common.MarketEvent` schema and publishes them to `market.raw`.

This service is offline-safe for CI and local tests:

- deterministic replay feed for repeatable runs
- `InMemoryBus` support for zero-network tests
- FastAPI `/health` and `/metrics` endpoints
- app lifespan starts the replay ingestion loop by default
- structured logging via `libs.common.get_logger`

## Disclaimer

No financial advice. No real trades. This repository is a portfolio project.

## Modules

- `app.py` — FastAPI app factory
- `exchanges/` — reusable Binance/Coinbase WebSocket clients with reconnect,
  circuit breaker, heartbeat tracking, and `market.raw` publishing
- `normalizer.py` — replay/Binance/Coinbase payload normalization
- `replay.py` — deterministic async replay feed with optional simulated disconnect
- `service.py` — publish loop to `market.raw` with deterministic message ids
- `Dockerfile` / `requirements.txt` — pinned service runtime

## Example

```python
from libs.common import InMemoryBus
from services.ingestion.app import create_app
from services.ingestion.replay import DeterministicReplayFeed, build_default_replay_events
from services.ingestion.service import IngestionService

bus = InMemoryBus()
service = IngestionService(
    bus=bus,
    feed_factory=lambda: DeterministicReplayFeed(build_default_replay_events()),
)
app = create_app(service)
```

Run the service with Uvicorn:

```bash
uvicorn services.ingestion.app:app --host 0.0.0.0 --port 8001
```

## Dependencies

Pinned in `requirements.txt`: FastAPI, Uvicorn, Pydantic v2, pydantic-settings,
structlog, Azure Service Bus, websockets, and orjson. Tests use the root
development requirements and the in-memory bus; no network or secrets are
required.
