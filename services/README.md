# services/

The five deployable services. Each is an independent container with its own
`Dockerfile`, pinned `requirements.txt`, `README.md`, and `tests/` package.

**Portfolio project only — no financial advice, no real trades.**

## Contents

| Service | Role | Host port |
|---|---|---:|
| [`ingestion/`](ingestion/README.md) | Exchange WebSocket + news/social collectors → `market.raw` / `news.raw` | 8001 |
| [`stream/`](stream/README.md) | SMA/EMA/RSI/volatility/anomaly indicators → Druid, Redis snapshot, `signals` | 8002 |
| [`ai/`](ai/README.md) | RAG over Elasticsearch kNN → LLM insight → `insights` | 8003 |
| [`alerting/`](alerting/README.md) | Rule evaluation and deduplication → `alerts` | 8004 |
| [`api/`](api/README.md) | FastAPI REST + WebSocket read layer | 8000 |

Data flows left to right through Azure Service Bus topics; see the root
`CLAUDE.md` for the full diagram.

## Shared shape

Every service follows the same layout, so one is a map for all five:

```
services/<name>/
  app.py            FastAPI app built by libs.common.service_app
  service.py        The worker/facade class + its metrics dataclass
  tests/            Unit tests (offline, fakes only)
  Dockerfile        python:3.12-slim, non-root `appuser`
  requirements.txt  Fully pinned
  README.md
```

`app.py` is deliberately thin. Logging, New Relic, the background-worker
lifespan, and the `/`, `/health`, `/metrics` routes all come from
`libs.common.service_app` — see [`libs/common/README.md`](../libs/common/README.md).
Put service behaviour in `service.py`, not in `app.py`.

## Usage

```bash
task up                     # build and run all five plus infra
task ps                     # container status
docker compose logs -f api  # follow one service
```

Run one locally against the offline fakes:

```bash
PYTHONPATH=. .venv/bin/uvicorn services.api.app:app --port 8000
```

## Inputs / outputs

- **Inputs**: Service Bus topics, exchange/news feeds, and `libs.common` settings.
  With no configuration every port resolves to its in-memory fake.
- **Outputs**: Service Bus topics, Druid, Redis, Elasticsearch, and the REST /
  WebSocket API. Every service exposes `/health` and `/metrics`.

## Dependencies

`libs.common` plus each service's pinned `requirements.txt`. Services never
import from one another — they communicate only over the bus.

## Tests

`services/<name>/tests/` — run with `task test`. Tests use fakes exclusively and
never reach the network.
