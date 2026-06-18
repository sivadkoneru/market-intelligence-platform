# CLAUDE.md — Market Intelligence Platform

Codebase guide and binding conventions for engineers and future Claude sessions.

---

## Overview

Multi-service Python platform that ingests live crypto market data and news events over WebSockets, streams them through Azure Service Bus, computes technical indicators (stream service) and RAG-driven LLM insights (AI analysis service), stores time-series in Apache Druid, and exposes data over REST + WebSocket APIs.

**Portfolio project only — no financial advice, no real trades, no real capital at risk.**

Data flow:

```
Exchange WebSockets / News REST
    → ingestion service
        → Azure Service Bus (market.raw, news.raw)
            → stream service  → Druid (ticks + indicators) → Redis (latest snapshot)
            → ai-analysis     → Elasticsearch kNN (RAG) → Redis (LLM cache)
            → alerting        → alerts topic
    → api service  (REST + WebSocket, Druid / Redis / ES backed)
```

---

## Tech Stack and Constraints

**Resume-tech constraint**: only use technologies listed on the owner's resume. Adding anything outside this list requires an ADR and explicit owner approval.

| Layer | Technology |
|---|---|
| Language | Python 3.12 (runtime target) / python3.11 (local venv — only 3.11 has all wheels locally) |
| API | FastAPI + Uvicorn |
| Messaging | Azure Service Bus (`azure-servicebus` SDK) + local emulator |
| Time-series | Apache Druid |
| Relational | PostgreSQL 16 (app metadata + Druid metadata store) |
| Cache | Redis 7 |
| Vector / log store | Elasticsearch 8 kNN (`dense_vector`) |
| LLM providers | Azure OpenAI, Claude (Anthropic), MOCK_LLM (default, no key required) |
| Heavy frameworks | LangChain, LlamaIndex, AutoGen (import-guarded optionals — see below) |
| Schema / config | Pydantic v2 + pydantic-settings |
| Logging | structlog (JSON only) |
| Retry / resilience | tenacity + CircuitBreaker helper in `libs/common` |
| ORM | SQLAlchemy 2.x async + asyncpg |
| Data | pandas / numpy |
| Tests | pytest + pytest-asyncio |
| Observability | New Relic APM (opt-in, key required), Grafana dashboards |
| Containers | Docker + Docker Compose |

**Banned** (not on resume): Prometheus, OpenTelemetry/Jaeger, Kafka/Redpanda, TimescaleDB, Qdrant.

---

## Repo Layout

```
libs/
  common/           Shared schema, config, logging, infra clients, resilience
services/
  ingestion/        Exchange WebSocket + news/social ingestion, normalizer, replay feed
  stream/           SMA/EMA/RSI/volatility/anomaly indicators; Druid ingest; Redis snapshot
  ai/               RAG pipeline (Elasticsearch kNN → LLM generation); AutoGen agent; MOCK_LLM
  alerting/         Rule evaluation, deduplication, dead-letter routing
  api/              FastAPI REST + WebSocket service
infra/              docker-compose.yml, servicebus-config.json, druid/environment, grafana/
docs/               ARCHITECTURE.md, SEQUENCE.md, API.md, adr/, AZURE_PRODUCTION.md, BENCHMARKS.md
scripts/            sb_peek.py, ws_smoke.py, replay utilities
tests/              Repo-level smoke tests (structure, README disclaimer, tooling)
```

Each service and `libs/common` has its own `README.md`, `pyproject.toml`/`requirements.txt`, and `Dockerfile`.

---

## Dev Workflow (Task)

`task` (go-task v3) is the project's task runner. The `Taskfile.yml` lives at the repo root.

```bash
task setup      # Create .venv (python3.11) and install pinned dev deps — idempotent
task test       # THE gate: must stay green for the whole repo at every commit
task lint       # ruff check .
task format     # black + ruff --fix
task up         # docker compose up -d
task down       # docker compose down -v
task ps         # docker compose ps
task clean      # remove .venv and all caches
```

**`task test` is the quality gate.** It must pass with zero live infra, zero secrets, and zero network access — using in-memory fakes and MOCK_LLM only.

The `.venv` is created with `python3.11` (the only locally available interpreter with compatible wheels). Docker images target `python:3.12-slim`. Do not change this split.

---

## Architecture Conventions

### Ports + Fakes (everything testable offline)

Every external dependency is accessed through a small interface (`typing.Protocol` or ABC) defined in `libs/common`. Each interface ships:
- A **real client** implementation (used when a real service URL/credential is present).
- An **in-memory fake** used by tests (no network required).

| Port | Fake | Real client |
|---|---|---|
| `MessageBus` | `InMemoryBus` | `ServiceBusBus` |
| `Cache` | `InMemoryCache` | `RedisCache` |
| `TimeSeriesStore` | `InMemoryTimeSeriesStore` | `DruidClient` |
| `SearchStore` | `InMemorySearchStore` | `ElasticsearchStore` |
| LLM provider | `MOCK_LLM` (deterministic) | `AzureOpenAIClient` / `AnthropicClient` |

Factory functions (e.g. `get_message_bus()`, `get_cache()`) select the fake automatically when the env var is absent or set to the default placeholder value. Tests use fakes exclusively — never reach the network.

### Common Schema

All Pydantic v2 event models live in `libs/common/schema.py`. Services import them; they never define their own duplicate models.

| Model | Topic |
|---|---|
| `MarketEvent` | `market.raw` |
| `NewsEvent` | `news.raw` |
| `Signal` | `signals` |
| `Insight` | `insights` |
| `Alert` | `alerts` |

Idempotency key: `(symbol, ts, source)` SHA-256 for market events; content hash for LLM cache.

### Heavy-Framework Policy

LangChain, LlamaIndex, AutoGen, vLLM, and New Relic are import-guarded:

```python
try:
    from langchain import ...
except ImportError:
    pass  # fall back to lightweight implementation
```

The **tested path** is always the lightweight implementation inside `libs/common` and the service packages. `task test` must pass without any of these installed.

---

## Logging

- structlog JSON only everywhere. **No bare `print` statements.**
- Every log event carries `correlation_id` and `trace_id` via `contextvars`.
- Use `from libs.common import get_logger; log = get_logger(__name__)`.
- Each service exposes `/health` and `/metrics`.

---

## Project Rules

1. **Every new module/dir ships a `README.md`** — purpose, usage example, inputs/outputs, dependencies.
2. **Write tests for every file you create or modify.** Tests must verify real behavior, not just mock echoes.
3. **No bare `print`** — structlog only.
4. **Resume-tech only** — see the banned list above.
5. **Disclaimer** (no financial advice, no real trades) must appear in `README.md` and in the API root response.
6. **`task test` must stay green** after every commit. Never commit broken tests.

---

## Commit Convention

- Conventional-commit subjects: `feat:`, `fix:`, `chore:`, `docs:`, `test:`, etc.
- One commit per task (per task brief).
- Stage only files the task created/changed. Do not commit `.venv/`, caches, or build artifacts.
- All work on branch `feat/market-intel-platform`. Never commit to `main`.
