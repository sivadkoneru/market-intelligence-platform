# AI-Powered Market Intelligence Platform

A multi-service Python platform that ingests live crypto market data and news/social events over WebSockets, normalizes them to a common schema, streams through Azure Service Bus to compute technical indicators and RAG-driven LLM insights, persists time-series to Apache Druid, and serves live data, signals, alerts, and AI explanations over REST and WebSocket APIs.

## Quick Start

```bash
cp .env.example .env
task up
```

For the local test loop:

```bash
task setup
task lint
task test
```

Smoke helpers:

```bash
task smoke:sb
task smoke:ws
```

## Links

- [Architecture](docs/ARCHITECTURE.md)
- [API](docs/API.md)
- [Benchmarks](docs/BENCHMARKS.md)
- [Azure production notes](docs/AZURE_PRODUCTION.md)
- [API service README](services/api/README.md)

See `CLAUDE.md` for the full codebase guide, architecture conventions, and commit rules.

## Layout

```
libs/                  # Shared libraries
  common/              # Common schema, config, interfaces
services/              # Application services
  ingestion/           # Market and news data ingestion
  stream/              # Stream processing and indicators
  ai/                  # AI analysis and RAG
  alerting/            # Alert rule evaluation
  api/                 # REST and WebSocket API
infra/                 # Infrastructure configuration
docs/                  # Architecture and design docs
scripts/               # Utility scripts
tests/                 # Test suite
```

## Disclaimer

**This is a portfolio/educational project only.** It demonstrates distributed systems, event streaming, and RAG-LLM integration patterns using market data.

**This platform provides no financial advice and is not for real trading.**

This platform is **NOT** for:
- **Real trading or order execution** — no brokerage integration
- **Financial advice** — all insights are illustrative only
- **Actual investment decisions** — do not rely on any output for real trades

The platform is runnable entirely offline with mock data. No real capital is at risk.

## Current Ports

- API: `http://localhost:8000` and container port `8005`
- Ingestion: `8001`
- Stream: `8002`
- AI analysis: `8003`
- Alerting: `8004`
- Grafana: `3000`
- Druid: `8888`
- PostgreSQL: `5432`
- Redis: `6379`
- Elasticsearch: `9200`
- Service Bus emulator: `5672` and `5300`
- SQL Server: `1433`
- ZooKeeper: `2181`

## Architecture

See `docs/ARCHITECTURE.md` for system design, data flow diagrams, and component descriptions (when available).

## Commands

```bash
task setup         # Create .venv with Python 3.11 and install dev dependencies
task lint          # Ruff gate
task test          # Pytest gate
task format        # Black + ruff format pass
task up            # Build and start infra + app services
task down          # Stop containers and remove volumes
task ps            # Show compose status
task smoke:sb      # Peek Service Bus topic messages
task smoke:ws      # Subscribe to the API websocket smoke stream
task clean         # Remove .venv and cache directories
```

## Stack

- **Language**: Python 3.12 (local dev and CI use Python 3.11 for the test environment)
- **Async**: asyncio throughout
- **API**: FastAPI + Uvicorn
- **Messaging**: Azure Service Bus (with local emulator)
- **Time-series**: Apache Druid
- **Vector search**: Elasticsearch kNN
- **Databases**: PostgreSQL, Redis
- **LLM**: one OpenAI-compatible client (point `OPENAI_BASE_URL` at OpenAI, Azure OpenAI, Anthropic, or a local server), or MOCK_LLM (default)
- **Linting/Formatting**: ruff, black
- **Testing**: pytest, pytest-asyncio
