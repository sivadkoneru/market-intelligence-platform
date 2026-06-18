# AI-Powered Market Intelligence Platform

A multi-service Python platform that ingests live crypto market data and news/social events over WebSockets, normalizes them to a common schema, streams through Azure Service Bus to compute technical indicators and RAG-driven LLM insights, persists time-series to Apache Druid, and serves live data, signals, alerts, and AI explanations over REST and WebSocket APIs.

## Quick Start

```bash
# Set up the local test environment
make setup
make test

# Start the entire platform (when compose file is ready)
docker compose up -d
```

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

## Architecture

See `docs/ARCHITECTURE.md` for system design, data flow diagrams, and component descriptions (when available).

## Testing

```bash
make test          # Run pytest suite
make clean         # Remove venv and caches
```

## Development

- **Language**: Python 3.12 (local dev with Python 3.11 venv for compatibility)
- **Async**: asyncio throughout
- **API**: FastAPI + Uvicorn
- **Messaging**: Azure Service Bus (with local emulator)
- **Time-series**: Apache Druid
- **Vector search**: Elasticsearch kNN
- **Databases**: PostgreSQL, Redis
- **LLM**: Azure OpenAI, Claude, or MOCK_LLM (default)
