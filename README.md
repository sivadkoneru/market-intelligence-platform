# AI-Powered Market Intelligence Platform

A multi-service Python platform that ingests live crypto market data and news/social events over WebSockets, normalizes them to a common schema, streams through Azure Service Bus to compute technical indicators and RAG-driven LLM insights, persists time-series to Apache Druid, and serves live data, signals, alerts, and AI explanations over REST and WebSocket APIs.

## Quick Start

```bash
# Set up the local test environment
task setup
task test

# Start the entire platform
cp .env.example .env
task up

# Smoke helpers
task smoke:sb
task smoke:ws
```

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

## Architecture

See `docs/ARCHITECTURE.md` for system design, data flow diagrams, and component descriptions (when available).

## Testing

```bash
task test          # Run pytest suite
task clean         # Remove venv and caches
make test          # Thin wrapper around task test
```

## Development

### Setup and Build Commands

```bash
task setup         # Create .venv and install all dev dependencies
task test          # Run pytest suite (auto-runs setup if needed)
task lint          # Check code style with ruff
task format        # Format code with black and ruff
task up            # Build and start infra + all five app services
task down          # Stop containers and remove volumes
task ps            # Show compose status
task smoke:sb      # Peek Service Bus topic messages
task smoke:ws      # Subscribe to the API websocket smoke stream
task clean         # Remove .venv and all cache directories
```

`make setup`, `make test`, and the other top-level targets are thin wrappers around the
same `task` commands for environments that expect `make`.

### Stack

- **Language**: Python 3.12 (local dev with Python 3.11 venv for compatibility)
- **Async**: asyncio throughout
- **API**: FastAPI + Uvicorn
- **Messaging**: Azure Service Bus (with local emulator)
- **Time-series**: Apache Druid
- **Vector search**: Elasticsearch kNN
- **Databases**: PostgreSQL, Redis
- **LLM**: Azure OpenAI, Claude, or MOCK_LLM (default)
- **Linting/Formatting**: ruff, black
- **Testing**: pytest, pytest-asyncio
