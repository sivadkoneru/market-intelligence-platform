# libs/

Shared libraries imported by every service. Nothing here talks to a specific
service; everything here is imported by all of them.

**Portfolio project only — no financial advice, no real trades.**

## Contents

| Package | Purpose |
|---|---|
| [`common/`](common/README.md) | Event schema, settings, structured logging, the infra ports (bus / cache / time-series / search) with their fakes and real clients, resilience helpers, and the shared FastAPI service bootstrap |

## Usage

Import from the package root — `libs.common` re-exports the public API, so
services never reach into submodules:

```python
from libs.common import MarketEvent, get_logger, get_message_bus
```

## Inputs / outputs

- **Inputs**: environment variables (via `libs.common.config.Settings`) and the
  objects callers pass in.
- **Outputs**: Pydantic models, port implementations, and configured loggers.

## Dependencies

Pydantic v2, pydantic-settings, structlog, tenacity, numpy, and the optional
real-client SDKs (`redis`, `elasticsearch`, `azure-servicebus`, `httpx`), which
are import-guarded so the offline test gate runs without them.

## Rules

- Services import from `libs.common`; `libs.common` never imports from `services`.
- Every external dependency is reached through a Protocol that ships both a real
  client and an in-memory fake — see [`common/README.md`](common/README.md).
- No duplicate event models: `libs/common/schema.py` is the only definition.

## Tests

`libs/common/tests/` — run with `task test`.
