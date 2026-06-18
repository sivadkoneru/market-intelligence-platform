# Alerting Service

Consumes `signals` and `insights` on the `alerting` subscription, evaluates a
deterministic rule set, publishes `alerts`, and routes malformed or poison
messages to the dead-letter queue. The worker is offline-safe by default
because it uses the shared `MessageBus` and `Cache` ports from `libs/common`,
which resolve to in-memory fakes when live infrastructure is not configured.

Portfolio project only. No financial advice. No real trades.

## Purpose

- Subscribe to `signals` and `insights` with the `alerting` subscription
- Validate payloads against `libs.common.Signal` and `libs.common.Insight`
- Evaluate threshold breaches, anomaly flags, and sentiment spikes
- Publish `libs.common.Alert` events to `alerts`
- Suppress duplicate input processing with cache-backed idempotency and short-lived
  processing locks
- Reuse `Alert.dedupe_key` as the outbound `message_id` and cache per-alert publish
  markers, so retrying a partial alert batch does not emit duplicates even though the
  local `alerts` topic leaves broker duplicate detection disabled
- Dead-letter malformed payloads and permanently failing messages
- Replay dead-lettered signal or insight messages with `python -m services.alerting.replay`;
  replay completes each original DLQ message after the replacement is published

## Inputs

- `signals` topic messages whose bodies validate as `libs.common.Signal`
- `insights` topic messages whose bodies validate as `libs.common.Insight`

## Outputs

- `alerts` topic messages built from `libs.common.Alert`
- Dead-letter entries for malformed or poison messages on the `alerting`
  subscription

## Runtime Endpoints

- `GET /`
- `GET /health`
- `GET /metrics`

Run locally:

```bash
uvicorn services.alerting.app:app --host 0.0.0.0 --port 8004
```

Replay dead-lettered messages for the alerting subscription:

```bash
python -m services.alerting.replay signals
python -m services.alerting.replay insights --suffix retry
```

## Example

```python
from libs.common import InMemoryBus, InMemoryCache
from services.alerting.service import AlertingService

bus = InMemoryBus()
cache = InMemoryCache()

service = AlertingService(bus=bus, cache=cache)
await bus.receive("signals", "alerting", max_messages=0)
await bus.receive("alerts", "observer", max_messages=0)
await service.poll_once()
```

## Dependencies

Pinned in `requirements.txt`: FastAPI, Uvicorn, Pydantic v2, pydantic-settings,
structlog, Azure Service Bus, Redis, and tenacity. Tests use the root
development requirements and the in-memory ports; no network or secrets are
required.
