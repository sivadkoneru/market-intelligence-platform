# ADR 0001: Service Bus Emulator Topology

Status: Accepted

## Context

The platform needs Azure Service Bus semantics locally: topics, subscriptions, duplicate detection, dead-letter queues, and a realistic publish/receive model.

## Decision

Use the Azure Service Bus emulator in `docker-compose.yml`, backed by **Azure SQL Edge** (the emulator's documented SQL store), and load the topic/subscription topology from `infra/servicebus-config.json`. SQL Edge ships a native arm64 image, so the stack runs on Apple Silicon without QEMU emulation — the full `mcr.microsoft.com/mssql/server` image is amd64-only and segfaults under emulation on arm64 Macs.

The configured topics are:

- `market.raw`
- `news.raw`
- `signals`
- `insights`
- `alerts`

## Consequences

- Local development exercises the same messaging shape as production.
- The compose stack carries one extra dependency (`mssql`, running Azure SQL Edge) purely for the emulator.
- Duplicate detection and dead-letter behavior are visible in tests and smoke runs.
- The emulator image is distroless (no shell), so it cannot run an in-container healthcheck; dependents gate on `condition: service_started` and rely on app-side connection retries (tenacity + CircuitBreaker).
- Azure SQL Edge is on a deprecation path but still pulls and runs; it is used only as the emulator's local metadata store and never in the Azure production path.
