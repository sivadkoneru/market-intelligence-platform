# ADR 0001: Service Bus Emulator Topology

Status: Accepted

## Context

The platform needs Azure Service Bus semantics locally: topics, subscriptions, duplicate detection, dead-letter queues, and a realistic publish/receive model.

## Decision

Use the Azure Service Bus emulator in `docker-compose.yml`, backed by SQL Server, and load the topic/subscription topology from `infra/servicebus-config.json`.

The configured topics are:

- `market.raw`
- `news.raw`
- `signals`
- `insights`
- `alerts`

## Consequences

- Local development exercises the same messaging shape as production.
- The compose stack carries one extra dependency (`mssql`) purely for the emulator.
- Duplicate detection and dead-letter behavior are visible in tests and smoke runs.
