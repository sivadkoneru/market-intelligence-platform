# ADR 0005: Observability with Structured Logs and Grafana

Status: Accepted

## Context

The services need operational visibility without adding a separate telemetry stack.

## Decision

Use structured JSON logs, per-service `/metrics`, Elasticsearch for log storage, and Grafana provisioning for dashboards over Elasticsearch and Druid.

## Consequences

- Every service exposes a consistent health and metrics surface.
- Logs, time-series, and dashboards stay in the repo and the compose stack.
- New Relic remains optional instead of being a hard dependency for tests or local work.
