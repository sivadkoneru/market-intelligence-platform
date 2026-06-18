# ADR 0003: Shared Schema and Common Clients

Status: Accepted

## Context

The services need the same event models, topic names, retries, and backing-store abstractions without duplicating code in each package.

## Decision

Keep the Pydantic event models, topic constants, and infrastructure ports in `libs/common`, then have each service depend on those shared definitions.

## Consequences

- The platform has one event contract for `MarketEvent`, `NewsEvent`, `Signal`, `Insight`, and `Alert`.
- Tests can swap in-memory fakes for buses, caches, stores, and search backends.
- API, stream, AI, alerting, and ingestion stay aligned on the same schema and idempotency helpers.
