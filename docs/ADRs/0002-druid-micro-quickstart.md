# ADR 0002: Druid Micro-Quickstart

Status: Accepted

## Context

The full Druid split topology is heavy for local development and slows down compose bring-up.

## Decision

Run Apache Druid in `micro-quickstart` single-node mode in local compose, with PostgreSQL as the metadata store and ZooKeeper for coordination.

## Consequences

- Local compose stays small enough for routine development.
- The local footprint is not production scaling shape, so `docs/AZURE_PRODUCTION.md` documents the multi-node target separately.
- Query and ingest paths remain realistic enough for the current API and observability work.
