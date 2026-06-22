# ADR 0002: Druid Micro-Quickstart

Status: Accepted

## Context

The full Druid split topology is heavy for local development and slows down compose bring-up. We initially tried to run the whole single-node profile in one container, but the official `apache/druid` image entrypoint (`/druid.sh`) runs exactly one named Druid service per container (`broker`, `historical`, `router`, …). It is not a launcher for the tarball `bin/start-*-quickstart` scripts (those need `perl`/`python`, which the image does not ship), and `DRUID_SINGLE_NODE_CONF` only selects which config a single service reads — it does not boot a full node. A container with no `command:` therefore starts and immediately exits.

## Decision

Run Apache Druid with the `micro-quickstart` configs (`DRUID_SINGLE_NODE_CONF=micro-quickstart`) as **one container per service** in local compose — `druid-coordinator` (overlord embedded via `asOverlord`), `druid-broker`, `druid-historical`, `druid-middlemanager`, `druid-router` — sharing PostgreSQL as the metadata store and ZooKeeper for coordination. Only the router publishes port 8888. Because deep storage is `local`, the segment and indexing-log directories are shared across the data/ingest/query nodes via named volumes.

## Consequences

- Local compose keeps the lighter micro-quickstart memory profile while matching how the `apache/druid` image is actually meant to run.
- It is five Druid containers rather than one; on Apple Silicon they run under `linux/amd64` emulation, so bring-up is slower. Heaps are kept small (~512m each).
- Healthchecks use `wget` rather than `curl` (the image ships no `curl`); a curl-based probe would never pass and would hang `depends_on: service_healthy`.
- The containers run as root (`user: "0:0"`) because Docker creates the shared-volume mountpoints as root and the image's `druid` user (uid 1000) otherwise cannot create `var/druid/{task,segment-cache,…}`.
- Each Druid node is given at least 2 CPUs (`cpus: 2`). The router sizes internal thread pools from `Runtime.availableProcessors()` and aborts at startup (exit 1, before logging) when pinned to a single CPU.
- The local footprint is not production scaling shape, so `docs/AZURE_PRODUCTION.md` documents the multi-node target separately.
- Query and ingest paths remain realistic enough for the current API and observability work.
