# infra/

Infrastructure configuration for the Market Intelligence Platform.

> **Portfolio disclaimer**: This project is for demonstration purposes only. It does not constitute financial advice and executes no real trades.

## Purpose

Holds config files for all local infrastructure services defined in `docker-compose.yml`:

- `servicebus-config.json` — Azure Service Bus emulator namespace/topic/subscription topology
- `druid/environment` — Apache Druid runtime environment variables (metadata, ZK, storage, memory)
- `grafana/provisioning/` — Grafana provisioned datasources and dashboards:
  Elasticsearch logs, Druid HTTP JSON datasource, and the
  `Market Intelligence Observability` dashboard

## Bringing Infrastructure Up

```bash
# Copy and edit env vars (required once)
cp .env.example .env
# Edit .env if you need non-default credentials

# Start all infra services in the background
task up

# Check status
task ps

# Tear down (removes volumes too)
task down
```

Services take ~30–90 s to reach healthy state. The SB emulator and Druid are the slowest to initialise.

## Service Topology

| Service | Image | Port(s) | Role |
|---|---|---|---|
| `mssql` | `mcr.microsoft.com/mssql/server:2022-latest` | 1433 | SQL Server backing store for SB emulator |
| `servicebus-emulator` | `mcr.microsoft.com/azure-messaging/servicebus-emulator:latest` | 5672 (AMQP), 5300 (HTTP) | Azure Service Bus local emulator |
| `postgres` | `postgres:16` | 5432 | App relational metadata + Druid metadata store |
| `zookeeper` | `zookeeper:3.8` | 2181 | Druid coordination |
| `druid` | `apache/druid:30.0.0` | 8888 | Time-series analytics (micro-quickstart profile) |
| `redis` | `redis:7` | 6379 | Caching, latest-value snapshots, idempotency keys |
| `elasticsearch` | `docker.elastic.co/elasticsearch/elasticsearch:8.17.0` | 9200 | Structured log store + RAG vector search (kNN) |
| `grafana` | `grafana/grafana:11.3.0` | 3000 | Operational dashboards, provisioned datasources, and dashboards |

## Service Bus Topics and Subscriptions

Defined in `servicebus-config.json`, namespace `sbemulatorns`:

| Topic | Subscriptions | Duplicate Detection |
|---|---|---|
| `market.raw` | `stream`, `api`, `api-ws` | yes |
| `news.raw` | `ai` | no |
| `signals` | `ai`, `alerting`, `api`, `api-ws` | yes |
| `insights` | `alerting`, `api`, `api-ws` | no |
| `alerts` | `api`, `api-ws` | no |

All subscriptions have `DeadLetteringOnMessageExpiration: true` and `MaxDeliveryCount: 5`.

**Connection string (from host machine):**
```
Endpoint=sb://localhost;SharedAccessKeyName=RootManageSharedAccessKey;SharedAccessKey=SAS_KEY_VALUE;UseDevelopmentEmulator=true;
```

**Connection string (from another container on `mip-net`):**
```
Endpoint=sb://servicebus-emulator;SharedAccessKeyName=RootManageSharedAccessKey;SharedAccessKey=SAS_KEY_VALUE;UseDevelopmentEmulator=true;
```

## Druid Profile Note

This compose file runs Druid in **micro-quickstart single-node** mode (`DRUID_SINGLE_NODE_CONF=micro-quickstart`). This reduces RAM requirements from ~6 GB (full split topology: coordinator + broker + historical + middlemanager + router) to ~1.5–2 GB, which is appropriate for local development and portfolio demos. PostgreSQL is used as the metadata store; ZooKeeper for coordination; local filesystem for deep storage.

For production on Azure, refer to `docs/AZURE_PRODUCTION.md` which maps to a proper multi-node AKS deployment.

## Environment Variables

See `.env.example` at the repo root for all supported variables with safe local defaults.

| Variable | Default | Description |
|---|---|---|
| `MSSQL_SA_PASSWORD` | `Str0ng!Passw0rd` | SQL Server SA password (also used by SB emulator) |
| `POSTGRES_USER` | `mip` | PostgreSQL superuser |
| `POSTGRES_PASSWORD` | `mip_local` | PostgreSQL password |
| `POSTGRES_DB` | `mip` | Default database name |
| `GRAFANA_ADMIN_USER` | `admin` | Grafana admin username |
| `GRAFANA_ADMIN_PASSWORD` | `admin` | Grafana admin password |

Grafana preinstalls the `marcusolsson-json-datasource` plugin at version `1.3.24` so the
Druid SQL endpoint can be queried from provisioned dashboards without adding a
custom image.

## Dependencies

- Docker Desktop >= 4.x with Compose v2 (`docker compose` sub-command)
- At least 4 GB RAM allocated to Docker (8 GB recommended)
- No cloud credentials required for infra-only bring-up
