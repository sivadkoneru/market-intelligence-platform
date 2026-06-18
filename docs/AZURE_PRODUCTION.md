# Azure Production Mapping

This platform is a portfolio project only. No financial advice. No real trades.

This document maps the local compose stack to a production Azure shape. It is not an IaC plan and does not describe Terraform or ARM/Bicep.

## Service Mapping

| Local component | Azure production target | Notes |
|---|---|---|
| `ingestion`, `stream`, `ai-analysis`, `alerting`, `api` | AKS deployments or Azure Container Apps | Stateless app pods; use autoscaling, readiness probes, and managed identity for secrets access |
| Azure Service Bus emulator | Azure Service Bus namespace | Recreate topics, subscriptions, duplicate detection, dead-lettering, and TTLs |
| SQL Server backing store for the emulator | Not needed | Emulator-only dependency; disappears in production |
| PostgreSQL 16 | Azure Database for PostgreSQL Flexible Server | Holds app metadata and the Druid metadata store |
| ZooKeeper 3.8 | ZooKeeper ensemble on AKS or VMs | Required for Druid coordination unless the production Druid packaging changes |
| Apache Druid micro-quickstart | Multi-node Druid cluster | Use separate coordinator, broker, historical, middlemanager, and router roles |
| Redis 7 | Azure Cache for Redis | Stores latest snapshots, cache entries, and idempotency markers |
| Elasticsearch 8 | Elastic Cloud on Azure or self-managed Elasticsearch on AKS | Supports structured logs and kNN retrieval for RAG |
| Grafana 11 | Azure Managed Grafana or self-hosted Grafana on AKS | Verify datasource/plugin support before choosing managed Grafana |

## Production Considerations

- Keep the event contract in `libs/common` unchanged so the local and Azure stacks stay wire-compatible.
- Use Azure Service Bus duplicate detection where the local emulator does.
- Preserve the `market.raw -> stream -> signals -> ai/alerting/api` topic split so consumers stay isolated.
- Move secrets to Key Vault and inject them with managed identities.
- Prefer private networking for Service Bus, PostgreSQL, Redis, Elasticsearch, and Druid.
- Size Druid for real retention and query fanout; the local micro-quickstart profile is only for development.
- Use object storage for Druid deep storage in production, not the local filesystem.
- Expect longer warm-up times for Druid and Elasticsearch than for the stateless app services.
- Keep `/health` and `/metrics` wired into Azure load balancer or ingress probes.
- Route logs into the production log store with the same structured JSON fields so Grafana and alerting stay usable.

## Operational Shape

1. Build and push one container image per Python service.
2. Deploy the app services onto AKS or equivalent container hosting.
3. Provision Azure Service Bus topics and subscriptions from the same topology encoded in `infra/servicebus-config.json`.
4. Provision PostgreSQL, Redis, Elasticsearch, and Druid with separate scaling and backup policies.
5. Keep Grafana pointed at the production log and time-series backends.

## Local to Azure Notes

- Local `mssql` exists only to satisfy the Service Bus emulator.
- Local Druid runs in micro-quickstart mode to keep the compose stack small; production should not.
- Local Redis, PostgreSQL, and Elasticsearch use single-node containers; production should use managed or clustered equivalents.
- The API WebSocket URL stays the same behind ingress, but the public host changes.
