# ADRs

This platform is a portfolio project only. No financial advice. No real trades.

Short architecture decision records for choices already present in the repo.

## Files

| File | Decision |
|---|---|
| `0001-service-bus-emulator-topology.md` | Azure Service Bus emulator with SQL Server backing store and explicit topic/subscription topology |
| `0002-druid-micro-quickstart.md` | Druid micro-quickstart for local compose instead of the full multi-node layout |
| `0003-shared-schema-common-clients.md` | Shared Pydantic events and common client ports in `libs/common` |
| `0004-mock-first-llm-rag-guardrails.md` | MOCK_LLM default, import-guarded real providers, and guardrails on all generations |
| `0005-observability-grafana.md` | Structured logs, service metrics, and Grafana provisioning over Elasticsearch and Druid |

## Writing Style

- Keep each ADR short and concrete.
- State the context, the decision, and the consequence.
- Reference repo files when helpful.
