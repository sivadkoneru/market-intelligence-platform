# ADR 0004: Mock-First LLM, RAG, and Guardrails

Status: Accepted

## Context

The AI service must run offline in tests, while still leaving room for real provider-backed generation when credentials are available.

## Decision

Default to `MOCK_LLM`, keep the real LLM and embedding providers import-guarded, and run lightweight guardrails on every generation result before publishing an insight.

## Consequences

- `task test` stays deterministic and does not require external keys.
- Real providers can be enabled later without changing the service contract.
- Guardrails keep ungrounded or low-confidence outputs from looking authoritative.
