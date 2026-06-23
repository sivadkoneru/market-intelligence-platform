# ADR 0006: Single OpenAI-Compatible LLM Provider

Status: Accepted

This platform is a portfolio project only. No financial advice. No real trades.

## Context

The AI analysis service originally carried three live LLM backends — `AzureOpenAIProvider`,
`AnthropicProvider`, and a direct OpenAI provider — each with its own settings
(`AZURE_OPENAI_*`, `ANTHROPIC_*`, `OPENAI_*`) and a `LLM_PROVIDER` / `EMBEDDING_PROVIDER`
selector. That is a lot of configuration surface and three SDK code paths to maintain for
what is, in practice, the same request/response shape.

The OpenAI Chat Completions and Embeddings APIs are now a de-facto standard: OpenAI, Azure
OpenAI (via its `/openai/v1` route), Anthropic (via its `/v1` OpenAI-compatibility layer),
OpenRouter, and local servers (vLLM, llama.cpp) all speak it. A single client pointed at a
configurable base URL reaches all of them.

## Decision

Collapse the live LLM/embedding path to **one** OpenAI-compatible provider, `OpenAIProvider`,
configured by a single set of variables:

- `OPENAI_API_KEY` — credential for hosted providers; may be empty for local
  OpenAI-compatible servers such as LM Studio
- `OPENAI_BASE_URL` — target any OpenAI-compatible endpoint (defaults to OpenAI)
- `OPENAI_CHAT_MODEL`, `OPENAI_EMBEDDING_MODEL` — model ids

`MockLLMProvider` remains the offline default. Selection is now purely:

- `MOCK_LLM=1` (default) → deterministic mock, fully offline.
- `MOCK_LLM=0` → OpenAI-compatible client. Hosted providers still need
  `OPENAI_API_KEY`; local providers may not.

The dedicated `AzureOpenAIProvider` and `AnthropicProvider` classes, their settings, the
`LLM_PROVIDER` / `EMBEDDING_PROVIDER` selectors, and their tests are removed. Azure OpenAI
and Anthropic are still reachable — through `OPENAI_BASE_URL` — so no backend is lost. The
SDK import stays lazy, so `task test` runs offline with the `openai` package absent.

This supersedes the provider-specific portions of ADR 0004 (which established the mock-first
default and groundedness guardrails — both retained). Owner approval for relying on the
OpenAI-compatible interface across providers is recorded here.

## Consequences

- One credential set and one code path for every live backend; far less configuration sprawl.
- `MOCK_LLM` stays the default; the tested path is unchanged and needs zero secrets.
- Switching providers is a `OPENAI_BASE_URL` change, not a code change.
- The Azure- and Anthropic-specific SDK integrations are no longer present as distinct code;
  the platform reaches them via their OpenAI-compatible endpoints instead.
