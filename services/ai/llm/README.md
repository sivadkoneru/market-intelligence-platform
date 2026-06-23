# services/ai/llm

Provider-neutral LLM generation, embeddings, and lightweight guardrails for the AI analysis
service.

> **Disclaimer:** This is a portfolio project only. No financial advice, no real trades.

## Purpose

This package gives the AI service one offline-safe surface for:

- structured text generation for sentiment, summary, and grounded explanations
- float-vector embeddings for retrieval
- lightweight groundedness / low-confidence checks before downstream publish
- deterministic `MOCK_LLM` behavior for tests and local work with no secrets

The package is intentionally small so T11/T12 can build RAG orchestration on top of it.

## Modules

| File | Responsibility |
|---|---|
| `models.py` | request / response dataclasses and context documents |
| `providers.py` | `MockLLMProvider` and one OpenAI-compatible `OpenAIProvider` |
| `guardrails.py` | citation, overlap, confidence, refusal, and empty-output checks |
| `factory.py` | provider-selection helpers (mock when offline, else OpenAI-compatible) |

## Usage

```python
import asyncio

from libs.common import get_settings
from services.ai.llm import (
    ContextDocument,
    EmbeddingRequest,
    GenerationRequest,
    get_provider_bundle,
)


async def main() -> tuple[str, int]:
    settings = get_settings()
    bundle = get_provider_bundle(settings)

    request = GenerationRequest(
        prompt="Summarize the latest BTC news and explain the likely market sentiment.",
        context=(
            ContextDocument(
                doc_id="news-1",
                url="https://example.test/news-1",
                title="Bitcoin miners report stronger margins",
                text="Bitcoin miners reported stronger operating margins after a rally in price.",
            ),
        ),
        metadata={"symbol": "BTCUSDT"},
    )

    result = await bundle.generator.generate(request)
    embeddings = await bundle.embedder.embed(EmbeddingRequest(texts=("btc news",)))
    return result.summary, len(embeddings.vectors[0])


asyncio.run(main())
```

## Inputs / Outputs

### Generation input

- `prompt`: primary task instruction
- `system_prompt`: optional provider-level instruction
- `context`: retrieved grounding documents with `doc_id` and optional `url`
- `messages`: optional chat history if the caller wants direct message control
- `metadata`: optional downstream hints such as `symbol`

### Generation output

- `summary`
- `explanation`
- `sentiment_score` in `[-1.0, 1.0]`
- `sentiment_label`
- `citations`
- `confidence` in `[0.0, 1.0]`
- `grounded`

### Embedding output

- tuple of float vectors
- provider name
- model name
- vector dimensionality

## Dependencies

- No live SDK is required for offline tests.
- `openai` is optional and imported lazily only when `OpenAIProvider` builds a real client.
  The same client reaches any OpenAI-compatible endpoint (OpenAI, Azure OpenAI, Anthropic,
  OpenRouter, local servers) via `OPENAI_BASE_URL`.
- OpenAI-compatible embedding responses are coerced to the requested RAG dimension so local
  servers that ignore the `dimensions` parameter still fit the configured vector index.
- JSON generation is preferred, but non-JSON local model responses are converted into a
  conservative structured result using retrieved-context citations.

Heavy/optional SDKs stay import-guarded so `task test` runs with the lightweight dependency set.

## Offline Behavior

- `Settings.mock_llm` defaults to `True`, so factories resolve to `MockLLMProvider` unless the
  caller sets `MOCK_LLM=0` and provides `OPENAI_API_KEY`.
- `MockLLMProvider` returns deterministic output for the same prompt/context and deterministic
  normalized embeddings for the same text.
- Guardrails still run in mock mode, which keeps tests deterministic for grounded and ungrounded
  cases.
