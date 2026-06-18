# AI Analysis Service

Consumes `news.raw` and `signals`, indexes event context into the shared RAG pipeline,
generates deterministic or provider-backed sentiment analysis, and publishes grounded
`Insight` events to `insights`.

Portfolio project only. No financial advice and no real trades.

## Purpose

- Subscribe to `news.raw` and `signals` with the `ai` subscription
- Index news and technical-signal context into the shared `SearchStore`
- Run retrieval through `services.ai.rag.RAGPipeline`
- Generate sentiment score, summary, and grounded explanation with `services.ai.llm`
- Perform AutoGen-compatible event detection through an import-guarded adapter with a
  deterministic fallback for tests
- Cache LLM results by content hash
- Cache the latest published insight under `insight:{symbol}`
- Suppress duplicate deliveries idempotently without blocking future identical content
- Publish `Insight` events to `insights`
- Dead-letter malformed or poison messages with a useful reason

## Inputs

- `news.raw` topic messages whose bodies validate as `libs.common.NewsEvent`
- `signals` topic messages whose bodies validate as `libs.common.Signal`
- Shared `SearchStore` documents for retrieval grounding

## Outputs

- `insights` topic events built from `libs.common.Insight`
- Vector-indexed event context in the configured `SearchStore`
- Cache entries for processed message hashes and LLM generations
- Dead-letter entries for malformed or poison messages

## Runtime Endpoints

- `GET /`
- `GET /health`
- `GET /metrics`

Run locally:

```bash
uvicorn services.ai.app:app --host 0.0.0.0 --port 8003
```

## Dependencies

- Python standard library
- `fastapi`
- `uvicorn`
- `pydantic`
- `structlog`
- Shared offline-safe ports from `libs/common`
- Existing `services.ai.llm` and `services.ai.rag` modules

The tested path is fully offline with `MockLLMProvider`, `InMemorySearchStore`,
`InMemoryCache`, and `InMemoryBus`.

## Usage

```python
from libs.common import InMemoryBus, InMemoryCache, InMemorySearchStore
from services.ai.llm import MockLLMProvider
from services.ai.rag import RAGPipeline
from services.ai.service import AIAnalysisService

bus = InMemoryBus()
cache = InMemoryCache()
search_store = InMemorySearchStore()
provider = MockLLMProvider()

service = AIAnalysisService(
    bus=bus,
    cache=cache,
    search_store=search_store,
    rag_pipeline=RAGPipeline(
        search_store=search_store,
        embedding_provider=provider,
    ),
    llm_provider=provider,
)
await bus.receive("news.raw", "ai", max_messages=0)  # prime in-memory subscriptions
await bus.receive("signals", "ai", max_messages=0)
await service.poll_once()
```

## Notes

- The AutoGen path is import-guarded. If `autogen` is not installed, the service uses a
  deterministic event detector without changing the runtime contract.
- LLM cache keys include stable content hashes, prompt shape, provider, and retrieved context.
  Processed markers use source event IDs, so duplicate deliveries stay quiet while future
  identical content can still publish a fresh insight and reuse the LLM result.
