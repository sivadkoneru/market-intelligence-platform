# services/ai/rag

Lightweight retrieval-augmented generation helpers for the AI service. This module performs deterministic semantic chunking, embeds chunks with the existing T10 embedding providers, stores them through the `SearchStore` port, retrieves them with Elasticsearch-compatible kNN search, and assembles grounded `ContextDocument` objects for LLM generation.

Portfolio only: no financial advice, no real trades, no real capital at risk.

## Purpose

- Chunk source documents into stable paragraph/sentence-based segments
- Create or validate the `embedding` vector mapping before kNN writes/queries
- Index chunk embeddings into `SearchStore.index_document(..., vector=...)`
- Retrieve candidate chunks with `SearchStore.knn_search(...)`
- Apply lightweight deterministic lexical reranking on near-ties
- Return `ContextDocument` objects with citations and metadata for generation

## Usage

```python
from libs.common import InMemorySearchStore
from services.ai.llm import MockLLMProvider
from services.ai.rag import RAGPipeline, SourceDocument

pipeline = RAGPipeline(
    search_store=InMemorySearchStore(),
    embedding_provider=MockLLMProvider(),
    index_name="news-rag",
)

document = SourceDocument(
    doc_id="news-1",
    title="ETF inflows stay positive",
    url="https://example.test/news-1",
    text="ETF inflows remained positive.\n\nAnalysts described the tone as bullish.",
    metadata={"source": "news", "symbol": "BTCUSDT"},
)

await pipeline.index_source_document(document)
context = await pipeline.build_context_documents("Why is BTC sentiment bullish?", top_k=2)
```

## Inputs and outputs

- Input document: `SourceDocument` with `doc_id`, `text`, optional `title`, `url`, and `metadata`
- Indexed chunk: `ChunkDocument` with source linkage, chunk offsets, token count, metadata, and citation
- Retrieval output: `RetrievedChunk` with `knn_score`, `lexical_score`, and final `ranking_score`
- Generation context: `services.ai.llm.ContextDocument` with citation and ranking metadata

## Dependencies

- Existing `EmbeddingProvider` implementations from `services.ai.llm`
- Existing `SearchStore` port from `libs.common.es`, including `ensure_vector_index()`
- Standard-library-only chunking and reranking logic

Optional production orchestration frameworks like LangChain or LlamaIndex can sit above this module, but they are not required and are not imported here.

## Offline behavior

- Tests use `InMemorySearchStore` and deterministic mock embeddings only
- No network access, secrets, or live Elasticsearch are required for the tested path
- Chunking and reranking are pure, deterministic, and directly unit-testable
