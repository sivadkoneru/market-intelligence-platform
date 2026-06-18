from __future__ import annotations

from dataclasses import dataclass

import pytest

from libs.common import InMemorySearchStore
from services.ai.llm import (
    EmbeddingRequest,
    EmbeddingResult,
    GenerationRequest,
    MockLLMProvider,
)
from services.ai.rag import (
    ChunkingConfig,
    RAGPipeline,
    SourceDocument,
    chunk_document,
    rerank_retrieved_chunks,
)


@dataclass
class FakeEmbeddingProvider:
    mapping: dict[str, tuple[float, ...]]
    dimensions: int = 3

    async def embed(self, request: EmbeddingRequest) -> EmbeddingResult:
        vectors = tuple(self.mapping[text] for text in request.texts)
        return EmbeddingResult(
            vectors=vectors,
            provider="fake",
            model="fake-embedder",
            dimensions=self.dimensions,
        )


def _source_document() -> SourceDocument:
    return SourceDocument(
        doc_id="news-42",
        title="Bitcoin miners report stronger margins",
        url="https://example.test/news-42",
        text=(
            "Bitcoin miners reported stronger margins after the latest rally. "
            "Analysts said revenue improved because network demand stayed strong.\n\n"
            "ETF inflows remained positive through the week. "
            "Traders described the tone as bullish despite macro uncertainty."
        ),
        metadata={"source": "news", "symbol": "BTCUSDT"},
    )


def test_chunk_document_respects_boundaries_overlap_and_metadata() -> None:
    chunks = chunk_document(
        _source_document(),
        ChunkingConfig(
            max_chars=120,
            max_tokens=16,
            overlap_chars=35,
            overlap_tokens=5,
        ),
    )

    assert len(chunks) >= 2
    assert chunks[0].text.startswith("Bitcoin miners reported stronger margins")
    assert any("ETF inflows remained positive" in chunk.text for chunk in chunks[1:])
    assert chunks[0].metadata["source"] == "news"
    assert chunks[0].citation == "https://example.test/news-42"
    assert all(chunk.metadata["citation"] == "https://example.test/news-42" for chunk in chunks)
    assert chunks[1].text.split()[:5] == chunks[0].text.split()[-5:]
    assert chunks[0].start_char < chunks[0].end_char <= len(_source_document().text)


def test_chunk_document_keeps_overlap_within_limits_and_source_bounds() -> None:
    document = SourceDocument(
        doc_id="bounded",
        text=(
            "Alpha market participants described steady liquidity and resilient demand. "
            "Beta desks reported tighter spreads and stronger order books. "
            "Gamma analysts still warned about macro risk and thinner weekend depth."
        ),
    )

    chunks = chunk_document(
        document,
        ChunkingConfig(
            max_chars=80,
            max_tokens=20,
            overlap_chars=45,
            overlap_tokens=8,
        ),
    )

    assert len(chunks) >= 2
    assert all(len(chunk.text) <= 80 for chunk in chunks)
    assert all(chunk.token_count <= 20 for chunk in chunks)
    assert all(0 <= chunk.start_char < chunk.end_char <= len(document.text) for chunk in chunks)


@pytest.mark.asyncio
async def test_index_source_document_stores_vectors_and_chunk_payloads() -> None:
    store = InMemorySearchStore()
    document = _source_document()
    chunks = chunk_document(document, ChunkingConfig(max_chars=140, max_tokens=20))
    mapping = {
        chunk.text: (float(index + 1), 0.0, 0.0)
        for index, chunk in enumerate(chunks)
    }
    pipeline = RAGPipeline(
        search_store=store,
        embedding_provider=FakeEmbeddingProvider(mapping),
        index_name="rag-index",
        chunking=ChunkingConfig(max_chars=140, max_tokens=20),
        embedding_dimensions=3,
    )

    indexed = await pipeline.index_source_document(document)

    assert tuple(chunk.chunk_id for chunk in indexed) == tuple(chunk.chunk_id for chunk in chunks)
    stored = store._indices["rag-index"]
    first_entry = stored[indexed[0].chunk_id]
    assert list(first_entry["_vector"]) == [1.0, 0.0, 0.0]
    assert first_entry["_doc"]["citation"] == "https://example.test/news-42"
    assert first_entry["_doc"]["metadata"]["symbol"] == "BTCUSDT"
    assert store._vector_indices["rag-index"]["dimensions"] == 3


@pytest.mark.asyncio
async def test_knn_retrieval_and_context_assembly_preserve_scores_and_citations() -> None:
    store = InMemorySearchStore()
    pipeline = RAGPipeline(
        search_store=store,
        embedding_provider=FakeEmbeddingProvider(
            {
                "Bitcoin sentiment and ETF flows": (1.0, 0.0, 0.0),
                "ETF inflows remained positive and bullish": (1.0, 0.0, 0.0),
                "Macro uncertainty kept traders cautious": (0.0, 1.0, 0.0),
            }
        ),
        index_name="rag-index",
        chunking=ChunkingConfig(max_chars=200, max_tokens=40),
        embedding_dimensions=3,
    )

    await store.index_document(
        "rag-index",
        "chunk-1",
        {
            "chunk_id": "chunk-1",
            "source_doc_id": "news-1",
            "chunk_index": 0,
            "text": "ETF inflows remained positive and bullish",
            "title": "ETF inflows stay positive",
            "url": "https://example.test/etf",
            "citation": "https://example.test/etf",
            "metadata": {"source": "news", "symbol": "BTCUSDT"},
        },
        vector=[1.0, 0.0, 0.0],
    )
    await store.index_document(
        "rag-index",
        "chunk-2",
        {
            "chunk_id": "chunk-2",
            "source_doc_id": "news-2",
            "chunk_index": 0,
            "text": "Macro uncertainty kept traders cautious",
            "title": "Mixed macro backdrop",
            "url": "https://example.test/macro",
            "citation": "https://example.test/macro",
            "metadata": {"source": "news", "symbol": "BTCUSDT"},
        },
        vector=[0.0, 1.0, 0.0],
    )

    retrieved = await pipeline.retrieve_chunks("Bitcoin sentiment and ETF flows", top_k=2)
    context = await pipeline.build_context_documents("Bitcoin sentiment and ETF flows", top_k=2)

    assert retrieved[0].chunk.chunk_id == "chunk-1"
    assert retrieved[0].knn_score > retrieved[1].knn_score
    assert context[0].citation == "https://example.test/etf"
    assert context[0].metadata["source_doc_id"] == "news-1"
    assert context[0].metadata["knn_score"] == retrieved[0].knn_score
    assert context[0].metadata["ranking_score"] == retrieved[0].ranking_score


@pytest.mark.asyncio
async def test_context_uses_metadata_only_citation_for_guardrails() -> None:
    store = InMemorySearchStore()
    pipeline = RAGPipeline(
        search_store=store,
        embedding_provider=FakeEmbeddingProvider(
            {
                "BTC source citation": (1.0, 0.0, 0.0),
                "Bitcoin demand improved": (1.0, 0.0, 0.0),
            }
        ),
        index_name="rag-index",
        embedding_dimensions=3,
    )
    await store.index_document(
        "rag-index",
        "chunk-1",
        {
            "chunk_id": "chunk-1",
            "source_doc_id": "news-1",
            "chunk_index": 0,
            "text": "Bitcoin demand improved",
            "metadata": {"citation": "source:terminal-feed", "source": "news"},
        },
        vector=[1.0, 0.0, 0.0],
    )

    context = await pipeline.build_context_documents("BTC source citation", top_k=1)
    result = await MockLLMProvider().generate(
        GenerationRequest(prompt="Summarize BTC demand.", context=context)
    )

    assert context[0].citation == "source:terminal-feed"
    assert result.citations == ("source:terminal-feed",)
    assert result.grounded is True


@pytest.mark.asyncio
async def test_rag_pipeline_rejects_embedding_dimension_mismatch() -> None:
    document = SourceDocument(doc_id="news-1", text="Bitcoin demand improved.")
    pipeline = RAGPipeline(
        search_store=InMemorySearchStore(),
        embedding_provider=FakeEmbeddingProvider(
            {"Bitcoin demand improved.": (1.0, 0.0)},
            dimensions=2,
        ),
        index_name="rag-index",
        embedding_dimensions=3,
    )

    with pytest.raises(ValueError, match="reported 2 dimensions"):
        await pipeline.index_source_document(document)


def test_reranking_can_reorder_near_ties_by_query_overlap() -> None:
    rows = [
        {
            "_id": "chunk-a",
            "_score": 0.8,
            "chunk_id": "chunk-a",
            "source_doc_id": "news-a",
            "chunk_index": 0,
            "text": "prices moved higher on demand",
            "title": "Market wrap",
            "metadata": {"source": "news"},
        },
        {
            "_id": "chunk-b",
            "_score": 0.78,
            "chunk_id": "chunk-b",
            "source_doc_id": "news-b",
            "chunk_index": 0,
            "text": "ETF inflows stayed positive for bitcoin",
            "title": "ETF inflows positive",
            "metadata": {"source": "news"},
        },
    ]

    reranked = rerank_retrieved_chunks("bitcoin ETF inflows", rows)

    assert reranked[0].chunk.chunk_id == "chunk-b"
    assert reranked[0].knn_score < reranked[1].knn_score
    assert reranked[0].lexical_score > reranked[1].lexical_score
    assert reranked[0].ranking_score > reranked[1].ranking_score


@pytest.mark.asyncio
async def test_context_documents_work_with_mock_generation() -> None:
    store = InMemorySearchStore()
    embedder = FakeEmbeddingProvider(
        {
            "Why is BTC sentiment positive?": (1.0, 0.0, 0.0),
            "Bitcoin miners reported stronger margins after the latest rally.": (1.0, 0.0, 0.0),
        }
    )
    pipeline = RAGPipeline(
        search_store=store,
        embedding_provider=embedder,
        index_name="rag-index",
        embedding_dimensions=3,
    )
    await store.index_document(
        "rag-index",
        "chunk-1",
        {
            "chunk_id": "chunk-1",
            "source_doc_id": "news-42",
            "chunk_index": 0,
            "text": "Bitcoin miners reported stronger margins after the latest rally.",
            "title": "Miners report stronger margins",
            "url": "https://example.test/news-42",
            "citation": "https://example.test/news-42",
            "metadata": {"source": "news", "symbol": "BTCUSDT"},
        },
        vector=[1.0, 0.0, 0.0],
    )

    context = await pipeline.build_context_documents("Why is BTC sentiment positive?", top_k=1)
    result = await MockLLMProvider().generate(
        GenerationRequest(
            prompt="Why is BTC sentiment positive?",
            context=context,
            metadata={"symbol": "BTCUSDT"},
        )
    )

    assert context[0].metadata["citation"] == "https://example.test/news-42"
    assert context[0].title == "Miners report stronger margins"
    assert result.citations == ("https://example.test/news-42",)
    assert result.grounded is True
    assert "stronger margins" in result.summary
