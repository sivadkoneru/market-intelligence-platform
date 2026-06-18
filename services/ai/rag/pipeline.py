"""Lightweight RAG pipeline over SearchStore kNN retrieval."""

from __future__ import annotations

import re
from dataclasses import dataclass

from libs.common import SearchStore
from services.ai.llm import (
    ContextDocument,
    EmbeddingProvider,
    EmbeddingRequest,
    EmbeddingResult,
)
from services.ai.rag.chunking import chunk_document
from services.ai.rag.models import (
    ChunkDocument,
    ChunkingConfig,
    RetrievedChunk,
    SourceDocument,
)

_TOKEN_PATTERN = re.compile(r"[a-z0-9]+")


def _tokenize(text: str) -> set[str]:
    return set(_TOKEN_PATTERN.findall(text.lower()))


def lexical_overlap_score(query: str, chunk: ChunkDocument) -> float:
    """Score lexical overlap between the query and chunk text/title/metadata."""
    query_terms = _tokenize(query)
    if not query_terms:
        return 0.0

    fields = [chunk.text]
    if chunk.title:
        fields.append(chunk.title)
    source = chunk.metadata.get("source")
    if source:
        fields.append(str(source))

    content_terms = _tokenize(" ".join(fields))
    if not content_terms:
        return 0.0

    overlap = len(query_terms & content_terms) / len(query_terms)
    title_bonus = 0.1 if chunk.title and query_terms & _tokenize(chunk.title) else 0.0
    return min(1.0, round(overlap + title_bonus, 6))


def rerank_retrieved_chunks(
    query: str,
    rows: list[dict[str, object]],
    *,
    lexical_weight: float = 0.04,
) -> tuple[RetrievedChunk, ...]:
    """
    Combine SearchStore kNN scores with a small lexical overlap bonus.

    The bonus is intentionally capped so it only reorders close neighbors.
    """
    reranked: list[RetrievedChunk] = []
    for row in rows:
        chunk = ChunkDocument.from_search_result(row)
        knn_score = float(row.get("_score", 0.0))
        lexical_score = lexical_overlap_score(query, chunk)
        ranking_score = knn_score + (lexical_score * lexical_weight)
        reranked.append(
            RetrievedChunk(
                chunk=chunk,
                knn_score=knn_score,
                lexical_score=lexical_score,
                ranking_score=ranking_score,
            )
        )

    reranked.sort(
        key=lambda item: (
            -item.ranking_score,
            -item.knn_score,
            -item.lexical_score,
            item.chunk.chunk_id,
        )
    )
    return tuple(reranked)


@dataclass
class RAGPipeline:
    """Chunk, embed, index, retrieve, and assemble context for generation."""

    search_store: SearchStore
    embedding_provider: EmbeddingProvider
    index_name: str = "rag-documents"
    chunking: ChunkingConfig = ChunkingConfig()
    embedding_dimensions: int = 16
    lexical_weight: float = 0.04

    def chunk_source_document(
        self,
        document: SourceDocument,
    ) -> tuple[ChunkDocument, ...]:
        return chunk_document(document, self.chunking)

    def _validate_embeddings(
        self,
        embeddings: EmbeddingResult,
        *,
        expected_count: int,
    ) -> None:
        if len(embeddings.vectors) != expected_count:
            raise ValueError(
                f"Embedding provider returned {len(embeddings.vectors)} vectors, "
                f"expected {expected_count}"
            )
        if embeddings.dimensions != self.embedding_dimensions:
            raise ValueError(
                f"Embedding provider reported {embeddings.dimensions} dimensions, "
                f"expected {self.embedding_dimensions}"
            )
        for vector in embeddings.vectors:
            if len(vector) != self.embedding_dimensions:
                raise ValueError(
                    f"Embedding vector has {len(vector)} dimensions, "
                    f"expected {self.embedding_dimensions}"
                )

    async def _embed_texts(self, texts: tuple[str, ...]) -> EmbeddingResult:
        embeddings = await self.embedding_provider.embed(
            EmbeddingRequest(texts=texts, dimensions=self.embedding_dimensions)
        )
        self._validate_embeddings(embeddings, expected_count=len(texts))
        return embeddings

    async def index_source_document(
        self,
        document: SourceDocument,
    ) -> tuple[ChunkDocument, ...]:
        chunks = self.chunk_source_document(document)
        if not chunks:
            return ()

        await self.search_store.ensure_vector_index(
            self.index_name,
            self.embedding_dimensions,
        )
        embeddings = await self._embed_texts(tuple(chunk.text for chunk in chunks))
        for chunk, vector in zip(chunks, embeddings.vectors, strict=True):
            await self.search_store.index_document(
                self.index_name,
                chunk.chunk_id,
                chunk.to_search_document(),
                vector=list(vector),
            )
        return chunks

    async def retrieve_chunks(
        self,
        query: str,
        *,
        top_k: int = 4,
        candidate_k: int | None = None,
    ) -> tuple[RetrievedChunk, ...]:
        if top_k <= 0:
            return ()

        await self.search_store.ensure_vector_index(
            self.index_name,
            self.embedding_dimensions,
        )
        embeddings = await self._embed_texts((query,))
        candidates = max(top_k, candidate_k or top_k)
        rows = await self.search_store.knn_search(
            self.index_name,
            list(embeddings.vectors[0]),
            k=candidates,
        )
        reranked = rerank_retrieved_chunks(
            query,
            rows,
            lexical_weight=self.lexical_weight,
        )
        return reranked[:top_k]

    async def build_context_documents(
        self,
        query: str,
        *,
        top_k: int = 4,
        candidate_k: int | None = None,
    ) -> tuple[ContextDocument, ...]:
        retrieved = await self.retrieve_chunks(query, top_k=top_k, candidate_k=candidate_k)
        return tuple(
            item.chunk.to_context_document(
                knn_score=item.knn_score,
                lexical_score=item.lexical_score,
                ranking_score=item.ranking_score,
            )
            for item in retrieved
        )
