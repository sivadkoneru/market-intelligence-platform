"""RAG document, chunk, and retrieval models."""

from __future__ import annotations

from dataclasses import dataclass, field
from typing import Any

from services.ai.llm.models import ContextDocument


@dataclass(frozen=True)
class SourceDocument:
    """Input document to chunk and index for retrieval."""

    doc_id: str
    text: str
    title: str | None = None
    url: str | None = None
    metadata: dict[str, Any] = field(default_factory=dict)

    @property
    def citation(self) -> str:
        return self.url or str(self.metadata.get("citation") or self.doc_id)


@dataclass(frozen=True)
class ChunkingConfig:
    """Deterministic chunking controls for paragraph and sentence splitting."""

    max_chars: int = 700
    max_tokens: int = 180
    overlap_chars: int = 120
    overlap_tokens: int = 30


@dataclass(frozen=True)
class ChunkDocument:
    """Chunked source content suitable for embeddings and retrieval."""

    chunk_id: str
    source_doc_id: str
    chunk_index: int
    text: str
    title: str | None = None
    url: str | None = None
    metadata: dict[str, Any] = field(default_factory=dict)
    start_char: int = 0
    end_char: int = 0
    token_count: int = 0

    @property
    def citation(self) -> str:
        return self.url or str(self.metadata.get("citation") or self.source_doc_id)

    def to_search_document(self) -> dict[str, Any]:
        """Serialize chunk fields for SearchStore indexing."""
        return {
            "chunk_id": self.chunk_id,
            "source_doc_id": self.source_doc_id,
            "chunk_index": self.chunk_index,
            "text": self.text,
            "title": self.title,
            "url": self.url,
            "citation": self.citation,
            "metadata": dict(self.metadata),
            "start_char": self.start_char,
            "end_char": self.end_char,
            "token_count": self.token_count,
        }

    @classmethod
    def from_search_result(cls, row: dict[str, Any]) -> "ChunkDocument":
        """Normalize a SearchStore row into a chunk model."""
        metadata = dict(row.get("metadata") or {})
        citation = row.get("citation")
        if citation and "citation" not in metadata:
            metadata["citation"] = citation
        return cls(
            chunk_id=str(row.get("chunk_id") or row["_id"]),
            source_doc_id=str(row.get("source_doc_id") or row.get("doc_id") or row["_id"]),
            chunk_index=int(row.get("chunk_index", 0)),
            text=str(row.get("text", "")),
            title=row.get("title"),
            url=row.get("url"),
            metadata=metadata,
            start_char=int(row.get("start_char", 0)),
            end_char=int(row.get("end_char", 0)),
            token_count=int(row.get("token_count", 0)),
        )

    def to_context_document(
        self,
        *,
        knn_score: float,
        lexical_score: float,
        ranking_score: float,
    ) -> ContextDocument:
        """Convert a retrieved chunk into the LLM context shape."""
        metadata = dict(self.metadata)
        metadata.update(
            {
                "source_doc_id": self.source_doc_id,
                "chunk_id": self.chunk_id,
                "chunk_index": self.chunk_index,
                "citation": self.citation,
                "knn_score": knn_score,
                "lexical_score": lexical_score,
                "ranking_score": ranking_score,
                "start_char": self.start_char,
                "end_char": self.end_char,
                "token_count": self.token_count,
            }
        )
        return ContextDocument(
            doc_id=self.chunk_id,
            text=self.text,
            title=self.title,
            url=self.url,
            metadata=metadata,
        )


@dataclass(frozen=True)
class RetrievedChunk:
    """Retrieved chunk plus ranking details."""

    chunk: ChunkDocument
    knn_score: float
    lexical_score: float
    ranking_score: float
