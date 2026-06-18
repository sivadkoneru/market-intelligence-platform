"""Provider-neutral request/response models for LLM generation and embeddings."""

from __future__ import annotations

from dataclasses import dataclass, field
from typing import Any, Literal

Role = Literal["system", "user", "assistant"]


@dataclass(frozen=True)
class ContextDocument:
    """Grounding material retrieved for an LLM request."""

    doc_id: str
    text: str
    title: str | None = None
    url: str | None = None
    metadata: dict[str, Any] = field(default_factory=dict)

    @property
    def citation(self) -> str:
        return self.url or str(self.metadata.get("citation") or self.doc_id)


@dataclass(frozen=True)
class ChatMessage:
    """Chat message in a provider-neutral shape."""

    role: Role
    content: str


@dataclass(frozen=True)
class GenerationRequest:
    """Structured LLM generation request."""

    prompt: str
    system_prompt: str | None = None
    context: tuple[ContextDocument, ...] = ()
    messages: tuple[ChatMessage, ...] = ()
    temperature: float = 0.0
    max_tokens: int = 500
    metadata: dict[str, Any] = field(default_factory=dict)


@dataclass(frozen=True)
class EmbeddingRequest:
    """Batch embedding request."""

    texts: tuple[str, ...]
    dimensions: int = 16


@dataclass(frozen=True)
class EmbeddingResult:
    """Embedding vectors returned by a provider."""

    vectors: tuple[tuple[float, ...], ...]
    provider: str
    model: str
    dimensions: int


@dataclass(frozen=True)
class GenerationResult:
    """Normalized generation output suitable for downstream insight creation."""

    summary: str
    explanation: str
    sentiment_score: float
    sentiment_label: str
    citations: tuple[str, ...]
    confidence: float
    grounded: bool
    provider: str
    model: str
    raw_text: str
    metadata: dict[str, Any] = field(default_factory=dict)


@dataclass(frozen=True)
class GuardrailReport:
    """Evaluation details for lightweight groundedness checks."""

    accepted: bool
    grounded: bool
    confidence_ok: bool
    citations_ok: bool
    overlap_ok: bool
    refusal_detected: bool
    empty_output: bool
    reasons: tuple[str, ...]
