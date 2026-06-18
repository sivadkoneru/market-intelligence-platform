"""AI analysis and RAG service, including provider-neutral LLM helpers."""

from services.ai.app import app, build_default_service, create_app
from services.ai.llm import (
    ChatMessage,
    ContextDocument,
    EmbeddingProvider,
    EmbeddingRequest,
    EmbeddingResult,
    GenerationRequest,
    GenerationResult,
    LLMProvider,
    MockLLMProvider,
)
from services.ai.rag import (
    ChunkDocument,
    ChunkingConfig,
    RAGPipeline,
    RetrievedChunk,
    SourceDocument,
    chunk_document,
    rerank_retrieved_chunks,
)
from services.ai.service import (
    AI_SUBSCRIPTION,
    AIAnalysisService,
    AIMetrics,
    AutoGenCompatibleEventDetector,
    DeterministicEventDetector,
    EventDetectionResult,
)

__all__ = [
    "AI_SUBSCRIPTION",
    "AIAnalysisService",
    "AIMetrics",
    "AutoGenCompatibleEventDetector",
    "ChatMessage",
    "ChunkDocument",
    "ChunkingConfig",
    "ContextDocument",
    "DeterministicEventDetector",
    "EmbeddingProvider",
    "EmbeddingRequest",
    "EmbeddingResult",
    "EventDetectionResult",
    "GenerationRequest",
    "GenerationResult",
    "LLMProvider",
    "MockLLMProvider",
    "RAGPipeline",
    "app",
    "build_default_service",
    "RetrievedChunk",
    "SourceDocument",
    "chunk_document",
    "create_app",
    "rerank_retrieved_chunks",
]
