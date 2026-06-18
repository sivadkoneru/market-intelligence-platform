"""Pluggable LLM providers, embeddings, and guardrails for the AI service."""

from services.ai.llm.factory import (
    ProviderBundle,
    get_embedding_provider,
    get_llm_provider,
    get_provider_bundle,
)
from services.ai.llm.guardrails import apply_guardrails, evaluate_result
from services.ai.llm.models import (
    ChatMessage,
    ContextDocument,
    EmbeddingRequest,
    EmbeddingResult,
    GenerationRequest,
    GenerationResult,
    GuardrailReport,
)
from services.ai.llm.providers import (
    AnthropicProvider,
    AzureOpenAIProvider,
    EmbeddingProvider,
    LLMProvider,
    MockLLMProvider,
)

__all__ = [
    "AnthropicProvider",
    "apply_guardrails",
    "AzureOpenAIProvider",
    "ChatMessage",
    "ContextDocument",
    "EmbeddingProvider",
    "EmbeddingRequest",
    "EmbeddingResult",
    "evaluate_result",
    "GenerationRequest",
    "GenerationResult",
    "get_embedding_provider",
    "get_llm_provider",
    "get_provider_bundle",
    "GuardrailReport",
    "LLMProvider",
    "MockLLMProvider",
    "ProviderBundle",
]
