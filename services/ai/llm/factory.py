"""Factories that select the LLM and embedding providers for the current mode.

A single OpenAI-compatible provider covers every live backend (OpenAI, Azure OpenAI,
Anthropic, OpenRouter, local servers) via ``OPENAI_BASE_URL``. The deterministic
``MockLLMProvider`` is used whenever ``MOCK_LLM`` is set, keeping the tested path offline.
"""

from __future__ import annotations

from dataclasses import dataclass
from typing import Any

from libs.common.config import Settings, get_settings
from services.ai.llm.providers import (
    EmbeddingProvider,
    LLMProvider,
    MockLLMProvider,
    OpenAIProvider,
)


@dataclass(frozen=True)
class ProviderBundle:
    """Resolved providers for generation and embeddings."""

    generator: LLMProvider
    embedder: EmbeddingProvider


def _resolved_settings(settings: Settings | None) -> Settings:
    return settings or get_settings()


def _build_openai(settings: Settings, client: Any | None) -> OpenAIProvider:
    return OpenAIProvider(
        api_key=settings.openai_api_key,
        base_url=settings.openai_base_url,
        chat_model=settings.openai_chat_model,
        embedding_model=settings.openai_embedding_model,
        client=client,
    )


def _missing_api_key() -> RuntimeError:
    return RuntimeError(
        "No LLM provider configured. Set OPENAI_API_KEY (any OpenAI-compatible endpoint "
        "via OPENAI_BASE_URL), or set MOCK_LLM=1 for offline mode."
    )


def get_llm_provider(
    settings: Settings | None = None,
    *,
    openai_client: Any | None = None,
) -> LLMProvider:
    """Return the generation provider: mock when offline, else OpenAI-compatible."""

    resolved = _resolved_settings(settings)
    if resolved.mock_llm:
        return MockLLMProvider()
    if resolved.openai_api_key:
        return _build_openai(resolved, openai_client)
    raise _missing_api_key()


def get_embedding_provider(
    settings: Settings | None = None,
    *,
    openai_client: Any | None = None,
) -> EmbeddingProvider:
    """Return the embedding provider: mock when offline, else OpenAI-compatible."""

    resolved = _resolved_settings(settings)
    if resolved.mock_llm:
        return MockLLMProvider()
    if resolved.openai_api_key:
        return _build_openai(resolved, openai_client)
    raise _missing_api_key()


def get_provider_bundle(
    settings: Settings | None = None,
    *,
    openai_client: Any | None = None,
) -> ProviderBundle:
    """Return a matched generator/embedder pair for the current runtime mode."""

    return ProviderBundle(
        generator=get_llm_provider(settings, openai_client=openai_client),
        embedder=get_embedding_provider(settings, openai_client=openai_client),
    )
