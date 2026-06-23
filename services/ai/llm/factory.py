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
        api_key=settings.openai_api_key or "local",
        base_url=settings.openai_base_url,
        chat_model=settings.openai_chat_model,
        embedding_model=settings.openai_embedding_model,
        client=client,
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
    return _build_openai(resolved, openai_client)


def get_embedding_provider(
    settings: Settings | None = None,
    *,
    openai_client: Any | None = None,
) -> EmbeddingProvider:
    """Return the embedding provider: mock when offline, else OpenAI-compatible."""

    resolved = _resolved_settings(settings)
    if resolved.mock_llm:
        return MockLLMProvider()
    return _build_openai(resolved, openai_client)


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
