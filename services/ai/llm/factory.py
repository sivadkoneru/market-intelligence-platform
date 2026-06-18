"""Factories that select the right LLM and embedding providers for the current mode."""

from __future__ import annotations

from dataclasses import dataclass
from typing import Any

from libs.common.config import Settings, get_settings
from services.ai.llm.providers import (
    AnthropicProvider,
    AzureOpenAIProvider,
    EmbeddingProvider,
    LLMProvider,
    MockLLMProvider,
)


@dataclass(frozen=True)
class ProviderBundle:
    """Resolved providers for generation and embeddings."""

    generator: LLMProvider
    embedder: EmbeddingProvider


def _resolved_settings(settings: Settings | None) -> Settings:
    return settings or get_settings()


def _has_azure_credentials(settings: Settings) -> bool:
    return bool(settings.azure_openai_api_key and settings.azure_openai_endpoint)


def _has_anthropic_credentials(settings: Settings) -> bool:
    return bool(settings.anthropic_api_key)


def _mock_allowed(settings: Settings, explicit_provider: str) -> bool:
    return settings.mock_llm and explicit_provider in {"auto", "mock"}


def _explicit_missing_credentials(provider: str) -> RuntimeError:
    if provider == "azure_openai":
        return RuntimeError(
            "LLM provider 'azure_openai' requires AZURE_OPENAI_API_KEY and "
            "AZURE_OPENAI_ENDPOINT."
        )
    if provider == "anthropic":
        return RuntimeError(
            "LLM provider 'anthropic' requires ANTHROPIC_API_KEY."
        )
    return RuntimeError(f"Unknown LLM provider selection: {provider}")


def _explicit_missing_embedding_credentials(provider: str) -> RuntimeError:
    if provider == "azure_openai":
        return RuntimeError(
            "Embedding provider 'azure_openai' requires AZURE_OPENAI_API_KEY and "
            "AZURE_OPENAI_ENDPOINT."
        )
    return RuntimeError(f"Unknown embedding provider selection: {provider}")


def get_llm_provider(
    settings: Settings | None = None,
    *,
    azure_client: Any | None = None,
    anthropic_client: Any | None = None,
) -> LLMProvider:
    """Return the configured generation provider, defaulting to deterministic mock mode."""

    resolved = _resolved_settings(settings)
    selection = resolved.llm_provider

    if selection == "mock" or _mock_allowed(resolved, selection):
        return MockLLMProvider()

    if selection == "azure_openai":
        if not _has_azure_credentials(resolved):
            raise _explicit_missing_credentials(selection)
        return AzureOpenAIProvider(
            api_key=resolved.azure_openai_api_key,
            endpoint=resolved.azure_openai_endpoint,
            client=azure_client,
        )

    if selection == "anthropic":
        if not _has_anthropic_credentials(resolved):
            raise _explicit_missing_credentials(selection)
        return AnthropicProvider(
            api_key=resolved.anthropic_api_key,
            client=anthropic_client,
        )

    if _has_azure_credentials(resolved):
        return AzureOpenAIProvider(
            api_key=resolved.azure_openai_api_key,
            endpoint=resolved.azure_openai_endpoint,
            client=azure_client,
        )
    if _has_anthropic_credentials(resolved):
        return AnthropicProvider(
            api_key=resolved.anthropic_api_key,
            client=anthropic_client,
        )
    return MockLLMProvider()


def get_embedding_provider(
    settings: Settings | None = None,
    *,
    azure_client: Any | None = None,
) -> EmbeddingProvider:
    """Return the configured embedding provider, defaulting to deterministic mock mode."""

    resolved = _resolved_settings(settings)
    selection = resolved.embedding_provider

    if selection == "mock" or _mock_allowed(resolved, selection):
        return MockLLMProvider()

    if selection == "azure_openai":
        if not _has_azure_credentials(resolved):
            raise _explicit_missing_embedding_credentials(selection)
        return AzureOpenAIProvider(
            api_key=resolved.azure_openai_api_key,
            endpoint=resolved.azure_openai_endpoint,
            client=azure_client,
        )

    if _has_azure_credentials(resolved):
        return AzureOpenAIProvider(
            api_key=resolved.azure_openai_api_key,
            endpoint=resolved.azure_openai_endpoint,
            client=azure_client,
        )
    if resolved.mock_llm:
        return MockLLMProvider()
    raise RuntimeError(
        "No real embedding provider is configured. Configure Azure OpenAI embeddings "
        "(AZURE_OPENAI_API_KEY and AZURE_OPENAI_ENDPOINT) or use MOCK_LLM."
    )


def get_provider_bundle(
    settings: Settings | None = None,
    *,
    azure_client: Any | None = None,
    anthropic_client: Any | None = None,
) -> ProviderBundle:
    """Return a matched generator/embedder pair for the current runtime mode."""

    return ProviderBundle(
        generator=get_llm_provider(
            settings,
            azure_client=azure_client,
            anthropic_client=anthropic_client,
        ),
        embedder=get_embedding_provider(settings, azure_client=azure_client),
    )
