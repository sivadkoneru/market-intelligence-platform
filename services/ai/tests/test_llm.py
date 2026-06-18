from __future__ import annotations

import sys
from types import SimpleNamespace

import pytest

from libs.common.config import Settings
from services.ai.llm import (
    AnthropicProvider,
    AzureOpenAIProvider,
    ChatMessage,
    ContextDocument,
    EmbeddingRequest,
    GenerationRequest,
    MockLLMProvider,
    apply_guardrails,
    evaluate_result,
    get_embedding_provider,
    get_llm_provider,
    get_provider_bundle,
)
from services.ai.llm.models import GenerationResult


def _request() -> GenerationRequest:
    return GenerationRequest(
        prompt="Explain the likely sentiment for BTC after the latest earnings and price rally.",
        context=(
            ContextDocument(
                doc_id="doc-1",
                url="https://example.test/btc-1",
                title="Bitcoin miners report stronger margins",
                text=(
                    "Bitcoin miners reported stronger margins after a price rally "
                    "and improved demand."
                ),
            ),
            ContextDocument(
                doc_id="doc-2",
                url="https://example.test/btc-2",
                title="ETF inflows stay positive",
                text="ETF inflows remained positive and traders described the tone as bullish.",
            ),
        ),
        metadata={"symbol": "BTCUSDT"},
    )


@pytest.mark.asyncio
async def test_mock_generation_is_deterministic_and_grounded() -> None:
    provider = MockLLMProvider()
    request = _request()

    first = await provider.generate(request)
    second = await provider.generate(request)

    assert first == second
    assert first.provider == "mock"
    assert first.grounded is True
    assert first.citations == ("https://example.test/btc-1",)
    assert "Bitcoin miners reported stronger margins" in first.summary
    assert "https://example.test/btc-1" in first.explanation


@pytest.mark.asyncio
async def test_mock_embeddings_are_stable_and_have_expected_shape() -> None:
    provider = MockLLMProvider()
    request = EmbeddingRequest(texts=("btc news", "eth selloff"), dimensions=12)

    first = await provider.embed(request)
    second = await provider.embed(request)

    assert first == second
    assert first.dimensions == 12
    assert len(first.vectors) == 2
    assert all(len(vector) == 12 for vector in first.vectors)
    assert first.vectors[0] != first.vectors[1]


def test_factory_defaults_to_mock_and_prefers_configured_providers() -> None:
    default_settings = Settings()
    assert isinstance(get_llm_provider(default_settings), MockLLMProvider)
    assert isinstance(get_embedding_provider(default_settings), MockLLMProvider)

    azure_settings = Settings(
        mock_llm=False,
        azure_openai_api_key="key",
        azure_openai_endpoint="https://azure.example",
    )
    assert isinstance(get_llm_provider(azure_settings), AzureOpenAIProvider)
    assert isinstance(get_embedding_provider(azure_settings), AzureOpenAIProvider)

    anthropic_settings = Settings(
        mock_llm=False,
        anthropic_api_key="anthropic-key",
    )
    assert isinstance(get_llm_provider(anthropic_settings), AnthropicProvider)
    with pytest.raises(RuntimeError, match="Configure Azure OpenAI embeddings"):
        get_embedding_provider(anthropic_settings)

    bundle = get_provider_bundle(azure_settings)
    assert isinstance(bundle.generator, AzureOpenAIProvider)
    assert isinstance(bundle.embedder, AzureOpenAIProvider)


def test_factory_honors_explicit_provider_selection_with_multiple_credentials() -> None:
    settings = Settings(
        mock_llm=False,
        llm_provider="anthropic",
        azure_openai_api_key="azure-key",
        azure_openai_endpoint="https://azure.example",
        anthropic_api_key="anthropic-key",
    )
    assert isinstance(get_llm_provider(settings), AnthropicProvider)

    azure_settings = Settings(
        mock_llm=True,
        llm_provider="azure_openai",
        azure_openai_api_key="azure-key",
        azure_openai_endpoint="https://azure.example",
        anthropic_api_key="anthropic-key",
    )
    assert isinstance(get_llm_provider(azure_settings), AzureOpenAIProvider)


def test_factory_raises_helpful_errors_for_explicit_missing_credentials() -> None:
    with pytest.raises(RuntimeError, match="LLM provider 'azure_openai' requires"):
        get_llm_provider(Settings(mock_llm=False, llm_provider="azure_openai"))

    with pytest.raises(RuntimeError, match="LLM provider 'anthropic' requires"):
        get_llm_provider(Settings(mock_llm=False, llm_provider="anthropic"))

    with pytest.raises(RuntimeError, match="Embedding provider 'azure_openai' requires"):
        get_embedding_provider(Settings(mock_llm=False, embedding_provider="azure_openai"))


def test_factory_raises_for_anthropic_without_real_embeddings_when_not_in_mock_mode() -> None:
    settings = Settings(
        mock_llm=False,
        llm_provider="anthropic",
        embedding_provider="auto",
        anthropic_api_key="anthropic-key",
    )

    with pytest.raises(RuntimeError, match="Configure Azure OpenAI embeddings"):
        get_provider_bundle(settings)


def test_import_guards_raise_helpful_error_without_optional_sdks(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    monkeypatch.setitem(sys.modules, "openai", None)
    with pytest.raises(RuntimeError, match="openai package is required"):
        AzureOpenAIProvider(api_key="key", endpoint="https://azure.example")._get_client()

    monkeypatch.setitem(sys.modules, "anthropic", None)
    with pytest.raises(RuntimeError, match="anthropic package is required"):
        AnthropicProvider(api_key="key")._get_client()


class _FakeOpenAIClient:
    def __init__(self) -> None:
        self.chat = SimpleNamespace(completions=SimpleNamespace(create=self._chat_create))
        self.embeddings = SimpleNamespace(create=self._embedding_create)
        self.chat_calls: list[dict[str, object]] = []
        self.embedding_calls: list[dict[str, object]] = []

    async def _chat_create(self, **kwargs: object) -> object:
        self.chat_calls.append(dict(kwargs))
        return {
            "choices": [
                {
                    "message": {
                        "content": (
                            '{"summary":"Bullish tone from miner margins.",'
                            '"explanation":"Positive margin expansion is cited in '
                            'https://example.test/btc-1.",'
                            '"sentiment_score":0.64,'
                            '"sentiment_label":"positive",'
                            '"citations":["https://example.test/btc-1"],'
                            '"confidence":0.81}'
                        )
                    }
                }
            ]
        }

    async def _embedding_create(self, **kwargs: object) -> object:
        self.embedding_calls.append(dict(kwargs))
        return {
            "data": [
                {"embedding": [0.1, 0.2, 0.3]},
                {"embedding": [0.3, 0.2, 0.1]},
            ]
        }


class _FakeAnthropicClient:
    def __init__(self) -> None:
        self.messages = SimpleNamespace(create=self._messages_create)
        self.calls: list[dict[str, object]] = []

    async def _messages_create(self, **kwargs: object) -> object:
        self.calls.append(dict(kwargs))
        return {
            "content": [
                {
                    "type": "text",
                    "text": (
                        '{"summary":"Neutral headline impact.",'
                        '"explanation":"The explanation cites '
                        'https://example.test/btc-2 and stays grounded.",'
                        '"sentiment_score":0.1,'
                        '"sentiment_label":"neutral",'
                        '"citations":["https://example.test/btc-2"],'
                        '"confidence":0.67}'
                    ),
                }
            ]
        }


@pytest.mark.asyncio
async def test_azure_provider_parses_fake_chat_and_embeddings() -> None:
    fake_client = _FakeOpenAIClient()
    provider = AzureOpenAIProvider(
        api_key="key",
        endpoint="https://azure.example",
        chat_deployment="gpt-test",
        embedding_deployment="embed-test",
        client=fake_client,
    )

    request = _request()
    result = await provider.generate(request)
    embeddings = await provider.embed(EmbeddingRequest(texts=("a", "b"), dimensions=3))

    assert result.provider == "azure_openai"
    assert result.model == "gpt-test"
    assert result.grounded is True
    assert result.sentiment_score == 0.64
    assert embeddings.provider == "azure_openai"
    assert embeddings.model == "embed-test"
    assert embeddings.vectors == ((0.1, 0.2, 0.3), (0.3, 0.2, 0.1))
    assert fake_client.chat_calls[0]["model"] == "gpt-test"
    assert fake_client.embedding_calls[0]["model"] == "embed-test"
    assert isinstance(fake_client.chat_calls[0]["messages"], list)


@pytest.mark.asyncio
async def test_anthropic_provider_parses_fake_response() -> None:
    fake_client = _FakeAnthropicClient()
    provider = AnthropicProvider(
        api_key="anthropic-key",
        model="claude-test",
        client=fake_client,
    )

    result = await provider.generate(_request())

    assert result.provider == "anthropic"
    assert result.model == "claude-test"
    assert result.sentiment_label == "neutral"
    assert result.citations == ("https://example.test/btc-2",)
    assert fake_client.calls[0]["model"] == "claude-test"


@pytest.mark.asyncio
async def test_anthropic_provider_preserves_chat_history_roles() -> None:
    fake_client = _FakeAnthropicClient()
    provider = AnthropicProvider(
        api_key="anthropic-key",
        model="claude-test",
        client=fake_client,
    )
    request = GenerationRequest(
        prompt="ignored when explicit messages are present",
        system_prompt="Use only provided context.",
        messages=(
            ChatMessage(role="system", content="Cite sources directly."),
            ChatMessage(role="user", content="What changed for BTC today?"),
            ChatMessage(role="assistant", content="BTC rallied after margin improvements."),
            ChatMessage(role="user", content="Give me a grounded summary."),
        ),
        context=(
            ContextDocument(
                doc_id="doc-2",
                url="https://example.test/btc-2",
                title="ETF inflows stay positive",
                text="ETF inflows remained positive and traders described the tone as bullish.",
            ),
        ),
    )

    await provider.generate(request)

    assert fake_client.calls[0]["system"] == "Use only provided context.\n\nCite sources directly."
    assert fake_client.calls[0]["messages"] == [
        {"role": "user", "content": "What changed for BTC today?"},
        {"role": "assistant", "content": "BTC rallied after margin improvements."},
        {"role": "user", "content": "Give me a grounded summary."},
    ]


def test_guardrails_accept_grounded_output_and_reject_ungrounded_output() -> None:
    request = _request()
    grounded_result = GenerationResult(
        summary="Bullish tone from miner margins.",
        explanation="https://example.test/btc-1 supports a bullish reading from stronger margins.",
        sentiment_score=0.7,
        sentiment_label="positive",
        citations=("https://example.test/btc-1",),
        confidence=0.8,
        grounded=True,
        provider="mock",
        model="mock",
        raw_text="{}",
    )
    guarded, report = apply_guardrails(request, grounded_result)

    assert guarded.grounded is True
    assert report.accepted is True
    assert report.reasons == ()

    ungrounded_result = GenerationResult(
        summary="",
        explanation="I do not have enough information to answer this safely.",
        sentiment_score=0.0,
        sentiment_label="neutral",
        citations=("missing-doc",),
        confidence=0.2,
        grounded=True,
        provider="mock",
        model="mock",
        raw_text="{}",
    )
    rejected = evaluate_result(request, ungrounded_result)

    assert rejected.accepted is False
    assert rejected.grounded is False
    assert rejected.refusal_detected is True
    assert rejected.empty_output is False
    assert "low_confidence" in rejected.reasons
    assert "citation_mismatch" in rejected.reasons
