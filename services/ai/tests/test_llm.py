from __future__ import annotations

import sys
from types import SimpleNamespace

import pytest

from libs.common.config import Settings
from services.ai.llm import (
    ContextDocument,
    EmbeddingRequest,
    GenerationRequest,
    MockLLMProvider,
    OpenAIProvider,
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


def test_factory_defaults_to_mock_when_offline() -> None:
    default_settings = Settings()
    assert isinstance(get_llm_provider(default_settings), MockLLMProvider)
    assert isinstance(get_embedding_provider(default_settings), MockLLMProvider)

    bundle = get_provider_bundle(default_settings)
    assert isinstance(bundle.generator, MockLLMProvider)
    assert isinstance(bundle.embedder, MockLLMProvider)


def test_factory_uses_openai_when_live_with_api_key() -> None:
    settings = Settings(mock_llm=False, openai_api_key="sk-test")
    assert isinstance(get_llm_provider(settings), OpenAIProvider)
    assert isinstance(get_embedding_provider(settings), OpenAIProvider)

    bundle = get_provider_bundle(settings)
    assert isinstance(bundle.generator, OpenAIProvider)
    assert isinstance(bundle.embedder, OpenAIProvider)


def test_factory_keeps_mock_when_mock_llm_set_even_with_key() -> None:
    settings = Settings(mock_llm=True, openai_api_key="sk-test")
    assert isinstance(get_llm_provider(settings), MockLLMProvider)
    assert isinstance(get_embedding_provider(settings), MockLLMProvider)

def test_factory_injects_configured_model_names() -> None:
    provider = get_llm_provider(
        Settings(
            mock_llm=False,
            openai_api_key="sk-test",
            openai_base_url="https://proxy.example/v1",
            openai_chat_model="gpt-custom",
            openai_embedding_model="embed-custom",
        )
    )
    assert isinstance(provider, OpenAIProvider)
    assert provider._chat_model == "gpt-custom"
    assert provider._embedding_model == "embed-custom"
    assert provider._base_url == "https://proxy.example/v1"


def test_context_document_citation_prefers_url_then_metadata() -> None:
    assert (
        ContextDocument(
            doc_id="doc-1",
            url="https://example.test/doc-1",
            text="one",
            metadata={"citation": "source:feed"},
        ).citation
        == "https://example.test/doc-1"
    )
    assert (
        ContextDocument(
            doc_id="doc-2",
            text="two",
            metadata={"citation": "source:feed"},
        ).citation
        == "source:feed"
    )


def test_import_guard_raises_helpful_error_without_openai_sdk(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    monkeypatch.setitem(sys.modules, "openai", None)
    with pytest.raises(RuntimeError, match="openai package is required"):
        OpenAIProvider(api_key="key")._get_client()


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


class _DimensionIgnoringOpenAIClient(_FakeOpenAIClient):
    async def _embedding_create(self, **kwargs: object) -> object:
        self.embedding_calls.append(dict(kwargs))
        return {
            "data": [
                {"embedding": [0.1, 0.2, 0.3, 0.4]},
                {"embedding": [0.5]},
            ]
        }


class _TextOnlyOpenAIClient(_FakeOpenAIClient):
    async def _chat_create(self, **kwargs: object) -> object:
        self.chat_calls.append(dict(kwargs))
        return {
            "choices": [
                {
                    "message": {
                        "content": (
                            "BTC sentiment looks constructive because ETF flow context "
                            "supports stronger demand."
                        )
                    }
                }
            ]
        }


class _EmptyOpenAIClient(_FakeOpenAIClient):
    async def _chat_create(self, **kwargs: object) -> object:
        self.chat_calls.append(dict(kwargs))
        return {"choices": [{"message": {"content": ""}}]}


@pytest.mark.asyncio
async def test_openai_provider_parses_fake_chat_and_embeddings() -> None:
    fake_client = _FakeOpenAIClient()
    provider = OpenAIProvider(
        api_key="key",
        base_url="https://api.openai.com/v1",
        chat_model="gpt-test",
        embedding_model="embed-test",
        client=fake_client,
    )

    request = _request()
    result = await provider.generate(request)
    embeddings = await provider.embed(EmbeddingRequest(texts=("a", "b"), dimensions=3))

    assert result.provider == "openai"
    assert result.model == "gpt-test"
    assert result.grounded is True
    assert result.sentiment_score == 0.64
    assert embeddings.provider == "openai"
    assert embeddings.model == "embed-test"
    assert embeddings.vectors == ((0.1, 0.2, 0.3), (0.3, 0.2, 0.1))
    assert fake_client.chat_calls[0]["model"] == "gpt-test"
    assert fake_client.embedding_calls[0]["model"] == "embed-test"
    assert fake_client.embedding_calls[0]["dimensions"] == 3
    assert isinstance(fake_client.chat_calls[0]["messages"], list)


@pytest.mark.asyncio
async def test_openai_provider_coerces_embedding_dimensions() -> None:
    fake_client = _DimensionIgnoringOpenAIClient()
    provider = OpenAIProvider(
        api_key="key",
        base_url="https://api.openai.com/v1",
        chat_model="gpt-test",
        embedding_model="embed-test",
        client=fake_client,
    )

    embeddings = await provider.embed(EmbeddingRequest(texts=("a", "b"), dimensions=3))

    assert embeddings.dimensions == 3
    assert embeddings.vectors == ((0.1, 0.2, 0.3), (0.5, 0.0, 0.0))
    assert fake_client.embedding_calls[0]["dimensions"] == 3


@pytest.mark.asyncio
async def test_openai_provider_falls_back_for_non_json_chat_response() -> None:
    fake_client = _TextOnlyOpenAIClient()
    provider = OpenAIProvider(
        api_key="key",
        base_url="https://api.openai.com/v1",
        chat_model="gpt-test",
        embedding_model="embed-test",
        client=fake_client,
    )

    result = await provider.generate(_request())

    assert result.provider == "openai"
    assert result.model == "gpt-test"
    assert result.sentiment_label == "positive"
    assert result.citations == ("https://example.test/btc-1", "https://example.test/btc-2")
    assert result.grounded is True


@pytest.mark.asyncio
async def test_openai_provider_fallback_summarizes_empty_chat_from_context() -> None:
    fake_client = _EmptyOpenAIClient()
    provider = OpenAIProvider(
        api_key="key",
        base_url="https://api.openai.com/v1",
        chat_model="gpt-test",
        embedding_model="embed-test",
        client=fake_client,
    )

    result = await provider.generate(_request())

    assert result.provider == "openai"
    assert "Model response did not include a summary" not in result.summary
    assert result.summary.startswith("Positive outlook for BTCUSDT")
    assert "Bitcoin miners reported stronger margins" in result.summary
    assert "https://example.test/btc-1" in result.explanation
    assert result.grounded is True


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
