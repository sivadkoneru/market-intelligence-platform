from __future__ import annotations

import asyncio
from datetime import datetime, timezone
from typing import Any

import pytest
from fastapi.testclient import TestClient

from libs.common import (
    TOPIC_INSIGHTS,
    TOPIC_NEWS_RAW,
    TOPIC_SIGNALS,
    InMemoryBus,
    InMemoryCache,
    InMemorySearchStore,
    NewsEvent,
    Signal,
)
from services.ai.app import app, build_default_service, create_app
from services.ai.llm import GenerationRequest, GenerationResult, MockLLMProvider
from services.ai.rag import RAGPipeline
from services.ai.service import (
    AI_SUBSCRIPTION,
    AIAnalysisService,
    indexed_symbols,
    news_processed_key,
    signal_processed_key,
)


class CountingMockLLMProvider(MockLLMProvider):
    def __init__(self) -> None:
        super().__init__()
        self.generate_calls = 0

    async def generate(self, request: GenerationRequest) -> GenerationResult:
        self.generate_calls += 1
        return await super().generate(request)


class FailingLLMProvider(MockLLMProvider):
    async def generate(self, request: GenerationRequest) -> GenerationResult:
        raise RuntimeError("synthetic provider failure")


class UnguardedLLMProvider(MockLLMProvider):
    async def generate(self, request: GenerationRequest) -> GenerationResult:
        return GenerationResult(
            summary="Bitcoin rallied on unrelated rumors.",
            explanation="This cites missing-source and should be rejected by guardrails.",
            sentiment_score=0.7,
            sentiment_label="positive",
            citations=("missing-source",),
            confidence=0.9,
            grounded=True,
            provider="unguarded",
            model="unguarded-test",
            raw_text="{}",
        )


class FlakyReceiveBus(InMemoryBus):
    def __init__(self) -> None:
        super().__init__()
        self.failures_remaining = 0

    async def receive(
        self,
        topic: str,
        subscription: str,
        max_messages: int = 10,
    ):
        if self.failures_remaining:
            self.failures_remaining -= 1
            raise ConnectionError("service bus not ready")
        return await super().receive(topic, subscription, max_messages=max_messages)


def _news_message(
    *,
    symbol: str = "BTCUSDT",
    message_id: str = "news-1",
    title: str = "Bitcoin miners report stronger margins after ETF inflows stay positive",
    body: str = (
        "Bitcoin miners reported stronger margins after the latest rally. "
        "ETF inflows remained positive and analysts described sentiment as bullish."
    ),
) -> tuple[dict[str, Any], str]:
    payload = {
        "event_id": f"ev-{message_id}",
        "ts": datetime(2026, 1, 1, tzinfo=timezone.utc).isoformat(),
        "source": "newswire",
        "title": title,
        "body": body,
        "url": f"https://example.test/{message_id}",
        "symbols": [symbol],
        "author": "Reporter",
        "correlation_id": f"corr-{message_id}",
        "trace_id": f"trace-{message_id}",
    }
    return payload, message_id


def _signal_message(
    *,
    symbol: str = "ETHUSDT",
    message_id: str = "signal-1",
    anomaly: bool = True,
) -> tuple[dict[str, Any], str]:
    payload = {
        "event_id": f"ev-{message_id}",
        "ts": datetime(2026, 1, 1, 0, 1, tzinfo=timezone.utc).isoformat(),
        "symbol": symbol,
        "source": "stream",
        "indicators": {
            "sma": 2500.0,
            "ema": 2510.0,
            "rsi": 72.0,
            "trend": 1.0,
            "volatility": 0.34,
        },
        "anomaly": anomaly,
        "correlation_id": f"corr-{message_id}",
        "trace_id": f"trace-{message_id}",
    }
    return payload, message_id


async def _build_service(
    *,
    provider: MockLLMProvider | None = None,
) -> tuple[AIAnalysisService, InMemoryBus, InMemoryCache, InMemorySearchStore, MockLLMProvider]:
    bus = InMemoryBus()
    cache = InMemoryCache()
    search_store = InMemorySearchStore()
    resolved_provider = provider or MockLLMProvider()
    await bus.receive(TOPIC_NEWS_RAW, AI_SUBSCRIPTION, max_messages=0)
    await bus.receive(TOPIC_SIGNALS, AI_SUBSCRIPTION, max_messages=0)
    await bus.receive(TOPIC_INSIGHTS, "observer", max_messages=0)
    service = AIAnalysisService(
        bus=bus,
        cache=cache,
        search_store=search_store,
        rag_pipeline=RAGPipeline(
            search_store=search_store,
            embedding_provider=resolved_provider,
        ),
        llm_provider=resolved_provider,
    )
    return service, bus, cache, search_store, resolved_provider


@pytest.mark.asyncio
async def test_service_processes_news_with_rag_and_publishes_grounded_insight() -> None:
    service, bus, cache, search_store, _ = await _build_service()
    body, message_id = _news_message()

    await bus.publish(TOPIC_NEWS_RAW, body, message_id=message_id)
    processed = await service.poll_once(max_messages=1)

    assert processed == 1
    insights = await bus.peek(TOPIC_INSIGHTS, "observer", n=10)
    assert len(insights) == 1
    payload = insights[0].body
    assert payload["symbol"] == "BTCUSDT"
    assert payload["grounded"] is True
    assert payload["sentiment_label"] == "positive"
    assert payload["citations"] == ["https://example.test/news-1"]
    assert "Detected" in payload["explanation"]
    assert await cache.get_snapshot("BTCUSDT") is None
    assert service.metrics.messages_processed == 1
    assert service.metrics.insights_published == 1
    assert service.metrics.news_indexed == 1
    assert indexed_symbols(search_store, "rag-documents") == ()
    cached_insight = await cache.get("insight:BTCUSDT")
    assert cached_insight is not None
    assert cached_insight["symbol"] == "BTCUSDT"


@pytest.mark.asyncio
async def test_service_processes_signal_with_grounded_explanation() -> None:
    service, bus, _, search_store, _ = await _build_service()
    body, message_id = _signal_message()

    await bus.publish(TOPIC_SIGNALS, body, message_id=message_id)
    processed = await service.poll_once(max_messages=1)

    assert processed == 1
    insights = await bus.peek(TOPIC_INSIGHTS, "observer", n=10)
    assert len(insights) == 1
    payload = insights[0].body
    assert payload["symbol"] == "ETHUSDT"
    assert payload["grounded"] is True
    assert payload["citations"] == ["signal:ev-signal-1"]
    assert "anomaly event" in payload["explanation"].lower()
    assert service.metrics.signal_contexts_indexed == 1
    assert service.metrics.messages_processed == 1
    assert "ETHUSDT" in indexed_symbols(search_store, "rag-documents")
    assert service.metrics.insights_published == 1


@pytest.mark.asyncio
async def test_service_applies_guardrails_before_publish() -> None:
    service, bus, _, _, _ = await _build_service(provider=UnguardedLLMProvider())
    body, message_id = _news_message(message_id="unguarded")

    await bus.publish(TOPIC_NEWS_RAW, body, message_id=message_id)
    await service.poll_once(max_messages=1)

    insights = await bus.peek(TOPIC_INSIGHTS, "observer", n=10)
    assert len(insights) == 1
    assert insights[0].body["grounded"] is False
    assert insights[0].body["citations"] == ["missing-source"]


@pytest.mark.asyncio
async def test_service_suppresses_duplicate_news_and_reuses_llm_cache_when_retried() -> None:
    provider = CountingMockLLMProvider()
    service, bus, cache, _, _ = await _build_service(provider=provider)
    body, _ = _news_message(message_id="news-cache")
    duplicate_body = dict(body)
    event = NewsEvent.model_validate(body)
    processed_key = news_processed_key(event, symbol="BTCUSDT")

    await bus.publish(TOPIC_NEWS_RAW, body, message_id="delivery-a")
    await bus.publish(TOPIC_NEWS_RAW, duplicate_body, message_id="delivery-b")
    await service.poll_once(max_messages=10)

    assert provider.generate_calls == 1
    assert service.metrics.duplicates_suppressed == 1
    assert await cache.get(processed_key) is True

    future_body = dict(body)
    future_body["event_id"] = "ev-news-cache-later"
    future_processed_key = news_processed_key(
        NewsEvent.model_validate(future_body),
        symbol="BTCUSDT",
    )
    await bus.publish(TOPIC_NEWS_RAW, future_body, message_id="delivery-c")
    await service.poll_once(max_messages=1)

    assert provider.generate_calls == 1
    assert service.metrics.llm_cache_hits == 1
    insights = await bus.peek(TOPIC_INSIGHTS, "observer", n=10)
    assert len(insights) == 2
    assert await cache.get(future_processed_key) is True


@pytest.mark.asyncio
async def test_service_dead_letters_invalid_payloads() -> None:
    service, bus, _, _, _ = await _build_service()

    await bus.publish(
        TOPIC_NEWS_RAW,
        {
            "event_id": "bad-news",
            "source": "newswire",
            "title": "Malformed",
            "symbols": ["BTCUSDT"],
        },
        message_id="bad-news",
    )
    await service.poll_once(max_messages=1)

    assert service.metrics.dead_lettered == 1
    assert service.metrics.last_error is not None
    assert "invalid news event payload" in service.metrics.last_error
    dlq = await bus.receive_dead_letter(TOPIC_NEWS_RAW, AI_SUBSCRIPTION)
    assert len(dlq) == 1
    assert dlq[0].body["event_id"] == "bad-news"


@pytest.mark.asyncio
async def test_provider_failure_dead_letters_and_allows_retry() -> None:
    body, _ = _signal_message(message_id="retry-me")
    failing_service, bus, cache, search_store, _ = await _build_service(
        provider=FailingLLMProvider(),
    )

    await bus.publish(TOPIC_SIGNALS, body, message_id="delivery-a")
    await failing_service.poll_once(max_messages=1)

    assert failing_service.metrics.dead_lettered == 1
    assert failing_service.metrics.messages_processed == 0
    assert failing_service.metrics.processing_retries == 2
    assert await cache.get(signal_processed_key(Signal.model_validate(body))) is None
    dlq = await bus.receive_dead_letter(TOPIC_SIGNALS, AI_SUBSCRIPTION)
    assert len(dlq) == 1

    retry_provider = CountingMockLLMProvider()
    retry_service = AIAnalysisService(
        bus=bus,
        cache=cache,
        search_store=search_store,
        rag_pipeline=RAGPipeline(
            search_store=search_store,
            embedding_provider=retry_provider,
        ),
        llm_provider=retry_provider,
    )
    await bus.publish(TOPIC_SIGNALS, body, message_id="delivery-b")
    await retry_service.poll_once(max_messages=1)

    insights = await bus.peek(TOPIC_INSIGHTS, "observer", n=10)
    assert len(insights) == 1
    assert retry_provider.generate_calls == 1


def test_app_health_and_metrics_endpoints() -> None:
    service = AIAnalysisService(
        bus=InMemoryBus(),
        cache=InMemoryCache(),
        search_store=InMemorySearchStore(),
        rag_pipeline=RAGPipeline(
            search_store=InMemorySearchStore(),
            embedding_provider=MockLLMProvider(),
        ),
        llm_provider=MockLLMProvider(),
    )
    test_app = create_app(service, run_on_startup=False)

    with TestClient(test_app) as client:
        root = client.get("/")
        health = client.get("/health")
        metrics = client.get(
            "/metrics",
            headers={"X-Correlation-ID": "ai-corr", "X-Trace-ID": "ai-trace"},
        )

    assert root.status_code == 200
    assert root.json()["service"] == "ai-analysis"
    assert root.json()["message"] == "Portfolio project only. No financial advice. No real trades."
    assert health.status_code == 200
    assert health.json()["service"] == "ai-analysis"
    assert metrics.status_code == 200
    assert metrics.headers["X-Correlation-ID"] == "ai-corr"
    assert metrics.headers["X-Trace-ID"] == "ai-trace"
    assert "ai_messages_seen" in metrics.text
    assert "ai_http_requests_total 2" in metrics.text


def test_module_level_app_uses_offline_default_service() -> None:
    with TestClient(app) as client:
        health = client.get("/health")

    assert health.status_code == 200
    assert health.json()["service"] == "ai-analysis"


def test_build_default_service_uses_offline_ports() -> None:
    service = build_default_service()

    assert isinstance(service, AIAnalysisService)


def test_app_lifespan_starts_background_worker() -> None:
    bus = InMemoryBus()
    cache = InMemoryCache()
    search_store = InMemorySearchStore()
    provider = MockLLMProvider()
    asyncio.run(bus.receive(TOPIC_NEWS_RAW, AI_SUBSCRIPTION, max_messages=0))
    asyncio.run(bus.receive(TOPIC_SIGNALS, AI_SUBSCRIPTION, max_messages=0))
    asyncio.run(bus.receive(TOPIC_INSIGHTS, "observer", max_messages=0))

    body, message_id = _news_message(message_id="bg-1")
    asyncio.run(bus.publish(TOPIC_NEWS_RAW, body, message_id=message_id))

    service = AIAnalysisService(
        bus=bus,
        cache=cache,
        search_store=search_store,
        rag_pipeline=RAGPipeline(
            search_store=search_store,
            embedding_provider=provider,
        ),
        llm_provider=provider,
    )
    test_app = create_app(service, run_on_startup=True)

    with TestClient(test_app):
        for _ in range(20):
            insights = asyncio.run(bus.peek(TOPIC_INSIGHTS, "observer", n=10))
            if insights:
                break
            asyncio.run(asyncio.sleep(0.01))

    assert len(insights) == 1


@pytest.mark.asyncio
async def test_run_forever_retries_after_transient_poll_failure() -> None:
    bus = FlakyReceiveBus()
    cache = InMemoryCache()
    search_store = InMemorySearchStore()
    provider = MockLLMProvider()
    await bus.receive(TOPIC_NEWS_RAW, AI_SUBSCRIPTION, max_messages=0)
    await bus.receive(TOPIC_SIGNALS, AI_SUBSCRIPTION, max_messages=0)
    await bus.receive(TOPIC_INSIGHTS, "observer", max_messages=0)
    bus.failures_remaining = 1

    body, message_id = _news_message(message_id="ai-retry")
    await bus.publish(TOPIC_NEWS_RAW, body, message_id=message_id)
    service = AIAnalysisService(
        bus=bus,
        cache=cache,
        search_store=search_store,
        rag_pipeline=RAGPipeline(
            search_store=search_store,
            embedding_provider=provider,
        ),
        llm_provider=provider,
    )

    worker = asyncio.create_task(
        service.run_forever(poll_interval_seconds=0.01, max_messages=1)
    )
    try:
        for _ in range(50):
            if service.metrics.messages_processed:
                break
            await asyncio.sleep(0.01)
    finally:
        worker.cancel()
        with pytest.raises(asyncio.CancelledError):
            await worker

    assert service.metrics.messages_processed == 1
    assert service.metrics.last_error == "ai polling failed: ConnectionError: service bus not ready"
    insights = await bus.peek(TOPIC_INSIGHTS, "observer", n=10)
    assert len(insights) == 1
