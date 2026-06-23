import asyncio
import time
from datetime import datetime

import pytest
from fastapi.testclient import TestClient

from libs.common import TOPIC_MARKET_RAW, TOPIC_NEWS_RAW, InMemoryBus, NewsEvent, market_event_key
from services.ingestion.app import app, build_default_service, create_app
from services.ingestion.replay import (
    DeterministicReplayFeed,
    build_default_replay_events,
    build_replay_feed_factory,
)
from services.ingestion.service import IngestionService


@pytest.mark.asyncio
async def test_service_publishes_replay_events_to_market_raw() -> None:
    bus = InMemoryBus()
    await bus.receive(TOPIC_MARKET_RAW, "stream", max_messages=1)

    service = IngestionService(
        bus=bus,
        feed_factory=lambda: DeterministicReplayFeed(build_default_replay_events()),
    )

    await service.run()
    published = await bus.peek(TOPIC_MARKET_RAW, "stream", n=10)

    assert len(published) == 2
    assert [msg.body["symbol"] for msg in published] == ["BTCUSDT", "ETHUSD"]


@pytest.mark.asyncio
async def test_service_publishes_mock_news_events_to_news_raw() -> None:
    bus = InMemoryBus()
    await bus.receive(TOPIC_NEWS_RAW, "ai", max_messages=1)
    service = IngestionService(
        bus=bus,
        feed_factory=lambda: DeterministicReplayFeed(build_default_replay_events()),
    )
    event = NewsEvent(
        event_id="mock-news-unit",
        source="mock.local",
        title="BTC ETF inflows accelerate",
        body="BTC sentiment is constructive after a local mock news catalyst.",
        symbols=["BTCUSDT"],
        correlation_id="corr-news-unit",
    )

    message_id = await service.publish_news_event(event)
    published = await bus.peek(TOPIC_NEWS_RAW, "ai", n=10)

    assert message_id == "mock-news-unit"
    assert service.metrics.news_publishes == 1
    assert published[0].message_id == "mock-news-unit"
    assert published[0].correlation_id == "corr-news-unit"
    assert published[0].body["symbols"] == ["BTCUSDT"]


@pytest.mark.asyncio
async def test_service_uses_deterministic_message_ids_for_duplicates() -> None:
    bus = InMemoryBus()
    await bus.receive(TOPIC_MARKET_RAW, "stream", max_messages=1)
    replay_events = build_default_replay_events()

    service = IngestionService(
        bus=bus,
        feed_factory=lambda: DeterministicReplayFeed(replay_events),
    )

    metrics = await service.run()
    published = await bus.peek(TOPIC_MARKET_RAW, "stream", n=10)

    expected_message_id = market_event_key(
        "BTCUSDT",
        datetime.fromisoformat(replay_events[0]["ts"]),
        "replay.binance",
    )
    assert metrics.duplicate_events == 1
    assert metrics.unique_publishes == 2
    assert published[0].message_id == expected_message_id


@pytest.mark.asyncio
async def test_service_reconnects_after_simulated_disconnect() -> None:
    bus = InMemoryBus()
    await bus.receive(TOPIC_MARKET_RAW, "stream", max_messages=1)

    service = IngestionService(
        bus=bus,
        feed_factory=build_replay_feed_factory(
            build_default_replay_events(),
            disconnect_at=1,
        ),
        reconnect_backoff_seconds=0,
    )

    metrics = await service.run(max_events=3)
    published = await bus.peek(TOPIC_MARKET_RAW, "stream", n=10)

    assert metrics.disconnects == 1
    assert metrics.reconnects == 1
    assert len(published) == 2


def test_app_health_and_metrics_endpoints() -> None:
    bus = InMemoryBus()
    service = IngestionService(
        bus=bus,
        feed_factory=lambda: DeterministicReplayFeed(build_default_replay_events()),
    )
    app = create_app(service)

    with TestClient(app) as client:
        health = client.get("/health")
        metrics = client.get(
            "/metrics",
            headers={"X-Correlation-ID": "ingestion-corr", "X-Trace-ID": "ingestion-trace"},
        )

    assert health.status_code == 200
    assert health.json()["status"] == "ok"
    assert metrics.status_code == 200
    assert metrics.headers["X-Correlation-ID"] == "ingestion-corr"
    assert metrics.headers["X-Trace-ID"] == "ingestion-trace"
    assert "ingestion_events_seen" in metrics.text
    assert "ingestion_http_requests_total 1" in metrics.text


def test_mock_news_endpoint_publishes_default_news_event() -> None:
    bus = InMemoryBus()
    asyncio.run(bus.receive(TOPIC_NEWS_RAW, "ai", max_messages=1))
    service = IngestionService(
        bus=bus,
        feed_factory=lambda: DeterministicReplayFeed(build_default_replay_events()),
    )
    app = create_app(service, run_on_startup=False)

    with TestClient(app) as client:
        response = client.post("/mock/news")

    assert response.status_code == 202
    payload = response.json()
    assert payload["topic"] == TOPIC_NEWS_RAW
    assert payload["event"]["symbols"] == ["BTCUSDT"]
    assert payload["insight_urls"] == ["/insights/BTCUSDT"]
    published = asyncio.run(bus.peek(TOPIC_NEWS_RAW, "ai", n=10))
    assert len(published) == 1
    assert published[0].message_id == payload["message_id"]
    assert published[0].body["title"] == "BTCUSDT mock market catalyst"


def test_mock_news_endpoint_accepts_custom_symbols_and_content() -> None:
    bus = InMemoryBus()
    asyncio.run(bus.receive(TOPIC_NEWS_RAW, "ai", max_messages=1))
    service = IngestionService(
        bus=bus,
        feed_factory=lambda: DeterministicReplayFeed(build_default_replay_events()),
    )
    app = create_app(service, run_on_startup=False)

    with TestClient(app) as client:
        response = client.post(
            "/mock/news",
            json={
                "event_id": "custom-news-1",
                "symbols": [" ethusdt ", "BTCUSDT", "ethusdt"],
                "title": "Cross-asset crypto flows improve",
                "body": "BTC and ETH both show positive local test sentiment.",
                "source": "mock.custom",
            },
        )

    assert response.status_code == 202
    payload = response.json()
    assert payload["message_id"] == "custom-news-1"
    assert payload["event"]["symbols"] == ["BTCUSDT", "ETHUSDT"]
    assert payload["event"]["source"] == "mock.custom"
    assert payload["insight_urls"] == ["/insights/BTCUSDT", "/insights/ETHUSDT"]


def test_mock_news_endpoint_rejects_empty_symbols() -> None:
    service = IngestionService(
        bus=InMemoryBus(),
        feed_factory=lambda: DeterministicReplayFeed(build_default_replay_events()),
    )
    app = create_app(service, run_on_startup=False)

    with TestClient(app) as client:
        response = client.post("/mock/news", json={"symbols": [" ", ""]})

    assert response.status_code == 400
    assert response.json()["detail"] == "At least one symbol is required"


def test_app_lifespan_starts_replay_ingestion() -> None:
    bus = InMemoryBus()
    asyncio.run(bus.receive(TOPIC_MARKET_RAW, "stream", max_messages=1))
    service = IngestionService(
        bus=bus,
        feed_factory=lambda: DeterministicReplayFeed(build_default_replay_events()),
    )
    app = create_app(service, startup_max_events=3)

    with TestClient(app):
        for _ in range(20):
            published = asyncio.run(bus.peek(TOPIC_MARKET_RAW, "stream", n=10))
            if len(published) == 2:
                break
            time.sleep(0.01)

    assert len(published) == 2


def test_module_level_app_uses_offline_default_service() -> None:
    with TestClient(app) as client:
        health = client.get("/health")

    assert health.status_code == 200
    assert health.json()["service"] == "ingestion"


def test_build_default_service_uses_replay_feed() -> None:
    service = build_default_service()

    assert isinstance(service._feed_factory(), DeterministicReplayFeed)
