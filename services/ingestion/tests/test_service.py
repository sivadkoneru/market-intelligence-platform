import asyncio
import time
from datetime import datetime

import pytest
from fastapi.testclient import TestClient

from libs.common import TOPIC_MARKET_RAW, InMemoryBus, market_event_key
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
        metrics = client.get("/metrics")

    assert health.status_code == 200
    assert health.json()["status"] == "ok"
    assert metrics.status_code == 200
    assert "ingestion_events_seen" in metrics.text


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
