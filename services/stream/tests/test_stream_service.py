import asyncio
from datetime import datetime, timedelta, timezone
from typing import Any

import pytest
from fastapi.testclient import TestClient

from libs.common import (
    TOPIC_MARKET_RAW,
    TOPIC_SIGNALS,
    InMemoryBus,
    InMemoryCache,
    InMemoryTimeSeriesStore,
    market_event_key,
)
from services.stream.app import app, build_default_service, create_app
from services.stream.service import (
    STREAM_SUBSCRIPTION,
    StreamProcessor,
    StreamService,
    price_history_for_symbol,
)


class RecordingStore(InMemoryTimeSeriesStore):
    def __init__(self) -> None:
        super().__init__()
        self.ingest_calls: list[list[dict[str, Any]]] = []

    async def ingest(self, rows: list[dict[str, Any]]) -> None:
        self.ingest_calls.append(rows)
        await super().ingest(rows)


class FailingIndicatorStore(InMemoryTimeSeriesStore):
    async def ingest(self, rows: list[dict[str, Any]]) -> None:
        if rows[0].get("_table") == "indicators":
            raise RuntimeError("indicator ingest failed")
        await super().ingest(rows)


def _market_message(
    *,
    symbol: str,
    ts: datetime,
    price: float,
    source: str = "replay.binance",
    message_id: str = "msg-1",
) -> tuple[dict[str, object], str]:
    body = {
        "event_id": f"ev-{message_id}",
        "ts": ts.isoformat(),
        "symbol": symbol,
        "source": source,
        "event_type": "trade",
        "price": price,
        "volume": 1.5,
        "correlation_id": f"corr-{message_id}",
        "trace_id": f"trace-{message_id}",
    }
    return body, message_id


async def _build_service(
    store: InMemoryTimeSeriesStore | None = None,
) -> tuple[StreamService, InMemoryBus, InMemoryCache, InMemoryTimeSeriesStore]:
    bus = InMemoryBus()
    cache = InMemoryCache()
    resolved_store = store or InMemoryTimeSeriesStore()
    await bus.receive(TOPIC_MARKET_RAW, STREAM_SUBSCRIPTION, max_messages=0)
    await bus.receive(TOPIC_SIGNALS, "observer", max_messages=0)
    return StreamService(bus=bus, cache=cache, store=resolved_store), bus, cache, resolved_store


@pytest.mark.asyncio
async def test_service_ingests_druid_rows_caches_snapshot_and_publishes_signal() -> None:
    store = RecordingStore()
    service, bus, cache, store = await _build_service(store=store)
    start = datetime(2026, 1, 1, tzinfo=timezone.utc)

    for index, price in enumerate((100.0, 101.0, 102.0, 103.0, 104.0, 140.0), start=1):
        body, message_id = _market_message(
            symbol="BTCUSDT",
            ts=start + timedelta(minutes=index),
            price=price,
            message_id=f"btc-{index}",
        )
        await bus.publish(TOPIC_MARKET_RAW, body, message_id=message_id)

    processed = 0
    for _ in range(6):
        processed += await service.poll_once(max_messages=1)

    assert processed == 6
    assert await store.count("ticks") == 6
    assert await store.count("indicators") == 6
    assert len(store.ingest_calls) == 12
    assert all(len(call) == 1 for call in store.ingest_calls)
    assert store.ingest_calls[0][0]["_table"] == "ticks"
    assert store.ingest_calls[1][0]["_table"] == "indicators"

    tick_rows = await store.query_sql('SELECT * FROM "ticks"')
    indicator_rows = await store.query_sql('SELECT * FROM "indicators"')
    latest_indicator = indicator_rows[-1]
    assert tick_rows[-1]["price"] == 140.0
    assert latest_indicator["symbol"] == "BTCUSDT"
    assert latest_indicator["sma"] is not None
    assert latest_indicator["ema"] is not None
    assert latest_indicator["volatility"] is not None
    assert latest_indicator["trend"] == "uptrend"
    assert latest_indicator["zscore_anomaly"] is True
    assert latest_indicator["ewma_anomaly"] is True
    assert latest_indicator["anomaly"] is True

    snapshot = await cache.get_snapshot("BTCUSDT")
    assert snapshot is not None
    assert snapshot["symbol"] == "BTCUSDT"
    assert snapshot["price"] == 140.0
    assert snapshot["trend"] == "uptrend"
    assert snapshot["anomaly"] is True

    signal_messages = await bus.peek(TOPIC_SIGNALS, "observer", n=10)
    assert len(signal_messages) == 6
    latest_signal = signal_messages[-1].body
    assert latest_signal["symbol"] == "BTCUSDT"
    assert latest_signal["anomaly"] is True
    assert latest_signal["indicators"]["trend"] == 1.0
    assert latest_signal["indicators"]["rsi"] is not None

    assert service.metrics.tick_rows_ingested == 6
    assert service.metrics.indicator_rows_ingested == 6
    assert service.metrics.signals_published == 6


@pytest.mark.asyncio
async def test_service_suppresses_duplicate_market_events() -> None:
    service, bus, cache, store = await _build_service()
    ts = datetime(2026, 1, 1, 0, 0, tzinfo=timezone.utc)
    body, _ = _market_message(symbol="ETHUSDT", ts=ts, price=2500.0, message_id="first")
    processed_key = (
        "stream:processed:" + market_event_key("ETHUSDT", ts, "replay.binance")
    )

    await bus.publish(TOPIC_MARKET_RAW, body, message_id="delivery-a")
    await bus.publish(TOPIC_MARKET_RAW, body, message_id="delivery-b")

    await service.poll_once(max_messages=10)

    assert await store.count("ticks") == 1
    assert await store.count("indicators") == 1
    assert service.metrics.messages_seen == 2
    assert service.metrics.messages_processed == 1
    assert service.metrics.duplicates_suppressed == 1
    assert await cache.get(processed_key) is True
    assert price_history_for_symbol(service._processor, "ETHUSDT") == (2500.0,)

    signal_messages = await bus.peek(TOPIC_SIGNALS, "observer", n=10)
    assert len(signal_messages) == 1


@pytest.mark.asyncio
async def test_service_dead_letters_invalid_payloads() -> None:
    service, bus, cache, store = await _build_service()
    invalid_body = {
        "event_id": "bad-1",
        "symbol": "BTCUSDT",
        "source": "replay.binance",
        "event_type": "trade",
        # missing ts and price
    }

    await bus.publish(TOPIC_MARKET_RAW, invalid_body, message_id="bad-message")
    await service.poll_once(max_messages=1)

    assert await store.count("ticks") == 0
    assert service.metrics.dead_lettered == 1
    dlq_messages = await bus.receive_dead_letter(TOPIC_MARKET_RAW, STREAM_SUBSCRIPTION)
    assert len(dlq_messages) == 1
    assert dlq_messages[0].body["symbol"] == "BTCUSDT"
    assert service.metrics.last_error is not None
    assert "invalid market event payload" in service.metrics.last_error
    assert await cache.get_snapshot("BTCUSDT") is None


@pytest.mark.asyncio
async def test_processing_failure_dead_letters_without_processed_marker_and_allows_retry() -> None:
    ts = datetime(2026, 1, 1, 0, 1, tzinfo=timezone.utc)
    body, _ = _market_message(symbol="ADAUSDT", ts=ts, price=1.25, message_id="ada-1")
    processed_key = (
        "stream:processed:" + market_event_key("ADAUSDT", ts, "replay.binance")
    )

    failing_store = FailingIndicatorStore()
    service, bus, cache, store = await _build_service(store=failing_store)
    await bus.publish(TOPIC_MARKET_RAW, body, message_id="delivery-a")
    await service.poll_once(max_messages=1)

    assert await store.count("ticks") == 1
    assert await store.count("indicators") == 0
    assert await cache.get_snapshot("ADAUSDT") is None
    assert await cache.get(processed_key) is None
    assert service.metrics.dead_lettered == 1
    assert service.metrics.messages_processed == 0
    assert service.metrics.signals_published == 0
    assert price_history_for_symbol(service._processor, "ADAUSDT") == ()

    dlq_messages = await bus.receive_dead_letter(TOPIC_MARKET_RAW, STREAM_SUBSCRIPTION)
    assert len(dlq_messages) == 1
    assert "stream processing failed" in service.metrics.last_error

    retry_store = RecordingStore()
    service._store = retry_store
    await bus.publish(TOPIC_MARKET_RAW, body, message_id="delivery-b")
    await service.poll_once(max_messages=1)

    assert await retry_store.count("ticks") == 1
    assert await retry_store.count("indicators") == 1
    assert await cache.get(processed_key) is True
    assert price_history_for_symbol(service._processor, "ADAUSDT") == (1.25,)
    retry_signals = await bus.peek(TOPIC_SIGNALS, "observer", n=10)
    assert len(retry_signals) == 1


def test_app_health_and_metrics_endpoints() -> None:
    service = StreamService(
        bus=InMemoryBus(),
        cache=InMemoryCache(),
        store=InMemoryTimeSeriesStore(),
    )
    test_app = create_app(service, run_on_startup=False)

    with TestClient(test_app) as client:
        root = client.get("/")
        health = client.get("/health")
        metrics = client.get("/metrics")

    assert root.status_code == 200
    assert root.json()["service"] == "stream"
    assert root.json()["message"] == "Portfolio project only. No financial advice. No real trades."
    assert health.status_code == 200
    assert health.json()["service"] == "stream"
    assert metrics.status_code == 200
    assert "stream_messages_seen" in metrics.text


def test_module_level_app_uses_offline_default_service() -> None:
    with TestClient(app) as client:
        health = client.get("/health")

    assert health.status_code == 200
    assert health.json()["service"] == "stream"


def test_build_default_service_uses_offline_ports() -> None:
    service = build_default_service()

    assert isinstance(service._processor, StreamProcessor)


def test_app_lifespan_starts_background_poller() -> None:
    bus = InMemoryBus()
    cache = InMemoryCache()
    store = InMemoryTimeSeriesStore()
    asyncio.run(bus.receive(TOPIC_MARKET_RAW, STREAM_SUBSCRIPTION, max_messages=0))
    asyncio.run(bus.receive(TOPIC_SIGNALS, "observer", max_messages=0))

    body, message_id = _market_message(
        symbol="SOLUSDT",
        ts=datetime(2026, 1, 1, tzinfo=timezone.utc),
        price=200.0,
        message_id="sol-1",
    )
    asyncio.run(bus.publish(TOPIC_MARKET_RAW, body, message_id=message_id))

    service = StreamService(bus=bus, cache=cache, store=store)
    test_app = create_app(service, run_on_startup=True)

    with TestClient(test_app):
        for _ in range(20):
            signals = asyncio.run(bus.peek(TOPIC_SIGNALS, "observer", n=10))
            if signals:
                break
            asyncio.run(asyncio.sleep(0.01))

    assert len(signals) == 1
