from __future__ import annotations

from datetime import datetime, timedelta, timezone

from fastapi.testclient import TestClient

from libs.common import (
    TOPIC_ALERTS,
    TOPIC_INSIGHTS,
    TOPIC_MARKET_RAW,
    TOPIC_SIGNALS,
    InMemoryBus,
    InMemoryCache,
    InMemoryTimeSeriesStore,
)
from services.api.app import app, build_default_service, create_app
from services.api.service import API_SUBSCRIPTION, API_WS_SUBSCRIPTION, APIService


class NoPrimeBus:
    def __init__(self) -> None:
        self.zero_message_receives: list[tuple[str, str]] = []

    async def receive(
        self,
        topic: str,
        subscription: str,
        max_messages: int = 10,
    ):
        if max_messages == 0:
            self.zero_message_receives.append((topic, subscription))
            raise AssertionError("non-in-memory buses should not be primed with receive")
        return []

    async def complete(self, msg) -> None:
        return None

    async def dead_letter(self, msg, reason: str = "") -> None:
        return None

    async def peek(self, topic: str, subscription: str, n: int = 10):
        return []

    async def receive_dead_letter(self, topic: str, subscription: str):
        return []


class CloseRecordingBus(NoPrimeBus):
    def __init__(self) -> None:
        super().__init__()
        self.closed = False

    async def close(self) -> None:
        self.closed = True


class CloseRecordingCache(InMemoryCache):
    def __init__(self) -> None:
        super().__init__()
        self.closed = False

    async def close(self) -> None:
        self.closed = True


def _seed_store(store: InMemoryTimeSeriesStore) -> None:
    base = datetime(2026, 1, 1, 0, 0, tzinfo=timezone.utc)
    rows = [
        {
            "_table": "ticks",
            "event_id": "tick-1",
            "ts": base.isoformat(),
            "symbol": "BTCUSDT",
            "source": "replay.binance",
            "event_type": "trade",
            "price": 100000.0,
            "volume": 1.0,
        },
        {
            "_table": "ticks",
            "event_id": "tick-2",
            "ts": (base + timedelta(minutes=1)).isoformat(),
            "symbol": "BTCUSDT",
            "source": "replay.binance",
            "event_type": "trade",
            "price": 100250.0,
            "volume": 1.1,
        },
        {
            "_table": "ticks",
            "event_id": "tick-3",
            "ts": (base + timedelta(minutes=2)).isoformat(),
            "symbol": "ETHUSDT",
            "source": "replay.coinbase",
            "event_type": "trade",
            "price": 3500.0,
            "volume": 5.0,
        },
        {
            "_table": "indicators",
            "event_id": "ind-1",
            "ts": (base + timedelta(minutes=1)).isoformat(),
            "symbol": "BTCUSDT",
            "source": "replay.binance",
            "price": 100250.0,
            "sma": 99950.0,
            "ema": 100120.0,
            "rsi": 66.5,
            "volatility": 0.21,
            "trend": "uptrend",
            "trend_score": 1.0,
            "zscore_anomaly": False,
            "ewma_anomaly": False,
            "anomaly": False,
        },
    ]

    import asyncio

    asyncio.run(store.ingest(rows))


def _seed_bus_and_cache(bus: InMemoryBus, cache: InMemoryCache) -> None:
    import asyncio

    async def seed() -> None:
        await bus.receive("signals", API_SUBSCRIPTION, max_messages=0)
        await bus.receive("alerts", API_SUBSCRIPTION, max_messages=0)
        await bus.receive("insights", API_SUBSCRIPTION, max_messages=0)
        await bus.publish(
            "signals",
            {
                "event_id": "sig-1",
                "ts": datetime(2026, 1, 1, 0, 1, tzinfo=timezone.utc).isoformat(),
                "symbol": "BTCUSDT",
                "source": "stream",
                "indicators": {
                    "sma": 99950.0,
                    "ema": 100120.0,
                    "rsi": 66.5,
                    "trend": 1.0,
                    "volatility": 0.21,
                },
                "anomaly": False,
            },
            message_id="sig-1",
        )
        await bus.publish(
            "alerts",
            {
                "event_id": "alt-1",
                "ts": datetime(2026, 1, 1, 0, 2, tzinfo=timezone.utc).isoformat(),
                "symbol": "BTCUSDT",
                "rule": "rsi-threshold",
                "severity": "medium",
                "message": "RSI is elevated",
                "dedupe_key": "btc-rsi-1",
            },
            message_id="alt-1",
        )
        await cache.set_snapshot(
            "BTCUSDT",
            {
                "symbol": "BTCUSDT",
                "ts": datetime(2026, 1, 1, 0, 1, tzinfo=timezone.utc).isoformat(),
                "source": "replay.binance",
                "price": 100250.0,
                "sma": 99950.0,
                "ema": 100120.0,
                "rsi": 66.5,
                "volatility": 0.21,
                "trend": "uptrend",
                "trend_score": 1.0,
                "zscore_anomaly": False,
                "ewma_anomaly": False,
                "anomaly": False,
            },
        )
        await cache.set(
            "insight:BTCUSDT",
            {
                "event_id": "ins-1",
                "ts": datetime(2026, 1, 1, 0, 3, tzinfo=timezone.utc).isoformat(),
                "symbol": "BTCUSDT",
                "sentiment_score": 0.72,
                "sentiment_label": "positive",
                "summary": "ETF demand remains constructive.",
                "explanation": "Detected a constructive flow-driven setup with grounded support.",
                "citations": ["https://example.test/btc-etf"],
                "confidence": 0.84,
                "grounded": True,
                "model": "mock-llm",
            },
        )

    asyncio.run(seed())


def _insight_payload(event_id: str, symbol: str) -> dict[str, object]:
    return {
        "event_id": event_id,
        "ts": datetime(2026, 1, 1, 0, 3, tzinfo=timezone.utc).isoformat(),
        "symbol": symbol,
        "sentiment_score": 0.72,
        "sentiment_label": "positive",
        "summary": f"{symbol} demand remains constructive.",
        "explanation": "Grounded support remains visible.",
        "citations": [f"https://example.test/{symbol.lower()}"],
        "confidence": 0.84,
        "grounded": True,
        "model": "mock-llm",
    }


def _build_test_app() -> TestClient:
    bus = InMemoryBus()
    cache = InMemoryCache()
    store = InMemoryTimeSeriesStore()
    _seed_store(store)
    _seed_bus_and_cache(bus, cache)
    service = APIService(store=store, cache=cache, bus=bus)
    return TestClient(create_app(service))


def _build_ws_test_context() -> tuple[TestClient, APIService, InMemoryBus]:
    bus = InMemoryBus()
    cache = InMemoryCache()
    store = InMemoryTimeSeriesStore()
    _seed_store(store)
    _seed_bus_and_cache(bus, cache)
    service = APIService(store=store, cache=cache, bus=bus)
    return TestClient(create_app(service)), service, bus


def test_api_routes_return_populated_market_indicator_and_insight_payloads() -> None:
    with _build_test_app() as client:
        latest = client.get("/market/BTCUSDT/latest")
        indicators = client.get("/indicators/BTCUSDT")
        insights = client.get("/insights/BTCUSDT")

    assert latest.status_code == 200
    assert latest.json()["symbol"] == "BTCUSDT"
    assert latest.json()["price"] == 100250.0

    assert indicators.status_code == 200
    payload = indicators.json()
    assert payload["symbol"] == "BTCUSDT"
    assert payload["indicators"]["rsi"] == 66.5
    assert payload["flags"]["trend"] == "uptrend"

    assert insights.status_code == 200
    insight_payload = insights.json()
    assert insight_payload["symbol"] == "BTCUSDT"
    assert insight_payload["grounded"] is True
    assert insight_payload["citations"] == ["https://example.test/btc-etf"]


def test_api_routes_return_symbols_history_signals_alerts_and_metrics() -> None:
    start = datetime(2026, 1, 1, 0, 0, tzinfo=timezone.utc).isoformat()
    end = datetime(2026, 1, 1, 0, 2, tzinfo=timezone.utc).isoformat()

    with _build_test_app() as client:
        root = client.get("/")
        health = client.get("/health")
        metrics = client.get("/metrics")
        symbols = client.get("/symbols")
        history = client.get("/market/BTCUSDT/history", params={"from": start, "to": end})
        signals = client.get("/signals")
        alerts = client.get("/alerts")

    assert root.status_code == 200
    assert root.json()["message"] == "Portfolio project only. No financial advice. No real trades."
    assert "/insights/{symbol}" in root.json()["routes"]
    assert "/ws/stream" in root.json()["routes"]

    assert health.status_code == 200
    assert health.json()["service"] == "api"
    assert health.json()["backends"]["timeseries"] == "inmemorytimeseries"

    assert metrics.status_code == 200
    assert "api_structured_logging_json 1" in metrics.text
    assert 'api_backend_info{kind="cache",backend="inmemorycache"} 1' in metrics.text

    assert symbols.status_code == 200
    assert symbols.json() == {"symbols": ["BTCUSDT", "ETHUSDT"], "count": 2}

    assert history.status_code == 200
    rows = history.json()["rows"]
    assert len(rows) == 2
    assert rows[0]["price"] == 100000.0
    assert rows[1]["price"] == 100250.0

    assert signals.status_code == 200
    assert signals.json()["count"] == 1
    assert signals.json()["signals"][0]["symbol"] == "BTCUSDT"

    assert alerts.status_code == 200
    assert alerts.json()["count"] == 1
    assert alerts.json()["alerts"][0]["rule"] == "rsi-threshold"

    with _build_test_app() as client:
        client.get("/symbols")
        client.get("/market/BTCUSDT/latest")
        metrics_after = client.get("/metrics")

    assert "api_requests_total 2" in metrics_after.text
    assert "api_symbols_requests 1" in metrics_after.text
    assert "api_market_latest_requests 1" in metrics_after.text


def test_indicators_fall_back_to_timeseries_when_cache_is_empty() -> None:
    bus = InMemoryBus()
    cache = InMemoryCache()
    store = InMemoryTimeSeriesStore()
    _seed_store(store)

    with TestClient(create_app(APIService(store=store, cache=cache, bus=bus))) as client:
        response = client.get("/indicators/BTCUSDT")

    assert response.status_code == 200
    payload = response.json()
    assert payload["symbol"] == "BTCUSDT"
    assert payload["indicators"]["rsi"] == 66.5
    assert payload["flags"]["trend"] == "uptrend"


def test_insights_fall_back_to_bus_beyond_first_fifty_messages() -> None:
    import asyncio

    bus = InMemoryBus()
    cache = InMemoryCache()
    store = InMemoryTimeSeriesStore()

    async def seed() -> None:
        await bus.receive("insights", API_SUBSCRIPTION, max_messages=0)
        await bus.publish(
            "insights",
            _insight_payload("target", "SOLUSDT"),
            message_id="target",
        )
        for index in range(75):
            await bus.publish(
                "insights",
                _insight_payload(f"filler-{index}", f"FILLER{index}"),
                message_id=f"filler-{index}",
            )

    asyncio.run(seed())

    with TestClient(create_app(APIService(store=store, cache=cache, bus=bus))) as client:
        response = client.get("/insights/SOLUSDT")

    assert response.status_code == 200
    assert response.json()["event_id"] == "target"


def test_signals_and_alerts_skip_invalid_bus_payloads() -> None:
    import asyncio

    bus = InMemoryBus()
    cache = InMemoryCache()
    store = InMemoryTimeSeriesStore()

    async def seed() -> None:
        await bus.receive("signals", API_SUBSCRIPTION, max_messages=0)
        await bus.receive("alerts", API_SUBSCRIPTION, max_messages=0)
        await bus.publish("signals", {"event_id": "bad"}, message_id="bad-signal")
        await bus.publish(
            "signals",
            {
                "event_id": "sig-1",
                "ts": datetime(2026, 1, 1, tzinfo=timezone.utc).isoformat(),
                "symbol": "BTCUSDT",
                "source": "stream",
                "indicators": {"rsi": 66.5},
                "anomaly": False,
            },
            message_id="good-signal",
        )
        await bus.publish("alerts", {"event_id": "bad"}, message_id="bad-alert")
        await bus.publish(
            "alerts",
            {
                "event_id": "alert-1",
                "ts": datetime(2026, 1, 1, tzinfo=timezone.utc).isoformat(),
                "symbol": "BTCUSDT",
                "rule": "rsi_overbought",
                "severity": "high",
                "message": "RSI elevated",
                "dedupe_key": "alert-1",
            },
            message_id="good-alert",
        )

    asyncio.run(seed())

    with TestClient(create_app(APIService(store=store, cache=cache, bus=bus))) as client:
        signals = client.get("/signals")
        alerts = client.get("/alerts")

    assert signals.status_code == 200
    assert signals.json()["count"] == 1
    assert signals.json()["signals"][0]["event_id"] == "sig-1"
    assert alerts.status_code == 200
    assert alerts.json()["count"] == 1
    assert alerts.json()["alerts"][0]["event_id"] == "alert-1"


def test_api_returns_404s_and_validates_history_range() -> None:
    with _build_test_app() as client:
        missing_market = client.get("/market/SOLUSDT/latest")
        missing_indicators = client.get("/indicators/SOLUSDT")
        missing_insight = client.get("/insights/SOLUSDT")
        bad_history = client.get(
            "/market/BTCUSDT/history",
            params={
                "from": datetime(2026, 1, 1, 0, 3, tzinfo=timezone.utc).isoformat(),
                "to": datetime(2026, 1, 1, 0, 2, tzinfo=timezone.utc).isoformat(),
            },
        )

    assert missing_market.status_code == 404
    assert missing_indicators.status_code == 404
    assert missing_insight.status_code == 404
    assert bad_history.status_code == 400


def test_ws_stream_validates_subscribe_payloads() -> None:
    with _build_ws_test_context()[0] as client:
        with client.websocket_connect("/ws/stream") as websocket:
            websocket.send_json({"action": "follow", "symbols": ["BTCUSDT"]})
            invalid_action = websocket.receive_json()
            websocket.send_json({"action": "subscribe", "symbols": ["", " "]})
            invalid_symbols = websocket.receive_json()
            websocket.send_json({"action": "subscribe", "symbols": ["btcusdt"]})
            valid = websocket.receive_json()

    assert invalid_action["type"] == "error"
    assert "subscribe" in invalid_action["detail"]
    assert invalid_symbols["type"] == "error"
    assert "non-empty symbol" in invalid_symbols["detail"]
    assert valid == {"type": "subscribed", "symbols": ["BTCUSDT"]}


def test_ws_stream_filters_by_symbol_and_fans_out_across_topics() -> None:
    import asyncio

    client, _, bus = _build_ws_test_context()

    async def publish() -> None:
        await bus.publish(
            TOPIC_MARKET_RAW,
            {
                "event_id": "mkt-1",
                "ts": datetime(2026, 1, 1, 0, 4, tzinfo=timezone.utc).isoformat(),
                "symbol": "BTCUSDT",
                "source": "replay.binance",
                "event_type": "trade",
                "price": 100500.0,
                "volume": 0.5,
            },
            message_id="mkt-1",
        )
        await bus.publish(
            TOPIC_SIGNALS,
            {
                "event_id": "sig-eth",
                "ts": datetime(2026, 1, 1, 0, 5, tzinfo=timezone.utc).isoformat(),
                "symbol": "ETHUSDT",
                "source": "stream",
                "indicators": {"rsi": 51.2},
                "anomaly": False,
            },
            message_id="sig-eth",
        )
        await bus.publish(
            TOPIC_ALERTS,
            {
                "event_id": "alt-2",
                "ts": datetime(2026, 1, 1, 0, 6, tzinfo=timezone.utc).isoformat(),
                "symbol": "BTCUSDT",
                "rule": "breakout",
                "severity": "high",
                "message": "Breakout confirmed",
                "dedupe_key": "btc-breakout",
            },
            message_id="alt-2",
        )
        await bus.publish(
            TOPIC_INSIGHTS,
            _insight_payload("ins-2", "BTCUSDT"),
            message_id="ins-2",
        )

    with client:
        with client.websocket_connect("/ws/stream") as websocket:
            websocket.send_json({"action": "subscribe", "symbols": ["BTCUSDT"]})
            assert websocket.receive_json() == {"type": "subscribed", "symbols": ["BTCUSDT"]}

            asyncio.run(publish())

            received = [websocket.receive_json() for _ in range(3)]

    assert {message["type"] for message in received} == {"market", "alert", "insight"}
    assert all(message["symbol"] == "BTCUSDT" for message in received)
    assert {message["payload"]["event_id"] for message in received} == {"mkt-1", "alt-2", "ins-2"}
    api_signals = asyncio.run(bus.peek(TOPIC_SIGNALS, API_SUBSCRIPTION, n=10))
    ws_signals = asyncio.run(bus.peek(TOPIC_SIGNALS, API_WS_SUBSCRIPTION, n=10))
    assert any(message.message_id == "sig-eth" for message in api_signals)
    assert all(message.message_id != "sig-eth" for message in ws_signals)


def test_ws_stream_disconnect_cleans_up_connection_registration() -> None:
    import time

    client, service, _ = _build_ws_test_context()

    with client:
        assert service.active_stream_connections == 0
        with client.websocket_connect("/ws/stream") as websocket:
            websocket.send_json({"action": "subscribe", "symbols": ["BTCUSDT"]})
            assert websocket.receive_json() == {"type": "subscribed", "symbols": ["BTCUSDT"]}
            assert service.active_stream_connections == 1

        deadline = time.time() + 0.5
        while time.time() < deadline and service.active_stream_connections != 0:
            time.sleep(0.01)

        assert service.active_stream_connections == 0


def test_module_level_app_uses_offline_default_service() -> None:
    with TestClient(app) as client:
        health = client.get("/health")

    assert health.status_code == 200
    assert health.json()["service"] == "api"


def test_build_default_service_uses_offline_ports() -> None:
    service = build_default_service()

    assert service.timeseries_backend == "inmemorytimeseries"
    assert service.cache_backend == "inmemorycache"
    assert service.bus_backend == "inmemorybus"


def test_app_startup_does_not_prime_non_in_memory_bus() -> None:
    service = APIService(
        store=InMemoryTimeSeriesStore(),
        cache=InMemoryCache(),
        bus=NoPrimeBus(),
    )

    with TestClient(create_app(service)) as client:
        assert client.get("/health").status_code == 200


def test_app_lifespan_closes_real_backends() -> None:
    bus = CloseRecordingBus()
    cache = CloseRecordingCache()
    service = APIService(
        store=InMemoryTimeSeriesStore(),
        cache=cache,
        bus=bus,
    )

    with TestClient(create_app(service)) as client:
        assert client.get("/health").status_code == 200

    assert bus.closed is True
    assert cache.closed is True
