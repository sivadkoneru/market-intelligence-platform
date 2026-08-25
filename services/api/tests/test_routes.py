from __future__ import annotations

from datetime import UTC, datetime, timedelta
from typing import Any

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

    async def publish(
        self,
        topic: str,
        body: dict[str, Any],
        *,
        message_id: str | None = None,
        correlation_id: str | None = None,
    ) -> None:
        return None

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

    async def peek(
        self,
        topic: str,
        subscription: str,
        n: int = 10,
        from_sequence_number: int | None = None,
    ):
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


class SymbolQueryRecordingStore(InMemoryTimeSeriesStore):
    def __init__(self) -> None:
        super().__init__()
        self.queries: list[str] = []

    async def query_sql(self, sql: str) -> list[dict[str, Any]]:
        self.queries.append(" ".join(sql.strip().split()))
        return await super().query_sql(sql)


class LatestFailingStore(InMemoryTimeSeriesStore):
    async def latest(self, symbol: str) -> dict[str, Any] | None:
        raise AssertionError("latest should use cached snapshot before Druid")


def _seed_store(store: InMemoryTimeSeriesStore) -> None:
    base = datetime(2026, 1, 1, 0, 0, tzinfo=UTC)
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
                "ts": datetime(2026, 1, 1, 0, 1, tzinfo=UTC).isoformat(),
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
                "ts": datetime(2026, 1, 1, 0, 2, tzinfo=UTC).isoformat(),
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
                "ts": datetime(2026, 1, 1, 0, 1, tzinfo=UTC).isoformat(),
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
                "ts": datetime(2026, 1, 1, 0, 3, tzinfo=UTC).isoformat(),
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
        "ts": datetime(2026, 1, 1, 0, 3, tzinfo=UTC).isoformat(),
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
    start = datetime(2026, 1, 1, 0, 0, tzinfo=UTC).isoformat()
    end = datetime(2026, 1, 1, 0, 2, tzinfo=UTC).isoformat()

    with _build_test_app() as client:
        root = client.get("/")
        health = client.get("/health")
        metrics = client.get(
            "/metrics",
            headers={"X-Correlation-ID": "api-corr", "X-Trace-ID": "api-trace"},
        )
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
    assert metrics.headers["X-Correlation-ID"] == "api-corr"
    assert metrics.headers["X-Trace-ID"] == "api-trace"
    assert "api_structured_logging_json 1" in metrics.text
    assert 'api_backend_info{kind="cache",backend="inmemorycache"} 1' in metrics.text
    assert "api_http_requests_total 2" in metrics.text

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
    assert "api_http_requests_total 2" in metrics_after.text


def test_symbols_query_uses_only_known_druid_datasources() -> None:
    bus = InMemoryBus()
    cache = InMemoryCache()
    store = SymbolQueryRecordingStore()
    _seed_store(store)

    with TestClient(create_app(APIService(store=store, cache=cache, bus=bus))) as client:
        response = client.get("/symbols")

    assert response.status_code == 200
    assert response.json() == {"symbols": ["BTCUSDT", "ETHUSDT"], "count": 2}
    assert len(store.queries) == 3
    assert "FROM INFORMATION_SCHEMA.TABLES" in store.queries[0]
    # The per-table symbol queries run concurrently, so their relative order
    # in store.queries is not guaranteed.
    assert any('FROM "ticks"' in query for query in store.queries[1:])
    assert any('FROM "indicators"' in query for query in store.queries[1:])


def test_symbols_skips_missing_indicator_datasource() -> None:
    import asyncio

    bus = InMemoryBus()
    cache = InMemoryCache()
    store = SymbolQueryRecordingStore()
    asyncio.run(
        store.ingest(
            [
                {"_table": "ticks", "symbol": "SOLUSDT", "ts": "2026-01-01T00:00:00Z"},
                {"_table": "ticks", "symbol": "BTCUSDT", "ts": "2026-01-01T00:01:00Z"},
            ]
        )
    )

    with TestClient(create_app(APIService(store=store, cache=cache, bus=bus))) as client:
        response = client.get("/symbols")

    assert response.status_code == 200
    assert response.json() == {"symbols": ["BTCUSDT", "SOLUSDT"], "count": 2}
    assert len(store.queries) == 2
    assert any('FROM "ticks"' in query for query in store.queries[1:])
    assert all('FROM "indicators"' not in query for query in store.queries[1:])


def test_symbols_returns_empty_when_druid_datasources_are_absent() -> None:
    bus = InMemoryBus()
    cache = InMemoryCache()
    store = SymbolQueryRecordingStore()

    with TestClient(create_app(APIService(store=store, cache=cache, bus=bus))) as client:
        response = client.get("/symbols")

    assert response.status_code == 200
    assert response.json() == {"symbols": [], "count": 0}
    assert len(store.queries) == 1
    assert "FROM INFORMATION_SCHEMA.TABLES" in store.queries[0]


def test_symbols_include_cached_snapshots_when_druid_datasources_are_absent() -> None:
    import asyncio

    bus = InMemoryBus()
    cache = InMemoryCache()
    store = SymbolQueryRecordingStore()
    asyncio.run(cache.set_snapshot("ADAUSDT", {"price": 1.25}))
    asyncio.run(cache.set_snapshot("BTCUSDT", {"price": 100000.0}))

    with TestClient(create_app(APIService(store=store, cache=cache, bus=bus))) as client:
        response = client.get("/symbols")

    assert response.status_code == 200
    assert response.json() == {"symbols": ["ADAUSDT", "BTCUSDT"], "count": 2}
    assert len(store.queries) == 1
    assert "FROM INFORMATION_SCHEMA.TABLES" in store.queries[0]


def test_latest_market_uses_cached_snapshot_before_druid() -> None:
    import asyncio

    bus = InMemoryBus()
    cache = InMemoryCache()
    store = LatestFailingStore()
    asyncio.run(
        cache.set_snapshot(
            "BTCUSDT",
            {
                "symbol": "BTCUSDT",
                "ts": "2026-01-01T00:00:00Z",
                "source": "seed.local",
                "event_type": "trade",
                "price": 42000.0,
                "volume": 1.0,
                "bid": 41999.75,
                "ask": 42000.25,
            },
        )
    )

    with TestClient(create_app(APIService(store=store, cache=cache, bus=bus))) as client:
        response = client.get("/market/BTCUSDT/latest")

    assert response.status_code == 200
    assert response.json() == {
        "symbol": "BTCUSDT",
        "ts": "2026-01-01T00:00:00Z",
        "source": "seed.local",
        "event_type": "trade",
        "price": 42000.0,
        "volume": 1.0,
        "bid": 41999.75,
        "ask": 42000.25,
    }


def test_market_history_includes_recent_cached_rows_when_druid_is_empty() -> None:
    import asyncio

    bus = InMemoryBus()
    cache = InMemoryCache()
    store = InMemoryTimeSeriesStore()

    async def seed() -> None:
        await cache.set(
            "history:BTCUSDT",
            [
                {
                    "event_id": "too-early",
                    "ts": "2025-12-31T23:59:59Z",
                    "symbol": "BTCUSDT",
                    "price": 99999.0,
                },
                {
                    "event_id": "btc-2",
                    "ts": "2026-01-01T00:00:02Z",
                    "symbol": "BTCUSDT",
                    "source": "seed.local",
                    "event_type": "trade",
                    "price": 100200.0,
                    "volume": 0.2,
                },
                {
                    "event_id": "btc-1",
                    "ts": "2026-01-01T00:00:01Z",
                    "symbol": "BTCUSDT",
                    "source": "seed.local",
                    "event_type": "trade",
                    "price": 100100.0,
                    "volume": 0.1,
                },
                {
                    "event_id": "eth-1",
                    "ts": "2026-01-01T00:00:01Z",
                    "symbol": "ETHUSDT",
                    "price": 3500.0,
                },
            ],
        )

    asyncio.run(seed())

    with TestClient(create_app(APIService(store=store, cache=cache, bus=bus))) as client:
        response = client.get(
            "/market/BTCUSDT/history",
            params={
                "from": "2026-01-01T00:00:00Z",
                "to": "2026-01-01T00:00:03Z",
            },
        )

    assert response.status_code == 200
    payload = response.json()
    assert payload["symbol"] == "BTCUSDT"
    assert [row["event_id"] for row in payload["rows"]] == ["btc-1", "btc-2"]
    assert [row["price"] for row in payload["rows"]] == [100100.0, 100200.0]


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
                "ts": datetime(2026, 1, 1, tzinfo=UTC).isoformat(),
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
                "ts": datetime(2026, 1, 1, tzinfo=UTC).isoformat(),
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
                "from": datetime(2026, 1, 1, 0, 3, tzinfo=UTC).isoformat(),
                "to": datetime(2026, 1, 1, 0, 2, tzinfo=UTC).isoformat(),
            },
        )

    assert missing_market.status_code == 404
    assert missing_indicators.status_code == 404
    assert missing_insight.status_code == 404
    assert bad_history.status_code == 400


def test_ws_stream_validates_subscribe_payloads() -> None:
    with (
        _build_ws_test_context()[0] as client,
        client.websocket_connect("/ws/stream") as websocket,
    ):
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
                "ts": datetime(2026, 1, 1, 0, 4, tzinfo=UTC).isoformat(),
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
                "ts": datetime(2026, 1, 1, 0, 5, tzinfo=UTC).isoformat(),
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
                "ts": datetime(2026, 1, 1, 0, 6, tzinfo=UTC).isoformat(),
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

    with client, client.websocket_connect("/ws/stream") as websocket:
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


# ---------------------------------------------------------------------------
# Druid-shaped rows (__time instead of ts)
# ---------------------------------------------------------------------------


def _druid_row(event_id: str, iso_ts: str, **extra: Any) -> dict[str, Any]:
    """A row as Druid returns it: the ingest `ts` column is promoted to `__time`."""
    return {"__time": iso_ts, "event_id": event_id, "symbol": "BTCUSDT", **extra}


def test_market_history_orders_druid_rows_by_time() -> None:
    store = InMemoryTimeSeriesStore()
    service = APIService(store=store, cache=InMemoryCache(), bus=InMemoryBus())
    rows = [
        _druid_row("d2", "2026-01-01T00:00:02+00:00", price=2.0),
        _druid_row("d1", "2026-01-01T00:00:01+00:00", price=1.0),
        _druid_row("d3", "2026-01-01T00:00:03+00:00", price=3.0),
    ]

    ordered = sorted(rows, key=service._ts_sort_key)

    assert [row["event_id"] for row in ordered] == ["d1", "d2", "d3"]


def test_normalise_row_exposes_druid_time_as_ts() -> None:
    """Cache-hit and Druid-fallback responses must have the same shape."""
    service = APIService(store=InMemoryTimeSeriesStore(), cache=InMemoryCache(), bus=InMemoryBus())

    normalised = service._normalise_row(_druid_row("d1", "2026-01-01T00:00:01+00:00", price=1.0))

    assert normalised["ts"] == "2026-01-01T00:00:01+00:00"


def test_ts_sort_key_picks_the_latest_druid_row() -> None:
    service = APIService(store=InMemoryTimeSeriesStore(), cache=InMemoryCache(), bus=InMemoryBus())
    matches = [
        _druid_row("old", "2026-01-01T00:00:01+00:00", rsi=11.0),
        _druid_row("new", "2026-01-01T00:00:09+00:00", rsi=99.0),
    ]

    assert max(matches, key=service._ts_sort_key)["rsi"] == 99.0


# ---------------------------------------------------------------------------
# Stream broker resource bounds and lifecycle
# ---------------------------------------------------------------------------


def test_stream_queue_drops_oldest_instead_of_growing_without_bound() -> None:
    import asyncio

    from services.api.service import MAX_STREAM_QUEUE_SIZE, LiveStreamBroker, StreamEnvelope

    async def scenario() -> tuple[int, int]:
        broker = LiveStreamBroker(bus=InMemoryBus())
        queue = broker.register("c1")
        broker.update_symbols("c1", ["BTCUSDT"])
        # A client that never reads.
        for index in range(MAX_STREAM_QUEUE_SIZE + 25):
            await broker._fanout(
                StreamEnvelope(
                    topic=TOPIC_MARKET_RAW,
                    event="market",
                    symbol="BTCUSDT",
                    payload={"symbol": "BTCUSDT", "seq": index},
                )
            )
        return queue.qsize(), broker.dropped_stream_messages

    qsize, dropped = asyncio.run(scenario())

    assert qsize == MAX_STREAM_QUEUE_SIZE
    assert dropped == 25


def test_stream_queue_keeps_the_newest_messages() -> None:
    import asyncio

    from services.api.service import MAX_STREAM_QUEUE_SIZE, LiveStreamBroker, StreamEnvelope

    async def scenario() -> dict[str, Any]:
        broker = LiveStreamBroker(bus=InMemoryBus())
        queue = broker.register("c1")
        broker.update_symbols("c1", ["BTCUSDT"])
        for index in range(MAX_STREAM_QUEUE_SIZE + 1):
            await broker._fanout(
                StreamEnvelope(
                    topic=TOPIC_MARKET_RAW,
                    event="market",
                    symbol="BTCUSDT",
                    payload={"symbol": "BTCUSDT", "seq": index},
                )
            )
        return queue.get_nowait()

    oldest_retained = asyncio.run(scenario())

    assert oldest_retained["payload"]["seq"] == 1


def test_update_symbols_for_an_unregistered_connection_raises_value_error() -> None:
    """ws.py catches ValueError; a KeyError would escape and kill the socket."""
    import pytest

    from services.api.service import LiveStreamBroker

    broker = LiveStreamBroker(bus=InMemoryBus())

    with pytest.raises(ValueError, match="no longer registered"):
        broker.update_symbols("never-registered", ["BTCUSDT"])


def test_api_service_close_releases_every_backend_when_one_fails() -> None:
    import asyncio

    class FailingCache(InMemoryCache):
        async def close(self) -> None:
            raise RuntimeError("redis down")

    class RecordingBus(InMemoryBus):
        def __init__(self) -> None:
            super().__init__()
            self.closed = False

        async def close(self) -> None:
            self.closed = True

    bus = RecordingBus()
    service = APIService(store=InMemoryTimeSeriesStore(), cache=FailingCache(), bus=bus)

    asyncio.run(service.close())

    assert bus.closed is True


def test_websocket_stream_reports_an_error_for_a_non_json_frame() -> None:
    """A malformed frame must produce an error frame, not an unhandled crash."""
    store = InMemoryTimeSeriesStore()
    service = APIService(store=store, cache=InMemoryCache(), bus=InMemoryBus())

    with (
        TestClient(create_app(service)) as client,
        client.websocket_connect("/ws/stream") as websocket,
    ):
        websocket.send_text("this-is-not-json")
        error = websocket.receive_json()
        assert error["type"] == "error"

        # The connection survives and still accepts a valid command.
        websocket.send_json({"action": "subscribe", "symbols": ["BTCUSDT"]})
        assert websocket.receive_json() == {
            "type": "subscribed",
            "symbols": ["BTCUSDT"],
        }


def test_websocket_stream_rejects_an_oversized_symbol_list() -> None:
    from services.api.ws import MAX_SUBSCRIBED_SYMBOLS

    service = APIService(store=InMemoryTimeSeriesStore(), cache=InMemoryCache(), bus=InMemoryBus())

    with (
        TestClient(create_app(service)) as client,
        client.websocket_connect("/ws/stream") as websocket,
    ):
        websocket.send_json(
            {
                "action": "subscribe",
                "symbols": [f"SYM{i}" for i in range(MAX_SUBSCRIBED_SYMBOLS + 1)],
            }
        )
        assert websocket.receive_json()["type"] == "error"


# ---------------------------------------------------------------------------
# Peek-based read model returns the NEWEST messages
# ---------------------------------------------------------------------------


def _signal_body(seq: int) -> dict[str, Any]:
    return {
        "event_id": f"sig-{seq:04d}",
        "ts": (datetime(2026, 1, 1, tzinfo=UTC) + timedelta(seconds=seq)).isoformat(),
        "symbol": "BTCUSDT",
        "source": "stream",
        "indicators": {"sma": float(seq)},
        "anomaly": False,
    }


def test_signals_returns_the_newest_messages_not_the_oldest() -> None:
    """
    peek() reads from the head of the subscription.

    Slicing the front returned the same oldest N forever, so a running platform
    served a frozen window while claiming to show the latest signals.
    """
    import asyncio

    async def scenario() -> list[dict[str, Any]]:
        bus = InMemoryBus()
        await bus.receive(TOPIC_SIGNALS, API_SUBSCRIPTION, max_messages=0)
        service = APIService(store=InMemoryTimeSeriesStore(), cache=InMemoryCache(), bus=bus)
        for seq in range(30):
            await bus.publish(TOPIC_SIGNALS, _signal_body(seq), message_id=f"sig-{seq}")
        return await service.signals(limit=5)

    payloads = asyncio.run(scenario())

    assert [p["event_id"] for p in payloads] == [
        "sig-0029",
        "sig-0028",
        "sig-0027",
        "sig-0026",
        "sig-0025",
    ]


def test_signals_advance_as_new_messages_arrive() -> None:
    import asyncio

    async def scenario() -> tuple[str, str]:
        bus = InMemoryBus()
        await bus.receive(TOPIC_SIGNALS, API_SUBSCRIPTION, max_messages=0)
        service = APIService(store=InMemoryTimeSeriesStore(), cache=InMemoryCache(), bus=bus)
        for seq in range(30):
            await bus.publish(TOPIC_SIGNALS, _signal_body(seq), message_id=f"sig-{seq}")
        first = (await service.signals(limit=3))[0]["event_id"]

        for seq in range(30, 40):
            await bus.publish(TOPIC_SIGNALS, _signal_body(seq), message_id=f"sig-{seq}")
        second = (await service.signals(limit=3))[0]["event_id"]
        return first, second

    first, second = asyncio.run(scenario())

    assert first == "sig-0029"
    assert second == "sig-0039"


class CountingAPIService(APIService):
    """APIService that records how many bus messages it validated."""

    validated_count = 0

    def _validated_payload(self, message, model):
        self.validated_count += 1
        return super()._validated_payload(message, model)


def test_insight_validates_each_message_once_across_requests() -> None:
    """
    Re-reading the peek window cost a full Pydantic pass over the backlog on
    every request. The read model validates each message once and remembers it,
    so a second request for the same symbol validates nothing new.
    """
    import asyncio

    async def scenario() -> tuple[dict[str, Any] | None, int, int]:
        bus = InMemoryBus()
        await bus.receive(TOPIC_INSIGHTS, API_SUBSCRIPTION, max_messages=0)
        service = CountingAPIService(
            store=InMemoryTimeSeriesStore(), cache=InMemoryCache(), bus=bus
        )
        for seq in range(50):
            await bus.publish(
                TOPIC_INSIGHTS,
                {
                    "event_id": f"ins-{seq}",
                    "ts": datetime(2026, 1, 1, tzinfo=UTC).isoformat(),
                    "symbol": "BTCUSDT",
                    "sentiment_score": 0.5,
                    "sentiment_label": "positive",
                    "summary": "s",
                    "explanation": "e",
                    "citations": [],
                    "confidence": 0.8,
                    "grounded": True,
                    "model": "mock",
                },
                message_id=f"ins-{seq}",
            )

        payload = await service.insight("BTCUSDT")
        after_first = service.validated_count
        await service.insight("BTCUSDT")
        return payload, after_first, service.validated_count

    payload, after_first, after_second = asyncio.run(scenario())

    assert payload is not None
    assert payload["event_id"] == "ins-49", "should return the most recent insight"
    assert after_first == 50, "the backlog is validated once, on the way into the read model"
    assert after_second == after_first, "the second request re-validated the backlog"


# ---------------------------------------------------------------------------
# Symbol validation and normalization
# ---------------------------------------------------------------------------


def _client_with_snapshot() -> TestClient:
    cache = InMemoryCache()
    service = APIService(store=InMemoryTimeSeriesStore(), cache=cache, bus=InMemoryBus())
    import asyncio

    asyncio.run(
        cache.set_snapshot(
            "BTCUSDT",
            {
                "symbol": "BTCUSDT",
                "ts": "2026-01-01T00:00:00+00:00",
                "source": "binance",
                "event_type": "trade",
                "price": 42000.0,
                "sma": 1.0,
                "trend_score": 1.0,
            },
        )
    )
    return TestClient(create_app(service))


def test_lowercase_symbol_resolves_to_the_same_data_as_uppercase() -> None:
    """The websocket path uppercases; REST used to 404 on data WS streamed fine."""
    with _client_with_snapshot() as client:
        upper = client.get("/market/BTCUSDT/latest")
        lower = client.get("/market/btcusdt/latest")

    assert upper.status_code == 200
    assert lower.status_code == 200
    assert lower.json()["price"] == upper.json()["price"]


def test_indicators_accept_lowercase_symbols() -> None:
    with _client_with_snapshot() as client:
        assert client.get("/indicators/btcusdt").status_code == 200


def test_oversized_symbol_is_rejected_before_reaching_a_backend() -> None:
    with _client_with_snapshot() as client:
        response = client.get(f"/market/{'A' * 200}/latest")

    assert response.status_code == 422


def test_symbol_with_illegal_characters_is_rejected() -> None:
    with _client_with_snapshot() as client:
        assert client.get("/market/BTC'USDT/latest").status_code == 422


def test_404_detail_echoes_the_normalized_symbol() -> None:
    with _client_with_snapshot() as client:
        response = client.get("/insights/ethusdt")

    assert response.status_code == 404
    assert "ETHUSDT" in response.json()["detail"]


# ---------------------------------------------------------------------------
# Query bounds
# ---------------------------------------------------------------------------


def test_history_limit_is_capped_and_returns_the_newest_rows() -> None:
    import asyncio

    from services.api.routes.market import MAX_HISTORY_ROWS

    store = InMemoryTimeSeriesStore()
    service = APIService(store=store, cache=InMemoryCache(), bus=InMemoryBus())
    base = datetime(2026, 1, 1, tzinfo=UTC)
    asyncio.run(
        store.ingest(
            [
                {
                    "symbol": "BTCUSDT",
                    "ts": (base + timedelta(seconds=i)).isoformat(),
                    "price": float(i),
                    "event_id": f"tick-{i}",
                }
                for i in range(50)
            ]
        )
    )

    rows = asyncio.run(
        service.market_history("BTCUSDT", frm=base, to=base + timedelta(hours=1), limit=5)
    )

    assert len(rows) == 5
    assert [r["event_id"] for r in rows] == [f"tick-{i}" for i in range(45, 50)]
    assert MAX_HISTORY_ROWS == 10_000


def test_history_limit_above_the_cap_is_rejected() -> None:
    with _client_with_snapshot() as client:
        response = client.get(
            "/market/BTCUSDT/history",
            params={"from": "2026-01-01T00:00:00Z", "to": "2026-01-02T00:00:00Z", "limit": 10_001},
        )

    assert response.status_code == 422


def test_indicators_fallback_queries_only_the_matching_symbol() -> None:
    """The fallback used to SELECT * the whole indicators table and filter in Python."""
    import asyncio

    class RecordingStore(InMemoryTimeSeriesStore):
        def __init__(self) -> None:
            super().__init__()
            self.sql_queries: list[str] = []

        async def query_sql(self, sql: str):
            self.sql_queries.append(sql)
            return await super().query_sql(sql)

    store = RecordingStore()
    service = APIService(store=store, cache=InMemoryCache(), bus=InMemoryBus())
    asyncio.run(
        store.ingest(
            [
                {
                    "_table": "indicators",
                    "symbol": "BTCUSDT",
                    "ts": "2026-01-01T00:00:01+00:00",
                    "rsi": 11.0,
                },
                {
                    "_table": "indicators",
                    "symbol": "BTCUSDT",
                    "ts": "2026-01-01T00:00:09+00:00",
                    "rsi": 99.0,
                },
                {
                    "_table": "indicators",
                    "symbol": "ETHUSDT",
                    "ts": "2026-01-01T00:00:09+00:00",
                    "rsi": 50.0,
                },
            ]
        )
    )

    payload = asyncio.run(service.indicators("BTCUSDT"))

    assert payload is not None
    assert payload["indicators"]["rsi"] == 99.0, "must return the latest row, not the first"
    assert store.sql_queries == [], "no unfiltered table scan should be issued"


class _PeekRecordingBus(InMemoryBus):
    """InMemoryBus that records the arguments of every peek."""

    def __init__(self) -> None:
        super().__init__()
        self.peek_calls: list[tuple[str, int, int | None]] = []

    async def peek(self, topic, subscription, n=10, from_sequence_number=None):
        self.peek_calls.append((topic, n, from_sequence_number))
        return await super().peek(topic, subscription, n, from_sequence_number)


def test_signals_reads_only_the_delta_on_a_second_request() -> None:
    """
    Re-peeking the whole window per request re-read and re-decoded the entire
    backlog. The cursor makes each request pay only for what has since arrived.
    """
    import asyncio

    async def scenario() -> tuple[list[dict[str, Any]], list[tuple[str, int, int | None]]]:
        bus = _PeekRecordingBus()
        await bus.receive(TOPIC_SIGNALS, API_SUBSCRIPTION, max_messages=0)
        service = APIService(store=InMemoryTimeSeriesStore(), cache=InMemoryCache(), bus=bus)
        for seq in range(30):
            await bus.publish(TOPIC_SIGNALS, _signal_body(seq), message_id=f"sig-{seq}")

        await service.signals(limit=5)
        bus.peek_calls.clear()
        await bus.publish(TOPIC_SIGNALS, _signal_body(30), message_id="sig-30")
        payloads = await service.signals(limit=5)
        return payloads, list(bus.peek_calls)

    payloads, peek_calls = asyncio.run(scenario())

    assert payloads[0]["event_id"] == "sig-0030", "the newest signal must be served first"
    resumed = [call for call in peek_calls if call[2] is not None]
    assert resumed, "the second request restarted from the head of the subscription"
    assert all(call[2] == 31 for call in resumed), "the cursor did not advance past the backlog"


def test_signals_serve_newest_first_on_a_bus_without_sequence_numbers() -> None:
    """The sequence number is optional on the port, so the fallback must work."""
    import asyncio

    class _UnnumberedBus(InMemoryBus):
        async def peek(self, topic, subscription, n=10, from_sequence_number=None):
            messages = await super().peek(topic, subscription, n, from_sequence_number)
            for message in messages:
                message.sequence_number = None
            return messages

    async def scenario() -> list[dict[str, Any]]:
        bus = _UnnumberedBus()
        await bus.receive(TOPIC_SIGNALS, API_SUBSCRIPTION, max_messages=0)
        service = APIService(store=InMemoryTimeSeriesStore(), cache=InMemoryCache(), bus=bus)
        for seq in range(10):
            await bus.publish(TOPIC_SIGNALS, _signal_body(seq), message_id=f"sig-{seq}")
        await service.signals(limit=2)
        return await service.signals(limit=2)

    payloads = asyncio.run(scenario())

    assert [p["event_id"] for p in payloads] == ["sig-0009", "sig-0008"]


def test_market_history_with_a_zero_limit_returns_no_rows() -> None:
    """``ordered[-0:]`` handed back every merged row instead of none."""
    import asyncio

    async def scenario() -> list[dict[str, Any]]:
        store = InMemoryTimeSeriesStore()
        await store.ingest(
            [
                {"symbol": "BTCUSDT", "ts": f"2026-01-01T00:00:{i:02d}+00:00", "price": 1.0 + i}
                for i in range(5)
            ]
        )
        service = APIService(store=store, cache=InMemoryCache(), bus=InMemoryBus())
        return await service.market_history(
            "BTCUSDT",
            frm=datetime(2026, 1, 1, tzinfo=UTC),
            to=datetime(2026, 1, 2, tzinfo=UTC),
            limit=0,
        )

    assert asyncio.run(scenario()) == []
