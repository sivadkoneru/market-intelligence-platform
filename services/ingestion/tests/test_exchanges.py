import asyncio
import json
from collections import deque
from datetime import UTC, datetime, timedelta
from typing import Any

import pytest

from libs.common import (
    TOPIC_MARKET_RAW,
    CircuitBreaker,
    CircuitOpenError,
    InMemoryBus,
    market_event_key,
)
from services.ingestion.exchanges import (
    BinanceWebSocketClient,
    CoinbaseWebSocketClient,
)


class FakeWebSocket:
    def __init__(self, messages: list[dict[str, Any]]) -> None:
        self._messages = deque(json.dumps(message) for message in messages)
        self.sent_messages: list[str] = []

    async def __aenter__(self) -> "FakeWebSocket":
        return self

    async def __aexit__(self, exc_type: Any, exc: Any, tb: Any) -> None:
        return None

    def __aiter__(self) -> "FakeWebSocket":
        return self

    async def __anext__(self) -> str:
        if not self._messages:
            raise StopAsyncIteration
        return self._messages.popleft()

    async def send(self, data: str) -> None:
        self.sent_messages.append(data)


class HangingWebSocket(FakeWebSocket):
    async def __anext__(self) -> str:
        await asyncio.Event().wait()
        raise StopAsyncIteration


class SequenceConnectFactory:
    def __init__(self, outcomes: list[Any]) -> None:
        self._outcomes = deque(outcomes)
        self.calls: list[str] = []

    def __call__(self, url: str) -> FakeWebSocket:
        self.calls.append(url)
        if not self._outcomes:
            raise AssertionError("No scripted connection outcome remaining")

        outcome = self._outcomes.popleft()
        if isinstance(outcome, Exception):
            raise outcome
        return outcome


class SequenceTime:
    def __init__(self, values: list[datetime]) -> None:
        self._values = deque(values)
        self._last = values[-1]

    def __call__(self) -> datetime:
        if self._values:
            self._last = self._values.popleft()
        return self._last


@pytest.mark.asyncio
async def test_binance_client_reconnects_after_failed_first_connection() -> None:
    bus = InMemoryBus()
    await bus.receive(TOPIC_MARKET_RAW, "stream", max_messages=1)
    websocket = FakeWebSocket(
        [
            {
                "e": "trade",
                "s": "ETHUSDT",
                "p": "3500.1",
                "q": "0.5",
                "T": 1704067200000,
            }
        ]
    )
    connect_factory = SequenceConnectFactory([ConnectionError("boom"), websocket])
    client = BinanceWebSocketClient(
        bus=bus,
        stream_name="ethusdt@trade",
        connect_factory=connect_factory,
        reconnect_backoff_seconds=0,
        max_reconnects=2,
    )

    state = await client.run(max_messages=1)
    published = await bus.peek(TOPIC_MARKET_RAW, "stream", n=10)

    assert len(connect_factory.calls) == 2
    assert connect_factory.calls[-1].endswith("/ethusdt@trade")
    assert state.reconnects == 1
    assert state.events_published == 1
    assert len(published) == 1
    assert published[0].message_id == market_event_key(
        "ETHUSDT",
        datetime(2024, 1, 1, tzinfo=UTC),
        "binance",
    )


@pytest.mark.asyncio
async def test_binance_client_reconnects_after_clean_socket_close() -> None:
    bus = InMemoryBus()
    await bus.receive(TOPIC_MARKET_RAW, "stream", max_messages=1)
    connect_factory = SequenceConnectFactory(
        [
            FakeWebSocket([]),
            FakeWebSocket(
                [
                    {
                        "e": "trade",
                        "s": "BTCUSDT",
                        "p": "42100.1",
                        "q": "0.25",
                        "T": 1704067200000,
                    }
                ]
            ),
        ]
    )
    client = BinanceWebSocketClient(
        bus=bus,
        connect_factory=connect_factory,
        reconnect_backoff_seconds=0,
        max_reconnects=2,
    )

    state = await client.run(max_messages=1)
    published = await bus.peek(TOPIC_MARKET_RAW, "stream", n=10)

    assert len(connect_factory.calls) == 2
    assert state.reconnects == 1
    assert len(published) == 1


@pytest.mark.asyncio
async def test_clean_socket_closes_open_circuit_after_repeated_failures() -> None:
    connect_factory = SequenceConnectFactory([FakeWebSocket([]), FakeWebSocket([])])
    client = BinanceWebSocketClient(
        bus=InMemoryBus(),
        connect_factory=connect_factory,
        reconnect_backoff_seconds=0,
        max_reconnects=5,
        circuit_breaker=CircuitBreaker(failure_threshold=2, reset_timeout=60),
    )

    with pytest.raises(CircuitOpenError):
        await client.run(max_messages=1)

    assert len(connect_factory.calls) == 2
    assert client.state.connect_failures == 2
    assert client.state.reconnects == 2
    assert client.state.last_error == "circuit open"


@pytest.mark.asyncio
async def test_coinbase_client_sends_subscribe_and_tracks_staleness() -> None:
    bus = InMemoryBus()
    await bus.receive(TOPIC_MARKET_RAW, "stream", max_messages=1)
    websocket = FakeWebSocket(
        [
            {
                "type": "subscriptions",
                "channels": [{"name": "ticker", "product_ids": ["BTC-USD"]}],
            },
            {"type": "heartbeat", "product_id": "BTC-USD"},
            {
                "type": "ticker",
                "product_id": "BTC-USD",
                "price": "42100.2",
                "best_bid": "42100.0",
                "best_ask": "42100.4",
                "time": "2024-01-01T00:00:02Z",
            },
        ]
    )
    base_time = datetime(2024, 1, 1, tzinfo=UTC)
    client = CoinbaseWebSocketClient(
        bus=bus,
        product_ids=["BTCUSD"],
        connect_factory=SequenceConnectFactory([websocket]),
        reconnect_backoff_seconds=0,
        time_fn=SequenceTime(
            [
                base_time,
                base_time + timedelta(seconds=1),
                base_time + timedelta(seconds=2),
            ]
        ),
    )

    state = await client.run(max_messages=1)
    published = await bus.peek(TOPIC_MARKET_RAW, "stream", n=10)
    subscribe_message = json.loads(websocket.sent_messages[0])

    assert subscribe_message == {
        "type": "subscribe",
        "product_ids": ["BTC-USD"],
        "channels": ["ticker"],
    }
    assert state.last_message_at == base_time + timedelta(seconds=2)
    assert state.last_heartbeat_at == base_time + timedelta(seconds=1)
    assert client.is_stale(
        stale_after_seconds=5,
        now=base_time + timedelta(seconds=6),
    ) is False
    assert client.is_stale(
        stale_after_seconds=5,
        now=base_time + timedelta(seconds=8),
    ) is True
    assert len(published) == 1
    assert published[0].body["symbol"] == "BTCUSD"


@pytest.mark.asyncio
async def test_coinbase_client_reconnects_after_stale_socket_timeout() -> None:
    bus = InMemoryBus()
    await bus.receive(TOPIC_MARKET_RAW, "stream", max_messages=1)
    connect_factory = SequenceConnectFactory(
        [
            HangingWebSocket([]),
            FakeWebSocket(
                [
                    {
                        "type": "ticker",
                        "product_id": "ETH-USD",
                        "price": "2250.5",
                        "best_bid": "2250.1",
                        "best_ask": "2250.9",
                        "time": "2024-01-01T00:00:02Z",
                    }
                ]
            ),
        ]
    )
    client = CoinbaseWebSocketClient(
        bus=bus,
        product_ids=["ETH-USD"],
        connect_factory=connect_factory,
        reconnect_backoff_seconds=0,
        heartbeat_timeout_seconds=0.001,
        max_reconnects=2,
    )

    state = await client.run(max_messages=1)
    published = await bus.peek(TOPIC_MARKET_RAW, "stream", n=10)

    assert len(connect_factory.calls) == 2
    assert state.reconnects == 1
    assert "stale" in (state.last_error or "")
    assert len(published) == 1
    assert published[0].body["symbol"] == "ETHUSD"


@pytest.mark.asyncio
async def test_binance_client_accepts_ticker_payload_and_publishes_market_raw() -> None:
    bus = InMemoryBus()
    await bus.receive(TOPIC_MARKET_RAW, "stream", max_messages=1)
    client = BinanceWebSocketClient(
        bus=bus,
        connect_factory=SequenceConnectFactory(
            [
                FakeWebSocket(
                    [
                        {
                            "stream": "btcusdt@ticker",
                            "data": {
                                "e": "24hrTicker",
                                "s": "BTCUSDT",
                                "c": "42100.2",
                                "v": "18.5",
                                "b": "42100.0",
                                "a": "42100.4",
                                "E": 1704067202000,
                            },
                        }
                    ]
                )
            ]
        ),
        reconnect_backoff_seconds=0,
    )

    await client.run(max_messages=1)
    published = await bus.peek(TOPIC_MARKET_RAW, "stream", n=10)

    assert len(published) == 1
    assert published[0].body["event_type"] == "ticker"
    assert published[0].body["source"] == "binance"


@pytest.mark.asyncio
async def test_duplicate_exchange_events_are_suppressed_on_market_raw() -> None:
    bus = InMemoryBus()
    await bus.receive(TOPIC_MARKET_RAW, "stream", max_messages=1)
    duplicate_trade = {
        "e": "trade",
        "s": "BTCUSDT",
        "p": "42100.1",
        "q": "0.25",
        "T": 1704067200000,
    }
    client = BinanceWebSocketClient(
        bus=bus,
        connect_factory=SequenceConnectFactory(
            [FakeWebSocket([duplicate_trade, duplicate_trade])]
        ),
        reconnect_backoff_seconds=0,
    )

    await client.run(max_messages=2)
    published = await bus.peek(TOPIC_MARKET_RAW, "stream", n=10)

    assert client.state.events_published == 2
    assert len(published) == 1


@pytest.mark.asyncio
async def test_exchange_client_opens_circuit_after_repeated_failures() -> None:
    connect_factory = SequenceConnectFactory(
        [ConnectionError("first failure"), ConnectionError("second failure")]
    )
    client = BinanceWebSocketClient(
        bus=InMemoryBus(),
        connect_factory=connect_factory,
        reconnect_backoff_seconds=0,
        max_reconnects=5,
        circuit_breaker=CircuitBreaker(failure_threshold=2, reset_timeout=60),
    )

    with pytest.raises(CircuitOpenError):
        await client.run(max_messages=1)

    assert len(connect_factory.calls) == 2
    assert client.state.connect_failures == 2
    assert client.state.reconnects == 2
    assert client.state.last_error == "circuit open"
