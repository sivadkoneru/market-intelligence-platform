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
    assert (
        client.is_stale(
            stale_after_seconds=5,
            now=base_time + timedelta(seconds=6),
        )
        is False
    )
    assert (
        client.is_stale(
            stale_after_seconds=5,
            now=base_time + timedelta(seconds=8),
        )
        is True
    )
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
        connect_factory=SequenceConnectFactory([FakeWebSocket([duplicate_trade, duplicate_trade])]),
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


class ProtocolWebSocket:
    """
    An open connection with NO ``__aenter__`` — what ``await websockets.connect``
    actually returns on the pinned ``websockets==13.1``.
    """

    def __init__(self, messages: list[dict[str, Any]]) -> None:
        self._messages = deque(json.dumps(message) for message in messages)
        self.sent_messages: list[str] = []
        self.closed = False

    def __aiter__(self) -> "ProtocolWebSocket":
        return self

    async def __anext__(self) -> str:
        if not self._messages:
            raise StopAsyncIteration
        return self._messages.popleft()

    async def send(self, data: str) -> None:
        self.sent_messages.append(data)

    async def close(self) -> None:
        self.closed = True


class ConnectLikeFactory:
    """
    Mirrors ``websockets.connect``: the returned object is BOTH awaitable and an
    async context manager, and awaiting it yields a bare protocol.
    """

    class _Connect:
        def __init__(self, protocol: ProtocolWebSocket) -> None:
            self._protocol = protocol

        def __await__(self):
            async def _resolve() -> ProtocolWebSocket:
                return self._protocol

            return _resolve().__await__()

        async def __aenter__(self) -> ProtocolWebSocket:
            return self._protocol

        async def __aexit__(self, exc_type: Any, exc: Any, tb: Any) -> None:
            await self._protocol.close()

    def __init__(self, protocol: ProtocolWebSocket) -> None:
        self._protocol = protocol
        self.calls: list[str] = []

    def __call__(self, url: str) -> "_Connect":
        self.calls.append(url)
        return ConnectLikeFactory._Connect(self._protocol)


@pytest.mark.asyncio
async def test_client_connects_through_a_websockets_style_connect_object() -> None:
    """The real ``websockets.connect`` shape must work, not just test doubles."""
    bus = InMemoryBus()
    await bus.receive(TOPIC_MARKET_RAW, "stream", max_messages=1)
    protocol = ProtocolWebSocket(
        [{"e": "trade", "s": "BTCUSDT", "p": "42100.5", "q": "0.25", "T": 1704067200000}]
    )
    client = BinanceWebSocketClient(
        bus=bus,
        connect_factory=ConnectLikeFactory(protocol),
        reconnect_backoff_seconds=0,
    )

    state = await client.run(max_messages=1)
    published = await bus.peek(TOPIC_MARKET_RAW, "stream", n=10)

    assert state.events_published == 1
    assert published[0].body["price"] == 42100.5
    assert protocol.closed is True


@pytest.mark.asyncio
async def test_malformed_frame_is_skipped_without_dropping_the_connection() -> None:
    bus = InMemoryBus()
    await bus.receive(TOPIC_MARKET_RAW, "stream", max_messages=1)
    websocket = FakeWebSocket(
        [
            {"e": "error", "msg": "rate limited"},
            {"e": "trade", "s": "BTCUSDT", "p": "42100.5", "q": "0.25", "T": 1704067200000},
        ]
    )
    connect_factory = SequenceConnectFactory([websocket])
    client = BinanceWebSocketClient(
        bus=bus,
        connect_factory=connect_factory,
        reconnect_backoff_seconds=0,
    )

    state = await client.run(max_messages=1)

    assert len(connect_factory.calls) == 1, "the socket must not have been torn down"
    assert state.malformed_messages == 1
    assert state.events_published == 1


@pytest.mark.asyncio
async def test_reconnect_budget_resets_after_a_productive_connection() -> None:
    """A blip days after an earlier one must not exhaust a lifetime budget."""
    bus = InMemoryBus()
    await bus.receive(TOPIC_MARKET_RAW, "stream", max_messages=1)
    trade = {"e": "trade", "s": "BTCUSDT", "p": "42100.5", "q": "0.25", "T": 1704067200000}
    connect_factory = SequenceConnectFactory(
        [
            ConnectionError("blip one"),
            FakeWebSocket([trade]),
            ConnectionError("blip two"),
            FakeWebSocket([{**trade, "T": 1704067201000}]),
        ]
    )
    client = BinanceWebSocketClient(
        bus=bus,
        connect_factory=connect_factory,
        reconnect_backoff_seconds=0,
        max_reconnects=1,
    )

    state = await client.run(max_messages=2)

    assert len(connect_factory.calls) == 4
    assert state.events_published == 2


def test_is_stale_uses_the_most_recent_signal_of_life() -> None:
    from services.ingestion.exchanges.base import ExchangeStreamState

    start = datetime(2026, 1, 1, tzinfo=UTC)
    state = ExchangeStreamState()
    state.mark_heartbeat(start)
    state.mark_message(start + timedelta(seconds=99))

    assert state.is_stale(stale_after_seconds=30, now=start + timedelta(seconds=100)) is False


def test_is_stale_when_every_signal_is_old() -> None:
    from services.ingestion.exchanges.base import ExchangeStreamState

    start = datetime(2026, 1, 1, tzinfo=UTC)
    state = ExchangeStreamState()
    state.mark_message(start)

    assert state.is_stale(stale_after_seconds=30, now=start + timedelta(seconds=31)) is True


@pytest.mark.asyncio
async def test_socket_recycle_is_not_recorded_as_a_connection_failure() -> None:
    """
    A close that already streamed messages is routine exchange churn. Counting
    it as a transport failure inflated connect_failures and pinned last_error to
    a benign close message for the rest of the client's life.
    """
    bus = InMemoryBus()
    await bus.receive(TOPIC_MARKET_RAW, "stream", max_messages=1)
    trade = {"e": "trade", "s": "BTCUSDT", "p": "42100.5", "q": "0.25", "T": 1704067200000}
    connect_factory = SequenceConnectFactory(
        [
            FakeWebSocket([trade]),
            FakeWebSocket([{**trade, "T": 1704067201000}]),
        ]
    )
    client = BinanceWebSocketClient(
        bus=bus,
        connect_factory=connect_factory,
        reconnect_backoff_seconds=0,
    )

    state = await client.run(max_messages=2)

    assert state.events_published == 2
    assert state.reconnects == 1, "the socket was recycled exactly once"
    assert state.connect_failures == 0, "a productive close is not a connect failure"
    assert state.last_error is None, "a productive close must not poison last_error"
