"""
Shared WebSocket client loop for exchange market-data ingestion.
"""

from __future__ import annotations

import asyncio
import json
from collections.abc import AsyncIterator, Awaitable, Callable
from contextlib import asynccontextmanager
from dataclasses import dataclass
from datetime import UTC, datetime
from inspect import isawaitable
from typing import Any, Protocol, TypeAlias

from libs.common import (
    TOPIC_MARKET_RAW,
    CircuitBreaker,
    CircuitOpenError,
    MessageBus,
    get_logger,
    market_event_key,
)
from services.ingestion.normalizer import normalize_market_payload


class WebSocketConnection(Protocol):
    async def __aenter__(self) -> WebSocketConnection: ...

    async def __aexit__(self, exc_type: Any, exc: Any, tb: Any) -> None: ...

    def __aiter__(self) -> AsyncIterator[str | bytes]: ...

    async def send(self, data: str) -> None: ...


# A factory may return an async context manager, an awaitable resolving to a
# connection, or an already-open protocol. ``websockets.connect`` is the first
# two at once, which is exactly the ambiguity ``_open_connection`` resolves —
# so the alias stays permissive rather than pretending one shape is canonical.
ConnectResult: TypeAlias = WebSocketConnection | Awaitable[WebSocketConnection]
ConnectFactory: TypeAlias = Callable[..., Any]
SleepFn: TypeAlias = Callable[[float], Awaitable[None]]
TimeFn: TypeAlias = Callable[[], datetime]


@dataclass
class ExchangeStreamState:
    last_message_at: datetime | None = None
    last_heartbeat_at: datetime | None = None
    reconnects: int = 0
    messages_seen: int = 0
    events_published: int = 0
    connect_failures: int = 0
    malformed_messages: int = 0
    last_error: str | None = None

    def mark_message(self, now: datetime) -> None:
        self.last_message_at = now
        self.messages_seen += 1

    def mark_heartbeat(self, now: datetime) -> None:
        self.last_heartbeat_at = now

    def is_stale(
        self,
        *,
        stale_after_seconds: float,
        now: datetime | None = None,
    ) -> bool:
        # Whichever signal of life arrived *last* decides staleness. Preferring
        # the heartbeat would pin the reference to an old ping on feeds that
        # send data far more often than heartbeats (Coinbase subscribes to
        # "ticker" only), reporting a busy stream as stale forever.
        timestamps = [t for t in (self.last_heartbeat_at, self.last_message_at) if t is not None]
        if not timestamps:
            return True
        reference_time = max(timestamps)

        current_time = now or datetime.now(tz=UTC)
        return (current_time - reference_time).total_seconds() > stale_after_seconds


@dataclass
class _ConnectionOutcome:
    """Result of one connection attempt, as seen by the circuit breaker."""

    published: int
    productive_close: ExchangeStreamClosed | None = None


class ExchangeStreamClosed(ConnectionError):
    """Raised when a websocket closes before the requested stream work finishes."""

    def __init__(self, source: str, published: int) -> None:
        self.published = published
        super().__init__(f"{source} websocket closed before ingestion completed")


class ExchangeStreamStale(ConnectionError):
    """Raised when no websocket message arrives within the heartbeat timeout."""


class ExchangeWebSocketClient:
    """
    Base exchange client with reconnect/backoff, circuit breaker, and publishing.
    """

    def __init__(
        self,
        *,
        bus: MessageBus,
        connect_factory: ConnectFactory,
        url: str,
        source: str,
        topic: str = TOPIC_MARKET_RAW,
        reconnect_backoff_seconds: float = 1.0,
        max_reconnects: int = 3,
        heartbeat_timeout_seconds: float | None = 30.0,
        circuit_breaker: CircuitBreaker | None = None,
        sleep_fn: SleepFn | None = None,
        time_fn: TimeFn | None = None,
    ) -> None:
        self._bus = bus
        self._connect_factory = connect_factory
        self._url = url
        self._source = source
        self._topic = topic
        self._reconnect_backoff_seconds = reconnect_backoff_seconds
        self._max_reconnects = max_reconnects
        self._heartbeat_timeout_seconds = heartbeat_timeout_seconds
        self._circuit_breaker = circuit_breaker or CircuitBreaker()
        self._sleep = sleep_fn or asyncio.sleep
        self._time_fn = time_fn or (lambda: datetime.now(tz=UTC))
        self._log = get_logger(__name__)
        self.state = ExchangeStreamState()

    @property
    def source(self) -> str:
        return self._source

    @property
    def circuit_breaker(self) -> CircuitBreaker:
        return self._circuit_breaker

    async def run(self, *, max_messages: int | None = None) -> ExchangeStreamState:
        published = 0
        reconnects_used = 0

        while max_messages is None or published < max_messages:
            try:
                remaining = None if max_messages is None else max_messages - published
                outcome = await self._circuit_breaker.call(
                    self._attempt_connection,
                    max_messages=remaining,
                )
                published += outcome.published
                if outcome.productive_close is not None:
                    self._record_reconnect(outcome.productive_close, reconnects_used)
                    reconnects_used = 0
                    self.state.reconnects += 1
                    await self._sleep(self._backoff_delay(1))
                    continue
                if max_messages is not None and published >= max_messages:
                    return self.state
            except CircuitOpenError:
                self.state.last_error = "circuit open"
                self._log.warning(
                    "ingestion.exchange_circuit_open",
                    source=self.source,
                    reconnects=self.state.reconnects,
                )
                raise
            except ExchangeStreamClosed as exc:
                # Only unproductive closes reach here — a socket that connected
                # but streamed nothing is indistinguishable from a broken one.
                published += exc.published
                self._record_reconnect(exc, reconnects_used)
                if reconnects_used >= self._max_reconnects:
                    raise
                reconnects_used += 1
                self.state.reconnects += 1
                await self._sleep(self._backoff_delay(reconnects_used))
            except Exception as exc:
                self._record_reconnect(exc, reconnects_used)
                if reconnects_used >= self._max_reconnects:
                    raise

                reconnects_used += 1
                self.state.reconnects += 1
                await self._sleep(self._backoff_delay(reconnects_used))

        return self.state

    async def _attempt_connection(self, *, max_messages: int | None = None) -> _ConnectionOutcome:
        """
        Run one connection under the circuit breaker.

        A close that already streamed messages is routine exchange churn —
        Binance recycles sockets on a schedule — not a transport failure. It is
        reported as a normal outcome so it neither trips the breaker nor
        consumes the reconnect budget; otherwise ``max_reconnects`` healthy
        socket recycles would retire the client permanently. A close that
        published nothing is indistinguishable from a broken connection and
        still propagates as a failure.
        """
        try:
            published = await self._run_connection(max_messages=max_messages)
            return _ConnectionOutcome(published=published)
        except ExchangeStreamClosed as exc:
            if not exc.published:
                raise
            return _ConnectionOutcome(published=exc.published, productive_close=exc)

    @asynccontextmanager
    async def _open_connection(self) -> AsyncIterator[WebSocketConnection]:
        """
        Open the websocket, accepting either shape ``ConnectFactory`` allows.

        ``websockets.connect(url)`` returns an object that is *both* awaitable
        and an async context manager. Awaiting it first yields a bare
        ``WebSocketClientProtocol``, which has no ``__aenter__`` — so checking
        awaitability first breaks every real connection. The context-manager
        form therefore has to win.
        """
        candidate: Any = self._connect_factory(self._url)
        if not hasattr(candidate, "__aenter__") and isawaitable(candidate):
            candidate = await candidate

        if hasattr(candidate, "__aenter__"):
            async with candidate as websocket:
                yield websocket
            return

        # A factory handing back an already-open protocol: closing is ours.
        try:
            yield candidate
        finally:
            closer = getattr(candidate, "close", None)
            if closer is not None:
                pending = closer()
                if isawaitable(pending):
                    await pending

    async def _run_connection(self, *, max_messages: int | None = None) -> int:
        published = 0
        async with self._open_connection() as websocket:
            await self._on_connected(websocket)
            iterator = websocket.__aiter__()
            while True:
                try:
                    raw_message = await self._receive_next(iterator)
                except StopAsyncIteration as exc:
                    raise ExchangeStreamClosed(self.source, published) from exc

                now = self._time_fn()
                self.state.mark_message(now)

                # A single unparsable frame must not tear down the socket: an
                # exchange error frame or an unlisted stream type would
                # otherwise count as a circuit-breaker failure and, after
                # three, retire the client for good.
                try:
                    payload = self._decode_message(raw_message)
                    if self._is_heartbeat_payload(payload):
                        self.state.mark_heartbeat(now)
                        continue

                    normalized_payload = self._extract_market_payload(payload)
                    if normalized_payload is None:
                        continue

                    market_event = normalize_market_payload(
                        normalized_payload,
                        source_override=self.source,
                    )
                except (ValueError, KeyError, TypeError) as exc:
                    self.state.malformed_messages += 1
                    self.state.last_error = f"{type(exc).__name__}: {exc}"
                    self._log.warning(
                        "ingestion.exchange_malformed_message",
                        source=self.source,
                        error=str(exc),
                    )
                    continue

                message_id = market_event_key(
                    market_event.symbol,
                    market_event.ts,
                    market_event.source,
                )
                await self._bus.publish(
                    self._topic,
                    market_event.model_dump(mode="json"),
                    message_id=message_id,
                    correlation_id=market_event.correlation_id,
                )
                self.state.events_published += 1
                published += 1
                self._log.info(
                    "ingestion.exchange_published",
                    topic=self._topic,
                    source=self.source,
                    symbol=market_event.symbol,
                    message_id=message_id,
                )
                if max_messages is not None and published >= max_messages:
                    return published

    async def _receive_next(self, iterator: AsyncIterator[str | bytes]) -> str | bytes:
        if self._heartbeat_timeout_seconds is None:
            return await iterator.__anext__()
        try:
            return await asyncio.wait_for(
                iterator.__anext__(),
                timeout=self._heartbeat_timeout_seconds,
            )
        except TimeoutError as exc:
            raise ExchangeStreamStale(
                f"{self.source} websocket stale for " f"{self._heartbeat_timeout_seconds:.3f}s"
            ) from exc

    def _backoff_delay(self, reconnect_attempt: int) -> float:
        return self._reconnect_backoff_seconds * (2 ** max(reconnect_attempt - 1, 0))

    def _record_reconnect(self, exc: Exception, reconnects_used: int) -> None:
        self.state.connect_failures += 1
        self.state.last_error = str(exc)
        self._log.warning(
            "ingestion.exchange_stream_error",
            source=self.source,
            reconnects_used=reconnects_used,
            error=str(exc),
        )

    def is_stale(
        self,
        *,
        stale_after_seconds: float,
        now: datetime | None = None,
    ) -> bool:
        return self.state.is_stale(
            stale_after_seconds=stale_after_seconds,
            now=now,
        )

    async def _on_connected(self, websocket: WebSocketConnection) -> None:
        return None

    def _decode_message(self, raw_message: str | bytes) -> dict[str, Any]:
        if isinstance(raw_message, bytes):
            raw_message = raw_message.decode("utf-8")
        payload = json.loads(raw_message)
        if not isinstance(payload, dict):
            raise ValueError(f"Expected JSON object message, got {payload!r}")
        return payload

    def _extract_market_payload(self, payload: dict[str, Any]) -> dict[str, Any] | None:
        return payload

    def _is_heartbeat_payload(self, payload: dict[str, Any]) -> bool:
        return False
