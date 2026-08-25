"""Shared service layer for the API routes."""

from __future__ import annotations

import asyncio
from collections import deque
from collections.abc import Sequence
from contextlib import suppress
from dataclasses import dataclass
from datetime import UTC, datetime
from typing import Any

from libs.common import (
    INSIGHT_CACHE_PREFIX,
    TOPIC_ALERTS,
    TOPIC_INSIGHTS,
    TOPIC_MARKET_RAW,
    TOPIC_SIGNALS,
    Alert,
    Cache,
    Insight,
    MarketEvent,
    MessageBus,
    ServiceMetrics,
    Signal,
    TimeSeriesStore,
    close_backends,
    get_logger,
    render_counters,
)

API_SUBSCRIPTION = "api"
API_WS_SUBSCRIPTION = "api-ws"
HISTORY_PREFIX = "history"
# Fallback window for a bus that cannot supply sequence numbers. peek() returns
# from the head of the subscription, so responses take the TAIL of this window.
BUS_PEEK_WINDOW = 1_000
# How many validated payloads per topic the read model keeps. Well above the
# routes' `le=100` cap, so /signals and /alerts are always served in full. An
# insight older than this is served from the Redis cache only.
RECENT_WINDOW = 500
# Messages read per peek call while catching up, and the cap on how many such
# calls one request may make. The cap bounds the cost of a cold first request
# against a large backlog; the cursor persists, so the next request continues.
PEEK_CHUNK = 100
MAX_PEEK_CHUNKS_PER_REQUEST = 20
STREAM_POLL_INTERVAL_SECONDS = 0.05
MAX_STREAM_QUEUE_SIZE = 1_000
TopicModel = type[MarketEvent] | type[Signal] | type[Alert] | type[Insight]


@dataclass(frozen=True)
class StreamEnvelope:
    topic: str
    event: str
    symbol: str
    payload: dict[str, Any]


@dataclass
class StreamSubscriber:
    symbols: set[str]
    queue: asyncio.Queue[dict[str, Any]]


class LiveStreamBroker:
    """Bus-backed live stream fanout shared across websocket clients."""

    def __init__(
        self,
        *,
        bus: MessageBus,
        subscription: str = API_WS_SUBSCRIPTION,
        poll_interval_seconds: float = STREAM_POLL_INTERVAL_SECONDS,
    ) -> None:
        self._bus = bus
        self._subscription = subscription
        self._poll_interval_seconds = poll_interval_seconds
        self._log = get_logger(__name__)
        self._subscribers: dict[str, StreamSubscriber] = {}
        self._tasks: list[asyncio.Task[None]] = []
        self._started = False
        self._stop_event = asyncio.Event()
        self.dropped_stream_messages = 0
        self._topics: tuple[tuple[str, str, TopicModel], ...] = (
            (TOPIC_MARKET_RAW, "market", MarketEvent),
            (TOPIC_SIGNALS, "signal", Signal),
            (TOPIC_ALERTS, "alert", Alert),
            (TOPIC_INSIGHTS, "insight", Insight),
        )

    @property
    def active_connections(self) -> int:
        return len(self._subscribers)

    async def start(self, *, prime_subscription: bool) -> None:
        if self._started:
            return
        self._started = True
        self._stop_event.clear()
        if prime_subscription:
            for topic, _, _ in self._topics:
                await self._bus.receive(topic, self._subscription, max_messages=0)
        self._tasks = [
            asyncio.create_task(
                self._pump_topic(topic=topic, event_name=event_name, model=model),
                name=f"api-stream-{topic}",
            )
            for topic, event_name, model in self._topics
        ]

    async def close(self) -> None:
        self._stop_event.set()
        for task in self._tasks:
            task.cancel()
        if self._tasks:
            await asyncio.gather(*self._tasks, return_exceptions=True)
        self._tasks = []
        self._subscribers.clear()
        self._started = False

    def register(self, connection_id: str) -> asyncio.Queue[dict[str, Any]]:
        # Bounded so a client that subscribes to a busy symbol and then stops
        # reading cannot grow the process without limit. On overflow the oldest
        # pending message is dropped: this is a live stream, so the newest data
        # is what the client actually wants.
        queue: asyncio.Queue[dict[str, Any]] = asyncio.Queue(maxsize=MAX_STREAM_QUEUE_SIZE)
        self._subscribers[connection_id] = StreamSubscriber(symbols=set(), queue=queue)
        return queue

    def update_symbols(self, connection_id: str, symbols: Sequence[str]) -> list[str]:
        subscriber = self._subscribers.get(connection_id)
        if subscriber is None:
            # The connection was dropped (or the broker closed) between the
            # client's frame arriving and this call.
            raise ValueError("stream connection is no longer registered")
        subscriber.symbols = {symbol.strip().upper() for symbol in symbols if symbol.strip()}
        return sorted(subscriber.symbols)

    def unregister(self, connection_id: str) -> None:
        self._subscribers.pop(connection_id, None)

    def _has_active_subscriptions(self) -> bool:
        return any(subscriber.symbols for subscriber in self._subscribers.values())

    async def _pump_topic(
        self,
        *,
        topic: str,
        event_name: str,
        model: TopicModel,
    ) -> None:
        while not self._stop_event.is_set():
            try:
                if not self._has_active_subscriptions():
                    await asyncio.sleep(self._poll_interval_seconds)
                    continue

                messages = await self._bus.receive(
                    topic,
                    self._subscription,
                    max_messages=25,
                )
                if not messages:
                    await asyncio.sleep(self._poll_interval_seconds)
                    continue

                for message in messages:
                    envelope = self._build_envelope(
                        message=message,
                        topic=topic,
                        event_name=event_name,
                        model=model,
                    )
                    if envelope is not None:
                        await self._fanout(envelope)
                    await self._bus.complete(message)
            except asyncio.CancelledError:
                raise
            except Exception as exc:
                self._log.warning(
                    "api.stream_broker_receive_failed",
                    topic=topic,
                    subscription=self._subscription,
                    error=str(exc),
                )
                await asyncio.sleep(self._poll_interval_seconds)

    def _build_envelope(
        self,
        *,
        message: Any,
        topic: str,
        event_name: str,
        model: TopicModel,
    ) -> StreamEnvelope | None:
        try:
            payload = model.model_validate(message.body).model_dump(mode="json")
        except Exception:
            self._log.warning(
                "api.invalid_stream_message_skipped",
                topic=topic,
                subscription=self._subscription,
                message_id=getattr(message, "message_id", ""),
                model=model.__name__,
            )
            return None
        symbol = str(payload["symbol"]).upper()
        return StreamEnvelope(topic=topic, event=event_name, symbol=symbol, payload=payload)

    async def _fanout(self, envelope: StreamEnvelope) -> None:
        message = {
            "type": envelope.event,
            "topic": envelope.topic,
            "symbol": envelope.symbol,
            "payload": envelope.payload,
        }
        for subscriber in list(self._subscribers.values()):
            if envelope.symbol not in subscriber.symbols:
                continue
            try:
                subscriber.queue.put_nowait(message)
            except asyncio.QueueFull:
                # Drop the oldest so a stalled reader cannot block the broker
                # (an awaiting put would stall fanout for every other client).
                self.dropped_stream_messages += 1
                with suppress(asyncio.QueueEmpty):  # drained concurrently
                    subscriber.queue.get_nowait()
                subscriber.queue.put_nowait(message)


def _backend_name(obj: object) -> str:
    name = type(obj).__name__
    return name.removesuffix("Client").removesuffix("Store").lower()


@dataclass
class APIMetrics(ServiceMetrics):
    requests_total: int = 0
    symbols_requests: int = 0
    latest_requests: int = 0
    history_requests: int = 0
    indicators_requests: int = 0
    signals_requests: int = 0
    alerts_requests: int = 0
    insights_requests: int = 0

    def render(
        self,
        *,
        timeseries_backend: str,
        cache_backend: str,
        bus_backend: str,
    ) -> str:
        lines = render_counters(
            "api",
            {
                "requests_total": self.requests_total,
                "symbols_requests": self.symbols_requests,
                # Published under the route they count, not the field name.
                "market_latest_requests": self.latest_requests,
                "market_history_requests": self.history_requests,
                "indicators_requests": self.indicators_requests,
                "signals_requests": self.signals_requests,
                "alerts_requests": self.alerts_requests,
                "insights_requests": self.insights_requests,
            },
        )
        lines.extend(
            [
                "# TYPE api_structured_logging_json gauge",
                "api_structured_logging_json 1",
                "# TYPE api_backend_info gauge",
                f'api_backend_info{{kind="timeseries",backend="{timeseries_backend}"}} 1',
                f'api_backend_info{{kind="cache",backend="{cache_backend}"}} 1',
                f'api_backend_info{{kind="bus",backend="{bus_backend}"}} 1',
            ]
        )
        lines.extend(self.http.render("api"))
        return "\n".join(lines) + "\n"


class APIService:
    """Query facade over the shared ports used by the API service."""

    def __init__(
        self,
        *,
        store: TimeSeriesStore,
        cache: Cache,
        bus: MessageBus,
        signal_topic: str = TOPIC_SIGNALS,
        alert_topic: str = TOPIC_ALERTS,
        insight_topic: str = TOPIC_INSIGHTS,
        subscription: str = API_SUBSCRIPTION,
    ) -> None:
        self._store = store
        self._cache = cache
        self._bus = bus
        self._signal_topic = signal_topic
        self._alert_topic = alert_topic
        self._insight_topic = insight_topic
        self._subscription = subscription
        self.metrics = APIMetrics()
        self._log = get_logger(__name__)
        self._stream_broker = LiveStreamBroker(bus=bus)
        # Incremental read model over the peeked topics: validated payloads in
        # arrival order, plus the sequence number to resume each peek from.
        self._recent: dict[str, deque[dict[str, Any]]] = {
            topic: deque(maxlen=RECENT_WINDOW)
            for topic in (signal_topic, alert_topic, insight_topic)
        }
        self._peek_cursor: dict[str, int] = {}

    @property
    def timeseries_backend(self) -> str:
        return _backend_name(self._store)

    @property
    def cache_backend(self) -> str:
        return _backend_name(self._cache)

    @property
    def bus_backend(self) -> str:
        return _backend_name(self._bus)

    async def prime_subscriptions(self) -> None:
        if self.bus_backend == "inmemorybus":
            for topic in (self._signal_topic, self._alert_topic, self._insight_topic):
                await self._bus.receive(topic, self._subscription, max_messages=0)
        await self._stream_broker.start(prime_subscription=self.bus_backend == "inmemorybus")

    async def close(self) -> None:
        """Release the stream broker and every backend, even if one of them fails."""
        await self._stream_broker.close()
        await close_backends(
            (self._store, self._cache, self._bus),
            log=self._log,
            service_name="api",
        )

    async def health(self) -> dict[str, Any]:
        return {
            "status": "ok",
            "service": "api",
            "subscription": self._subscription,
            "backends": {
                "timeseries": self.timeseries_backend,
                "cache": self.cache_backend,
                "bus": self.bus_backend,
            },
            "structured_logging": "json",
        }

    async def list_symbols(self) -> list[str]:
        self.metrics.symbols_requests += 1
        self.metrics.requests_total += 1
        candidate_tables = ("ticks", "indicators")
        table_rows = await self._store.query_sql(
            """
            SELECT "TABLE_NAME"
            FROM INFORMATION_SCHEMA.TABLES
            WHERE "TABLE_SCHEMA" = 'druid'
              AND "TABLE_NAME" IN ('ticks', 'indicators')
            """
        )
        known_tables = {str(row["TABLE_NAME"]) for row in table_rows if row.get("TABLE_NAME")}
        rows: list[dict[str, Any]] = []
        for table in candidate_tables:
            if table not in known_tables:
                continue
            rows.extend(
                await self._store.query_sql(
                    f"""
                    SELECT DISTINCT "symbol" AS "symbol"
                    FROM "{table}"
                    WHERE "symbol" IS NOT NULL
                    """
                )
            )
        symbols = {str(row["symbol"]) for row in rows if row.get("symbol")}
        symbols.update(await self._cache.list_snapshot_symbols())
        return sorted(symbols)

    async def latest_market(self, symbol: str) -> dict[str, Any] | None:
        self.metrics.latest_requests += 1
        self.metrics.requests_total += 1
        snapshot = await self._cache.get_snapshot(symbol)
        if snapshot is not None:
            return self._normalise_row(
                {
                    "symbol": symbol,
                    "ts": snapshot.get("ts"),
                    "source": snapshot.get("source"),
                    "event_type": snapshot.get("event_type"),
                    "price": snapshot.get("price"),
                    "volume": snapshot.get("volume"),
                    "bid": snapshot.get("bid"),
                    "ask": snapshot.get("ask"),
                }
            )
        row = await self._store.latest(symbol)
        return self._normalise_optional_row(row)

    async def market_history(
        self,
        symbol: str,
        *,
        frm: datetime,
        to: datetime,
        limit: int | None = None,
    ) -> list[dict[str, Any]]:
        self.metrics.history_requests += 1
        self.metrics.requests_total += 1
        rows = await self._store.history(symbol, frm, to, limit=limit)
        cached_rows = await self._cached_history(symbol, frm=frm, to=to)
        merged_rows = self._dedupe_rows([*rows, *cached_rows])
        ordered = sorted(merged_rows, key=self._ts_sort_key)
        if limit is not None:
            # Cached rows are merged in after the store's own LIMIT, so cap the
            # combined result too — newest wins. A non-positive limit asks for
            # no rows; ``[-0:]`` would hand back every one of them.
            ordered = ordered[-limit:] if limit > 0 else []
        return [self._normalise_row(row) for row in ordered]

    async def indicators(self, symbol: str) -> dict[str, Any] | None:
        self.metrics.indicators_requests += 1
        self.metrics.requests_total += 1
        snapshot = await self._cache.get_snapshot(symbol)
        if snapshot is not None:
            return {
                "symbol": symbol,
                "ts": snapshot.get("ts"),
                "source": snapshot.get("source"),
                "price": snapshot.get("price"),
                "anomaly": snapshot.get("anomaly"),
                "indicators": {
                    "sma": snapshot.get("sma"),
                    "ema": snapshot.get("ema"),
                    "rsi": snapshot.get("rsi"),
                    "volatility": snapshot.get("volatility"),
                    "trend": snapshot.get("trend_score"),
                },
                "flags": {
                    "trend": snapshot.get("trend"),
                    "zscore_anomaly": snapshot.get("zscore_anomaly"),
                    "ewma_anomaly": snapshot.get("ewma_anomaly"),
                },
            }

        latest = await self._store.latest_indicator(symbol)
        if latest is None:
            return None
        latest = self._normalise_row(latest)
        return {
            "symbol": symbol,
            "ts": latest.get("ts"),
            "source": latest.get("source"),
            "price": latest.get("price"),
            "anomaly": latest.get("anomaly"),
            "indicators": {
                "sma": latest.get("sma"),
                "ema": latest.get("ema"),
                "rsi": latest.get("rsi"),
                "volatility": latest.get("volatility"),
                "trend": latest.get("trend_score"),
            },
            "flags": {
                "trend": latest.get("trend"),
                "zscore_anomaly": latest.get("zscore_anomaly"),
                "ewma_anomaly": latest.get("ewma_anomaly"),
            },
        }

    async def _refresh_recent(
        self,
        topic: str,
        model: type[Signal] | type[Alert] | type[Insight],
    ) -> list[dict[str, Any]]:
        """
        Bring the read model for *topic* up to date and return it, oldest first.

        ``peek`` reads from the head of the subscription — the lowest sequence
        numbers — so asking it for *limit* messages returns the OLDEST ones and
        pins the response to the same backlog on every call. Reading the whole
        window instead fixed that but re-read (and re-decoded) the entire
        backlog on every request. So the window is walked once and remembered:
        each request resumes at the sequence number the last one stopped at and
        pays only for messages that arrived since.
        """
        recent = self._recent[topic]
        cursor = self._peek_cursor.get(topic)
        for _ in range(MAX_PEEK_CHUNKS_PER_REQUEST):
            messages = await self._bus.peek(
                topic,
                self._subscription,
                n=PEEK_CHUNK,
                from_sequence_number=cursor,
            )
            if not messages:
                break
            if messages[-1].sequence_number is None:
                # A bus that cannot number its messages gives no cursor to
                # resume from, so re-reading a fixed window is the only correct
                # option left. Nothing is remembered: appending would duplicate
                # the same messages on the next request.
                return self._validated_payloads(
                    await self._bus.peek(topic, self._subscription, n=BUS_PEEK_WINDOW),
                    model,
                )
            recent.extend(self._validated_payloads(messages, model))
            cursor = messages[-1].sequence_number + 1
            self._peek_cursor[topic] = cursor
            if len(messages) < PEEK_CHUNK:
                break
        else:
            # Still behind after the per-request cap. The cursor is saved, so
            # the next request picks up where this one stopped.
            self._log.warning(
                "api.recent_catch_up_truncated",
                topic=topic,
                subscription=self._subscription,
                cursor=cursor,
            )
        return list(recent)

    async def _recent_payloads(
        self,
        topic: str,
        model: type[Signal] | type[Alert] | type[Insight],
        *,
        limit: int,
    ) -> list[dict[str, Any]]:
        """Return the newest *limit* payloads on *topic*, newest first."""
        if limit <= 0:
            return []
        payloads = await self._refresh_recent(topic, model)
        return list(reversed(payloads[-limit:]))

    async def signals(self, *, limit: int = 20) -> list[dict[str, Any]]:
        self.metrics.signals_requests += 1
        self.metrics.requests_total += 1
        return await self._recent_payloads(self._signal_topic, Signal, limit=limit)

    async def alerts(self, *, limit: int = 20) -> list[dict[str, Any]]:
        self.metrics.alerts_requests += 1
        self.metrics.requests_total += 1
        return await self._recent_payloads(self._alert_topic, Alert, limit=limit)

    async def insight(self, symbol: str) -> dict[str, Any] | None:
        self.metrics.insights_requests += 1
        self.metrics.requests_total += 1
        cached = await self._cache.get(f"{INSIGHT_CACHE_PREFIX}:{symbol}")
        if cached is not None:
            return Insight.model_validate(cached).model_dump(mode="json")

        # Served from the same incremental read model as /signals and /alerts,
        # so a cache miss no longer re-peeks the whole backlog: each message is
        # peeked and validated once, then scanned newest-first for the symbol.
        payloads = await self._refresh_recent(self._insight_topic, Insight)
        for payload in reversed(payloads):
            if payload.get("symbol") == symbol:
                return payload
        return None

    def render_metrics(self) -> str:
        return self.metrics.render(
            timeseries_backend=self.timeseries_backend,
            cache_backend=self.cache_backend,
            bus_backend=self.bus_backend,
        )

    @property
    def active_stream_connections(self) -> int:
        return self._stream_broker.active_connections

    def register_stream(self, connection_id: str) -> asyncio.Queue[dict[str, Any]]:
        return self._stream_broker.register(connection_id)

    def subscribe_stream(self, connection_id: str, symbols: Sequence[str]) -> list[str]:
        return self._stream_broker.update_symbols(connection_id, symbols)

    def unregister_stream(self, connection_id: str) -> None:
        self._stream_broker.unregister(connection_id)

    async def _cached_history(
        self,
        symbol: str,
        *,
        frm: datetime,
        to: datetime,
    ) -> list[dict[str, Any]]:
        history = await self._cache.get(f"{HISTORY_PREFIX}:{symbol}")
        if not isinstance(history, list):
            return []

        rows: list[dict[str, Any]] = []
        for row in history:
            if not isinstance(row, dict) or row.get("symbol") != symbol:
                continue
            ts = self._parse_ts(row.get("ts") or row.get("__time"))
            if ts is None or not self._within_range(ts, frm=frm, to=to):
                continue
            rows.append(row)
        return rows

    @classmethod
    def _dedupe_rows(cls, rows: Sequence[dict[str, Any]]) -> list[dict[str, Any]]:
        deduped: dict[tuple[str, str, str], dict[str, Any]] = {}
        for row in rows:
            ts = cls._parse_ts(row.get("ts") or row.get("__time"))
            raw_ts = row.get("ts") or row.get("__time") or ""
            ts_key = ts.isoformat() if ts is not None else str(raw_ts)
            key = (
                str(row.get("event_id") or ""),
                str(row.get("symbol") or ""),
                ts_key,
            )
            deduped[key] = row
        return list(deduped.values())

    def _validated_payload(
        self,
        message: Any,
        model: type[Signal] | type[Alert] | type[Insight],
    ) -> dict[str, Any] | None:
        """Validate one bus message, returning None (and logging) if it is bad."""
        try:
            return model.model_validate(message.body).model_dump(mode="json")
        except Exception:
            self._log.warning(
                "api.invalid_message_skipped",
                topic=getattr(message, "topic", "unknown"),
                subscription=getattr(message, "subscription", self._subscription),
                message_id=getattr(message, "message_id", ""),
                model=model.__name__,
            )
            return None

    def _validated_payloads(
        self,
        messages: Sequence[Any],
        model: type[Signal] | type[Alert] | type[Insight],
    ) -> list[dict[str, Any]]:
        payloads = (self._validated_payload(message, model) for message in messages)
        return [payload for payload in payloads if payload is not None]

    @classmethod
    def _normalise_optional_row(cls, row: dict[str, Any] | None) -> dict[str, Any] | None:
        """``_normalise_row`` for call sites where the backend may return nothing."""
        return None if row is None else cls._normalise_row(row)

    @classmethod
    def _normalise_row(cls, row: dict[str, Any]) -> dict[str, Any]:
        """Emit a consistent ``ts`` regardless of which backend produced the row."""
        normalised = dict(row)
        ts = cls._row_timestamp(normalised)
        if hasattr(ts, "isoformat"):
            ts = ts.isoformat()
        if ts is not None:
            normalised["ts"] = ts
        return normalised

    @staticmethod
    def _parse_ts(value: Any) -> datetime | None:
        if value is None:
            return None
        if isinstance(value, datetime):
            return value
        if isinstance(value, (int, float)):
            return datetime.fromtimestamp(value, tz=UTC)
        if isinstance(value, str):
            try:
                return datetime.fromisoformat(value.replace("Z", "+00:00"))
            except ValueError:
                return None
        return None

    @classmethod
    def _within_range(cls, ts: datetime, *, frm: datetime, to: datetime) -> bool:
        return (
            cls._normalise_datetime(frm)
            <= cls._normalise_datetime(ts)
            <= cls._normalise_datetime(to)
        )

    @staticmethod
    def _normalise_datetime(value: datetime) -> datetime:
        if value.tzinfo is None:
            return value
        return value.astimezone(UTC).replace(tzinfo=None)

    @staticmethod
    def _row_timestamp(row: dict[str, Any]) -> Any:
        """
        Read a row's timestamp under either column name.

        Druid promotes the ingest ``timestampSpec`` column to ``__time``, so
        rows read back from Druid carry ``__time`` where snapshot/cache rows
        carry ``ts``. ``_cached_history`` and ``_dedupe_rows`` already accept
        both; ordering and response shaping must too.
        """
        return row.get("ts") if row.get("ts") is not None else row.get("__time")

    @classmethod
    def _ts_sort_key(cls, row: dict[str, Any]) -> datetime:
        """
        Total ordering over rows from either backend.

        The previous key read ``ts`` only and fell back to a constant, so every
        sort and ``max()`` over Druid rows silently became a no-op — history
        came back in arbitrary order and ``/indicators`` served whichever row
        happened to be first rather than the latest.
        """
        ts = cls._parse_ts(cls._row_timestamp(row))
        return cls._normalise_datetime(ts) if ts is not None else datetime.min
