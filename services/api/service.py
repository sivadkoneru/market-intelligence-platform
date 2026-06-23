"""Shared service layer for the API routes."""

from __future__ import annotations

import asyncio
from collections.abc import Sequence
from dataclasses import dataclass, field
from datetime import datetime, timezone
from inspect import isawaitable
from typing import Any

from libs.common import (
    TOPIC_ALERTS,
    TOPIC_INSIGHTS,
    TOPIC_MARKET_RAW,
    TOPIC_SIGNALS,
    Alert,
    Cache,
    HTTPMetrics,
    Insight,
    MarketEvent,
    MessageBus,
    Signal,
    TimeSeriesStore,
    get_logger,
)

API_SUBSCRIPTION = "api"
API_WS_SUBSCRIPTION = "api-ws"
INSIGHT_CACHE_PREFIX = "insight"
HISTORY_PREFIX = "history"
INSIGHT_BUS_FALLBACK_LIMIT = 1_000
STREAM_POLL_INTERVAL_SECONDS = 0.05
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
        queue: asyncio.Queue[dict[str, Any]] = asyncio.Queue()
        self._subscribers[connection_id] = StreamSubscriber(symbols=set(), queue=queue)
        return queue

    def update_symbols(self, connection_id: str, symbols: Sequence[str]) -> list[str]:
        subscriber = self._subscribers[connection_id]
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
            if envelope.symbol in subscriber.symbols:
                await subscriber.queue.put(message)


def _backend_name(obj: object) -> str:
    name = type(obj).__name__
    return name.removesuffix("Client").removesuffix("Store").lower()


@dataclass
class APIMetrics:
    requests_total: int = 0
    symbols_requests: int = 0
    latest_requests: int = 0
    history_requests: int = 0
    indicators_requests: int = 0
    signals_requests: int = 0
    alerts_requests: int = 0
    insights_requests: int = 0
    http: HTTPMetrics = field(default_factory=HTTPMetrics)

    def record_http_request(
        self,
        *,
        method: str,
        path: str,
        status_code: int,
        duration_ms: float,
        trace_context_provided: bool,
        correlation_context_provided: bool,
    ) -> None:
        self.http.record_http_request(
            method=method,
            path=path,
            status_code=status_code,
            duration_ms=duration_ms,
            trace_context_provided=trace_context_provided,
            correlation_context_provided=correlation_context_provided,
        )

    def render(
        self,
        *,
        timeseries_backend: str,
        cache_backend: str,
        bus_backend: str,
    ) -> str:
        lines = [
            "# TYPE api_requests_total counter",
            f"api_requests_total {self.requests_total}",
            "# TYPE api_symbols_requests counter",
            f"api_symbols_requests {self.symbols_requests}",
            "# TYPE api_market_latest_requests counter",
            f"api_market_latest_requests {self.latest_requests}",
            "# TYPE api_market_history_requests counter",
            f"api_market_history_requests {self.history_requests}",
            "# TYPE api_indicators_requests counter",
            f"api_indicators_requests {self.indicators_requests}",
            "# TYPE api_signals_requests counter",
            f"api_signals_requests {self.signals_requests}",
            "# TYPE api_alerts_requests counter",
            f"api_alerts_requests {self.alerts_requests}",
            "# TYPE api_insights_requests counter",
            f"api_insights_requests {self.insights_requests}",
            "# TYPE api_structured_logging_json gauge",
            "api_structured_logging_json 1",
            "# TYPE api_backend_info gauge",
            f'api_backend_info{{kind="timeseries",backend="{timeseries_backend}"}} 1',
            f'api_backend_info{{kind="cache",backend="{cache_backend}"}} 1',
            f'api_backend_info{{kind="bus",backend="{bus_backend}"}} 1',
        ]
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
        await self._stream_broker.close()
        for backend in (self._cache, self._bus):
            close = getattr(backend, "close", None)
            if close is None:
                continue
            result = close()
            if isawaitable(result):
                await result

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
        known_tables = {
            str(row["TABLE_NAME"])
            for row in table_rows
            if row.get("TABLE_NAME")
        }
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
        symbols = {
            str(row["symbol"])
            for row in rows
            if row.get("symbol")
        }
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
        return self._normalise_row(row)

    async def market_history(
        self,
        symbol: str,
        *,
        frm: datetime,
        to: datetime,
    ) -> list[dict[str, Any]]:
        self.metrics.history_requests += 1
        self.metrics.requests_total += 1
        rows = await self._store.history(symbol, frm, to)
        cached_rows = await self._cached_history(symbol, frm=frm, to=to)
        merged_rows = self._dedupe_rows([*rows, *cached_rows])
        return [
            self._normalise_row(row)
            for row in sorted(merged_rows, key=self._ts_sort_key)
        ]

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

        rows = await self._rows_for_tables(("indicators",))
        matches = [row for row in rows if row.get("symbol") == symbol]
        if not matches:
            return None
        latest = max(matches, key=self._ts_sort_key)
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

    async def signals(self, *, limit: int = 20) -> list[dict[str, Any]]:
        self.metrics.signals_requests += 1
        self.metrics.requests_total += 1
        messages = await self._bus.peek(self._signal_topic, self._subscription, n=limit)
        return self._validated_payloads(messages, Signal)

    async def alerts(self, *, limit: int = 20) -> list[dict[str, Any]]:
        self.metrics.alerts_requests += 1
        self.metrics.requests_total += 1
        messages = await self._bus.peek(self._alert_topic, self._subscription, n=limit)
        return self._validated_payloads(messages, Alert)

    async def insight(self, symbol: str) -> dict[str, Any] | None:
        self.metrics.insights_requests += 1
        self.metrics.requests_total += 1
        cached = await self._cache.get(f"{INSIGHT_CACHE_PREFIX}:{symbol}")
        if cached is not None:
            return Insight.model_validate(cached).model_dump(mode="json")

        messages = await self._bus.peek(
            self._insight_topic,
            self._subscription,
            n=INSIGHT_BUS_FALLBACK_LIMIT,
        )
        for payload in reversed(self._validated_payloads(messages, Insight)):
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

    async def _rows_for_tables(self, tables: Sequence[str]) -> list[dict[str, Any]]:
        rows: list[dict[str, Any]] = []
        for table in tables:
            rows.extend(await self._store.query_sql(f'SELECT * FROM "{table}"'))
        return rows

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

    def _validated_payloads(
        self,
        messages: Sequence[Any],
        model: type[Signal] | type[Alert] | type[Insight],
    ) -> list[dict[str, Any]]:
        payloads: list[dict[str, Any]] = []
        for message in messages:
            try:
                payloads.append(model.model_validate(message.body).model_dump(mode="json"))
            except Exception:
                self._log.warning(
                    "api.invalid_message_skipped",
                    topic=getattr(message, "topic", "unknown"),
                    subscription=getattr(message, "subscription", self._subscription),
                    message_id=getattr(message, "message_id", ""),
                    model=model.__name__,
                )
        return payloads

    @staticmethod
    def _normalise_row(row: dict[str, Any] | None) -> dict[str, Any] | None:
        if row is None:
            return None
        normalised = dict(row)
        ts = normalised.get("ts")
        if hasattr(ts, "isoformat"):
            normalised["ts"] = ts.isoformat()
        return normalised

    @staticmethod
    def _parse_ts(value: Any) -> datetime | None:
        if value is None:
            return None
        if isinstance(value, datetime):
            return value
        if isinstance(value, (int, float)):
            return datetime.fromtimestamp(value, tz=timezone.utc)
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
        return value.astimezone(timezone.utc).replace(tzinfo=None)

    @staticmethod
    def _ts_sort_key(row: dict[str, Any]) -> tuple[int, str]:
        ts = row.get("ts")
        if hasattr(ts, "isoformat"):
            return (1, ts.isoformat())
        if ts is None:
            return (0, "")
        return (1, str(ts))
