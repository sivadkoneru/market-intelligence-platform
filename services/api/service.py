"""Shared service layer for the API routes."""

from __future__ import annotations

from collections.abc import Sequence
from dataclasses import dataclass
from datetime import datetime
from inspect import isawaitable
from typing import Any

from libs.common import (
    TOPIC_ALERTS,
    TOPIC_INSIGHTS,
    TOPIC_SIGNALS,
    Alert,
    Cache,
    Insight,
    MessageBus,
    Signal,
    TimeSeriesStore,
    get_logger,
)

API_SUBSCRIPTION = "api"
INSIGHT_CACHE_PREFIX = "insight"
INSIGHT_BUS_FALLBACK_LIMIT = 1_000


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
        if self.bus_backend != "inmemorybus":
            return
        for topic in (self._signal_topic, self._alert_topic, self._insight_topic):
            await self._bus.receive(topic, self._subscription, max_messages=0)

    async def close(self) -> None:
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
        rows = await self._rows_for_tables(("ticks", "indicators"))
        symbols = {
            str(row["symbol"])
            for row in rows
            if row.get("symbol")
        }
        return sorted(symbols)

    async def latest_market(self, symbol: str) -> dict[str, Any] | None:
        self.metrics.latest_requests += 1
        self.metrics.requests_total += 1
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
        return [self._normalise_row(row) for row in sorted(rows, key=self._ts_sort_key)]

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

    async def _rows_for_tables(self, tables: Sequence[str]) -> list[dict[str, Any]]:
        rows: list[dict[str, Any]] = []
        for table in tables:
            rows.extend(await self._store.query_sql(f'SELECT * FROM "{table}"'))
        return rows

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
    def _ts_sort_key(row: dict[str, Any]) -> tuple[int, str]:
        ts = row.get("ts")
        if hasattr(ts, "isoformat"):
            return (1, ts.isoformat())
        if ts is None:
            return (0, "")
        return (1, str(ts))
