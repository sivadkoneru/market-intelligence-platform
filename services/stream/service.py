"""
Core stream-processing service loop.
"""

from __future__ import annotations

import asyncio
from collections import defaultdict
from collections.abc import Sequence
from dataclasses import dataclass, field
from typing import Any

from pydantic import ValidationError

from libs.common import (
    TOPIC_MARKET_RAW,
    TOPIC_SIGNALS,
    Cache,
    HTTPMetrics,
    MarketEvent,
    MessageBus,
    ReceivedMessage,
    Signal,
    TimeSeriesStore,
    get_logger,
    market_event_key,
)
from services.stream.indicators import (
    detect_trend,
    ewma_anomaly,
    exponential_moving_average,
    relative_strength_index,
    rolling_volatility,
    simple_moving_average,
    z_score_anomaly,
)

STREAM_SUBSCRIPTION = "stream"
IDEMPOTENCY_PREFIX = "stream:processed"
HISTORY_PREFIX = "history"
MAX_CACHED_HISTORY_ROWS = 500


@dataclass(frozen=True)
class IndicatorConfig:
    sma_window: int = 5
    ema_window: int = 5
    rsi_period: int = 5
    volatility_window: int = 5
    trend_window: int = 5
    zscore_window: int = 5
    zscore_threshold: float = 2.0
    ewma_span: int = 5
    ewma_threshold: float = 3.0


@dataclass
class StreamMetrics:
    messages_seen: int = 0
    messages_processed: int = 0
    duplicates_suppressed: int = 0
    signals_published: int = 0
    dead_lettered: int = 0
    tick_rows_ingested: int = 0
    indicator_rows_ingested: int = 0
    last_error: str | None = None
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

    def render(self) -> str:
        lines = [
            "# TYPE stream_messages_seen counter",
            f"stream_messages_seen {self.messages_seen}",
            "# TYPE stream_messages_processed counter",
            f"stream_messages_processed {self.messages_processed}",
            "# TYPE stream_duplicates_suppressed counter",
            f"stream_duplicates_suppressed {self.duplicates_suppressed}",
            "# TYPE stream_signals_published counter",
            f"stream_signals_published {self.signals_published}",
            "# TYPE stream_dead_lettered counter",
            f"stream_dead_lettered {self.dead_lettered}",
            "# TYPE stream_tick_rows_ingested counter",
            f"stream_tick_rows_ingested {self.tick_rows_ingested}",
            "# TYPE stream_indicator_rows_ingested counter",
            f"stream_indicator_rows_ingested {self.indicator_rows_ingested}",
        ]
        lines.extend(self.http.render("stream"))
        return "\n".join(lines) + "\n"


@dataclass
class ProcessedSignal:
    tick_row: dict[str, Any]
    indicator_row: dict[str, Any]
    snapshot: dict[str, Any]
    signal: Signal


class StreamProcessor:
    """Stateful per-symbol indicator processor."""

    def __init__(self, config: IndicatorConfig | None = None) -> None:
        self._config = config or IndicatorConfig()
        self._price_history: dict[str, list[float]] = defaultdict(list)

    def process(self, event: MarketEvent) -> ProcessedSignal:
        prices = self._candidate_history(event)

        sma = simple_moving_average(prices, window=self._config.sma_window)
        ema = exponential_moving_average(prices, window=self._config.ema_window)
        rsi = relative_strength_index(prices, period=self._config.rsi_period)
        volatility = rolling_volatility(prices, window=self._config.volatility_window)
        trend = detect_trend(prices, window=self._config.trend_window)
        zscore_flag = z_score_anomaly(
            prices,
            window=self._config.zscore_window,
            threshold=self._config.zscore_threshold,
        )
        ewma_flag = ewma_anomaly(
            prices,
            span=self._config.ewma_span,
            threshold=self._config.ewma_threshold,
        )

        anomaly = bool(zscore_flag or ewma_flag)
        trend_score = _trend_score(trend)
        indicators = {
            "sma": sma,
            "ema": ema,
            "rsi": rsi,
            "volatility": volatility,
            "trend": trend_score,
        }

        snapshot = {
            "symbol": event.symbol,
            "ts": event.ts.isoformat(),
            "source": event.source,
            "event_type": event.event_type,
            "price": float(event.price),
            "volume": event.volume,
            "bid": event.bid,
            "ask": event.ask,
            "sma": sma,
            "ema": ema,
            "rsi": rsi,
            "volatility": volatility,
            "trend": trend,
            "trend_score": trend_score,
            "zscore_anomaly": zscore_flag,
            "ewma_anomaly": ewma_flag,
            "anomaly": anomaly,
        }

        tick_row = {
            "_table": "ticks",
            "event_id": event.event_id,
            "ts": event.ts.isoformat(),
            "symbol": event.symbol,
            "source": event.source,
            "event_type": event.event_type,
            "price": float(event.price),
            "volume": event.volume,
            "bid": event.bid,
            "ask": event.ask,
            "correlation_id": event.correlation_id,
            "trace_id": event.trace_id,
        }
        indicator_row = {
            "_table": "indicators",
            "event_id": event.event_id,
            "ts": event.ts.isoformat(),
            "symbol": event.symbol,
            "source": event.source,
            "price": float(event.price),
            "sma": sma,
            "ema": ema,
            "rsi": rsi,
            "volatility": volatility,
            "trend": trend,
            "trend_score": trend_score,
            "zscore_anomaly": zscore_flag,
            "ewma_anomaly": ewma_flag,
            "anomaly": anomaly,
            "correlation_id": event.correlation_id,
            "trace_id": event.trace_id,
        }
        signal = Signal(
            symbol=event.symbol,
            ts=event.ts,
            correlation_id=event.correlation_id,
            trace_id=event.trace_id,
            indicators=indicators,
            anomaly=anomaly,
        )
        return ProcessedSignal(
            tick_row=tick_row,
            indicator_row=indicator_row,
            snapshot=snapshot,
            signal=signal,
        )

    def commit(self, event: MarketEvent) -> None:
        self._price_history[event.symbol].append(float(event.price))

    def _candidate_history(self, event: MarketEvent) -> list[float]:
        prices = list(self._price_history[event.symbol])
        prices.append(float(event.price))
        return prices


class StreamService:
    def __init__(
        self,
        *,
        bus: MessageBus,
        cache: Cache,
        store: TimeSeriesStore,
        processor: StreamProcessor | None = None,
        topic: str = TOPIC_MARKET_RAW,
        subscription: str = STREAM_SUBSCRIPTION,
        signal_topic: str = TOPIC_SIGNALS,
    ) -> None:
        self._bus = bus
        self._cache = cache
        self._store = store
        self._processor = processor or StreamProcessor()
        self._topic = topic
        self._subscription = subscription
        self._signal_topic = signal_topic
        self.metrics = StreamMetrics()
        self._log = get_logger(__name__)

    async def poll_once(self, *, max_messages: int = 10) -> int:
        messages = await self._bus.receive(
            self._topic,
            self._subscription,
            max_messages=max_messages,
        )
        for message in messages:
            self.metrics.messages_seen += 1
            await self._handle_message(message)
        return len(messages)

    async def run_forever(
        self,
        *,
        poll_interval_seconds: float = 0.25,
        max_messages: int = 10,
    ) -> None:
        while True:
            try:
                processed = await self.poll_once(max_messages=max_messages)
            except Exception as exc:
                self.metrics.last_error = (
                    f"stream polling failed: {type(exc).__name__}: {exc}"
                )
                self._log.warning(
                    "stream.poll_failed",
                    error_type=type(exc).__name__,
                    error=str(exc),
                )
                await asyncio.sleep(poll_interval_seconds)
                continue
            if processed == 0:
                await asyncio.sleep(poll_interval_seconds)

    async def health(self) -> dict[str, Any]:
        return {
            "status": "ok",
            "service": "stream",
            "topic": self._topic,
            "subscription": self._subscription,
            "messages_processed": self.metrics.messages_processed,
            "duplicates_suppressed": self.metrics.duplicates_suppressed,
            "dead_lettered": self.metrics.dead_lettered,
        }

    async def _handle_message(self, message: ReceivedMessage) -> None:
        try:
            event = MarketEvent.model_validate(message.body)
        except ValidationError as exc:
            await self._dead_letter(message, f"invalid market event payload: {exc.errors()}")
            return

        event_key = market_event_key(event.symbol, event.ts, event.source)
        dedupe_key = f"{IDEMPOTENCY_PREFIX}:{event_key}"
        if await self._cache.get(dedupe_key):
            self.metrics.duplicates_suppressed += 1
            await self._bus.complete(message)
            self._log.info(
                "stream.duplicate_suppressed",
                symbol=event.symbol,
                message_id=message.message_id,
                dedupe_key=dedupe_key,
            )
            return

        try:
            processed = self._processor.process(event)
            await self._store.ingest([processed.tick_row])
            self.metrics.tick_rows_ingested += 1
            await self._store.ingest([processed.indicator_row])
            self.metrics.indicator_rows_ingested += 1

            await self._cache.set_snapshot(event.symbol, processed.snapshot)
            await self._append_cached_history(event.symbol, processed.tick_row)
            await self._bus.publish(
                self._signal_topic,
                processed.signal.model_dump(mode="json"),
                message_id=dedupe_key,
                correlation_id=event.correlation_id,
            )
            self.metrics.signals_published += 1
            await self._cache.set(dedupe_key, True)
            self._processor.commit(event)
            self.metrics.messages_processed += 1
            self._log.info(
                "stream.processed",
                symbol=event.symbol,
                message_id=message.message_id,
                signal_message_id=dedupe_key,
                anomaly=processed.signal.anomaly,
            )
            await self._bus.complete(message)
        except Exception as exc:
            await self._dead_letter(
                message,
                f"stream processing failed: {type(exc).__name__}: {exc}",
            )

    async def _dead_letter(self, message: ReceivedMessage, reason: str) -> None:
        self.metrics.dead_lettered += 1
        self.metrics.last_error = reason
        await self._bus.dead_letter(message, reason=reason)
        self._log.warning(
            "stream.dead_lettered",
            topic=message.topic,
            subscription=message.subscription,
            message_id=message.message_id,
            reason=reason,
        )

    async def _append_cached_history(self, symbol: str, row: dict[str, Any]) -> None:
        key = f"{HISTORY_PREFIX}:{symbol}"
        history = await self._cache.get(key)
        rows = list(history) if isinstance(history, list) else []
        cached_row = dict(row)
        cached_row.pop("_table", None)
        rows.append(cached_row)
        await self._cache.set(key, rows[-MAX_CACHED_HISTORY_ROWS:])


def _trend_score(trend: str | None) -> float | None:
    mapping = {"uptrend": 1.0, "flat": 0.0, "downtrend": -1.0}
    return mapping.get(trend) if trend is not None else None


def price_history_for_symbol(
    processor: StreamProcessor, symbol: str
) -> Sequence[float]:
    """Test helper for inspecting retained per-symbol history."""
    return tuple(processor._price_history.get(symbol, ()))
