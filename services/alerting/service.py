"""Core alerting worker for signal and insight events."""

from __future__ import annotations

import asyncio
from dataclasses import dataclass, field
from typing import Any

from pydantic import ValidationError
from tenacity import RetryError

from libs.common import (
    TOPIC_ALERTS,
    TOPIC_INSIGHTS,
    TOPIC_SIGNALS,
    Alert,
    Cache,
    CircuitBreaker,
    CircuitOpenError,
    HTTPMetrics,
    Insight,
    MessageBus,
    ReceivedMessage,
    Signal,
    get_logger,
    retry_async,
)
from services.alerting.rules import RuleEngine

ALERTING_SUBSCRIPTION = "alerting"
ALERT_PROCESSED_PREFIX = "alerting:processed"
ALERT_PUBLISHED_PREFIX = "alerting:published"
ALERT_LOCK_PREFIX = "alerting:lock"


def alert_processed_key(topic: str, event_id: str) -> str:
    return f"{ALERT_PROCESSED_PREFIX}:{topic}:{event_id}"


def alert_lock_key(topic: str, event_id: str) -> str:
    return f"{ALERT_LOCK_PREFIX}:{topic}:{event_id}"


def alert_published_key(dedupe_key: str) -> str:
    return f"{ALERT_PUBLISHED_PREFIX}:{dedupe_key}"


@dataclass
class AlertingMetrics:
    messages_seen: int = 0
    messages_processed: int = 0
    duplicates_suppressed: int = 0
    alerts_published: int = 0
    processing_retries: int = 0
    circuit_open_rejections: int = 0
    dead_lettered: int = 0
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
            "# TYPE alerting_messages_seen counter",
            f"alerting_messages_seen {self.messages_seen}",
            "# TYPE alerting_messages_processed counter",
            f"alerting_messages_processed {self.messages_processed}",
            "# TYPE alerting_duplicates_suppressed counter",
            f"alerting_duplicates_suppressed {self.duplicates_suppressed}",
            "# TYPE alerting_alerts_published counter",
            f"alerting_alerts_published {self.alerts_published}",
            "# TYPE alerting_processing_retries counter",
            f"alerting_processing_retries {self.processing_retries}",
            "# TYPE alerting_circuit_open_rejections counter",
            f"alerting_circuit_open_rejections {self.circuit_open_rejections}",
            "# TYPE alerting_dead_lettered counter",
            f"alerting_dead_lettered {self.dead_lettered}",
        ]
        lines.extend(self.http.render("alerting"))
        return "\n".join(lines) + "\n"


class AlertingService:
    def __init__(
        self,
        *,
        bus: MessageBus,
        cache: Cache,
        rule_engine: RuleEngine | None = None,
        signal_topic: str = TOPIC_SIGNALS,
        insight_topic: str = TOPIC_INSIGHTS,
        alert_topic: str = TOPIC_ALERTS,
        subscription: str = ALERTING_SUBSCRIPTION,
        max_processing_attempts: int = 3,
        retry_wait_min_seconds: float = 0.0,
        retry_wait_max_seconds: float = 0.0,
        circuit_breaker: CircuitBreaker | None = None,
    ) -> None:
        self._bus = bus
        self._cache = cache
        self._rule_engine = rule_engine or RuleEngine()
        self._signal_topic = signal_topic
        self._insight_topic = insight_topic
        self._alert_topic = alert_topic
        self._subscription = subscription
        self._max_processing_attempts = max(1, max_processing_attempts)
        self._retry_wait_min_seconds = retry_wait_min_seconds
        self._retry_wait_max_seconds = retry_wait_max_seconds
        self._circuit_breaker = circuit_breaker or CircuitBreaker()
        self.metrics = AlertingMetrics()
        self._log = get_logger(__name__)

    async def poll_once(self, *, max_messages: int = 10) -> int:
        total = 0
        for topic in (self._signal_topic, self._insight_topic):
            messages = await self._bus.receive(
                topic,
                self._subscription,
                max_messages=max_messages,
            )
            total += len(messages)
            for message in messages:
                self.metrics.messages_seen += 1
                await self._handle_message(message)
        return total

    async def run_forever(
        self,
        *,
        poll_interval_seconds: float = 0.25,
        max_messages: int = 10,
    ) -> None:
        while True:
            processed = await self.poll_once(max_messages=max_messages)
            if processed == 0:
                await asyncio.sleep(poll_interval_seconds)

    async def health(self) -> dict[str, Any]:
        return {
            "status": "ok",
            "service": "alerting",
            "topics": [self._signal_topic, self._insight_topic],
            "subscription": self._subscription,
            "messages_processed": self.metrics.messages_processed,
            "duplicates_suppressed": self.metrics.duplicates_suppressed,
            "dead_lettered": self.metrics.dead_lettered,
            "circuit_state": self._circuit_breaker.state.value,
        }

    async def _handle_message(self, message: ReceivedMessage) -> None:
        if message.topic == self._signal_topic:
            await self._handle_signal(message)
            return
        if message.topic == self._insight_topic:
            await self._handle_insight(message)
            return
        await self._dead_letter(message, f"unsupported topic: {message.topic}")

    async def _handle_signal(self, message: ReceivedMessage) -> None:
        try:
            signal = Signal.model_validate(message.body)
        except ValidationError as exc:
            await self._dead_letter(message, f"invalid signal payload: {exc.errors()}")
            return
        await self._process_event(
            message=message,
            topic=self._signal_topic,
            event=signal,
            alerts=self._rule_engine.evaluate_signal(signal),
        )

    async def _handle_insight(self, message: ReceivedMessage) -> None:
        try:
            insight = Insight.model_validate(message.body)
        except ValidationError as exc:
            await self._dead_letter(message, f"invalid insight payload: {exc.errors()}")
            return
        await self._process_event(
            message=message,
            topic=self._insight_topic,
            event=insight,
            alerts=self._rule_engine.evaluate_insight(insight),
        )

    async def _process_event(
        self,
        *,
        message: ReceivedMessage,
        topic: str,
        event: Signal | Insight,
        alerts: list[Alert],
    ) -> None:
        processed_key = alert_processed_key(topic, event.event_id)
        if await self._cache.get(processed_key):
            self.metrics.duplicates_suppressed += 1
            await self._bus.complete(message)
            self._log.info(
                "alerting.duplicate_suppressed",
                topic=topic,
                message_id=message.message_id,
                processed_key=processed_key,
            )
            return

        lock_key = alert_lock_key(topic, event.event_id)
        if not await self._cache.set_if_absent(lock_key, True, ttl=300):
            self.metrics.duplicates_suppressed += 1
            await self._bus.complete(message)
            self._log.info(
                "alerting.duplicate_in_progress_suppressed",
                topic=topic,
                message_id=message.message_id,
                lock_key=lock_key,
            )
            return

        try:
            await self._circuit_breaker.call(
                self._process_with_retry,
                event=event,
                alerts=alerts,
                processed_key=processed_key,
            )
        except CircuitOpenError as exc:
            self.metrics.circuit_open_rejections += 1
            await self._cache.delete(lock_key)
            await self._dead_letter(
                message,
                f"alerting processing blocked by open circuit: {exc}",
            )
            return
        except Exception as exc:
            await self._cache.delete(lock_key)
            await self._dead_letter(
                message,
                f"alerting processing failed: {type(exc).__name__}: {exc}",
            )
            return

        await self._cache.delete(lock_key)
        self.metrics.messages_processed += 1
        self.metrics.alerts_published += len(alerts)
        await self._bus.complete(message)
        self._log.info(
            "alerting.processed",
            topic=topic,
            message_id=message.message_id,
            alerts_emitted=len(alerts),
        )

    async def _process_with_retry(
        self,
        *,
        event: Signal | Insight,
        alerts: list[Alert],
        processed_key: str,
    ) -> None:
        attempts = 0

        async def operation() -> None:
            nonlocal attempts
            attempts += 1
            if attempts > 1:
                self.metrics.processing_retries += 1
            for alert in alerts:
                published_key = alert_published_key(alert.dedupe_key)
                if await self._cache.get(published_key):
                    continue
                await self._bus.publish(
                    self._alert_topic,
                    alert.model_dump(mode="json"),
                    message_id=alert.dedupe_key,
                    correlation_id=alert.correlation_id,
                )
                await self._cache.set(published_key, True)
            await self._cache.set(processed_key, True)

        try:
            await retry_async(
                operation,
                max_attempts=self._max_processing_attempts,
                wait_min=self._retry_wait_min_seconds,
                wait_max=self._retry_wait_max_seconds,
            )
        except RetryError as exc:
            last_exc = exc.last_attempt.exception()
            if last_exc is None:
                raise
            raise last_exc

    async def _dead_letter(self, message: ReceivedMessage, reason: str) -> None:
        self.metrics.dead_lettered += 1
        self.metrics.last_error = reason
        await self._bus.dead_letter(message, reason=reason)
        self._log.warning(
            "alerting.dead_lettered",
            topic=message.topic,
            subscription=message.subscription,
            message_id=message.message_id,
            reason=reason,
        )
