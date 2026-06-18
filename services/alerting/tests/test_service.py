from datetime import datetime, timezone
from typing import Any

import pytest
from fastapi.testclient import TestClient

from libs.common import (
    TOPIC_ALERTS,
    TOPIC_INSIGHTS,
    TOPIC_SIGNALS,
    CircuitBreaker,
    CircuitState,
    InMemoryBus,
    InMemoryCache,
)
from services.alerting.app import app, build_default_service, create_app
from services.alerting.service import (
    ALERTING_SUBSCRIPTION,
    AlertingService,
    alert_lock_key,
    alert_processed_key,
)


class FailingAlertBus(InMemoryBus):
    def __init__(self, *, failures_before_success: int, fail_forever: bool = False) -> None:
        super().__init__()
        self.failures_before_success = failures_before_success
        self.fail_forever = fail_forever
        self.alert_publish_attempts = 0

    async def publish(
        self,
        topic: str,
        body: dict[str, Any],
        *,
        message_id: str | None = None,
        correlation_id: str | None = None,
    ) -> None:
        if topic == TOPIC_ALERTS:
            self.alert_publish_attempts += 1
            if self.fail_forever or self.alert_publish_attempts <= self.failures_before_success:
                raise RuntimeError("alert topic unavailable")
        await super().publish(
            topic,
            body,
            message_id=message_id,
            correlation_id=correlation_id,
        )


class FailingOnSecondAlertBus(InMemoryBus):
    def __init__(self) -> None:
        super().__init__()
        self.alert_publish_attempts = 0

    async def publish(
        self,
        topic: str,
        body: dict[str, Any],
        *,
        message_id: str | None = None,
        correlation_id: str | None = None,
    ) -> None:
        if topic == TOPIC_ALERTS:
            self.alert_publish_attempts += 1
            if self.alert_publish_attempts == 2:
                raise RuntimeError("second alert publish failed once")
        await super().publish(
            topic,
            body,
            message_id=message_id,
            correlation_id=correlation_id,
        )


async def _build_service(
    bus: InMemoryBus | None = None,
    *,
    max_processing_attempts: int = 3,
    circuit_breaker: CircuitBreaker | None = None,
) -> tuple[AlertingService, InMemoryBus, InMemoryCache]:
    resolved_bus = bus or InMemoryBus()
    cache = InMemoryCache()
    await resolved_bus.receive(TOPIC_SIGNALS, ALERTING_SUBSCRIPTION, max_messages=0)
    await resolved_bus.receive(TOPIC_INSIGHTS, ALERTING_SUBSCRIPTION, max_messages=0)
    await resolved_bus.receive(TOPIC_ALERTS, "observer", max_messages=0)
    service = AlertingService(
        bus=resolved_bus,
        cache=cache,
        max_processing_attempts=max_processing_attempts,
        retry_wait_min_seconds=0.0,
        retry_wait_max_seconds=0.0,
        circuit_breaker=circuit_breaker,
    )
    return service, resolved_bus, cache


def _signal_body(
    *,
    event_id: str,
    symbol: str = "BTCUSDT",
    anomaly: bool = False,
    rsi: float = 55.0,
    volatility: float = 0.02,
) -> dict[str, Any]:
    return {
        "event_id": event_id,
        "ts": datetime(2026, 1, 1, tzinfo=timezone.utc).isoformat(),
        "symbol": symbol,
        "source": "stream",
        "indicators": {
            "rsi": rsi,
            "volatility": volatility,
            "trend": 1.0,
        },
        "anomaly": anomaly,
        "correlation_id": f"corr-{event_id}",
        "trace_id": f"trace-{event_id}",
    }


def _insight_body(
    *,
    event_id: str,
    symbol: str = "BTCUSDT",
    sentiment_score: float = 0.82,
) -> dict[str, Any]:
    return {
        "event_id": event_id,
        "ts": datetime(2026, 1, 1, tzinfo=timezone.utc).isoformat(),
        "symbol": symbol,
        "sentiment_score": sentiment_score,
        "sentiment_label": "positive" if sentiment_score > 0 else "negative",
        "summary": "Narrative momentum shifted.",
        "explanation": "A strong catalyst moved sentiment quickly.",
        "citations": ["signal:abc"],
        "confidence": 0.86,
        "grounded": True,
        "model": "mock-llm",
        "correlation_id": f"corr-{event_id}",
        "trace_id": f"trace-{event_id}",
    }


@pytest.mark.asyncio
async def test_service_publishes_alerts_for_signals_and_insights() -> None:
    service, bus, cache = await _build_service()

    await bus.publish(
        TOPIC_SIGNALS,
        _signal_body(event_id="sig-1", anomaly=True, rsi=77.0, volatility=0.08),
    )
    await bus.publish(TOPIC_INSIGHTS, _insight_body(event_id="ins-1", sentiment_score=-0.84))

    processed = await service.poll_once(max_messages=10)
    alerts = await bus.peek(TOPIC_ALERTS, "observer", n=10)

    assert processed == 2
    assert len(alerts) == 4
    assert service.metrics.messages_processed == 2
    assert service.metrics.alerts_published == 4
    assert await cache.get(alert_processed_key(TOPIC_SIGNALS, "sig-1")) is True
    assert await cache.get(alert_processed_key(TOPIC_INSIGHTS, "ins-1")) is True
    assert await cache.get(alert_lock_key(TOPIC_SIGNALS, "sig-1")) is None


@pytest.mark.asyncio
async def test_service_is_idempotent_for_duplicate_input_deliveries() -> None:
    service, bus, cache = await _build_service()
    body = _signal_body(event_id="sig-dup", anomaly=True, rsi=79.0, volatility=0.07)

    await bus.publish(TOPIC_SIGNALS, body, message_id="delivery-a")
    await bus.publish(TOPIC_SIGNALS, body, message_id="delivery-b")

    await service.poll_once(max_messages=10)
    alerts = await bus.peek(TOPIC_ALERTS, "observer", n=10)

    assert len(alerts) == 3
    assert service.metrics.messages_seen == 2
    assert service.metrics.messages_processed == 1
    assert service.metrics.duplicates_suppressed == 1
    assert await cache.get(alert_processed_key(TOPIC_SIGNALS, "sig-dup")) is True


@pytest.mark.asyncio
async def test_service_dead_letters_invalid_payloads() -> None:
    service, bus, cache = await _build_service()
    await bus.publish(
        TOPIC_INSIGHTS,
        {
            "event_id": "bad-insight",
            "symbol": "BTCUSDT",
            "summary": "Missing required fields",
        },
        message_id="bad-message",
    )

    await service.poll_once(max_messages=1)
    dlq_messages = await bus.receive_dead_letter(TOPIC_INSIGHTS, ALERTING_SUBSCRIPTION)

    assert len(dlq_messages) == 1
    assert dlq_messages[0].body["event_id"] == "bad-insight"
    assert service.metrics.dead_lettered == 1
    assert "invalid insight payload" in service.metrics.last_error
    assert await cache.get(alert_processed_key(TOPIC_INSIGHTS, "bad-insight")) is None


@pytest.mark.asyncio
async def test_service_retries_transient_alert_publish_failures() -> None:
    bus = FailingAlertBus(failures_before_success=2)
    service, bus, cache = await _build_service(bus, max_processing_attempts=3)
    await bus.publish(TOPIC_INSIGHTS, _insight_body(event_id="ins-retry"))

    await service.poll_once(max_messages=1)
    alerts = await bus.peek(TOPIC_ALERTS, "observer", n=10)

    assert len(alerts) == 1
    assert service.metrics.processing_retries == 2
    assert service.metrics.dead_lettered == 0
    assert bus.alert_publish_attempts == 3
    assert await cache.get(alert_processed_key(TOPIC_INSIGHTS, "ins-retry")) is True


@pytest.mark.asyncio
async def test_partial_publish_retry_skips_already_published_alerts() -> None:
    bus = FailingOnSecondAlertBus()
    service, bus, cache = await _build_service(bus, max_processing_attempts=3)
    await bus.publish(
        TOPIC_SIGNALS,
        _signal_body(event_id="sig-partial", anomaly=True, rsi=80.0, volatility=0.08),
    )

    await service.poll_once(max_messages=1)
    alerts = await bus.peek(TOPIC_ALERTS, "observer", n=10)

    assert bus.alert_publish_attempts == 4
    assert len(alerts) == 3
    assert [alert.body["rule"] for alert in alerts] == [
        "anomaly_flag",
        "rsi_overbought",
        "volatility_threshold_breach",
    ]
    assert service.metrics.processing_retries == 1
    assert service.metrics.dead_lettered == 0
    assert await cache.get(alert_processed_key(TOPIC_SIGNALS, "sig-partial")) is True
    assert await cache.get(alert_lock_key(TOPIC_SIGNALS, "sig-partial")) is None


@pytest.mark.asyncio
async def test_circuit_breaker_opens_after_repeated_failures() -> None:
    breaker = CircuitBreaker(failure_threshold=2, reset_timeout=60.0)
    bus = FailingAlertBus(failures_before_success=0, fail_forever=True)
    service, bus, cache = await _build_service(
        bus,
        max_processing_attempts=1,
        circuit_breaker=breaker,
    )

    await bus.publish(TOPIC_INSIGHTS, _insight_body(event_id="ins-fail-1"), message_id="m1")
    await bus.publish(TOPIC_INSIGHTS, _insight_body(event_id="ins-fail-2"), message_id="m2")
    await bus.publish(TOPIC_INSIGHTS, _insight_body(event_id="ins-fail-3"), message_id="m3")

    await service.poll_once(max_messages=10)
    dlq_messages = await bus.receive_dead_letter(TOPIC_INSIGHTS, ALERTING_SUBSCRIPTION)

    assert breaker.state == CircuitState.OPEN
    assert len(dlq_messages) == 3
    assert service.metrics.dead_lettered == 3
    assert service.metrics.circuit_open_rejections == 1
    assert bus.alert_publish_attempts == 2
    assert await cache.get(alert_processed_key(TOPIC_INSIGHTS, "ins-fail-1")) is None
    assert await cache.get(alert_processed_key(TOPIC_INSIGHTS, "ins-fail-2")) is None
    assert await cache.get(alert_processed_key(TOPIC_INSIGHTS, "ins-fail-3")) is None
    assert await cache.get(alert_lock_key(TOPIC_INSIGHTS, "ins-fail-1")) is None
    assert "open circuit" in service.metrics.last_error


def test_app_endpoints_and_module_app() -> None:
    bus = InMemoryBus()
    cache = InMemoryCache()
    service = AlertingService(bus=bus, cache=cache)
    local_app = create_app(service, run_on_startup=False)

    with TestClient(local_app) as client:
        root = client.get("/")
        health = client.get("/health")
        metrics = client.get("/metrics")

    assert root.status_code == 200
    assert "No financial advice" in root.json()["message"]
    assert health.status_code == 200
    assert health.json()["service"] == "alerting"
    assert metrics.status_code == 200
    assert "alerting_messages_seen" in metrics.text

    with TestClient(app) as client:
        assert client.get("/health").json()["service"] == "alerting"


def test_build_default_service_uses_offline_ports() -> None:
    service = build_default_service()

    assert isinstance(service._bus, InMemoryBus)
    assert isinstance(service._cache, InMemoryCache)
