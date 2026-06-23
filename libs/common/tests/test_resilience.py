"""Tests for libs.common.resilience — retry, circuit breaker, and consumer helpers."""

from dataclasses import dataclass
from types import SimpleNamespace

import pytest
from tenacity import RetryError

from libs.common.resilience import (
    CircuitBreaker,
    CircuitOpenError,
    CircuitState,
    close_backends,
    dead_letter_message,
    retry_async,
    with_retry,
)

# ---------------------------------------------------------------------------
# Retry helpers
# ---------------------------------------------------------------------------


@pytest.mark.asyncio
async def test_retry_succeeds_on_first_attempt():
    calls = []

    async def ok():
        calls.append(1)
        return "ok"

    result = await retry_async(ok, max_attempts=3, wait_min=0, wait_max=0)
    assert result == "ok"
    assert len(calls) == 1


@pytest.mark.asyncio
async def test_retry_retries_then_succeeds():
    """Fails twice, succeeds on third attempt."""
    attempts = []

    async def flaky():
        attempts.append(1)
        if len(attempts) < 3:
            raise ValueError("transient")
        return "done"

    result = await retry_async(flaky, max_attempts=3, wait_min=0, wait_max=0)
    assert result == "done"
    assert len(attempts) == 3


@pytest.mark.asyncio
async def test_retry_exhausts_and_raises():
    calls = []

    async def always_fails():
        calls.append(1)
        raise RuntimeError("permanent")

    with pytest.raises(RetryError):
        await retry_async(always_fails, max_attempts=3, wait_min=0, wait_max=0)

    assert len(calls) == 3


@pytest.mark.asyncio
async def test_with_retry_decorator():
    attempts = []

    @with_retry(max_attempts=2, wait_min=0, wait_max=0)
    async def decorated():
        attempts.append(1)
        if len(attempts) < 2:
            raise ValueError("retry me")
        return "success"

    result = await decorated()
    assert result == "success"
    assert len(attempts) == 2


# ---------------------------------------------------------------------------
# CircuitBreaker
# ---------------------------------------------------------------------------


@pytest.mark.asyncio
async def test_circuit_starts_closed():
    cb = CircuitBreaker(failure_threshold=3, reset_timeout=60)
    assert cb.state == CircuitState.CLOSED


@pytest.mark.asyncio
async def test_circuit_opens_after_threshold():
    cb = CircuitBreaker(failure_threshold=3, reset_timeout=60)

    async def boom():
        raise RuntimeError("fail")

    for _ in range(3):
        with pytest.raises(RuntimeError):
            await cb.call(boom)

    assert cb.state == CircuitState.OPEN


@pytest.mark.asyncio
async def test_circuit_raises_circuit_open_error_when_open():
    cb = CircuitBreaker(failure_threshold=1, reset_timeout=60)

    async def boom():
        raise RuntimeError("fail")

    with pytest.raises(RuntimeError):
        await cb.call(boom)

    assert cb.state == CircuitState.OPEN

    async def good():
        return "hello"

    with pytest.raises(CircuitOpenError):
        await cb.call(good)


@pytest.mark.asyncio
async def test_circuit_transitions_to_half_open_after_timeout():
    now = [0.0]

    def clock():
        return now[0]

    cb = CircuitBreaker(failure_threshold=1, reset_timeout=30.0, time_fn=clock)

    async def boom():
        raise RuntimeError("fail")

    with pytest.raises(RuntimeError):
        await cb.call(boom)

    assert cb.state == CircuitState.OPEN

    # Advance clock past reset_timeout
    now[0] = 31.0
    assert cb.state == CircuitState.HALF_OPEN


@pytest.mark.asyncio
async def test_circuit_closes_on_success_in_half_open():
    now = [0.0]

    def clock():
        return now[0]

    cb = CircuitBreaker(failure_threshold=1, reset_timeout=30.0, time_fn=clock)

    async def boom():
        raise RuntimeError("fail")

    async def ok():
        return "ok"

    with pytest.raises(RuntimeError):
        await cb.call(boom)

    now[0] = 31.0  # → HALF_OPEN
    assert cb.state == CircuitState.HALF_OPEN

    result = await cb.call(ok)
    assert result == "ok"
    assert cb.state == CircuitState.CLOSED


@pytest.mark.asyncio
async def test_circuit_returns_to_open_on_failure_in_half_open():
    now = [0.0]

    def clock():
        return now[0]

    cb = CircuitBreaker(failure_threshold=1, reset_timeout=30.0, time_fn=clock)

    async def boom():
        raise RuntimeError("fail")

    with pytest.raises(RuntimeError):
        await cb.call(boom)

    now[0] = 31.0  # → HALF_OPEN
    assert cb.state == CircuitState.HALF_OPEN

    with pytest.raises(RuntimeError):
        await cb.call(boom)

    assert cb.state == CircuitState.OPEN


@pytest.mark.asyncio
async def test_circuit_success_resets_failure_count():
    cb = CircuitBreaker(failure_threshold=3, reset_timeout=60)

    async def boom():
        raise RuntimeError("fail")

    async def ok():
        return "ok"

    # 2 failures — not yet open
    for _ in range(2):
        with pytest.raises(RuntimeError):
            await cb.call(boom)

    assert cb.state == CircuitState.CLOSED

    # Success resets count
    await cb.call(ok)
    assert cb.state == CircuitState.CLOSED
    assert cb._failure_count == 0


# ---------------------------------------------------------------------------
# dead_letter_message
# ---------------------------------------------------------------------------


class _RecordingBus:
    def __init__(self) -> None:
        self.dead_lettered: list[tuple[object, str]] = []

    async def dead_letter(self, message, reason=""):
        self.dead_lettered.append((message, reason))


class _RecordingLog:
    def __init__(self) -> None:
        self.warnings: list[tuple[str, dict]] = []

    def warning(self, event, **kwargs):
        self.warnings.append((event, kwargs))


@dataclass
class _DLMetrics:
    dead_lettered: int = 0
    last_error: str | None = None


def _message(topic="signals", subscription="alerting", message_id="m-1"):
    return SimpleNamespace(topic=topic, subscription=subscription, message_id=message_id)


@pytest.mark.asyncio
async def test_dead_letter_message_hands_the_message_to_the_bus():
    bus, log, metrics = _RecordingBus(), _RecordingLog(), _DLMetrics()
    msg = _message()

    await dead_letter_message(
        msg, "bad payload", bus=bus, log=log, metrics=metrics, service_name="alerting"
    )

    assert bus.dead_lettered == [(msg, "bad payload")]


@pytest.mark.asyncio
async def test_dead_letter_message_records_the_reason_on_metrics():
    bus, log, metrics = _RecordingBus(), _RecordingLog(), _DLMetrics()

    await dead_letter_message(
        _message(), "bad payload", bus=bus, log=log, metrics=metrics, service_name="alerting"
    )

    assert metrics.dead_lettered == 1
    assert metrics.last_error == "bad payload"


@pytest.mark.asyncio
async def test_dead_letter_message_logs_a_service_scoped_event():
    """The event name keeps each service distinguishable in the shared log stream."""
    bus, log, metrics = _RecordingBus(), _RecordingLog(), _DLMetrics()

    await dead_letter_message(
        _message(message_id="m-9"),
        "boom",
        bus=bus,
        log=log,
        metrics=metrics,
        service_name="stream",
    )

    event, fields = log.warnings[0]
    assert event == "stream.dead_lettered"
    assert fields["topic"] == "signals"
    assert fields["subscription"] == "alerting"
    assert fields["message_id"] == "m-9"
    assert fields["reason"] == "boom"


@pytest.mark.asyncio
async def test_dead_letter_message_accumulates_across_calls():
    bus, log, metrics = _RecordingBus(), _RecordingLog(), _DLMetrics()

    await dead_letter_message(
        _message(), "first", bus=bus, log=log, metrics=metrics, service_name="ai"
    )
    await dead_letter_message(
        _message(), "second", bus=bus, log=log, metrics=metrics, service_name="ai"
    )

    assert metrics.dead_lettered == 2
    assert metrics.last_error == "second"


# ---------------------------------------------------------------------------
# close_backends
# ---------------------------------------------------------------------------


class _Backend:
    def __init__(self, *, fails: bool = False, sync: bool = False) -> None:
        self.closed = 0
        self._fails = fails
        self._sync = sync

    def close(self):
        if self._sync:
            self.closed += 1
            if self._fails:
                raise RuntimeError("sync close failed")
            return None
        return self._aclose()

    async def _aclose(self):
        self.closed += 1
        if self._fails:
            raise RuntimeError("async close failed")


class _NoCloseBackend:
    pass


@pytest.mark.asyncio
async def test_close_backends_closes_every_backend():
    backends = [_Backend(), _Backend(), _Backend(sync=True)]

    await close_backends(backends, log=_RecordingLog(), service_name="api")

    assert [b.closed for b in backends] == [1, 1, 1]


@pytest.mark.asyncio
async def test_close_backends_skips_backends_without_close():
    log = _RecordingLog()

    await close_backends([_NoCloseBackend()], log=log, service_name="api")

    assert log.warnings == []


@pytest.mark.asyncio
async def test_close_backends_continues_past_a_failing_backend():
    """One unreachable backend must not strand the sockets held by the others."""
    failing, healthy = _Backend(fails=True), _Backend()

    await close_backends([failing, healthy], log=_RecordingLog(), service_name="api")

    assert healthy.closed == 1


@pytest.mark.asyncio
async def test_close_backends_logs_each_failure():
    log = _RecordingLog()

    await close_backends([_Backend(fails=True)], log=log, service_name="ai")

    event, fields = log.warnings[0]
    assert event == "ai.backend_close_failed"
    assert fields["backend"] == "_Backend"
    assert "async close failed" in fields["error"]
