"""Tests for libs.common.resilience — retry helper and circuit breaker."""

import pytest
from tenacity import RetryError

from libs.common.resilience import (
    CircuitBreaker,
    CircuitOpenError,
    CircuitState,
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
