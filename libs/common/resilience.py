"""
Retry and circuit-breaker helpers for the market intelligence platform.

All external calls (Service Bus, Redis, Druid, Elasticsearch) must be wrapped
in these helpers as per platform conventions.
"""

from __future__ import annotations

import asyncio
import enum
import functools
import time as _time
from collections.abc import Callable, Coroutine, Iterable
from inspect import isawaitable
from typing import Any, Protocol, TypeVar

from tenacity import (
    AsyncRetrying,
    RetryError,
    stop_after_attempt,
    wait_exponential,
)

__all__ = [
    "CircuitOpenError",
    "CircuitState",
    "CircuitBreaker",
    "retry_async",
    "with_retry",
    "run_poll_loop",
    "dead_letter_message",
    "close_backends",
]

T = TypeVar("T")


class CircuitOpenError(Exception):
    """Raised when a call is attempted while the circuit breaker is OPEN."""


class CircuitState(enum.Enum):
    CLOSED = "CLOSED"
    OPEN = "OPEN"
    HALF_OPEN = "HALF_OPEN"


class CircuitBreaker:
    """
    Async-callable circuit breaker.

    States:
      CLOSED   — normal operation; failures are counted.
      OPEN     — calls are rejected immediately with CircuitOpenError.
      HALF_OPEN — one probe call is allowed; success → CLOSED, failure → OPEN.

    Injectable ``time_fn`` lets tests advance time without sleeping.
    """

    def __init__(
        self,
        failure_threshold: int = 3,
        reset_timeout: float = 60.0,
        time_fn: Callable[[], float] = _time.monotonic,
    ) -> None:
        self.failure_threshold = failure_threshold
        self.reset_timeout = reset_timeout
        self._time_fn = time_fn

        self._state: CircuitState = CircuitState.CLOSED
        self._failure_count: int = 0
        self._opened_at: float | None = None
        self._half_open_lock: asyncio.Lock = asyncio.Lock()

    # ------------------------------------------------------------------
    # Public API
    # ------------------------------------------------------------------

    @property
    def state(self) -> CircuitState:
        self._maybe_transition_half_open()
        return self._state

    async def call(
        self,
        coro_fn: Callable[..., Coroutine[Any, Any, T]],
        *args: Any,
        **kwargs: Any,
    ) -> T:
        """Execute ``coro_fn(*args, **kwargs)`` with circuit-breaker protection."""
        self._maybe_transition_half_open()

        if self._state == CircuitState.OPEN:
            raise CircuitOpenError(
                f"Circuit is OPEN; failing fast. "
                f"(threshold={self.failure_threshold}, timeout={self.reset_timeout}s)"
            )

        if self._state == CircuitState.HALF_OPEN:
            # Only one probe call is allowed through at a time — an extra
            # caller that arrives while a probe is already in flight fails
            # fast instead of silently piling onto a backend that hasn't
            # been confirmed recovered yet.
            if self._half_open_lock.locked():
                raise CircuitOpenError(
                    "Circuit is HALF_OPEN and a probe is already in flight; failing fast."
                )
            async with self._half_open_lock:
                return await self._do_call(coro_fn, *args, **kwargs)

        return await self._do_call(coro_fn, *args, **kwargs)

    # ------------------------------------------------------------------
    # Internal helpers
    # ------------------------------------------------------------------

    async def _do_call(
        self,
        coro_fn: Callable[..., Coroutine[Any, Any, T]],
        *args: Any,
        **kwargs: Any,
    ) -> T:
        try:
            result = await coro_fn(*args, **kwargs)
        except Exception:
            self._record_failure()
            raise
        else:
            self._record_success()
            return result

    def _maybe_transition_half_open(self) -> None:
        if (
            self._state == CircuitState.OPEN
            and self._opened_at is not None
            and self._time_fn() - self._opened_at >= self.reset_timeout
        ):
            self._state = CircuitState.HALF_OPEN

    def _record_failure(self) -> None:
        if self._state == CircuitState.HALF_OPEN:
            # Probe failed — go back to OPEN and reset timer
            self._state = CircuitState.OPEN
            self._opened_at = self._time_fn()
        else:
            self._failure_count += 1
            if self._failure_count >= self.failure_threshold:
                self._state = CircuitState.OPEN
                self._opened_at = self._time_fn()

    def _record_success(self) -> None:
        self._state = CircuitState.CLOSED
        self._failure_count = 0
        self._opened_at = None


# ---------------------------------------------------------------------------
# Retry helpers (built on tenacity)
# ---------------------------------------------------------------------------


async def retry_async(
    coro_fn: Callable[..., Coroutine[Any, Any, T]],
    *args: Any,
    max_attempts: int = 3,
    wait_min: float = 1.0,
    wait_max: float = 10.0,
    **kwargs: Any,
) -> T:
    """
    Retry ``coro_fn(*args, **kwargs)`` up to ``max_attempts`` times using
    exponential back-off between ``wait_min`` and ``wait_max`` seconds.

    Raises the last underlying exception wrapped in ``tenacity.RetryError``
    when all attempts are exhausted.
    """
    async for attempt in AsyncRetrying(
        stop=stop_after_attempt(max_attempts),
        wait=wait_exponential(multiplier=1, min=wait_min, max=wait_max),
        reraise=False,
    ):
        with attempt:
            return await coro_fn(*args, **kwargs)
    # Should be unreachable — tenacity will raise RetryError before here.
    raise RetryError(None)  # type: ignore[arg-type]


def with_retry(
    max_attempts: int = 3,
    wait_min: float = 1.0,
    wait_max: float = 10.0,
) -> Callable[[Callable[..., Coroutine[Any, Any, T]]], Callable[..., Coroutine[Any, Any, T]]]:
    """
    Decorator factory: wrap an async function with the retry policy.

    Example::

        @with_retry(max_attempts=5, wait_min=0.5, wait_max=30.0)
        async def fetch_data():
            ...
    """

    def decorator(
        fn: Callable[..., Coroutine[Any, Any, T]],
    ) -> Callable[..., Coroutine[Any, Any, T]]:
        @functools.wraps(fn)
        async def wrapper(*args: Any, **kwargs: Any) -> T:
            return await retry_async(
                fn,
                *args,
                max_attempts=max_attempts,
                wait_min=wait_min,
                wait_max=wait_max,
                **kwargs,
            )

        return wrapper

    return decorator


# ---------------------------------------------------------------------------
# Consumer poll loop
# ---------------------------------------------------------------------------


class _LoopMetrics(Protocol):
    last_error: str | None


async def run_poll_loop(
    poll_once: Callable[..., Coroutine[Any, Any, int]],
    *,
    service_name: str,
    log: Any,
    metrics: _LoopMetrics,
    poll_interval_seconds: float = 0.25,
    max_messages: int = 10,
) -> None:
    """
    Drive ``poll_once`` forever, surviving transient failures.

    Every service consumer runs this same loop, and a consumer that dies on the
    first Service Bus or Redis blip fails invisibly: the background task ends
    while ``/health`` keeps reporting ``ok`` and no messages flow. Errors are
    recorded on ``metrics.last_error``, logged, and retried after a pause.

    Returning to the caller only happens via cancellation.
    """
    while True:
        try:
            processed = await poll_once(max_messages=max_messages)
        except Exception as exc:
            metrics.last_error = f"{service_name} polling failed: {type(exc).__name__}: {exc}"
            log.warning(
                f"{service_name}.poll_failed",
                error_type=type(exc).__name__,
                error=str(exc),
            )
            await asyncio.sleep(poll_interval_seconds)
            continue
        if processed == 0:
            await asyncio.sleep(poll_interval_seconds)


# ---------------------------------------------------------------------------
# Consumer shutdown / dead-letter helpers
# ---------------------------------------------------------------------------


class _DeadLetterMetrics(Protocol):
    dead_lettered: int
    last_error: str | None


async def dead_letter_message(
    message: Any,
    reason: str,
    *,
    bus: Any,
    log: Any,
    metrics: _DeadLetterMetrics,
    service_name: str,
) -> None:
    """
    Dead-letter *message*, recording the reason on ``metrics`` and in the log.

    Each consumer had its own copy of this three-step sequence (count, remember
    the reason, hand the message to the broker) and dropping any one step is
    silent: a message vanishes with no counter and no log line. The service name
    only selects the log event, so ``stream.dead_lettered`` and
    ``alerting.dead_lettered`` stay distinguishable in the log stream.
    """
    metrics.dead_lettered += 1
    metrics.last_error = reason
    await bus.dead_letter(message, reason=reason)
    log.warning(
        f"{service_name}.dead_lettered",
        topic=message.topic,
        subscription=message.subscription,
        message_id=message.message_id,
        reason=reason,
    )


async def close_backends(
    backends: Iterable[Any],
    *,
    log: Any,
    service_name: str,
) -> None:
    """
    Close every backend that exposes ``close()``, even if one of them fails.

    A plain loop strands every backend after the first one that raises, leaking
    AMQP links and Redis sockets on each crash-loop restart. Failures are logged
    per backend rather than raised, so one unreachable service cannot block the
    shutdown of the others.
    """
    for backend in backends:
        close = getattr(backend, "close", None)
        if close is None:
            continue
        try:
            result = close()
            if isawaitable(result):
                await result
        except Exception as exc:
            log.warning(
                f"{service_name}.backend_close_failed",
                backend=type(backend).__name__,
                error=str(exc),
            )
