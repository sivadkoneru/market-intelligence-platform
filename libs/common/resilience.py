"""
Retry and circuit-breaker helpers for the market intelligence platform.

All external calls (Service Bus, Redis, Druid, Elasticsearch) must be wrapped
in these helpers as per platform conventions.
"""

from __future__ import annotations

import enum
import time as _time
from typing import Any, Callable, Coroutine, TypeVar

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

        try:
            result = await coro_fn(*args, **kwargs)
        except Exception:
            self._record_failure()
            raise
        else:
            self._record_success()
            return result

    # ------------------------------------------------------------------
    # Internal helpers
    # ------------------------------------------------------------------

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
        async def wrapper(*args: Any, **kwargs: Any) -> T:
            return await retry_async(
                fn, *args,
                max_attempts=max_attempts,
                wait_min=wait_min,
                wait_max=wait_max,
                **kwargs,
            )

        wrapper.__name__ = fn.__name__
        wrapper.__doc__ = fn.__doc__
        return wrapper  # type: ignore[return-value]

    return decorator
