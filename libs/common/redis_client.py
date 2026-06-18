"""
Cache port: Redis-backed key-value store with TTL, snapshots, and idempotency.

NOTE: The module is named ``redis_client.py`` (not ``redis.py``) to avoid
shadowing the installed ``redis`` package on the Python path.  Public symbols
are re-exported from ``libs.common`` as usual.

Public API
----------
Cache           — Protocol (interface)
InMemoryCache   — In-memory fake; TTL via injectable clock.
RedisCache      — Thin wrapper over ``redis.asyncio`` (real client).
get_cache()     — Factory.
"""

from __future__ import annotations

import time as _time
from typing import Any, Callable, Protocol, runtime_checkable

__all__ = [
    "Cache",
    "InMemoryCache",
    "RedisCache",
    "get_cache",
]


# ---------------------------------------------------------------------------
# Protocol
# ---------------------------------------------------------------------------


@runtime_checkable
class Cache(Protocol):
    async def get(self, key: str) -> Any | None:
        ...

    async def set(self, key: str, value: Any, ttl: int | None = None) -> None:
        ...

    async def set_if_absent(
        self,
        key: str,
        value: Any,
        ttl: int | None = None,
    ) -> bool:
        ...

    async def delete(self, key: str) -> None:
        ...

    async def set_snapshot(self, symbol: str, data: dict[str, Any]) -> None:
        ...

    async def get_snapshot(self, symbol: str) -> dict[str, Any] | None:
        ...

    async def seen(self, key: str) -> bool:
        """Idempotency check: returns False the first time, True every subsequent time."""
        ...


# ---------------------------------------------------------------------------
# InMemoryCache
# ---------------------------------------------------------------------------


class InMemoryCache:
    """
    In-memory cache with TTL support for unit tests.

    Pass a custom ``time_fn`` (returns a float) to control time in tests
    without sleeping.
    """

    def __init__(self, time_fn: Callable[[], float] = _time.monotonic) -> None:
        self._store: dict[str, Any] = {}
        self._expiry: dict[str, float] = {}
        self._seen_keys: set[str] = set()
        self._time_fn = time_fn

    def _is_expired(self, key: str) -> bool:
        exp = self._expiry.get(key)
        return exp is not None and self._time_fn() > exp

    async def get(self, key: str) -> Any | None:
        if self._is_expired(key):
            self._store.pop(key, None)
            self._expiry.pop(key, None)
            return None
        return self._store.get(key)

    async def set(self, key: str, value: Any, ttl: int | None = None) -> None:
        self._store[key] = value
        if ttl is not None:
            self._expiry[key] = self._time_fn() + ttl
        else:
            self._expiry.pop(key, None)

    async def set_if_absent(
        self,
        key: str,
        value: Any,
        ttl: int | None = None,
    ) -> bool:
        if await self.get(key) is not None:
            return False
        await self.set(key, value, ttl=ttl)
        return True

    async def delete(self, key: str) -> None:
        self._store.pop(key, None)
        self._expiry.pop(key, None)

    async def set_snapshot(self, symbol: str, data: dict[str, Any]) -> None:
        await self.set(f"snapshot:{symbol}", data)

    async def get_snapshot(self, symbol: str) -> dict[str, Any] | None:
        return await self.get(f"snapshot:{symbol}")

    async def seen(self, key: str) -> bool:
        if key in self._seen_keys:
            return True
        self._seen_keys.add(key)
        return False


# ---------------------------------------------------------------------------
# RedisCache (real client — import-guarded)
# ---------------------------------------------------------------------------


class RedisCache:
    """
    Cache backed by Redis using ``redis.asyncio``.

    Integration tests only — skip without a live Redis instance.
    """

    def __init__(self, url: str) -> None:
        import redis.asyncio as aioredis  # absolute import — not this module

        self._client = aioredis.from_url(url, decode_responses=False)

    async def get(self, key: str) -> Any | None:
        import pickle

        raw = await self._client.get(key)
        if raw is None:
            return None
        return pickle.loads(raw)

    async def set(self, key: str, value: Any, ttl: int | None = None) -> None:
        import pickle

        serialised = pickle.dumps(value)
        if ttl is not None:
            await self._client.setex(key, ttl, serialised)
        else:
            await self._client.set(key, serialised)

    async def set_if_absent(
        self,
        key: str,
        value: Any,
        ttl: int | None = None,
    ) -> bool:
        import pickle

        serialised = pickle.dumps(value)
        return bool(await self._client.set(key, serialised, ex=ttl, nx=True))

    async def delete(self, key: str) -> None:
        await self._client.delete(key)

    async def set_snapshot(self, symbol: str, data: dict[str, Any]) -> None:
        await self.set(f"snapshot:{symbol}", data)

    async def get_snapshot(self, symbol: str) -> dict[str, Any] | None:
        return await self.get(f"snapshot:{symbol}")

    async def seen(self, key: str) -> bool:
        """
        SETNX-like idempotency check.
        Returns False the first time (key was absent), True thereafter.
        Uses a long TTL (24 h) as the idempotency window.
        """
        result = await self._client.setnx(f"seen:{key}", b"1")
        if result:
            await self._client.expire(f"seen:{key}", 86400)
            return False
        return True

    async def close(self) -> None:
        await self._client.aclose()


# ---------------------------------------------------------------------------
# Factory
# ---------------------------------------------------------------------------


def get_cache(settings: Any = None) -> Cache:
    """
    Return InMemoryCache when REDIS_URL is unset or uses the default placeholder,
    else return RedisCache.
    """
    if settings is None:
        from libs.common.config import get_settings

        settings = get_settings()

    redis_url: str = settings.redis_url or ""
    if not redis_url or redis_url == "redis://localhost:6379/0":
        # Default placeholder — use fake so tests run offline
        return InMemoryCache()
    return RedisCache(redis_url)
