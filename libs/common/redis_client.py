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

import json
import time as _time
from collections.abc import Callable
from typing import Any, Protocol, runtime_checkable

__all__ = [
    "Cache",
    "InMemoryCache",
    "RedisCache",
    "get_cache",
    "encode_cache_value",
    "decode_cache_value",
    "snapshot_key",
    "seen_key",
    "history_key",
    "IDEMPOTENCY_TTL_SECONDS",
    "SNAPSHOT_PREFIX",
    "HISTORY_PREFIX",
]

# How long a "already handled this event" marker is retained. It has to outlast
# the broker's redelivery and duplicate-detection windows, but not forever:
# without a TTL every unique event leaves a permanent key, so the store grows
# without bound for as long as the platform runs.
IDEMPOTENCY_TTL_SECONDS = 24 * 60 * 60

# Key namespaces. These are a cross-process contract — the stream service writes
# snapshots and the API reads them — so they are named once here rather than
# spelled out at each call site, where the fake and the real client could drift
# apart without any test noticing.
SNAPSHOT_PREFIX = "snapshot"
SEEN_PREFIX = "seen"
HISTORY_PREFIX = "history"


def snapshot_key(symbol: str) -> str:
    """Return the cache key holding the latest snapshot for *symbol*."""
    return f"{SNAPSHOT_PREFIX}:{symbol}"


def seen_key(key: str) -> str:
    """Return the cache key holding the idempotency marker for *key*."""
    return f"{SEEN_PREFIX}:{key}"


def history_key(symbol: str) -> str:
    """Return the cache key holding the recent-history mirror for *symbol*."""
    return f"{HISTORY_PREFIX}:{symbol}"


# ---------------------------------------------------------------------------
# Serialisation codec
# ---------------------------------------------------------------------------
#
# JSON, deliberately — never pickle. Redis contents are data from outside this
# process, and ``pickle.loads`` on attacker-influenced bytes is arbitrary code
# execution. Every value the platform caches (snapshots, indicator history,
# ``model_dump(mode="json")`` payloads, boolean idempotency markers) is already
# JSON-native, so JSON costs nothing and keeps the cache readable by other
# tools.


def encode_cache_value(value: Any) -> bytes:
    """Serialise *value* for storage in Redis."""
    return json.dumps(value).encode("utf-8")


def decode_cache_value(key: str, raw: bytes) -> Any:
    """
    Deserialise a Redis payload written by :func:`encode_cache_value`.

    A payload that is not valid JSON — corruption, or a value left behind by an
    older pickle-based build — is reported as a cache *miss*: one warning naming
    the key, then ``None``. Raising instead made a single legacy value fatal
    forever, because snapshot keys carry no TTL: ``/market/{symbol}/latest``
    answered 500 on every request and the stream/AI/alerting poll loops stalled.
    A miss is recoverable — the caller falls back to the store and the next write
    replaces the value. Decoding stays JSON-only; pickle is never read back.

    The value itself is never logged: it is untrusted input, and may be exactly
    the pickle payload that must not be deserialised.
    """
    try:
        return json.loads(raw)
    except (UnicodeDecodeError, json.JSONDecodeError):
        # Imported here rather than at module scope: ``libs.common.logging``
        # pulls in the search-store port, and this module is imported from
        # ``libs.common.__init__``.
        from libs.common.logging import get_logger

        get_logger(__name__).warning(
            "cache.undecodable_value_ignored",
            key=key,
            bytes=len(raw),
        )
        return None


# ---------------------------------------------------------------------------
# Protocol
# ---------------------------------------------------------------------------


@runtime_checkable
class Cache(Protocol):
    async def get(self, key: str) -> Any | None: ...

    async def set(self, key: str, value: Any, ttl: int | None = None) -> None: ...

    async def set_if_absent(
        self,
        key: str,
        value: Any,
        ttl: int | None = None,
    ) -> bool: ...

    async def delete(self, key: str) -> None: ...

    async def set_snapshot(self, symbol: str, data: dict[str, Any]) -> None: ...

    async def get_snapshot(self, symbol: str) -> dict[str, Any] | None: ...

    async def list_snapshot_symbols(self) -> list[str]: ...

    async def seen(self, key: str) -> bool:
        """Idempotency check: returns False the first time, True every subsequent time."""
        ...

    async def append_history(
        self, symbol: str, row: dict[str, Any], *, max_rows: int = 500
    ) -> None:
        """Append *row* to the recent-history mirror for *symbol*, capped at *max_rows*."""
        ...

    async def get_history(self, symbol: str) -> list[dict[str, Any]]:
        """Return the cached recent-history rows for *symbol* (empty if none cached)."""
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
        # Keyed on presence, not on truthiness. ``get()`` returns None both for
        # a missing key and for one holding a stored None, so testing its result
        # made a key holding None look absent — while ``RedisCache`` (SET NX)
        # treats any existing key as present. A lock is exactly where the fake
        # and the real client must not disagree.
        if self._is_expired(key):
            self._store.pop(key, None)
            self._expiry.pop(key, None)
        if key in self._store:
            return False
        await self.set(key, value, ttl=ttl)
        return True

    async def delete(self, key: str) -> None:
        self._store.pop(key, None)
        self._expiry.pop(key, None)

    async def set_snapshot(self, symbol: str, data: dict[str, Any]) -> None:
        await self.set(snapshot_key(symbol), data)

    async def get_snapshot(self, symbol: str) -> dict[str, Any] | None:
        return await self.get(snapshot_key(symbol))

    async def list_snapshot_symbols(self) -> list[str]:
        prefix = f"{SNAPSHOT_PREFIX}:"
        symbols: list[str] = []
        for key in list(self._store):
            if not key.startswith(prefix):
                continue
            if await self.get(key) is None:
                continue
            symbol = key.removeprefix(prefix)
            if symbol:
                symbols.append(symbol)
        return sorted(symbols)

    async def seen(self, key: str) -> bool:
        if key in self._seen_keys:
            return True
        self._seen_keys.add(key)
        return False

    async def append_history(
        self, symbol: str, row: dict[str, Any], *, max_rows: int = 500
    ) -> None:
        key = history_key(symbol)
        history = await self.get(key)
        rows = list(history) if isinstance(history, list) else []
        rows.append(dict(row))
        await self.set(key, rows[-max_rows:])

    async def get_history(self, symbol: str) -> list[dict[str, Any]]:
        history = await self.get(history_key(symbol))
        return list(history) if isinstance(history, list) else []


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
        raw = await self._client.get(key)
        if raw is None:
            return None
        return decode_cache_value(key, raw)

    async def set(self, key: str, value: Any, ttl: int | None = None) -> None:
        serialised = encode_cache_value(value)
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
        serialised = encode_cache_value(value)
        return bool(await self._client.set(key, serialised, ex=ttl, nx=True))

    async def delete(self, key: str) -> None:
        await self._client.delete(key)

    async def set_snapshot(self, symbol: str, data: dict[str, Any]) -> None:
        await self.set(snapshot_key(symbol), data)

    async def get_snapshot(self, symbol: str) -> dict[str, Any] | None:
        return await self.get(snapshot_key(symbol))

    async def list_snapshot_symbols(self) -> list[str]:
        prefix = f"{SNAPSHOT_PREFIX}:"
        symbols: list[str] = []
        async for key in self._client.scan_iter(match=f"{prefix}*"):
            key_text = key.decode("utf-8") if isinstance(key, bytes) else str(key)
            symbol = key_text.removeprefix(prefix)
            if symbol:
                symbols.append(symbol)
        return sorted(symbols)

    async def seen(self, key: str) -> bool:
        """
        Idempotency check: returns False the first time, True thereafter.

        Set-and-expire is issued as one ``SET ... NX EX`` rather than SETNX
        followed by EXPIRE. The two-command form is not atomic: a process that
        dies between them leaves a marker with no TTL, which never expires and
        so defeats the bound ``IDEMPOTENCY_TTL_SECONDS`` exists to enforce.
        """
        created = await self._client.set(
            seen_key(key),
            b"1",
            ex=IDEMPOTENCY_TTL_SECONDS,
            nx=True,
        )
        return not created

    async def append_history(
        self, symbol: str, row: dict[str, Any], *, max_rows: int = 500
    ) -> None:
        key = history_key(symbol)
        history = await self.get(key)
        rows = list(history) if isinstance(history, list) else []
        rows.append(dict(row))
        await self.set(key, rows[-max_rows:])

    async def get_history(self, symbol: str) -> list[dict[str, Any]]:
        history = await self.get(history_key(symbol))
        return list(history) if isinstance(history, list) else []

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
    from libs.common.config import is_default, resolve_settings

    redis_url: str = resolve_settings(settings).redis_url or ""
    if not redis_url or is_default("redis_url", redis_url):
        # Default placeholder — use fake so tests run offline
        return InMemoryCache()
    return RedisCache(redis_url)
