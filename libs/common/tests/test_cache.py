"""Tests for libs.common.redis_client — InMemoryCache and factory."""

import pytest

from libs.common.redis_client import InMemoryCache, get_cache

# ---------------------------------------------------------------------------
# Basic set / get / delete
# ---------------------------------------------------------------------------


@pytest.mark.asyncio
async def test_set_and_get():
    cache = InMemoryCache()
    await cache.set("key1", {"value": 42})
    result = await cache.get("key1")
    assert result == {"value": 42}


@pytest.mark.asyncio
async def test_get_missing_returns_none():
    cache = InMemoryCache()
    assert await cache.get("nonexistent") is None


@pytest.mark.asyncio
async def test_delete():
    cache = InMemoryCache()
    await cache.set("k", "v")
    await cache.delete("k")
    assert await cache.get("k") is None


@pytest.mark.asyncio
async def test_set_if_absent_writes_only_first_value():
    cache = InMemoryCache()

    first = await cache.set_if_absent("lock", "first")
    second = await cache.set_if_absent("lock", "second")

    assert first is True
    assert second is False
    assert await cache.get("lock") == "first"


@pytest.mark.asyncio
async def test_set_if_absent_respects_ttl():
    now = [0.0]
    cache = InMemoryCache(time_fn=lambda: now[0])

    assert await cache.set_if_absent("lock", "first", ttl=10) is True
    now[0] = 10.1
    assert await cache.set_if_absent("lock", "second") is True
    assert await cache.get("lock") == "second"


# ---------------------------------------------------------------------------
# TTL expiry (via injected clock — no sleeping)
# ---------------------------------------------------------------------------


@pytest.mark.asyncio
async def test_ttl_not_expired():
    now = [0.0]
    cache = InMemoryCache(time_fn=lambda: now[0])
    await cache.set("k", "alive", ttl=10)
    now[0] = 9.9
    assert await cache.get("k") == "alive"


@pytest.mark.asyncio
async def test_ttl_expired():
    now = [0.0]
    cache = InMemoryCache(time_fn=lambda: now[0])
    await cache.set("k", "mortal", ttl=10)
    now[0] = 10.1
    assert await cache.get("k") is None


@pytest.mark.asyncio
async def test_no_ttl_does_not_expire():
    now = [0.0]
    cache = InMemoryCache(time_fn=lambda: now[0])
    await cache.set("k", "immortal")
    now[0] = 1_000_000.0
    assert await cache.get("k") == "immortal"


# ---------------------------------------------------------------------------
# Snapshots
# ---------------------------------------------------------------------------


@pytest.mark.asyncio
async def test_set_and_get_snapshot():
    cache = InMemoryCache()
    data = {"sma": 100.0, "rsi": 55.0}
    await cache.set_snapshot("BTCUSDT", data)
    result = await cache.get_snapshot("BTCUSDT")
    assert result == data


@pytest.mark.asyncio
async def test_snapshot_missing_returns_none():
    cache = InMemoryCache()
    assert await cache.get_snapshot("MISSING") is None


# ---------------------------------------------------------------------------
# seen() — idempotency
# ---------------------------------------------------------------------------


@pytest.mark.asyncio
async def test_seen_first_call_returns_false():
    cache = InMemoryCache()
    assert await cache.seen("evt-001") is False


@pytest.mark.asyncio
async def test_seen_second_call_returns_true():
    cache = InMemoryCache()
    await cache.seen("evt-001")
    assert await cache.seen("evt-001") is True


@pytest.mark.asyncio
async def test_seen_different_keys_independent():
    cache = InMemoryCache()
    await cache.seen("a")
    assert await cache.seen("b") is False
    assert await cache.seen("a") is True


# ---------------------------------------------------------------------------
# Factory
# ---------------------------------------------------------------------------


def test_get_cache_returns_in_memory_by_default():
    cache = get_cache()
    assert isinstance(cache, InMemoryCache)
