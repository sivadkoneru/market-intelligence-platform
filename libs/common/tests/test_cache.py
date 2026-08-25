"""Tests for libs.common.redis_client — InMemoryCache, RedisCache codec, and factory."""

import json
import pickle

import pytest

from libs.common.redis_client import (
    IDEMPOTENCY_TTL_SECONDS,
    InMemoryCache,
    RedisCache,
    decode_cache_value,
    encode_cache_value,
    get_cache,
    history_key,
    seen_key,
    snapshot_key,
)

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


@pytest.mark.asyncio
async def test_list_snapshot_symbols_returns_sorted_snapshot_keys():
    cache = InMemoryCache()
    await cache.set_snapshot("ETHUSDT", {"price": 3000})
    await cache.set_snapshot("BTCUSDT", {"price": 100000})
    await cache.set("regular-key", {"symbol": "IGNORED"})

    assert await cache.list_snapshot_symbols() == ["BTCUSDT", "ETHUSDT"]


@pytest.mark.asyncio
async def test_list_snapshot_symbols_ignores_expired_snapshots():
    now = [0.0]
    cache = InMemoryCache(time_fn=lambda: now[0])
    await cache.set("snapshot:BTCUSDT", {"price": 100000}, ttl=10)
    await cache.set_snapshot("ETHUSDT", {"price": 3000})
    now[0] = 10.1

    assert await cache.list_snapshot_symbols() == ["ETHUSDT"]


# ---------------------------------------------------------------------------
# History mirror — append_history / get_history
# ---------------------------------------------------------------------------


@pytest.mark.asyncio
async def test_get_history_returns_empty_list_when_absent():
    cache = InMemoryCache()
    assert await cache.get_history("BTCUSDT") == []


@pytest.mark.asyncio
async def test_append_history_accumulates_rows_under_the_shared_key():
    cache = InMemoryCache()
    await cache.append_history("BTCUSDT", {"ts": "2024-01-01T00:00:00Z", "close": 1.0})
    await cache.append_history("BTCUSDT", {"ts": "2024-01-01T00:01:00Z", "close": 2.0})

    rows = await cache.get_history("BTCUSDT")
    assert [row["close"] for row in rows] == [1.0, 2.0]
    assert await cache.get(history_key("BTCUSDT")) == rows


@pytest.mark.asyncio
async def test_append_history_caps_at_max_rows():
    cache = InMemoryCache()
    for i in range(5):
        await cache.append_history("BTCUSDT", {"close": float(i)}, max_rows=3)

    rows = await cache.get_history("BTCUSDT")
    assert [row["close"] for row in rows] == [2.0, 3.0, 4.0]


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


# ---------------------------------------------------------------------------
# RedisCache serialisation codec
# ---------------------------------------------------------------------------


@pytest.mark.parametrize(
    "value",
    [
        True,
        None,
        3.5,
        "text",
        {"symbol": "BTCUSDT", "sma": 100.5, "anomaly": False, "trend": None},
        [{"ts": "2026-01-01T00:00:00+00:00", "price": 42000.0}],
    ],
)
def test_cache_codec_round_trips_platform_payloads(value):
    assert decode_cache_value("k", encode_cache_value(value)) == value


def test_encode_cache_value_emits_json_bytes():
    """Stored bytes stay readable by any JSON tool, not just this process."""
    encoded = encode_cache_value({"price": 1.5})

    assert isinstance(encoded, bytes)
    assert json.loads(encoded) == {"price": 1.5}


def test_decode_cache_value_ignores_pickle_payloads():
    """A pickle blob must never be executed (CWE-502) — it reads as a miss."""
    assert decode_cache_value("snapshot:BTCUSDT", pickle.dumps({"price": 1.5})) is None


def test_decode_cache_value_ignores_invalid_json_and_bytes():
    assert decode_cache_value("snapshot:BTCUSDT", b"{not json") is None
    assert decode_cache_value("snapshot:BTCUSDT", b"\xff\xfe not utf-8") is None


# ---------------------------------------------------------------------------
# RedisCache against an in-process fake client (no live Redis)
# ---------------------------------------------------------------------------


class _FakeRedisClient:
    """Minimal stand-in for ``redis.asyncio.Redis`` covering the commands used."""

    def __init__(self) -> None:
        self.store: dict[str, bytes] = {}
        self.ttls: dict[str, int] = {}

    async def get(self, key):
        return self.store.get(key)

    async def set(self, key, value, ex=None, nx=False):
        if nx and key in self.store:
            return None
        self.store[key] = value
        if ex is not None:
            self.ttls[key] = ex
        return True

    async def setex(self, key, ttl, value):
        self.store[key] = value
        self.ttls[key] = ttl

    async def delete(self, key):
        self.store.pop(key, None)
        self.ttls.pop(key, None)

    async def setnx(self, key, value):
        if key in self.store:
            return 0
        self.store[key] = value
        return 1

    async def expire(self, key, ttl):
        self.ttls[key] = ttl

    def scan_iter(self, match="*"):
        prefix = match.rstrip("*")

        async def _iter():
            for key in list(self.store):
                if key.startswith(prefix):
                    yield key.encode("utf-8")

        return _iter()


@pytest.fixture
def redis_cache(monkeypatch):
    """A RedisCache wired to an in-process fake — exercises the real codec path."""
    import redis.asyncio as aioredis

    fake = _FakeRedisClient()
    monkeypatch.setattr(aioredis, "from_url", lambda *args, **kwargs: fake)
    return RedisCache("redis://cache.example:6379/0"), fake


@pytest.mark.asyncio
async def test_redis_cache_round_trips_a_snapshot(redis_cache):
    cache, fake = redis_cache
    snapshot = {"symbol": "BTCUSDT", "price": 42000.0, "anomaly": False}

    await cache.set_snapshot("BTCUSDT", snapshot)

    assert json.loads(fake.store["snapshot:BTCUSDT"]) == snapshot
    assert await cache.get_snapshot("BTCUSDT") == snapshot


@pytest.mark.asyncio
async def test_redis_cache_get_missing_returns_none(redis_cache):
    cache, _ = redis_cache
    assert await cache.get("absent") is None


@pytest.mark.asyncio
async def test_redis_cache_set_applies_ttl(redis_cache):
    cache, fake = redis_cache

    await cache.set("k", {"v": 1}, ttl=30)

    assert fake.ttls["k"] == 30


@pytest.mark.asyncio
async def test_redis_cache_set_if_absent_writes_only_first_value(redis_cache):
    cache, _ = redis_cache

    assert await cache.set_if_absent("lock", "first") is True
    assert await cache.set_if_absent("lock", "second") is False
    assert await cache.get("lock") == "first"


@pytest.mark.asyncio
async def test_redis_cache_seen_is_false_then_true(redis_cache):
    cache, fake = redis_cache

    assert await cache.seen("evt-1") is False
    assert await cache.seen("evt-1") is True
    assert fake.ttls["seen:evt-1"] == 86400


@pytest.mark.asyncio
async def test_redis_cache_list_snapshot_symbols_is_sorted(redis_cache):
    cache, _ = redis_cache
    await cache.set_snapshot("ETHUSDT", {"price": 3000})
    await cache.set_snapshot("BTCUSDT", {"price": 100000})
    await cache.set("other-key", {"ignored": True})

    assert await cache.list_snapshot_symbols() == ["BTCUSDT", "ETHUSDT"]


@pytest.mark.asyncio
async def test_redis_cache_treats_legacy_payloads_as_a_miss(redis_cache):
    """
    Snapshot keys carry no TTL, so a value left by the pre-JSON build survives
    every restart. Raising made ``/market/{symbol}/latest`` answer 500 forever;
    a miss lets the caller fall back to the store and the next write replace it.
    """
    cache, fake = redis_cache
    fake.store["snapshot:BTCUSDT"] = pickle.dumps({"price": 1.0})

    assert await cache.get_snapshot("BTCUSDT") is None

    await cache.set_snapshot("BTCUSDT", {"price": 2.0})
    assert await cache.get_snapshot("BTCUSDT") == {"price": 2.0}


# ---------------------------------------------------------------------------
# seen() atomicity — the marker and its TTL must be set by one command
# ---------------------------------------------------------------------------


class _CommandLoggingRedisClient(_FakeRedisClient):
    """Fake client that records which commands were issued, in order."""

    def __init__(self) -> None:
        super().__init__()
        self.commands: list[tuple] = []

    async def set(self, key, value, ex=None, nx=False):
        self.commands.append(("set", key, ex, nx))
        return await super().set(key, value, ex=ex, nx=nx)

    async def setnx(self, key, value):
        self.commands.append(("setnx", key))
        return await super().setnx(key, value)

    async def expire(self, key, ttl):
        self.commands.append(("expire", key, ttl))
        return await super().expire(key, ttl)


@pytest.fixture
def logging_redis_cache(monkeypatch):
    import redis.asyncio as aioredis

    fake = _CommandLoggingRedisClient()
    monkeypatch.setattr(aioredis, "from_url", lambda *args, **kwargs: fake)
    return RedisCache("redis://cache.example:6379/0"), fake


@pytest.mark.asyncio
async def test_seen_sets_marker_and_ttl_in_a_single_command(logging_redis_cache):
    """
    SETNX-then-EXPIRE is not atomic.

    A process that dies between the two commands leaves a marker with no TTL,
    which then never expires — exactly the unbounded growth the idempotency
    window exists to prevent.
    """
    cache, fake = logging_redis_cache

    await cache.seen("evt-1")

    assert fake.commands == [("set", "seen:evt-1", IDEMPOTENCY_TTL_SECONDS, True)]
    assert not any(command[0] == "expire" for command in fake.commands)


@pytest.mark.asyncio
async def test_seen_does_not_extend_the_window_on_a_repeat(logging_redis_cache):
    """A second sighting must not refresh the TTL, or a hot key never expires."""
    cache, fake = logging_redis_cache

    assert await cache.seen("evt-1") is False
    fake.ttls["seen:evt-1"] = 10  # simulate the clock advancing
    assert await cache.seen("evt-1") is True

    assert fake.ttls["seen:evt-1"] == 10


@pytest.mark.asyncio
async def test_seen_keys_are_namespaced_away_from_snapshots(logging_redis_cache):
    cache, fake = logging_redis_cache

    await cache.seen("BTCUSDT")
    await cache.set_snapshot("BTCUSDT", {"price": 1.0})

    assert set(fake.store) == {"seen:BTCUSDT", "snapshot:BTCUSDT"}


# ---------------------------------------------------------------------------
# Key helpers — one definition of the cross-process key contract
# ---------------------------------------------------------------------------


def test_snapshot_key_matches_what_the_cache_writes():
    assert snapshot_key("BTCUSDT") == "snapshot:BTCUSDT"


def test_seen_key_matches_what_the_cache_writes():
    assert seen_key("evt-1") == "seen:evt-1"


@pytest.mark.asyncio
async def test_in_memory_and_redis_agree_on_the_snapshot_key(redis_cache):
    """The fake and the real client must write the same key, or the API reads nothing."""
    redis_backed, fake = redis_cache
    in_memory = InMemoryCache()

    await redis_backed.set_snapshot("BTCUSDT", {"price": 1.0})
    await in_memory.set_snapshot("BTCUSDT", {"price": 1.0})

    assert snapshot_key("BTCUSDT") in fake.store
    assert await in_memory.get(snapshot_key("BTCUSDT")) == {"price": 1.0}


# ---------------------------------------------------------------------------
# Conformance: the fake and the real client must behave identically
# ---------------------------------------------------------------------------
#
# Tests run against InMemoryCache but production runs against RedisCache, so any
# behavioural gap between them is a bug the suite cannot see. These run the same
# assertions against both.


@pytest.fixture(params=["in_memory", "redis"])
def conformant_cache(request, monkeypatch):
    if request.param == "in_memory":
        return InMemoryCache()

    import redis.asyncio as aioredis

    fake = _FakeRedisClient()
    monkeypatch.setattr(aioredis, "from_url", lambda *args, **kwargs: fake)
    return RedisCache("redis://cache.example:6379/0")


@pytest.mark.asyncio
async def test_conformance_set_if_absent_claims_an_unheld_key(conformant_cache):
    assert await conformant_cache.set_if_absent("lock", "first") is True


@pytest.mark.asyncio
async def test_conformance_set_if_absent_refuses_a_held_key(conformant_cache):
    await conformant_cache.set_if_absent("lock", "first")

    assert await conformant_cache.set_if_absent("lock", "second") is False
    assert await conformant_cache.get("lock") == "first"


@pytest.mark.asyncio
async def test_conformance_set_if_absent_keys_on_presence_not_truthiness(conformant_cache):
    """
    A key holding a falsy value is still held.

    ``get()`` cannot distinguish "absent" from "stores None", so an
    implementation that tested its result would hand out a lock that another
    worker already owns.
    """
    for held_value in (None, False, 0, ""):
        await conformant_cache.delete("lock")
        await conformant_cache.set("lock", held_value)

        assert (
            await conformant_cache.set_if_absent("lock", "stolen") is False
        ), f"lock holding {held_value!r} was treated as free"


@pytest.mark.asyncio
async def test_conformance_set_if_absent_frees_the_key_after_delete(conformant_cache):
    await conformant_cache.set_if_absent("lock", "first")
    await conformant_cache.delete("lock")

    assert await conformant_cache.set_if_absent("lock", "second") is True


@pytest.mark.asyncio
async def test_conformance_seen_is_false_then_true(conformant_cache):
    assert await conformant_cache.seen("evt-1") is False
    assert await conformant_cache.seen("evt-1") is True
    assert await conformant_cache.seen("evt-2") is False


@pytest.mark.asyncio
async def test_conformance_snapshot_round_trip(conformant_cache):
    snapshot = {"symbol": "BTCUSDT", "price": 42000.0, "anomaly": False}

    await conformant_cache.set_snapshot("BTCUSDT", snapshot)

    assert await conformant_cache.get_snapshot("BTCUSDT") == snapshot
    assert await conformant_cache.get_snapshot("ETHUSDT") is None


@pytest.mark.asyncio
async def test_conformance_list_snapshot_symbols_is_sorted(conformant_cache):
    await conformant_cache.set_snapshot("ETHUSDT", {"price": 3000})
    await conformant_cache.set_snapshot("BTCUSDT", {"price": 100000})
    await conformant_cache.set("unrelated-key", {"ignored": True})

    assert await conformant_cache.list_snapshot_symbols() == ["BTCUSDT", "ETHUSDT"]


@pytest.mark.asyncio
async def test_conformance_get_missing_key_returns_none(conformant_cache):
    assert await conformant_cache.get("absent") is None
