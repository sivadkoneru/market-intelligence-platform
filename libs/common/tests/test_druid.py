"""Tests for libs.common.druid — InMemoryTimeSeriesStore, DruidClient, and factory."""

import json
from datetime import datetime

import pytest

from libs.common.druid import DruidClient, InMemoryTimeSeriesStore, get_timeseries_store


def _ts(year: int, month: int, day: int, hour: int = 0) -> datetime:
    return datetime(year, month, day, hour, 0, 0)


# ---------------------------------------------------------------------------
# ingest + count
# ---------------------------------------------------------------------------


@pytest.mark.asyncio
async def test_ingest_and_count():
    store = InMemoryTimeSeriesStore()
    rows = [
        {"symbol": "BTCUSDT", "ts": _ts(2024, 1, 1), "price": 45000.0},
        {"symbol": "BTCUSDT", "ts": _ts(2024, 1, 2), "price": 46000.0},
    ]
    await store.ingest(rows)
    assert await store.count() == 2


@pytest.mark.asyncio
async def test_count_empty():
    store = InMemoryTimeSeriesStore()
    assert await store.count() == 0


@pytest.mark.asyncio
async def test_ingest_multiple_batches():
    store = InMemoryTimeSeriesStore()
    await store.ingest([{"symbol": "A", "ts": _ts(2024, 1, 1), "price": 1.0}])
    await store.ingest([{"symbol": "B", "ts": _ts(2024, 1, 2), "price": 2.0}])
    assert await store.count() == 2


# ---------------------------------------------------------------------------
# latest
# ---------------------------------------------------------------------------


@pytest.mark.asyncio
async def test_latest_returns_most_recent():
    store = InMemoryTimeSeriesStore()
    await store.ingest([
        {"symbol": "BTCUSDT", "ts": _ts(2024, 1, 1, 8), "price": 40000.0},
        {"symbol": "BTCUSDT", "ts": _ts(2024, 1, 1, 12), "price": 42000.0},
        {"symbol": "BTCUSDT", "ts": _ts(2024, 1, 1, 6), "price": 39000.0},
    ])
    row = await store.latest("BTCUSDT")
    assert row is not None
    assert row["price"] == 42000.0


@pytest.mark.asyncio
async def test_latest_symbol_not_found():
    store = InMemoryTimeSeriesStore()
    assert await store.latest("MISSING") is None


@pytest.mark.asyncio
async def test_latest_only_for_given_symbol():
    store = InMemoryTimeSeriesStore()
    await store.ingest([
        {"symbol": "BTCUSDT", "ts": _ts(2024, 1, 1), "price": 50000.0},
        {"symbol": "ETHUSDT", "ts": _ts(2024, 1, 2), "price": 3000.0},
    ])
    row = await store.latest("BTCUSDT")
    assert row["price"] == 50000.0


# ---------------------------------------------------------------------------
# history (range filter)
# ---------------------------------------------------------------------------


@pytest.mark.asyncio
async def test_history_range_filter():
    store = InMemoryTimeSeriesStore()
    await store.ingest([
        {"symbol": "BTC", "ts": _ts(2024, 1, 1), "price": 1.0},
        {"symbol": "BTC", "ts": _ts(2024, 1, 5), "price": 2.0},
        {"symbol": "BTC", "ts": _ts(2024, 1, 10), "price": 3.0},
        {"symbol": "BTC", "ts": _ts(2024, 1, 15), "price": 4.0},
    ])
    rows = await store.history("BTC", frm=_ts(2024, 1, 4), to=_ts(2024, 1, 11))
    prices = [r["price"] for r in rows]
    assert sorted(prices) == [2.0, 3.0]


@pytest.mark.asyncio
async def test_history_empty_range():
    store = InMemoryTimeSeriesStore()
    await store.ingest([
        {"symbol": "BTC", "ts": _ts(2024, 6, 1), "price": 99.0},
    ])
    rows = await store.history("BTC", frm=_ts(2024, 1, 1), to=_ts(2024, 1, 31))
    assert rows == []


@pytest.mark.asyncio
async def test_history_symbol_isolation():
    store = InMemoryTimeSeriesStore()
    await store.ingest([
        {"symbol": "BTC", "ts": _ts(2024, 1, 5), "price": 1.0},
        {"symbol": "ETH", "ts": _ts(2024, 1, 5), "price": 2.0},
    ])
    rows = await store.history("ETH", frm=_ts(2024, 1, 1), to=_ts(2024, 1, 31))
    assert len(rows) == 1
    assert rows[0]["price"] == 2.0


# ---------------------------------------------------------------------------
# query_sql (minimal inline SQL)
# ---------------------------------------------------------------------------


@pytest.mark.asyncio
async def test_query_sql_count():
    store = InMemoryTimeSeriesStore()
    await store.ingest([{"symbol": "X", "ts": _ts(2024, 1, 1), "p": 1}] * 7)
    result = await store.query_sql('SELECT COUNT(*) FROM "ticks"')
    assert result[0]["EXPR$0"] == 7


# ---------------------------------------------------------------------------
# Druid ingest spec builder
# ---------------------------------------------------------------------------


def test_build_ingest_specs_splits_mixed_tables_and_strips_table_field():
    rows = [
        {"_table": "ticks", "symbol": "BTCUSDT", "ts": "2024-01-01T00:00:00Z", "price": 1.0},
        {
            "_table": "indicators",
            "symbol": "BTCUSDT",
            "ts": "2024-01-01T00:00:00Z",
            "sma": 1.1,
        },
    ]

    specs = DruidClient._build_ingest_specs(rows)

    assert len(specs) == 2
    by_source = {spec["spec"]["dataSchema"]["dataSource"]: spec for spec in specs}
    assert set(by_source) == {"ticks", "indicators"}
    assert "_table" not in by_source["ticks"]["spec"]["dataSchema"]["dimensionsSpec"]["dimensions"]
    assert (
        "_table"
        not in by_source["indicators"]["spec"]["dataSchema"]["dimensionsSpec"]["dimensions"]
    )

    tick_data = by_source["ticks"]["spec"]["ioConfig"]["inputSource"]["data"].splitlines()
    indicator_data = by_source["indicators"]["spec"]["ioConfig"]["inputSource"]["data"].splitlines()
    assert "_table" not in json.loads(tick_data[0])
    assert "_table" not in json.loads(indicator_data[0])


def test_build_ingest_specs_defaults_rows_without_table_to_ticks():
    rows = [{"symbol": "ETHUSDT", "ts": "2024-01-01T00:00:00Z", "price": 2.0}]

    specs = DruidClient._build_ingest_specs(rows)

    assert len(specs) == 1
    spec = specs[0]
    assert spec["spec"]["dataSchema"]["dataSource"] == "ticks"
    payload = json.loads(spec["spec"]["ioConfig"]["inputSource"]["data"])
    assert payload["symbol"] == "ETHUSDT"
    assert "_table" not in payload


def test_build_ingest_specs_empty_rows_is_noop():
    assert DruidClient._build_ingest_specs([]) == []


# ---------------------------------------------------------------------------
# Factory
# ---------------------------------------------------------------------------


def test_get_timeseries_store_returns_in_memory_by_default():
    store = get_timeseries_store()
    assert isinstance(store, InMemoryTimeSeriesStore)
