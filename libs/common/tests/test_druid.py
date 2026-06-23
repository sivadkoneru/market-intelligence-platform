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


@pytest.mark.asyncio
async def test_query_sql_information_schema_tables():
    store = InMemoryTimeSeriesStore()
    await store.ingest([
        {"_table": "ticks", "symbol": "BTCUSDT", "ts": _ts(2024, 1, 1)},
        {"_table": "indicators", "symbol": "ETHUSDT", "ts": _ts(2024, 1, 2)},
    ])

    result = await store.query_sql(
        """
        SELECT "TABLE_NAME"
        FROM INFORMATION_SCHEMA.TABLES
        WHERE "TABLE_SCHEMA" = 'druid'
        """
    )

    assert result == [{"TABLE_NAME": "indicators"}, {"TABLE_NAME": "ticks"}]


@pytest.mark.asyncio
async def test_query_sql_distinct_symbols_ignores_empty_values():
    store = InMemoryTimeSeriesStore()
    await store.ingest([
        {"_table": "ticks", "symbol": "BTCUSDT", "ts": _ts(2024, 1, 1)},
        {"_table": "ticks", "symbol": "BTCUSDT", "ts": _ts(2024, 1, 2)},
        {"_table": "ticks", "symbol": "ETHUSDT", "ts": _ts(2024, 1, 3)},
        {"_table": "ticks", "symbol": "", "ts": _ts(2024, 1, 4)},
        {"_table": "ticks", "ts": _ts(2024, 1, 5)},
    ])

    result = await store.query_sql(
        """
        SELECT DISTINCT "symbol" AS "symbol"
        FROM "ticks"
        WHERE "symbol" IS NOT NULL
        """
    )

    assert result == [{"symbol": "BTCUSDT"}, {"symbol": "ETHUSDT"}]


# ---------------------------------------------------------------------------
# DruidClient.query_sql — HTTP error handling (mocked httpx)
# ---------------------------------------------------------------------------


class _FakeAsyncClient:
    """Minimal async-context stand-in for httpx.AsyncClient that returns a
    pre-built response from post()."""

    def __init__(self, response):
        self._response = response

    async def __aenter__(self):
        return self

    async def __aexit__(self, *exc):
        return False

    async def post(self, *args, **kwargs):
        return self._response


def _patch_httpx(monkeypatch, response):
    import httpx

    monkeypatch.setattr(httpx, "AsyncClient", lambda *a, **k: _FakeAsyncClient(response))


def _druid_response(status_code: int, *, json_body=None, text: str = ""):
    import httpx

    req = httpx.Request("POST", "http://druid-router:8888/druid/v2/sql")
    if json_body is not None:
        return httpx.Response(status_code, json=json_body, request=req)
    return httpx.Response(status_code, text=text, request=req)


@pytest.mark.asyncio
async def test_query_sql_missing_datasource_returns_empty(monkeypatch):
    """A not-yet-created datasource (Druid 400 'Object ... not found') reads as empty."""
    resp = _druid_response(
        400,
        json_body={
            "errorCode": "invalidInput",
            "errorMessage": "Object 'ticks' not found (line [1], column [29])",
        },
    )
    _patch_httpx(monkeypatch, resp)
    client = DruidClient("http://druid-router:8888")
    assert await client.query_sql("SELECT DISTINCT symbol FROM ticks") == []


@pytest.mark.asyncio
async def test_latest_returns_none_when_datasource_missing(monkeypatch):
    """latest() builds on query_sql; missing datasource → None (so callers 404, not 500)."""
    resp = _druid_response(
        400, json_body={"errorMessage": "Object 'ticks' not found"}
    )
    _patch_httpx(monkeypatch, resp)
    client = DruidClient("http://druid-router:8888")
    assert await client.latest("BTCUSDT") is None


@pytest.mark.asyncio
async def test_query_sql_other_400_still_raises(monkeypatch):
    """A genuine SQL error (e.g. unknown column) must NOT be swallowed as empty."""
    import httpx

    resp = _druid_response(
        400, json_body={"errorMessage": "Column 'nope' not found in any table"}
    )
    _patch_httpx(monkeypatch, resp)
    client = DruidClient("http://druid-router:8888")
    with pytest.raises(httpx.HTTPStatusError):
        await client.query_sql("SELECT nope FROM ticks")


@pytest.mark.asyncio
async def test_query_sql_server_error_still_raises(monkeypatch):
    import httpx

    resp = _druid_response(500, text="boom")
    _patch_httpx(monkeypatch, resp)
    client = DruidClient("http://druid-router:8888")
    with pytest.raises(httpx.HTTPStatusError):
        await client.query_sql("SELECT 1")


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
    assert by_source["ticks"]["spec"]["ioConfig"]["appendToExisting"] is True
    assert by_source["indicators"]["spec"]["ioConfig"]["appendToExisting"] is True
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


@pytest.mark.asyncio
async def test_druid_latest_uses_quoted_druid_time_query():
    class RecordingDruidClient(DruidClient):
        def __init__(self) -> None:
            super().__init__("http://druid.test")
            self.sql: str | None = None

        async def query_sql(self, sql: str):
            self.sql = sql
            return []

    client = RecordingDruidClient()

    assert await client.latest("BTC'USDT") is None

    assert client.sql == (
        'SELECT * FROM "ticks" '
        'WHERE "symbol" = \'BTC\'\'USDT\' '
        'ORDER BY "__time" DESC LIMIT 1'
    )


# ---------------------------------------------------------------------------
# Factory
# ---------------------------------------------------------------------------


def test_get_timeseries_store_returns_in_memory_by_default():
    store = get_timeseries_store()
    assert isinstance(store, InMemoryTimeSeriesStore)
