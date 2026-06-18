"""Test that core dependencies are installed and importable."""


def test_pydantic_import_and_version():
    """Test pydantic is installed and check its version."""
    import pydantic

    assert hasattr(pydantic, "VERSION")
    assert pydantic.VERSION.startswith("2"), f"Expected pydantic v2, got {pydantic.VERSION}"


def test_fastapi_import():
    """Test fastapi is installed."""
    import fastapi

    assert hasattr(fastapi, "FastAPI")


def test_pandas_import():
    """Test pandas is installed."""
    import pandas

    assert hasattr(pandas, "DataFrame")


def test_numpy_import():
    """Test numpy is installed."""
    import numpy

    assert hasattr(numpy, "array")


def test_structlog_import():
    """Test structlog is installed."""
    import structlog

    assert hasattr(structlog, "get_logger")


def test_tenacity_import():
    """Test tenacity is installed."""
    import tenacity

    assert hasattr(tenacity, "retry")


def test_redis_import():
    """Test redis is installed."""
    import redis

    assert hasattr(redis, "Redis")


def test_elasticsearch_import():
    """Test elasticsearch is installed."""
    import elasticsearch

    assert hasattr(elasticsearch, "Elasticsearch")


def test_azure_servicebus_import():
    """Test azure.servicebus is installed."""
    import azure.servicebus

    assert hasattr(azure.servicebus, "ServiceBusClient")


def test_sqlalchemy_import():
    """Test sqlalchemy is installed."""
    import sqlalchemy

    assert hasattr(sqlalchemy, "create_engine")


def test_httpx_import():
    """Test httpx is installed."""
    import httpx

    assert hasattr(httpx, "Client")


def test_websockets_import():
    """Test websockets is installed."""
    import websockets

    assert hasattr(websockets, "connect")


def test_aiohttp_import():
    """Test aiohttp is installed."""
    import aiohttp

    assert hasattr(aiohttp, "ClientSession")


def test_pydantic_settings_import():
    """Test pydantic_settings is installed."""
    from pydantic_settings import BaseSettings

    assert BaseSettings is not None


def test_asyncpg_import():
    """Test asyncpg is installed."""
    import asyncpg

    assert hasattr(asyncpg, "connect")


def test_python_dateutil_import():
    """Test python_dateutil is installed."""
    from dateutil import parser

    assert hasattr(parser, "parse")


def test_orjson_import():
    """Test orjson is installed."""
    import orjson

    assert hasattr(orjson, "dumps")


def test_uvicorn_import():
    """Test uvicorn is installed."""
    import uvicorn

    assert hasattr(uvicorn, "run")


def test_ruff_available():
    """Test ruff is available in venv."""
    import subprocess

    result = subprocess.run([".venv/bin/ruff", "--version"], capture_output=True, text=True)
    assert result.returncode == 0, f"ruff not available: {result.stderr}"


def test_black_available():
    """Test black is available in venv."""
    import subprocess

    result = subprocess.run([".venv/bin/black", "--version"], capture_output=True, text=True)
    assert result.returncode == 0, f"black not available: {result.stderr}"
