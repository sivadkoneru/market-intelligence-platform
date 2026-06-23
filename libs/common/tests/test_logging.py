"""Tests for libs/common/logging.py — structlog JSON output with correlation IDs."""

import ast
import asyncio
import json
import pathlib
import sys
import types

import pytest
from fastapi import FastAPI
from fastapi.testclient import TestClient


def test_configure_logging_does_not_raise():
    from libs.common.logging import configure_logging

    configure_logging(level="DEBUG")  # must not raise


def test_get_logger_returns_bound_logger():
    from libs.common.logging import configure_logging, get_logger

    configure_logging(level="INFO")
    logger = get_logger("test.module")
    assert logger is not None


def test_json_output_parseable(capsys):
    """A logged event must produce parseable JSON lines."""
    from libs.common.logging import configure_logging, get_logger

    configure_logging(level="DEBUG")
    logger = get_logger("test.json")
    logger.info("hello world", key="value")
    captured = capsys.readouterr()
    # At least one line should be valid JSON
    lines = [line for line in captured.out.splitlines() if line.strip()]
    if not lines:
        # Some structlog configs write to stderr
        lines = [line for line in captured.err.splitlines() if line.strip()]
    assert lines, "Expected at least one log line"
    parsed = json.loads(lines[-1])
    assert "event" in parsed or "message" in parsed or "msg" in parsed


def test_correlation_id_appears_in_log(capsys):
    """bind_correlation_id should inject correlation_id into every subsequent log line."""
    from libs.common.logging import bind_correlation_id, configure_logging, get_logger

    configure_logging(level="DEBUG")
    bind_correlation_id("corr-999")
    logger = get_logger("test.corr")
    logger.info("checking correlation")
    captured = capsys.readouterr()
    all_out = captured.out + captured.err
    lines = [line for line in all_out.splitlines() if line.strip()]
    assert lines, "Expected at least one log line"
    last = json.loads(lines[-1])
    assert last.get("correlation_id") == "corr-999"


def test_trace_id_appears_in_log(capsys):
    """bind_trace_id should inject trace_id into every subsequent log line."""
    from libs.common.logging import bind_trace_id, configure_logging, get_logger

    configure_logging(level="DEBUG")
    bind_trace_id("trace-abc")
    logger = get_logger("test.trace")
    logger.info("checking trace")
    captured = capsys.readouterr()
    all_out = captured.out + captured.err
    lines = [line for line in all_out.splitlines() if line.strip()]
    assert lines, "Expected at least one log line"
    last = json.loads(lines[-1])
    assert last.get("trace_id") == "trace-abc"


def test_bind_context_injects_fields(capsys):
    """bind_context should inject arbitrary key/value pairs."""
    from libs.common.logging import bind_context, configure_logging, get_logger

    configure_logging(level="DEBUG")
    bind_context(service="test-svc", env="ci")
    logger = get_logger("test.ctx")
    logger.info("context check")
    captured = capsys.readouterr()
    all_out = captured.out + captured.err
    lines = [line for line in all_out.splitlines() if line.strip()]
    assert lines
    last = json.loads(lines[-1])
    assert last.get("service") == "test-svc"
    assert last.get("env") == "ci"


def test_log_level_field_present(capsys):
    """Each log line must contain a level field."""
    from libs.common.logging import configure_logging, get_logger

    configure_logging(level="DEBUG")
    logger = get_logger("test.level")
    logger.warning("level test")
    captured = capsys.readouterr()
    all_out = captured.out + captured.err
    lines = [line for line in all_out.splitlines() if line.strip()]
    assert lines
    last = json.loads(lines[-1])
    level_val = last.get("level") or last.get("severity") or last.get("log_level") or ""
    assert level_val  # some level field is present


def test_no_bare_print():
    """The logging module itself must not use bare print calls."""
    src = (pathlib.Path(__file__).parents[1] / "logging.py").read_text(
        encoding="utf-8"
    )
    tree = ast.parse(src)
    for node in ast.walk(tree):
        if isinstance(node, ast.Call):
            if isinstance(node.func, ast.Name) and node.func.id == "print":
                pytest.fail("Found bare print() call in libs/common/logging.py")


def test_configure_logging_persists_to_in_memory_search_store(capsys):
    from libs.common.es import InMemorySearchStore
    from libs.common.logging import configure_logging, get_logger, reset_context

    store = InMemorySearchStore()
    configure_logging(level="INFO", service_name="test-svc", search_store=store)
    reset_context()
    get_logger("test.es").info("persist me", answer=42)
    captured = capsys.readouterr()

    assert captured.out
    assert "logs-test-svc" in store._logs
    assert store._logs["logs-test-svc"][-1]["event"] == "persist me"
    assert store._logs["logs-test-svc"][-1]["answer"] == 42
    assert store._logs["logs-test-svc"][-1]["service"] == "test-svc"


def test_configure_logging_clears_previous_search_store(capsys):
    from libs.common.es import InMemorySearchStore
    from libs.common.logging import configure_logging, get_logger, reset_context

    store = InMemorySearchStore()
    configure_logging(level="INFO", service_name="first", search_store=store)
    reset_context()
    get_logger("test.es").info("persisted")
    assert store._logs["logs-first"][-1]["event"] == "persisted"

    configure_logging(level="INFO")
    reset_context()
    get_logger("test.es").info("not persisted")
    captured = capsys.readouterr()
    event = json.loads(captured.out.splitlines()[-1])

    assert captured.out
    assert event["service"] == "market-intel"
    assert "logs-market-intel" not in store._logs


def test_configure_logging_swallow_search_store_failures(capsys):
    from libs.common.logging import configure_logging, get_logger, reset_context

    class FailingSearchStore:
        async def index_log(self, index, log):
            raise RuntimeError("es down")

    configure_logging(
        level="INFO",
        service_name="test-svc",
        search_store=FailingSearchStore(),
    )
    reset_context()
    get_logger("test.es").info("survives sink failure")
    captured = capsys.readouterr()

    assert "survives sink failure" in captured.out


def test_configure_logging_swallow_async_search_store_failures(capsys):
    from libs.common.logging import configure_logging, get_logger, reset_context

    class FailingSearchStore:
        async def index_log(self, index, log):
            raise RuntimeError("es down")

    async def emit() -> None:
        configure_logging(
            level="INFO",
            service_name="test-svc",
            search_store=FailingSearchStore(),
        )
        reset_context()
        get_logger("test.es").info("survives async sink failure")
        await asyncio.sleep(0)

    asyncio.run(emit())
    captured = capsys.readouterr()

    assert "survives async sink failure" in captured.out


def test_observability_middleware_propagates_ids_and_records_metrics():
    from libs.common.logging import HTTPMetrics, install_observability

    app = FastAPI()
    metrics = HTTPMetrics()
    install_observability(app, service_name="test-svc", metrics=metrics)

    @app.get("/items/{item_id}")
    async def read_item(item_id: str) -> dict[str, str]:
        return {"item_id": item_id}

    with TestClient(app) as client:
        response = client.get(
            "/items/123",
            headers={
                "X-Correlation-ID": "corr-123",
                "X-Trace-ID": "trace-123",
            },
        )

    assert response.status_code == 200
    assert response.headers["X-Correlation-ID"] == "corr-123"
    assert response.headers["X-Trace-ID"] == "trace-123"
    assert metrics.requests_total == 1
    assert metrics.trace_context_provided_total == 1
    assert metrics.correlation_context_provided_total == 1
    assert metrics.requests_by_path["/items/{item_id}"] == 1


def test_observability_middleware_generates_ids_when_headers_missing():
    from libs.common.logging import HTTPMetrics, install_observability

    app = FastAPI()
    metrics = HTTPMetrics()
    install_observability(app, service_name="test-svc", metrics=metrics)

    @app.get("/health")
    async def health() -> dict[str, str]:
        return {"status": "ok"}

    with TestClient(app) as client:
        response = client.get("/health")

    assert response.status_code == 200
    assert response.headers["X-Correlation-ID"]
    assert response.headers["X-Trace-ID"]
    assert metrics.requests_by_path["/health"] == 1


def test_observability_middleware_adds_ids_to_error_responses():
    from libs.common.logging import HTTPMetrics, install_observability

    app = FastAPI()
    metrics = HTTPMetrics()
    install_observability(app, service_name="test-svc", metrics=metrics)

    @app.get("/boom")
    async def boom() -> dict[str, str]:
        raise RuntimeError("boom")

    with TestClient(app, raise_server_exceptions=False) as client:
        response = client.get(
            "/boom",
            headers={
                "X-Correlation-ID": "corr-error",
                "X-Trace-ID": "trace-error",
            },
        )

    assert response.status_code == 500
    assert response.json() == {"detail": "Internal Server Error"}
    assert response.headers["X-Correlation-ID"] == "corr-error"
    assert response.headers["X-Trace-ID"] == "trace-error"
    assert metrics.requests_total == 1
    assert metrics.request_errors_total == 1
    assert metrics.requests_by_status["500"] == 1


def test_configure_new_relic_is_noop_without_config(monkeypatch):
    from libs.common.logging import configure_new_relic

    settings = types.SimpleNamespace(
        new_relic_license_key=None,
        new_relic_config_file=None,
        new_relic_app_name=None,
        new_relic_environment=None,
    )

    assert configure_new_relic(settings, service_name="api") is False


def test_configure_new_relic_is_noop_when_module_missing(monkeypatch):
    import importlib

    from libs.common.logging import configure_new_relic

    settings = types.SimpleNamespace(
        new_relic_license_key="license-key",
        new_relic_config_file=None,
        new_relic_app_name="api",
        new_relic_environment="test",
    )
    original_import_module = importlib.import_module

    def fake_import_module(name: str, package=None):
        if name == "newrelic.agent":
            raise ImportError("missing newrelic")
        return original_import_module(name, package)

    monkeypatch.setattr(importlib, "import_module", fake_import_module)

    assert configure_new_relic(settings, service_name="api") is False


def test_configure_new_relic_initializes_fake_agent(monkeypatch):
    from libs.common.logging import configure_new_relic

    calls: list[tuple[str, tuple, dict]] = []

    def initialize(*args, **kwargs):
        calls.append(("initialize", args, kwargs))

    def register_application(*args, **kwargs):
        calls.append(("register_application", args, kwargs))

    agent_module = types.SimpleNamespace(
        initialize=initialize,
        register_application=register_application,
    )
    newrelic_module = types.ModuleType("newrelic")
    newrelic_module.agent = agent_module
    monkeypatch.setitem(sys.modules, "newrelic", newrelic_module)
    monkeypatch.setitem(sys.modules, "newrelic.agent", agent_module)

    settings = types.SimpleNamespace(
        new_relic_license_key="license-key",
        new_relic_config_file=None,
        new_relic_app_name="api",
        new_relic_environment="test",
    )

    assert configure_new_relic(settings, service_name="api") is True
    assert calls[0] == ("initialize", (), {})
    assert calls[1] == ("register_application", (), {"timeout": 0})
