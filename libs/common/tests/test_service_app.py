"""Tests for libs/common/service_app.py — the shared FastAPI service bootstrap."""

import asyncio
import logging
from dataclasses import dataclass

import pytest
from fastapi.testclient import TestClient

from libs.common.logging import ServiceMetrics
from libs.common.service_app import (
    DISCLAIMER,
    SERVICE_VERSION,
    bootstrap_service_logging,
    create_service_app,
    worker_lifespan,
)


@dataclass
class _FakeMetrics(ServiceMetrics):
    calls: int = 0

    def render(self) -> str:
        return f"fake_calls {self.calls}\n"


class _FakeService:
    def __init__(self) -> None:
        self.metrics = _FakeMetrics()
        self.closed = 0
        self.started = 0
        self.cancelled = 0

    async def health(self) -> dict[str, object]:
        return {"status": "ok", "service": "fake"}

    async def run_forever(self) -> None:
        self.started += 1
        try:
            await asyncio.Event().wait()
        except asyncio.CancelledError:
            self.cancelled += 1
            raise

    async def close(self) -> None:
        self.closed += 1


def _build_app(service, **kwargs):
    return create_service_app(
        service_name="fake",
        title="Fake Service",
        summary="Portfolio service for tests.",
        service=service,
        state_attr="fake_service",
        render_metrics=service.metrics.render,
        **kwargs,
    )


# ---------------------------------------------------------------------------
# create_service_app — shared routes
# ---------------------------------------------------------------------------


def test_root_reports_service_name_and_disclaimer():
    service = _FakeService()

    with TestClient(_build_app(service)) as client:
        payload = client.get("/").json()

    assert payload["service"] == "fake"
    assert payload["message"] == DISCLAIMER
    # No routes advertised unless the service asked for it.
    assert "routes" not in payload


def test_root_advertises_routes_when_supplied():
    service = _FakeService()

    with TestClient(_build_app(service, routes=("/health", "/widgets"))) as client:
        payload = client.get("/").json()

    assert payload["routes"] == ["/health", "/widgets"]


def test_display_name_overrides_the_name_reported_on_root():
    service = _FakeService()

    with TestClient(_build_app(service, display_name="fake-analysis")) as client:
        payload = client.get("/").json()

    # The logging/metrics short name stays "fake"; only the public name changes.
    assert payload["service"] == "fake-analysis"


def test_description_always_carries_the_no_advice_disclaimer():
    app = _build_app(_FakeService())

    assert app.description == "Portfolio service for tests. No financial advice. No real trades."
    assert app.version == SERVICE_VERSION


def test_health_delegates_to_the_service():
    service = _FakeService()

    with TestClient(_build_app(service)) as client:
        response = client.get("/health")

    assert response.status_code == 200
    assert response.json() == {"status": "ok", "service": "fake"}


def test_metrics_renders_as_plain_text_from_the_supplied_renderer():
    service = _FakeService()
    service.metrics.calls = 7

    with TestClient(_build_app(service)) as client:
        response = client.get("/metrics")

    assert response.status_code == 200
    assert response.text == "fake_calls 7\n"
    assert response.headers["content-type"].startswith("text/plain")


def test_service_is_published_on_app_state():
    service = _FakeService()
    app = _build_app(service)

    assert app.state.fake_service is service


def test_observability_middleware_is_installed():
    """Every service must echo correlation headers and count its requests."""
    service = _FakeService()

    with TestClient(_build_app(service)) as client:
        response = client.get("/health", headers={"X-Correlation-ID": "abc-123"})

    assert response.headers["X-Correlation-ID"] == "abc-123"
    assert service.metrics.http.requests_total == 1


# ---------------------------------------------------------------------------
# worker_lifespan
# ---------------------------------------------------------------------------


def test_worker_lifespan_runs_and_cancels_the_background_task():
    service = _FakeService()
    app = _build_app(
        service,
        lifespan=worker_lifespan(
            service.run_forever,
            task_name="fake-worker",
            state_attr="fake_task",
        ),
    )

    with TestClient(app) as client:
        client.get("/health")
        assert service.started == 1
        task = app.state.fake_task
        assert task.get_name() == "fake-worker"

    # Leaving the context runs shutdown: the worker must not outlive the app.
    assert service.cancelled == 1
    assert task.cancelled()


def test_worker_lifespan_skips_the_task_when_start_is_none():
    service = _FakeService()
    app = _build_app(
        service,
        lifespan=worker_lifespan(None, task_name="fake-worker", state_attr="fake_task"),
    )

    with TestClient(app) as client:
        client.get("/health")

    assert service.started == 0
    assert not hasattr(app.state, "fake_task")


def test_worker_lifespan_closes_backends_on_shutdown():
    service = _FakeService()
    app = _build_app(
        service,
        lifespan=worker_lifespan(
            service.run_forever,
            task_name="fake-worker",
            state_attr="fake_task",
            close=service.close,
        ),
    )

    with TestClient(app) as client:
        client.get("/health")
        assert service.closed == 0

    assert service.closed == 1


def test_worker_lifespan_closes_backends_even_when_the_worker_dies():
    """A crashed worker must still release the connections the app opened."""
    service = _FakeService()

    async def crash() -> None:
        raise RuntimeError("worker exploded")

    app = _build_app(
        service,
        lifespan=worker_lifespan(
            crash,
            task_name="fake-worker",
            state_attr="fake_task",
            close=service.close,
        ),
    )

    with TestClient(app) as client:
        client.get("/health")

    assert service.closed == 1


def test_worker_lifespan_reports_a_worker_that_died_on_its_own(caplog):
    """
    A dead worker must be surfaced, not swallowed.

    An unretrieved task exception only appears as an asyncio warning at GC
    time, so a crashed consumer would otherwise stop processing while
    ``/health`` kept answering ``ok``.
    """
    service = _FakeService()

    async def crash() -> None:
        raise RuntimeError("worker exploded")

    app = _build_app(
        service,
        lifespan=worker_lifespan(crash, task_name="fake-worker", state_attr="fake_task"),
    )

    with (
        caplog.at_level(logging.ERROR, logger="libs.common.service_app"),
        TestClient(app) as client,
    ):
        client.get("/health")

    logged = [record.getMessage() for record in caplog.records]
    assert any("service.worker_failed" in message for message in logged)
    assert any("worker exploded" in message for message in logged)


def test_worker_lifespan_stays_quiet_when_the_worker_is_cancelled_normally(caplog):
    service = _FakeService()
    app = _build_app(
        service,
        lifespan=worker_lifespan(
            service.run_forever, task_name="fake-worker", state_attr="fake_task"
        ),
    )

    with (
        caplog.at_level(logging.ERROR, logger="libs.common.service_app"),
        TestClient(app) as client,
    ):
        client.get("/health")

    assert not any("service.worker_failed" in record.getMessage() for record in caplog.records)


# ---------------------------------------------------------------------------
# bootstrap_service_logging
# ---------------------------------------------------------------------------


def test_bootstrap_service_logging_returns_settings_and_binds_the_service_name(capsys):
    from libs.common.logging import get_logger

    settings = bootstrap_service_logging("bootstrap-test")
    get_logger(__name__).info("probe.event")

    assert settings.log_level
    assert '"service": "bootstrap-test"' in capsys.readouterr().out


@pytest.mark.parametrize("service_name", ["api", "ai", "stream", "alerting", "ingestion"])
def test_every_service_app_module_builds(service_name):
    """The five shipped services must all construct through the shared factory."""
    import importlib

    module = importlib.import_module(f"services.{service_name}.app")
    app = module.create_app() if service_name == "api" else module.create_app(run_on_startup=False)

    paths = {route.path for route in app.routes}
    assert {"/", "/health", "/metrics"} <= paths
    assert app.version == SERVICE_VERSION
    assert "No financial advice" in app.description


# ---------------------------------------------------------------------------
# Every shipped service releases its backends on shutdown
# ---------------------------------------------------------------------------


def _real_service(service_name: str):
    """Build a service wired to the offline fakes, as its app would."""
    import importlib

    return importlib.import_module(f"services.{service_name}.app").build_default_service()


@pytest.mark.parametrize("service_name", ["api", "ai", "stream", "alerting", "ingestion"])
def test_every_service_releases_its_backends_on_shutdown(service_name, monkeypatch):
    """
    DruidClient and RedisCache hold persistent connections.

    A service whose app never calls close() leaks one set of sockets per
    crash-loop restart, so the lifespan must invoke it for all five.
    """
    import importlib

    module = importlib.import_module(f"services.{service_name}.app")
    service = _real_service(service_name)

    calls = []
    original_close = service.close

    async def counting_close():
        calls.append(1)
        await original_close()

    monkeypatch.setattr(service, "close", counting_close)

    app = (
        module.create_app(service)
        if service_name == "api"
        else module.create_app(service, run_on_startup=False)
    )
    with TestClient(app) as client:
        client.get("/health")

    assert calls == [1], f"services.{service_name}.app never closed its service"
