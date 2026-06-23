"""Tests for the shared metric base classes and counter rendering in libs/common/logging.py."""

from dataclasses import dataclass

import pytest

from libs.common.logging import HTTPMetrics, ServiceMetrics, WorkerMetrics, render_counters

# ---------------------------------------------------------------------------
# render_counters
# ---------------------------------------------------------------------------


def test_render_counters_emits_a_type_header_per_metric():
    lines = render_counters("svc", {"widgets_made": 3, "widgets_dropped": 0})

    assert lines == [
        "# TYPE svc_widgets_made counter",
        "svc_widgets_made 3",
        "# TYPE svc_widgets_dropped counter",
        "svc_widgets_dropped 0",
    ]


def test_render_counters_preserves_insertion_order():
    """Rendered output is a stable contract — scrapers and tests diff on it."""
    lines = render_counters("svc", {"c": 1, "a": 2, "b": 3})

    assert [line for line in lines if not line.startswith("#")] == [
        "svc_c 1",
        "svc_a 2",
        "svc_b 3",
    ]


def test_render_counters_returns_empty_for_no_counters():
    assert render_counters("svc", {}) == []


# ---------------------------------------------------------------------------
# ServiceMetrics / WorkerMetrics
# ---------------------------------------------------------------------------


@dataclass
class _ExampleMetrics(ServiceMetrics):
    processed: int = 0


@dataclass
class _ExampleWorkerMetrics(WorkerMetrics):
    processed: int = 0


def test_service_metrics_provides_its_own_http_counters():
    a = _ExampleMetrics()
    b = _ExampleMetrics()

    a.http.requests_total += 1

    # default_factory, not a shared class-level instance.
    assert isinstance(a.http, HTTPMetrics)
    assert b.http.requests_total == 0


def test_record_http_request_delegates_to_the_http_counters():
    metrics = _ExampleMetrics()

    metrics.record_http_request(
        method="GET",
        path="/health",
        status_code=200,
        duration_ms=1.5,
        trace_context_provided=True,
        correlation_context_provided=False,
    )

    assert metrics.http.requests_total == 1
    assert metrics.http.requests_by_method["GET"] == 1
    assert metrics.http.requests_by_path["/health"] == 1
    assert metrics.http.requests_by_status["200"] == 1
    assert metrics.http.trace_context_provided_total == 1
    assert metrics.http.correlation_context_provided_total == 0
    assert metrics.http.request_duration_ms_total == pytest.approx(1.5)


def test_record_http_request_counts_server_errors():
    metrics = _ExampleMetrics()

    metrics.record_http_request(
        method="GET",
        path="/boom",
        status_code=500,
        duration_ms=1.0,
        trace_context_provided=False,
        correlation_context_provided=False,
    )

    assert metrics.http.request_errors_total == 1


def test_worker_metrics_adds_last_error_for_the_poll_loop():
    metrics = _ExampleWorkerMetrics()

    assert metrics.last_error is None
    metrics.last_error = "boom"
    assert metrics.last_error == "boom"


def test_subclass_fields_keep_their_defaults():
    """Inheriting a defaulted base field must not break subclass construction."""
    assert _ExampleMetrics().processed == 0
    assert _ExampleWorkerMetrics().processed == 0


@pytest.mark.parametrize(
    ("module_path", "class_name", "prefix"),
    [
        ("services.api.service", "APIMetrics", "api"),
        ("services.ai.service", "AIMetrics", "ai"),
        ("services.stream.service", "StreamMetrics", "stream"),
        ("services.alerting.service", "AlertingMetrics", "alerting"),
        ("services.ingestion.service", "IngestionMetrics", "ingestion"),
    ],
)
def test_every_service_metrics_shares_the_middleware_contract(module_path, class_name, prefix):
    """
    The observability middleware calls ``record_http_request`` by duck-typing.

    Each service used to carry its own copy; they must all still satisfy it and
    fold the shared HTTP counters into their rendered output.
    """
    import importlib

    metrics = getattr(importlib.import_module(module_path), class_name)()
    metrics.record_http_request(
        method="GET",
        path="/health",
        status_code=200,
        duration_ms=2.0,
        trace_context_provided=False,
        correlation_context_provided=False,
    )

    assert isinstance(metrics, ServiceMetrics)
    rendered = (
        metrics.render(timeseries_backend="x", cache_backend="y", bus_backend="z")
        if prefix == "api"
        else metrics.render()
    )
    assert f"{prefix}_http_requests_total 1" in rendered
    assert rendered.endswith("\n")
