"""
Structured JSON logging for the market intelligence platform.

Uses structlog with contextvars-based context propagation so that
correlation_id and trace_id are injected into every log line automatically.

Usage::

    from libs.common.logging import configure_logging, get_logger, bind_correlation_id

    configure_logging(level="INFO")
    bind_correlation_id("req-abc-123")
    log = get_logger(__name__)
    log.info("ingestion.start", symbol="BTCUSDT")

No bare ``print`` is used anywhere in this module.
"""

from __future__ import annotations

import asyncio
import importlib
import logging
import os
import sys
import time
import uuid
from contextvars import ContextVar
from dataclasses import dataclass, field
from typing import Any

import structlog
from fastapi import FastAPI, Request
from fastapi.responses import JSONResponse
from starlette.responses import Response

from libs.common.es import InMemorySearchStore, SearchStore

CORRELATION_ID_HEADER = "X-Correlation-ID"
TRACE_ID_HEADER = "X-Trace-ID"
TRACEPARENT_HEADER = "traceparent"


@dataclass
class HTTPMetrics:
    """Shared HTTP-level counters rendered into each service /metrics response."""

    requests_total: int = 0
    request_errors_total: int = 0
    request_duration_ms_total: float = 0.0
    trace_context_provided_total: int = 0
    correlation_context_provided_total: int = 0
    requests_by_method: dict[str, int] = field(default_factory=dict)
    requests_by_path: dict[str, int] = field(default_factory=dict)
    requests_by_status: dict[str, int] = field(default_factory=dict)

    def record_http_request(
        self,
        *,
        method: str,
        path: str,
        status_code: int,
        duration_ms: float,
        trace_context_provided: bool,
        correlation_context_provided: bool,
    ) -> None:
        self.requests_total += 1
        self.request_duration_ms_total += duration_ms
        if status_code >= 500:
            self.request_errors_total += 1
        if trace_context_provided:
            self.trace_context_provided_total += 1
        if correlation_context_provided:
            self.correlation_context_provided_total += 1
        self.requests_by_method[method] = self.requests_by_method.get(method, 0) + 1
        self.requests_by_path[path] = self.requests_by_path.get(path, 0) + 1
        status_key = str(status_code)
        self.requests_by_status[status_key] = self.requests_by_status.get(status_key, 0) + 1

    def render(self, prefix: str) -> list[str]:
        lines = [
            f"# TYPE {prefix}_http_requests_total counter",
            f"{prefix}_http_requests_total {self.requests_total}",
            f"# TYPE {prefix}_http_request_errors_total counter",
            f"{prefix}_http_request_errors_total {self.request_errors_total}",
            f"# TYPE {prefix}_http_request_duration_ms_total counter",
            f"{prefix}_http_request_duration_ms_total {self.request_duration_ms_total:.3f}",
            f"# TYPE {prefix}_http_trace_context_provided_total counter",
            f"{prefix}_http_trace_context_provided_total {self.trace_context_provided_total}",
            f"# TYPE {prefix}_http_correlation_context_provided_total counter",
            (
                f"{prefix}_http_correlation_context_provided_total "
                f"{self.correlation_context_provided_total}"
            ),
            f"# TYPE {prefix}_http_requests_by_method_total counter",
        ]
        for method, count in sorted(self.requests_by_method.items()):
            lines.append(f'{prefix}_http_requests_by_method_total{{method="{method}"}} {count}')
        lines.append(f"# TYPE {prefix}_http_requests_by_path_total counter")
        for path, count in sorted(self.requests_by_path.items()):
            lines.append(f'{prefix}_http_requests_by_path_total{{path="{path}"}} {count}')
        lines.append(f"# TYPE {prefix}_http_requests_by_status_total counter")
        for status, count in sorted(self.requests_by_status.items()):
            lines.append(
                f'{prefix}_http_requests_by_status_total{{status="{status}"}} {count}'
            )
        return lines

# ---------------------------------------------------------------------------
# ContextVars for per-request / per-task context
# ---------------------------------------------------------------------------

_correlation_id: ContextVar[str | None] = ContextVar("correlation_id", default=None)
_trace_id: ContextVar[str | None] = ContextVar("trace_id", default=None)
_extra_context: ContextVar[dict[str, Any]] = ContextVar("extra_context", default={})

_configured = False
_service_name = "market-intel"
_log_index = "logs-market-intel"
_search_store: SearchStore | None = None


# ---------------------------------------------------------------------------
# Context injection processor
# ---------------------------------------------------------------------------


def _inject_context(
    logger: Any, method: str, event_dict: dict[str, Any]
) -> dict[str, Any]:
    """Structlog processor: injects correlation/trace IDs and extra context into every event."""
    cid = _correlation_id.get()
    if cid is not None:
        event_dict.setdefault("correlation_id", cid)

    tid = _trace_id.get()
    if tid is not None:
        event_dict.setdefault("trace_id", tid)

    extra = _extra_context.get()
    for k, v in extra.items():
        event_dict.setdefault(k, v)

    event_dict.setdefault("service", _service_name)
    return event_dict


def _persist_event(
    logger: Any, method: str, event_dict: dict[str, Any]
) -> dict[str, Any]:
    """Best-effort side effect: mirror structured logs into the configured search store."""
    if _search_store is None:
        return event_dict

    payload = dict(event_dict)
    if isinstance(_search_store, InMemorySearchStore):
        _search_store._logs.setdefault(_log_index, []).append(payload)
        return event_dict

    try:
        loop = asyncio.get_running_loop()
    except RuntimeError:
        asyncio.run(_index_log_safely(_search_store, _log_index, payload))
        return event_dict

    loop.create_task(_index_log_safely(_search_store, _log_index, payload))
    return event_dict


async def _index_log_safely(
    search_store: SearchStore,
    index: str,
    payload: dict[str, Any],
) -> None:
    """Write a log event to the search store without letting sink failures escape."""
    try:
        await search_store.index_log(index, payload)
    except Exception:
        return


def _generate_correlation_id() -> str:
    return str(uuid.uuid4())


def _generate_trace_id() -> str:
    return uuid.uuid4().hex


def _extract_trace_id(request: Request) -> tuple[str, bool]:
    trace_header = request.headers.get(TRACE_ID_HEADER)
    if trace_header:
        return trace_header, True

    traceparent = request.headers.get(TRACEPARENT_HEADER)
    if traceparent:
        parts = traceparent.split("-")
        if len(parts) >= 4 and len(parts[1]) == 32:
            return parts[1], True
    return _generate_trace_id(), False


def _extract_correlation_id(request: Request) -> tuple[str, bool]:
    correlation_id = request.headers.get(CORRELATION_ID_HEADER)
    if correlation_id:
        return correlation_id, True

    request_id = request.headers.get("X-Request-ID")
    if request_id:
        return request_id, True
    return _generate_correlation_id(), False


def _safe_route_path(request: Request) -> str:
    route = request.scope.get("route")
    route_path = getattr(route, "path", None)
    if isinstance(route_path, str):
        return route_path
    return request.url.path


def _record_request_metrics(
    metrics: Any,
    *,
    method: str,
    path: str,
    status_code: int,
    duration_ms: float,
    trace_context_provided: bool,
    correlation_context_provided: bool,
) -> None:
    recorder = getattr(metrics, "record_http_request", None)
    if recorder is None:
        return
    recorder(
        method=method,
        path=path,
        status_code=status_code,
        duration_ms=duration_ms,
        trace_context_provided=trace_context_provided,
        correlation_context_provided=correlation_context_provided,
    )


# ---------------------------------------------------------------------------
# Public API
# ---------------------------------------------------------------------------


def configure_logging(
    level: str | None = None,
    *,
    service_name: str | None = None,
    search_store: SearchStore | None = None,
    log_index: str | None = None,
) -> None:
    """
    Configure structlog for JSON output with ISO timestamps.

    Safe to call multiple times (idempotent). The *level* parameter overrides
    the standard library root logger level; defaults to "INFO".
    """
    global _configured, _log_index, _search_store, _service_name  # noqa: PLW0603

    resolved_service_name = service_name or "market-intel"
    _service_name = resolved_service_name
    if log_index:
        _log_index = log_index
    else:
        _log_index = (
            f"logs-{resolved_service_name.replace('_', '-').replace(' ', '-').lower()}"
        )
    _search_store = search_store

    resolved_level = (level or "INFO").upper()
    numeric_level = getattr(logging, resolved_level, logging.INFO)

    # Configure standard-library logging (structlog routes through it)
    logging.basicConfig(
        format="%(message)s",
        stream=sys.stdout,
        level=numeric_level,
        force=True,
    )

    shared_processors: list[Any] = [
        structlog.contextvars.merge_contextvars,
        _inject_context,
        structlog.stdlib.add_log_level,
        structlog.stdlib.add_logger_name,
        structlog.processors.TimeStamper(fmt="iso", utc=True),
        _persist_event,
    ]

    structlog.configure(
        processors=shared_processors
        + [
            structlog.stdlib.ProcessorFormatter.wrap_for_formatter,
        ],
        wrapper_class=structlog.make_filtering_bound_logger(numeric_level),
        context_class=dict,
        logger_factory=structlog.stdlib.LoggerFactory(),
        cache_logger_on_first_use=True,
    )

    formatter = structlog.stdlib.ProcessorFormatter(
        processors=[
            structlog.stdlib.ProcessorFormatter.remove_processors_meta,
            structlog.processors.JSONRenderer(),
        ],
        foreign_pre_chain=shared_processors,
    )

    root_logger = logging.getLogger()
    # Replace handlers to avoid duplicate output on re-configure
    for handler in root_logger.handlers[:]:
        root_logger.removeHandler(handler)

    handler = logging.StreamHandler(sys.stdout)
    handler.setFormatter(formatter)
    root_logger.addHandler(handler)
    root_logger.setLevel(numeric_level)

    _configured = True


def get_logger(name: str) -> structlog.stdlib.BoundLogger:
    """Return a structlog bound logger for *name*."""
    if not _configured:
        configure_logging()
    return structlog.get_logger(name)


def bind_correlation_id(value: str) -> None:
    """Bind *value* as ``correlation_id`` on the current context."""
    _correlation_id.set(value)


def bind_trace_id(value: str) -> None:
    """Bind *value* as ``trace_id`` on the current context."""
    _trace_id.set(value)


def bind_context(**kwargs: Any) -> None:
    """Bind arbitrary key/value pairs into the current logging context."""
    current = dict(_extra_context.get())
    current.update(kwargs)
    _extra_context.set(current)


def reset_context() -> None:
    """Clear all bound context variables (useful between tests)."""
    _correlation_id.set(None)
    _trace_id.set(None)
    _extra_context.set({})
    structlog.contextvars.clear_contextvars()


def create_observability_middleware(
    *,
    service_name: str,
    metrics: Any | None = None,
):
    """Return shared FastAPI middleware for trace IDs, headers, logs, and request counters."""
    log = get_logger("libs.common.observability")

    async def observability_middleware(request: Request, call_next) -> Response:
        reset_context()
        correlation_id, correlation_from_headers = _extract_correlation_id(request)
        trace_id, trace_from_headers = _extract_trace_id(request)
        bind_correlation_id(correlation_id)
        bind_trace_id(trace_id)
        bind_context(
            service=service_name,
            http_method=request.method,
            http_path=request.url.path,
        )
        structlog.contextvars.bind_contextvars(
            correlation_id=correlation_id,
            trace_id=trace_id,
            service=service_name,
        )
        request.state.correlation_id = correlation_id
        request.state.trace_id = trace_id

        start = time.perf_counter()
        route_path = request.url.path
        try:
            response = await call_next(request)
            route_path = _safe_route_path(request)
            status_code = response.status_code
        except Exception:
            duration_ms = (time.perf_counter() - start) * 1000
            route_path = _safe_route_path(request)
            _record_request_metrics(
                metrics,
                method=request.method,
                path=route_path,
                status_code=500,
                duration_ms=duration_ms,
                trace_context_provided=trace_from_headers,
                correlation_context_provided=correlation_from_headers,
            )
            log.exception(
                "http.request_failed",
                service=service_name,
                method=request.method,
                path=route_path,
                duration_ms=round(duration_ms, 3),
            )
            reset_context()
            return JSONResponse(
                status_code=500,
                content={"detail": "Internal Server Error"},
                headers={
                    CORRELATION_ID_HEADER: correlation_id,
                    TRACE_ID_HEADER: trace_id,
                },
            )

        response.headers[CORRELATION_ID_HEADER] = correlation_id
        response.headers[TRACE_ID_HEADER] = trace_id
        duration_ms = (time.perf_counter() - start) * 1000
        _record_request_metrics(
            metrics,
            method=request.method,
            path=route_path,
            status_code=status_code,
            duration_ms=duration_ms,
            trace_context_provided=trace_from_headers,
            correlation_context_provided=correlation_from_headers,
        )
        log.info(
            "http.request",
            service=service_name,
            method=request.method,
            path=route_path,
            status_code=status_code,
            duration_ms=round(duration_ms, 3),
        )
        reset_context()
        return response

    return observability_middleware


def install_observability(
    app: FastAPI,
    *,
    service_name: str,
    metrics: Any | None = None,
) -> None:
    """Attach shared request observability middleware to a FastAPI app."""
    app.middleware("http")(
        create_observability_middleware(service_name=service_name, metrics=metrics)
    )


def configure_new_relic(
    settings: Any,
    *,
    service_name: str | None = None,
) -> bool:
    """Best-effort New Relic bootstrap; stays silent when config or module is absent."""
    license_key = getattr(settings, "new_relic_license_key", None)
    config_file = getattr(settings, "new_relic_config_file", None)
    if not license_key and not config_file:
        return False

    try:
        agent = importlib.import_module("newrelic.agent")
    except ImportError:
        return False

    if license_key:
        os.environ.setdefault("NEW_RELIC_LICENSE_KEY", license_key)
    app_name = getattr(settings, "new_relic_app_name", None) or service_name
    if app_name:
        os.environ.setdefault("NEW_RELIC_APP_NAME", app_name)
    environment = getattr(settings, "new_relic_environment", None)
    if environment:
        os.environ.setdefault("NEW_RELIC_ENVIRONMENT", environment)

    if config_file:
        agent.initialize(config_file, environment=environment)
    else:
        agent.initialize()

    register_application = getattr(agent, "register_application", None)
    if callable(register_application):
        register_application(timeout=0)
    return True
