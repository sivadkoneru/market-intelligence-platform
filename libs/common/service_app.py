"""
Shared FastAPI bootstrap for the platform's services.

Every service stood up its own app the same way — resolve settings, configure
structlog and New Relic, wrap a background worker in a lifespan that cancels it
on shutdown, then register identical ``/``, ``/health``, and ``/metrics``
handlers. Five copies meant five places to update whenever the observability
contract moved, and the copies had already drifted: one service was missing its
root route entirely.

Public API
----------
bootstrap_service_logging() — configure structlog + New Relic for a service.
worker_lifespan()           — lifespan that runs a coroutine as a cancellable task.
create_service_app()        — build a FastAPI app with the shared routes attached.
"""

from __future__ import annotations

import asyncio
from collections.abc import AsyncIterator, Awaitable, Callable, Coroutine, Sequence
from contextlib import asynccontextmanager, suppress
from typing import Any

from fastapi import FastAPI
from fastapi.responses import PlainTextResponse

from libs.common.config import Settings, get_settings
from libs.common.es import get_search_store
from libs.common.logging import (
    close_log_sink,
    configure_logging,
    configure_new_relic,
    get_logger,
    install_observability,
)

__all__ = [
    "DISCLAIMER",
    "SERVICE_VERSION",
    "bootstrap_service_logging",
    "worker_lifespan",
    "create_service_app",
]

# Portfolio-project disclaimer. Required in the API root response by project
# rule 5; every service repeats it so the constraint is visible wherever a
# service is introspected.
DISCLAIMER = "Portfolio project only. No financial advice. No real trades."

SERVICE_VERSION = "0.1.0"

Lifespan = Callable[[FastAPI], Any]


def bootstrap_service_logging(service_name: str) -> Settings:
    """
    Configure structured logging and New Relic for *service_name*.

    Returns the resolved settings so callers can keep using them without a
    second ``get_settings()`` lookup.
    """
    settings = get_settings()
    configure_logging(
        level=settings.log_level,
        service_name=service_name,
        search_store=get_search_store(settings),
        log_index=settings.elasticsearch_log_index,
    )
    configure_new_relic(settings, service_name=service_name)
    return settings


def worker_lifespan(
    start: Callable[[], Coroutine[Any, Any, Any]] | None,
    *,
    task_name: str,
    state_attr: str,
    close: Callable[[], Awaitable[None]] | None = None,
) -> Lifespan:
    """
    Build a lifespan that runs ``start()`` as a background task for the app's life.

    The task is cancelled and awaited on shutdown, so a worker cannot outlive
    the app that owns it. A worker that died on its own is not merely dropped:
    its exception is retrieved and logged, because an unretrieved task exception
    surfaces only as an asyncio warning at garbage-collection time — meaning the
    worker stops consuming while ``/health`` keeps answering ``ok``.

    ``close`` runs afterwards in a ``finally``, releasing backends even when
    startup or the request phase raised. Pass ``start=None`` for a service with
    no background worker.
    """
    log = get_logger(__name__)

    @asynccontextmanager
    async def lifespan(app: FastAPI) -> AsyncIterator[None]:
        task: asyncio.Task[Any] | None = None
        if start is not None:
            task = asyncio.create_task(start(), name=task_name)
            setattr(app.state, state_attr, task)

        try:
            yield
        finally:
            if task is not None:
                if task.done():
                    _log_worker_failure(log, task, task_name)
                else:
                    task.cancel()
                    with suppress(asyncio.CancelledError):
                        await task
            if close is not None:
                await close()
            # The log sink's search store is built by
            # ``bootstrap_service_logging``, not by the service, so no
            # ``close_backends`` call reaches it.
            await close_log_sink()

    return lifespan


def _log_worker_failure(log: Any, task: asyncio.Task[Any], task_name: str) -> None:
    """Retrieve and report the exception of a worker that ended before shutdown."""
    if task.cancelled():
        return
    exc = task.exception()
    if exc is None:
        return
    log.error(
        "service.worker_failed",
        task=task_name,
        error_type=type(exc).__name__,
        error=str(exc),
    )


def create_service_app(
    *,
    service_name: str,
    title: str,
    summary: str,
    service: Any,
    state_attr: str,
    render_metrics: Callable[[], str],
    lifespan: Lifespan | None = None,
    routes: Sequence[str] | None = None,
    display_name: str | None = None,
) -> FastAPI:
    """
    Build a FastAPI app with the platform's shared routes and middleware.

    *summary* describes what the service does; the disclaimer is appended to it
    so no service can ship without one. *routes* is advertised on ``/`` when the
    service wants to list its endpoints. *display_name* overrides the name
    reported by ``/`` for services whose public name differs from the short
    name used for logs and metric prefixes (``ai`` publishes ``ai-analysis``).
    """
    reported_name = display_name or service_name
    app = FastAPI(
        title=title,
        version=SERVICE_VERSION,
        description=f"{summary} No financial advice. No real trades.",
        lifespan=lifespan,
    )
    setattr(app.state, state_attr, service)
    install_observability(app, service_name=service_name, metrics=service.metrics)

    @app.get("/")
    async def root() -> dict[str, object]:
        payload: dict[str, object] = {"service": reported_name, "message": DISCLAIMER}
        if routes is not None:
            payload["routes"] = list(routes)
        return payload

    @app.get("/health")
    async def health() -> dict[str, object]:
        return await service.health()

    @app.get("/metrics", response_class=PlainTextResponse)
    async def metrics() -> str:
        return render_metrics()

    return app
