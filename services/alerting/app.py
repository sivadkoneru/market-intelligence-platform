"""FastAPI app for the alerting service."""

from __future__ import annotations

import asyncio
from collections.abc import AsyncIterator
from contextlib import asynccontextmanager, suppress

from fastapi import FastAPI
from fastapi.responses import PlainTextResponse

from libs.common import (
    configure_logging,
    configure_new_relic,
    get_cache,
    get_message_bus,
    get_search_store,
    get_settings,
    install_observability,
)
from services.alerting.service import AlertingService


def build_default_service() -> AlertingService:
    """Build the offline-safe default alerting service used by Uvicorn."""
    return AlertingService(
        bus=get_message_bus(),
        cache=get_cache(),
    )


def create_app(
    service: AlertingService | None = None,
    *,
    run_on_startup: bool = True,
) -> FastAPI:
    settings = get_settings()
    configure_logging(
        level=settings.log_level,
        service_name="alerting",
        search_store=get_search_store(settings),
        log_index=settings.elasticsearch_log_index,
    )
    configure_new_relic(settings, service_name="alerting")
    resolved_service = service or build_default_service()

    @asynccontextmanager
    async def lifespan(app: FastAPI) -> AsyncIterator[None]:
        worker_task: asyncio.Task[object] | None = None
        if run_on_startup:
            worker_task = asyncio.create_task(resolved_service.run_forever())
            app.state.alerting_task = worker_task

        try:
            yield
        finally:
            if worker_task is not None and not worker_task.done():
                worker_task.cancel()
                with suppress(asyncio.CancelledError):
                    await worker_task

    app = FastAPI(
        title="Market Intelligence Alerting Service",
        version="0.1.0",
        description=(
            "Portfolio service for offline-safe alert evaluation. "
            "No financial advice. No real trades."
        ),
        lifespan=lifespan,
    )
    app.state.alerting_service = resolved_service
    install_observability(app, service_name="alerting", metrics=resolved_service.metrics)

    @app.get("/")
    async def root() -> dict[str, object]:
        return {
            "service": "alerting",
            "message": "Portfolio project only. No financial advice. No real trades.",
        }

    @app.get("/health")
    async def health() -> dict[str, object]:
        return await resolved_service.health()

    @app.get("/metrics", response_class=PlainTextResponse)
    async def metrics() -> str:
        return resolved_service.metrics.render()

    return app


app = create_app()
