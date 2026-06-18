"""
FastAPI app for the stream service.
"""

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
    get_timeseries_store,
    install_observability,
)
from services.stream.service import StreamService


def build_default_service() -> StreamService:
    """Build the offline-safe default stream service used by Uvicorn."""
    return StreamService(
        bus=get_message_bus(),
        cache=get_cache(),
        store=get_timeseries_store(),
    )


def create_app(
    service: StreamService | None = None,
    *,
    run_on_startup: bool = True,
) -> FastAPI:
    settings = get_settings()
    configure_logging(
        level=settings.log_level,
        service_name="stream",
        search_store=get_search_store(settings),
        log_index=settings.elasticsearch_log_index,
    )
    configure_new_relic(settings, service_name="stream")
    resolved_service = service or build_default_service()

    @asynccontextmanager
    async def lifespan(app: FastAPI) -> AsyncIterator[None]:
        stream_task: asyncio.Task[object] | None = None
        if run_on_startup:
            stream_task = asyncio.create_task(resolved_service.run_forever())
            app.state.stream_task = stream_task

        try:
            yield
        finally:
            if stream_task is not None and not stream_task.done():
                stream_task.cancel()
                with suppress(asyncio.CancelledError):
                    await stream_task

    app = FastAPI(
        title="Market Intelligence Stream Service",
        version="0.1.0",
        description=(
            "Portfolio service for offline-safe market stream processing. "
            "No financial advice. No real trades."
        ),
        lifespan=lifespan,
    )
    app.state.stream_service = resolved_service
    install_observability(app, service_name="stream", metrics=resolved_service.metrics)

    @app.get("/")
    async def root() -> dict[str, object]:
        return {
            "service": "stream",
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
