"""
FastAPI app for the ingestion service.
"""

from __future__ import annotations

import asyncio
from collections.abc import AsyncIterator
from contextlib import asynccontextmanager, suppress

from fastapi import FastAPI
from fastapi.responses import PlainTextResponse

from libs.common import configure_logging, get_message_bus
from services.ingestion.replay import DeterministicReplayFeed, build_default_replay_events
from services.ingestion.service import IngestionService


def build_default_service() -> IngestionService:
    """Build the offline-safe default ingestion service used by Uvicorn."""
    return IngestionService(
        bus=get_message_bus(),
        feed_factory=lambda: DeterministicReplayFeed(build_default_replay_events()),
    )


def create_app(
    service: IngestionService | None = None,
    *,
    run_on_startup: bool = True,
    startup_max_events: int | None = None,
) -> FastAPI:
    configure_logging()
    resolved_service = service or build_default_service()

    @asynccontextmanager
    async def lifespan(app: FastAPI) -> AsyncIterator[None]:
        ingestion_task: asyncio.Task[object] | None = None
        if run_on_startup:
            ingestion_task = asyncio.create_task(
                resolved_service.run(max_events=startup_max_events)
            )
            app.state.ingestion_task = ingestion_task

        try:
            yield
        finally:
            if ingestion_task is not None and not ingestion_task.done():
                ingestion_task.cancel()
                with suppress(asyncio.CancelledError):
                    await ingestion_task

    app = FastAPI(
        title="Market Intelligence Ingestion Service",
        version="0.1.0",
        description=(
            "Portfolio service for offline-safe market ingestion. "
            "No financial advice. No real trades."
        ),
        lifespan=lifespan,
    )
    app.state.ingestion_service = resolved_service

    @app.get("/health")
    async def health() -> dict[str, object]:
        return await resolved_service.health()

    @app.get("/metrics", response_class=PlainTextResponse)
    async def metrics() -> str:
        return resolved_service.metrics.render()

    return app


app = create_app()
