"""
FastAPI app for the stream service.
"""

from __future__ import annotations

from fastapi import FastAPI

from libs.common import get_cache, get_message_bus, get_timeseries_store
from libs.common.service_app import (
    bootstrap_service_logging,
    create_service_app,
    worker_lifespan,
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
    bootstrap_service_logging("stream")
    resolved_service = service or build_default_service()

    return create_service_app(
        service_name="stream",
        title="Market Intelligence Stream Service",
        summary="Portfolio service for offline-safe market stream processing.",
        service=resolved_service,
        state_attr="stream_service",
        render_metrics=resolved_service.metrics.render,
        lifespan=worker_lifespan(
            resolved_service.run_forever if run_on_startup else None,
            task_name="stream-worker",
            state_attr="stream_task",
            close=resolved_service.close,
        ),
    )


app = create_app()
