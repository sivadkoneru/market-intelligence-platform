"""FastAPI app for the alerting service."""

from __future__ import annotations

from fastapi import FastAPI

from libs.common import get_cache, get_message_bus
from libs.common.service_app import (
    bootstrap_service_logging,
    create_service_app,
    worker_lifespan,
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
    bootstrap_service_logging("alerting")
    resolved_service = service or build_default_service()

    return create_service_app(
        service_name="alerting",
        title="Market Intelligence Alerting Service",
        summary="Portfolio service for offline-safe alert evaluation.",
        service=resolved_service,
        state_attr="alerting_service",
        render_metrics=resolved_service.metrics.render,
        lifespan=worker_lifespan(
            resolved_service.run_forever if run_on_startup else None,
            task_name="alerting-worker",
            state_attr="alerting_task",
            close=resolved_service.close,
        ),
    )


app = create_app()
