"""FastAPI app for the API service."""

from __future__ import annotations

from collections.abc import AsyncIterator
from contextlib import asynccontextmanager

from fastapi import FastAPI

from libs.common import get_cache, get_message_bus, get_timeseries_store
from libs.common.service_app import bootstrap_service_logging, create_service_app
from services.api.routes.alerts import router as alerts_router
from services.api.routes.insights import router as insights_router
from services.api.routes.market import router as market_router
from services.api.routes.signals import router as signals_router
from services.api.service import APIService
from services.api.ws import router as ws_router

ROUTES = (
    "/health",
    "/metrics",
    "/symbols",
    "/market/{symbol}/latest",
    "/market/{symbol}/history",
    "/indicators/{symbol}",
    "/signals",
    "/alerts",
    "/insights/{symbol}",
    "/ws/stream",
)


def build_default_service() -> APIService:
    """Build the offline-safe default API service used by Uvicorn."""
    return APIService(
        store=get_timeseries_store(),
        cache=get_cache(),
        bus=get_message_bus(),
    )


def create_app(service: APIService | None = None) -> FastAPI:
    bootstrap_service_logging("api")
    resolved_service = service or build_default_service()

    @asynccontextmanager
    async def lifespan(app: FastAPI) -> AsyncIterator[None]:
        # Priming has to be inside the guard: it opens receivers, so a failure
        # there would otherwise strand every client build_default_service()
        # constructed (Redis, Service Bus, Druid) on each restart.
        try:
            await resolved_service.prime_subscriptions()
            yield
        finally:
            await resolved_service.close()

    app = create_service_app(
        service_name="api",
        title="Market Intelligence API Service",
        summary="Portfolio service for offline-safe market data APIs.",
        service=resolved_service,
        state_attr="api_service",
        render_metrics=resolved_service.render_metrics,
        lifespan=lifespan,
        routes=ROUTES,
    )

    app.include_router(market_router)
    app.include_router(signals_router)
    app.include_router(alerts_router)
    app.include_router(insights_router)
    app.include_router(ws_router)

    return app


app = create_app()
