"""FastAPI dependency helpers for the API service."""

from __future__ import annotations

from fastapi import Request

from services.api.service import APIService


def get_api_service(request: Request) -> APIService:
    """Return the API service stored on the FastAPI app state."""
    return request.app.state.api_service
