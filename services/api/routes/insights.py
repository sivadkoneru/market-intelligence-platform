"""Insight routes."""

from __future__ import annotations

from fastapi import APIRouter, Depends, HTTPException

from services.api.dependencies import get_api_service
from services.api.service import APIService

router = APIRouter(tags=["insights"])


@router.get("/insights/{symbol}")
async def get_insight(
    symbol: str,
    service: APIService = Depends(get_api_service),
) -> dict[str, object]:
    payload = await service.insight(symbol)
    if payload is None:
        raise HTTPException(status_code=404, detail=f"No insight found for {symbol}")
    return payload
