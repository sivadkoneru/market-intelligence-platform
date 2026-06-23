"""Insight routes."""

from __future__ import annotations

from fastapi import APIRouter, Depends, HTTPException

from services.api.dependencies import SymbolPath, get_api_service, normalize_symbol
from services.api.service import APIService

router = APIRouter(tags=["insights"])


@router.get("/insights/{symbol}")
async def get_insight(
    symbol: SymbolPath,
    service: APIService = Depends(get_api_service),
) -> dict[str, object]:
    resolved = normalize_symbol(symbol)
    payload = await service.insight(resolved)
    if payload is None:
        raise HTTPException(status_code=404, detail=f"No insight found for {resolved}")
    return payload
