"""Signal routes."""

from __future__ import annotations

from fastapi import APIRouter, Depends, Query

from services.api.dependencies import get_api_service
from services.api.service import APIService

router = APIRouter(tags=["signals"])


@router.get("/signals")
async def list_signals(
    limit: int = Query(20, ge=1, le=100),
    service: APIService = Depends(get_api_service),
) -> dict[str, object]:
    signals = await service.signals(limit=limit)
    return {"signals": signals, "count": len(signals)}
