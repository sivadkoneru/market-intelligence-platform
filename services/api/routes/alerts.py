"""Alert routes."""

from __future__ import annotations

from fastapi import APIRouter, Depends, Query

from services.api.dependencies import get_api_service
from services.api.service import APIService

router = APIRouter(tags=["alerts"])


@router.get("/alerts")
async def list_alerts(
    limit: int = Query(20, ge=1, le=100),
    service: APIService = Depends(get_api_service),
) -> dict[str, object]:
    alerts = await service.alerts(limit=limit)
    return {"alerts": alerts, "count": len(alerts)}
