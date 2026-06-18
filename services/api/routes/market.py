"""Market and symbol routes."""

from __future__ import annotations

from datetime import datetime

from fastapi import APIRouter, Depends, HTTPException, Query

from services.api.dependencies import get_api_service
from services.api.service import APIService

router = APIRouter(tags=["market"])


@router.get("/symbols")
async def list_symbols(
    service: APIService = Depends(get_api_service),
) -> dict[str, object]:
    symbols = await service.list_symbols()
    return {"symbols": symbols, "count": len(symbols)}


@router.get("/market/{symbol}/latest")
async def get_latest_market(
    symbol: str,
    service: APIService = Depends(get_api_service),
) -> dict[str, object]:
    payload = await service.latest_market(symbol)
    if payload is None:
        raise HTTPException(status_code=404, detail=f"No market data found for {symbol}")
    return payload


@router.get("/market/{symbol}/history")
async def get_market_history(
    symbol: str,
    frm: datetime = Query(..., alias="from"),
    to: datetime = Query(...),
    service: APIService = Depends(get_api_service),
) -> dict[str, object]:
    if frm > to:
        raise HTTPException(status_code=400, detail="'from' must be before or equal to 'to'")
    rows = await service.market_history(symbol, frm=frm, to=to)
    return {"symbol": symbol, "from": frm.isoformat(), "to": to.isoformat(), "rows": rows}


@router.get("/indicators/{symbol}")
async def get_indicators(
    symbol: str,
    service: APIService = Depends(get_api_service),
) -> dict[str, object]:
    payload = await service.indicators(symbol)
    if payload is None:
        raise HTTPException(status_code=404, detail=f"No indicators found for {symbol}")
    return payload
