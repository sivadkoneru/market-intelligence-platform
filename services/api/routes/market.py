"""Market and symbol routes."""

from __future__ import annotations

from datetime import datetime

from fastapi import APIRouter, Depends, HTTPException, Query

from services.api.dependencies import SymbolPath, get_api_service, normalize_symbol
from services.api.service import APIService

router = APIRouter(tags=["market"])

MAX_HISTORY_ROWS = 10_000


@router.get("/symbols")
async def list_symbols(
    service: APIService = Depends(get_api_service),
) -> dict[str, object]:
    symbols = await service.list_symbols()
    return {"symbols": symbols, "count": len(symbols)}


@router.get("/market/{symbol}/latest")
async def get_latest_market(
    symbol: SymbolPath,
    service: APIService = Depends(get_api_service),
) -> dict[str, object]:
    resolved = normalize_symbol(symbol)
    payload = await service.latest_market(resolved)
    if payload is None:
        raise HTTPException(status_code=404, detail=f"No market data found for {resolved}")
    return payload


@router.get("/market/{symbol}/history")
async def get_market_history(
    symbol: SymbolPath,
    frm: datetime = Query(..., alias="from"),
    to: datetime = Query(...),
    limit: int = Query(1_000, ge=1, le=MAX_HISTORY_ROWS),
    service: APIService = Depends(get_api_service),
) -> dict[str, object]:
    if frm > to:
        raise HTTPException(status_code=400, detail="'from' must be before or equal to 'to'")
    resolved = normalize_symbol(symbol)
    rows = await service.market_history(resolved, frm=frm, to=to, limit=limit)
    return {
        "symbol": resolved,
        "from": frm.isoformat(),
        "to": to.isoformat(),
        "rows": rows,
        "count": len(rows),
    }


@router.get("/indicators/{symbol}")
async def get_indicators(
    symbol: SymbolPath,
    service: APIService = Depends(get_api_service),
) -> dict[str, object]:
    resolved = normalize_symbol(symbol)
    payload = await service.indicators(resolved)
    if payload is None:
        raise HTTPException(status_code=404, detail=f"No indicators found for {resolved}")
    return payload
