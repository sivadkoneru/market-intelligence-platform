"""FastAPI dependency helpers for the API service."""

from __future__ import annotations

from typing import Annotated

from fastapi import Path
from starlette.requests import HTTPConnection

from services.api.service import APIService

SYMBOL_MAX_LENGTH = 32
SYMBOL_PATTERN = r"^[A-Za-z0-9._-]+$"

# One definition of what a symbol is, shared by every route that takes one.
#
# Symbols are unvalidated path input that ends up in Redis keys, Druid SQL
# literals, and 404 bodies, so they are bounded and charset-restricted here.
# They are also case-normalised: producers write ``snapshot:BTCUSDT`` and the
# websocket path uppercases, so leaving REST case-sensitive made
# ``/market/btcusdt/latest`` 404 on data the websocket happily streamed.
SymbolPath = Annotated[
    str,
    Path(
        min_length=1,
        max_length=SYMBOL_MAX_LENGTH,
        pattern=SYMBOL_PATTERN,
        description="Ticker symbol, case-insensitive (e.g. BTCUSDT).",
    ),
]


def normalize_symbol(symbol: str) -> str:
    """Return the canonical form used as the identity of a symbol platform-wide."""
    return symbol.strip().upper()


def get_api_service(request: HTTPConnection) -> APIService:
    """Return the API service stored on the FastAPI app state."""
    return request.app.state.api_service
