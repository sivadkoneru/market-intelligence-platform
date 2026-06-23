"""WebSocket route for live symbol-scoped market intelligence streaming."""

from __future__ import annotations

import asyncio
from typing import Any
from uuid import uuid4

from fastapi import APIRouter, Depends, WebSocket, WebSocketDisconnect
from pydantic import BaseModel, Field, ValidationError

from services.api.dependencies import get_api_service
from services.api.service import APIService

router = APIRouter(tags=["stream"])


MAX_SUBSCRIBED_SYMBOLS = 50


class SubscribeRequest(BaseModel):
    action: str
    # Bounded: the set is retained per connection and checked on every fanout,
    # so an unbounded list is free memory and CPU for any client that asks.
    symbols: list[str] = Field(max_length=MAX_SUBSCRIBED_SYMBOLS)


async def _receive_commands(
    websocket: WebSocket,
    service: APIService,
    connection_id: str,
    outbound: asyncio.Queue[dict[str, object]],
) -> None:
    while True:
        # receive_json() itself raises on a non-JSON text frame (JSONDecodeError)
        # and on a binary frame (KeyError), so it has to sit inside the guard —
        # otherwise a one-byte frame kills the connection with a traceback
        # instead of the error frame this loop is built to send.
        try:
            payload = await websocket.receive_json()
            request = SubscribeRequest.model_validate(payload)
            if request.action != "subscribe":
                raise ValueError("action must be 'subscribe'")
            symbols = service.subscribe_stream(connection_id, request.symbols)
            if not symbols:
                raise ValueError("symbols must contain at least one non-empty symbol")
        except (ValidationError, ValueError, KeyError, TypeError) as exc:
            await outbound.put({"type": "error", "detail": str(exc)})
            continue

        await outbound.put({"type": "subscribed", "symbols": symbols})


@router.websocket("/ws/stream")
async def websocket_stream(
    websocket: WebSocket,
    service: APIService = Depends(get_api_service),
) -> None:
    await websocket.accept()
    connection_id = str(uuid4())
    outbound = service.register_stream(connection_id)
    command_task = asyncio.create_task(
        _receive_commands(websocket, service, connection_id, outbound),
        name=f"ws-stream-{connection_id}",
    )
    outbound_task: asyncio.Task[dict[str, object]] | None = None
    try:
        while True:
            outbound_task = asyncio.create_task(outbound.get())
            done, pending = await asyncio.wait(
                {command_task, outbound_task},
                return_when=asyncio.FIRST_COMPLETED,
            )

            if command_task in done:
                if outbound_task in pending:
                    outbound_task.cancel()
                    await asyncio.gather(outbound_task, return_exceptions=True)
                exception = command_task.exception()
                if exception is None or isinstance(exception, WebSocketDisconnect):
                    break
                raise exception

            if command_task in pending:
                pending.remove(command_task)
            for task in pending:
                task.cancel()
            if pending:
                await asyncio.gather(*pending, return_exceptions=True)

            message = outbound_task.result()
            await websocket.send_json(message)
    except (WebSocketDisconnect, RuntimeError):
        pass
    finally:
        service.unregister_stream(connection_id)
        command_task.cancel()
        tasks: list[asyncio.Task[Any]] = [command_task]
        if outbound_task is not None and not outbound_task.done():
            outbound_task.cancel()
            tasks.append(outbound_task)
        await asyncio.gather(*tasks, return_exceptions=True)
