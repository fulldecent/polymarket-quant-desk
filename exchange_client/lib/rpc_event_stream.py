"""RPC WebSocket implementation of the settled trade stream.

Connects to a Polygon JSON-RPC WebSocket endpoint and subscribes to
OrderFilled / OrdersMatched logs. v1 and v2 exchanges use different
addresses and topic0 hashes, so they are two separate connections — do
not mix v2 addresses into the v1 filter.
"""

from __future__ import annotations

import asyncio
import json
from collections.abc import Callable
from typing import AsyncIterator

import websockets

from .event_stream import (
    V1_ADDRESSES,
    V1_TOPICS,
    V2_ADDRESSES,
    V2_TOPICS,
    TradeEvent,
    decode_event,
)

StatusFn = Callable[[str], None]


class RpcSettledEventStream:
    """Stream Polymarket trade events from a Polygon RPC WebSocket."""

    def __init__(
        self,
        ws_url: str,
        *,
        on_status: StatusFn | None = None,
    ) -> None:
        self._ws_url = ws_url
        self._on_status = on_status
        self._connected = False
        self._ready = asyncio.Event()
        self._queue: asyncio.Queue[TradeEvent] = asyncio.Queue()
        self._tasks: list[asyncio.Task] = []
        self._last_error: str | None = None

    def _status(self, message: str) -> None:
        if self._on_status is not None:
            self._on_status(message)

    async def connect(self) -> None:
        if self._tasks:
            return
        self._ready.clear()
        self._tasks = [
            asyncio.create_task(
                self._listen_loop(V1_ADDRESSES, V1_TOPICS, "v1"),
                name="rpc-listen-v1",
            ),
            asyncio.create_task(
                self._listen_loop(V2_ADDRESSES, V2_TOPICS, "v2"),
                name="rpc-listen-v2",
            ),
        ]
        try:
            await asyncio.wait_for(self._ready.wait(), timeout=20)
        except TimeoutError as exc:
            detail = self._last_error or "subscribe did not complete"
            raise ConnectionError(f"rpc listen failed: {detail}") from exc

    async def disconnect(self) -> None:
        for task in self._tasks:
            task.cancel()
        for task in self._tasks:
            try:
                await task
            except asyncio.CancelledError:
                pass
        self._tasks = []
        self._connected = False
        self._ready.clear()

    def __aiter__(self) -> AsyncIterator[TradeEvent]:
        return self

    async def __anext__(self) -> TradeEvent:
        if not self._tasks:
            await self.connect()
        return await self._queue.get()

    async def _listen_loop(
        self,
        addresses: list[str],
        topics: list[str],
        label: str,
    ) -> None:
        reconnect_delay = 1.0
        while True:
            try:
                async with websockets.connect(
                    self._ws_url,
                    ping_interval=30,
                    ping_timeout=10,
                    close_timeout=2,
                ) as ws:
                    await _subscribe(ws, 1, addresses, topics)
                    self._connected = True
                    reconnect_delay = 1.0
                    self._ready.set()
                    self._status(f"connected listen=rpc {label}")
                    async for message in ws:
                        try:
                            data = json.loads(message)
                        except json.JSONDecodeError:
                            continue
                        if data.get("method") != "eth_subscription":
                            continue
                        log = data.get("params", {}).get("result", {})
                        event = decode_event(log)
                        if event is not None:
                            await self._queue.put(event)

            except asyncio.CancelledError:
                raise

            except Exception as exc:
                self._connected = False
                self._last_error = f"{type(exc).__name__}: {exc}"
                self._status(f"listen error={self._last_error}  retry={reconnect_delay:.0f}s {label}")
                await asyncio.sleep(reconnect_delay)
                reconnect_delay = min(reconnect_delay * 2, 60)


async def _subscribe(ws, req_id: int, addresses: list[str], topics: list[str]) -> None:
    await ws.send(
        json.dumps(
            {
                "jsonrpc": "2.0",
                "id": req_id,
                "method": "eth_subscribe",
                "params": [
                    "logs",
                    {"address": addresses, "topics": [topics]},
                ],
            }
        )
    )
    while True:
        response = await ws.recv()
        data = json.loads(response)
        if data.get("id") != req_id:
            continue
        if "error" in data:
            raise ConnectionError(f"eth_subscribe error: {data['error']}")
        return


# Backward-compatible alias while callers migrate.
RpcEventStream = RpcSettledEventStream
