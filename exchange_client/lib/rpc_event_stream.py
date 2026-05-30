"""RPC WebSocket implementation of TradeEventStream.

Connects to a Polygon JSON-RPC WebSocket endpoint, subscribes to
OrderFilled and OrdersMatched logs from the Polymarket exchange
contracts, and yields decoded TradeEvent objects.

Usage:
    from exchange_client.lib.event_stream import TradeEvent
    from exchange_client.lib.rpc_event_stream import RpcSettledEventStream

    stream = RpcSettledEventStream(ws_url="wss://polygon-mainnet.infura.io/ws/v3/KEY")
    async for event in stream:
        print(event.maker, event.maker_asset_id)
"""

from __future__ import annotations

import asyncio
import json
from typing import AsyncIterator

import websockets

from .event_stream import (
    ALL_ADDRESSES,
    WANTED_TOPICS,
    TradeEvent,
    decode_event,
)


class RpcSettledEventStream:
    """Stream Polymarket trade events from a Polygon RPC WebSocket.

    Handles connection, eth_subscribe, automatic reconnection with
    exponential backoff, and decoding of raw EVM log data into
    TradeEvent objects.
    """

    def __init__(self, ws_url: str) -> None:
        self._ws_url = ws_url
        self._ws = None
        self._connected = False
        self._queue: asyncio.Queue[TradeEvent] = asyncio.Queue()
        self._listen_task: asyncio.Task | None = None

    async def connect(self) -> None:
        if self._listen_task is None or self._listen_task.done():
            self._listen_task = asyncio.create_task(self._listen_loop())

    async def disconnect(self) -> None:
        if self._listen_task is not None:
            self._listen_task.cancel()
            try:
                await self._listen_task
            except asyncio.CancelledError:
                pass
            self._listen_task = None
        if self._ws is not None:
            await self._ws.close()
            self._ws = None
        self._connected = False

    def __aiter__(self) -> AsyncIterator[TradeEvent]:
        return self

    async def __anext__(self) -> TradeEvent:
        if self._listen_task is None:
            await self.connect()
        return await self._queue.get()

    async def _listen_loop(self) -> None:
        reconnect_delay = 1.0

        while True:
            try:
                async with websockets.connect(
                    self._ws_url,
                    ping_interval=30,
                    ping_timeout=10,
                    close_timeout=2,
                ) as ws:
                    self._ws = ws
                    self._connected = True
                    reconnect_delay = 1.0

                    subscribe_msg = {
                        "jsonrpc": "2.0",
                        "id": 1,
                        "method": "eth_subscribe",
                        "params": [
                            "logs",
                            {
                                "address": ALL_ADDRESSES,
                                "topics": [WANTED_TOPICS],
                            },
                        ],
                    }
                    await ws.send(json.dumps(subscribe_msg))
                    response = await ws.recv()
                    data = json.loads(response)

                    if "error" in data:
                        raise ConnectionError(
                            f"eth_subscribe error: {data['error']}")

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

            except Exception:
                self._connected = False
                await asyncio.sleep(reconnect_delay)
                reconnect_delay = min(reconnect_delay * 2, 60)


# Backward-compatible alias while callers migrate.
RpcEventStream = RpcSettledEventStream
