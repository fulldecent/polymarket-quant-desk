"""Polynode low-latency trade stream implementation.

Subscribes to Polynode's low-latency trades feed and emits TradeEvent
objects from both initial snapshots and live messages.
"""

from __future__ import annotations

import asyncio
import json
from typing import AsyncIterator

import websockets

from .event_stream import TradeEvent


class PolynodeMempoolEventStream:
    """Stream low-latency trade fills from Polynode WebSocket."""

    def __init__(
        self,
        api_key: str,
        ws_url: str = "wss://ws.polynode.dev/ws",
    ) -> None:
        self._api_key = api_key
        self._ws_url = ws_url
        self._ws = None
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

    def __aiter__(self) -> AsyncIterator[TradeEvent]:
        return self

    async def __anext__(self) -> TradeEvent:
        if self._listen_task is None:
            await self.connect()
        return await self._queue.get()

    async def _listen_loop(self) -> None:
        reconnect_delay = 1.0
        ws_url_with_key = f"{self._ws_url}?key={self._api_key}"

        while True:
            try:
                async with websockets.connect(
                    ws_url_with_key,
                    ping_interval=30,
                    ping_timeout=10,
                    close_timeout=2,
                ) as ws:
                    self._ws = ws
                    reconnect_delay = 1.0

                    subscribe_msg = {
                        "action": "subscribe",
                        "type": "trades",
                    }
                    await ws.send(json.dumps(subscribe_msg))

                    async for raw in ws:
                        try:
                            msg = json.loads(raw)
                        except json.JSONDecodeError:
                            continue

                        events = self._extract_trade_messages(msg)
                        for trade_msg in events:
                            event = self._to_trade_event(trade_msg.get("data") or {})
                            if event is not None:
                                await self._queue.put(event)

            except asyncio.CancelledError:
                raise

            except Exception:
                await asyncio.sleep(reconnect_delay)
                reconnect_delay = min(reconnect_delay * 2, 60)

    def _extract_trade_messages(self, msg: dict) -> list[dict]:
        out: list[dict] = []

        # Live message shape: {"type":"trade","data":{...}}
        if msg.get("type") == "trade" and isinstance(msg.get("data"), dict):
            out.append(msg)
            return out

        # Snapshot shape: {"count":N,"events":[{"type":"trade","data":...}, ...]}
        events = msg.get("events")
        if isinstance(events, list):
            for item in events:
                if not isinstance(item, dict):
                    continue
                if item.get("type") != "trade":
                    continue
                if not isinstance(item.get("data"), dict):
                    continue
                out.append(item)

        return out

    def _to_trade_event(self, data: dict) -> TradeEvent | None:
        block_number = data.get("block_number")
        log_index = data.get("log_index")
        side = str(data.get("side", "")).upper()
        token_id = str(data.get("token_id", "0"))

        if side == "BUY":
            maker_asset_id = "0"
            taker_asset_id = token_id
        elif side == "SELL":
            maker_asset_id = token_id
            taker_asset_id = "0"
        else:
            return None

        return TradeEvent(
            block_number=int(block_number) if block_number is not None else None,
            tx_hash=str(data.get("tx_hash", "")),
            log_index=int(log_index) if log_index is not None else None,
            contract_address=str(data.get("exchange", "")).lower(),
            event_type="OrderFilled",
            maker=str(data.get("maker", "")).lower(),
            taker=str(data.get("taker", "")).lower(),
            maker_asset_id=maker_asset_id,
            taker_asset_id=taker_asset_id,
            maker_amount=str(data.get("maker_amount", "0")),
            taker_amount=str(data.get("taker_amount", "0")),
            fee=str(data.get("fee", "0")),
        )


# Backward-compatible alias while callers migrate.
PolynodeEventStream = PolynodeMempoolEventStream
