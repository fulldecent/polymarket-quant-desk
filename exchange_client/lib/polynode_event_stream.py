"""Polynode pre-confirmation settlement stream.

Subscribes to Polynode's ``settlements`` channel, which emits a
``settlement`` event the moment a matchOrders tx is decoded from the
mempool (pre-confirmation) and again when the block is confirmed.

Only the pending-status event is yielded, so ``polynode_seen[tx]``
captures the mempool detection timestamp -- the correct reference for a
lead-time comparison against an on-chain confirmation feed.
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
                        "type": "settlements",
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

        # Live message shape: {"type":"settlement","data":{...}}
        if msg.get("type") == "settlement" and isinstance(msg.get("data"), dict):
            out.append(msg)
            return out

        # Snapshot shape: {"count":N,"events":[{"type":"settlement","data":...}, ...]}
        events = msg.get("events")
        if isinstance(events, list):
            for item in events:
                if not isinstance(item, dict):
                    continue
                if item.get("type") != "settlement":
                    continue
                if not isinstance(item.get("data"), dict):
                    continue
                out.append(item)

        return out

    def _to_trade_event(self, data: dict) -> TradeEvent | None:
        tx_hash = str(data.get("tx_hash", ""))
        if not tx_hash:
            return None

        # Only emit on the pending detection so polynode_seen captures the
        # pre-confirmation timestamp. The confirmed status_update is ignored.
        if str(data.get("status", "")).lower() != "pending":
            return None

        side = str(data.get("taker_side", "")).upper()
        token_id = str(data.get("taker_token", "0"))

        if side == "BUY":
            maker_asset_id = "0"
            taker_asset_id = token_id
        elif side == "SELL":
            maker_asset_id = token_id
            taker_asset_id = "0"
        else:
            maker_asset_id = "0"
            taker_asset_id = token_id

        return TradeEvent(
            block_number=None,
            tx_hash=tx_hash,
            log_index=None,
            contract_address="",
            event_type="Settlement",
            maker="",
            taker=str(data.get("taker_wallet", "")).lower(),
            maker_asset_id=maker_asset_id,
            taker_asset_id=taker_asset_id,
            maker_amount="0",
            taker_amount=str(data.get("taker_size", "0")),
            fee="0",
        )


# Backward-compatible alias while callers migrate.
PolynodeEventStream = PolynodeMempoolEventStream
