"""Polynode trade stream implementations.

Polynode's ``settlements`` channel carries both pre-confirmation and
confirmed updates:

- PolynodeMempoolEventStream: pending trades observed before inclusion.
- PolynodeSettledEventStream: confirmed trades with a block number.
"""

from __future__ import annotations

import asyncio
import json
import zlib
from decimal import Decimal, InvalidOperation
from collections.abc import Callable
from typing import AsyncIterator

import websockets

StatusFn = Callable[[str], None]

from .event_stream import MempoolTradeEvent, TradeEvent, v2_maker_taker_assets


def _with_api_key(ws_url: str, api_key: str) -> str:
    sep = "&" if "?" in ws_url else "?"
    out = f"{ws_url}{sep}key={api_key}"
    if "compress=" not in out:
        out += "&compress=zlib"
    return out


def _decode_message(raw: str | bytes) -> dict | None:
    try:
        if isinstance(raw, bytes):
            text = zlib.decompress(raw, -zlib.MAX_WBITS).decode()
        else:
            text = raw
        msg = json.loads(text)
    except (zlib.error, UnicodeDecodeError, json.JSONDecodeError):
        return None
    return msg if isinstance(msg, dict) else None


def _extract_trade_messages(msg: dict) -> list[tuple[str, dict]]:
    out: list[tuple[str, dict]] = []

    msg_type = str(msg.get("type", ""))
    if msg_type in ("settlement", "status_update") and isinstance(msg.get("data"), dict):
        out.append((msg_type, msg["data"]))
        return out

    if msg_type != "snapshot":
        return out

    events = msg.get("events")
    if not isinstance(events, list):
        return out

    for item in events:
        if not isinstance(item, dict):
            continue
        item_type = str(item.get("type", ""))
        data = item.get("data")
        if item_type not in ("settlement", "status_update") or not isinstance(data, dict):
            continue
        out.append((item_type, data))

    return out


def _str_from_keys(data: dict, *keys: str, default: str = "") -> str:
    for key in keys:
        val = data.get(key)
        if val is None:
            continue
        s = str(val)
        if s:
            return s
    return default


def _numeric_str_from_keys(data: dict, *keys: str, default: str = "0") -> str:
    first_parseable: str | None = None
    for key in keys:
        val = data.get(key)
        if val in (None, ""):
            continue
        s = str(val)
        try:
            parsed = Decimal(s)
        except (InvalidOperation, ValueError):
            continue
        if first_parseable is None:
            first_parseable = s
        if parsed != 0:
            return s
    return first_parseable if first_parseable is not None else default


def _int_from_keys(data: dict, *keys: str) -> int | None:
    for key in keys:
        val = data.get(key)
        if val in (None, ""):
            continue
        try:
            return int(str(val), 10)
        except ValueError:
            continue
    return None


def _is_pending(msg_type: str, data: dict) -> bool:
    status = str(data.get("status", "")).lower()
    return status == "pending" or (msg_type == "settlement" and not status)


def _is_confirmed(msg_type: str, data: dict) -> bool:
    if msg_type == "status_update":
        return True
    return str(data.get("status", "")).lower() in {
        "confirmed",
        "settled",
        "completed",
        "mined",
    }


def _normalize_side(raw: str) -> str:
    s = raw.strip().upper()
    if s in {"0", "BUY"}:
        return "BUY"
    if s in {"1", "SELL"}:
        return "SELL"
    return s


def _build_trade_fields(data: dict) -> dict | None:
    tx_hash = _str_from_keys(data, "tx_hash")
    if not tx_hash:
        return None

    confirmed_fills = data.get("confirmed_fills") if isinstance(data.get("confirmed_fills"), list) else []

    representative_fill: dict | None = None
    top_level_token_id = _str_from_keys(data, "token_id", "taker_token", "maker_token", default="0")
    if confirmed_fills:
        for fill in confirmed_fills:
            if not isinstance(fill, dict):
                continue
            if _str_from_keys(fill, "token_id", default="") == top_level_token_id:
                representative_fill = fill
                break
        if representative_fill is None:
            for fill in confirmed_fills:
                if isinstance(fill, dict):
                    representative_fill = fill
                    break

    side = _normalize_side(_str_from_keys(data, "taker_side", "side"))
    if not side and representative_fill is not None:
        side = _normalize_side(_str_from_keys(representative_fill, "side"))

    token_id = _str_from_keys(data, "taker_token", "maker_token", "token_id", default="0")
    if token_id == "0" and representative_fill is not None:
        token_id = _str_from_keys(representative_fill, "token_id", default="0")

    outcome = _str_from_keys(data, "outcome").strip().lower()
    tokens_map = data.get("tokens")
    if token_id == "0" and isinstance(tokens_map, dict) and outcome:
        for candidate_token_id, candidate_outcome in tokens_map.items():
            if str(candidate_outcome).strip().lower() == outcome:
                token_id = str(candidate_token_id)
                break

    token_amount = _numeric_str_from_keys(
        data,
        "taker_size",
        "size",
        "token_size",
        "matched_size",
        default=_numeric_str_from_keys(representative_fill or {}, "size", default="0"),
    )

    taker_price = _numeric_str_from_keys(
        data,
        "taker_price",
        "price",
        default=_numeric_str_from_keys(representative_fill or {}, "price", default="0"),
    )
    try:
        derived_usdc = str(Decimal(token_amount) * Decimal(taker_price))
    except (InvalidOperation, ValueError):
        derived_usdc = "0"
    usdc_amount = _numeric_str_from_keys(
        data,
        "trade_value_usd",
        "usdc_size",
        "quote_size",
        "amount",
        default=derived_usdc,
    )

    if side == "BUY":
        maker_asset_id, taker_asset_id = v2_maker_taker_assets(0, token_id)
        maker_amount = usdc_amount
        taker_amount = token_amount
    elif side == "SELL":
        maker_asset_id, taker_asset_id = v2_maker_taker_assets(1, token_id)
        maker_amount = token_amount
        taker_amount = usdc_amount
    else:
        maker_asset_id = _str_from_keys(data, "maker_asset_id", default="0")
        taker_asset_id = _str_from_keys(data, "taker_asset_id", default=token_id or "0")
        maker_amount = _numeric_str_from_keys(
            data,
            "maker_amount",
            "making_amount",
            "maker_size",
            default=_numeric_str_from_keys(representative_fill or {}, "maker_amount", default="0"),
        )
        taker_amount = _numeric_str_from_keys(
            data,
            "taker_amount",
            "taking_amount",
            "taker_size",
            default=_numeric_str_from_keys(representative_fill or {}, "taker_amount", default="0"),
        )

    return {
        "tx_hash": tx_hash,
        "contract_address": _str_from_keys(
            data,
            "contract_address",
            "contractAddress",
            "exchange_address",
            default="",
        ).lower(),
        "event_type": _str_from_keys(data, "event_type", default="Settlement"),
        "maker": _str_from_keys(
            data,
            "maker_wallet",
            "maker",
            default=_str_from_keys(representative_fill or {}, "maker", default=""),
        ).lower(),
        "taker": _str_from_keys(
            data,
            "taker_wallet",
            "taker",
            default=_str_from_keys(representative_fill or {}, "taker", default=""),
        ).lower(),
        "maker_asset_id": maker_asset_id,
        "taker_asset_id": taker_asset_id,
        "maker_amount": maker_amount,
        "taker_amount": taker_amount,
        "fee": _str_from_keys(data, "fee", default="0"),
        "condition_id": _str_from_keys(data, "condition_id", "conditionId", default=""),
        "market_title": _str_from_keys(data, "market_title", "event_title", default=""),
        "token_ids_csv": ",".join(
            str(x) for x in (data.get("token_ids") or []) if x is not None
        ),
        "tokens_json": json.dumps(data.get("tokens") or {}, separators=(",", ":")),
        "outcome": _str_from_keys(data, "outcome", default=""),
        "tick_size": _str_from_keys(data, "tick_size", default=""),
        "neg_risk": bool(data.get("neg_risk", False)),
    }


class PolynodeMempoolEventStream:
    """Stream low-latency pre-confirmation trade fills from Polynode."""

    def __init__(
        self,
        api_key: str,
        ws_url: str = "wss://ws.polynode.dev/ws",
        *,
        on_status: StatusFn | None = None,
    ) -> None:
        self._api_key = api_key
        self._ws_url = ws_url
        self._on_status = on_status
        self._ws = None
        self._queue: asyncio.Queue[MempoolTradeEvent] = asyncio.Queue()
        self._listen_task: asyncio.Task | None = None
        self._ready = asyncio.Event()

    def _status(self, message: str) -> None:
        if self._on_status is not None:
            self._on_status(message)

    async def connect(self) -> None:
        if self._listen_task is None or self._listen_task.done():
            self._ready.clear()
            self._listen_task = asyncio.create_task(self._listen_loop())
        try:
            await asyncio.wait_for(self._ready.wait(), timeout=20)
        except TimeoutError as exc:
            raise ConnectionError(
                "polynode mempool listen failed: subscribe did not complete"
            ) from exc

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

    def __aiter__(self) -> AsyncIterator[MempoolTradeEvent]:
        return self

    async def __anext__(self) -> MempoolTradeEvent:
        if self._listen_task is None:
            await self.connect()
        return await self._queue.get()

    async def _listen_loop(self) -> None:
        reconnect_delay = 1.0
        ws_url_with_key = _with_api_key(self._ws_url, self._api_key)

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
                    await ws.send(json.dumps({"action": "subscribe", "type": "settlements"}))
                    self._ready.set()
                    self._status("connected listen=polynode mempool")
                    async for raw in ws:
                        msg = _decode_message(raw)
                        if msg is None:
                            continue
                        for msg_type, data in _extract_trade_messages(msg):
                            event = self._to_trade_event(msg_type, data)
                            if event is not None:
                                await self._queue.put(event)

            except asyncio.CancelledError:
                raise

            except Exception as exc:
                self._status(
                    f"listen error: {type(exc).__name__}: {exc}  retry: {reconnect_delay:.0f}s  mempool"
                )
                await asyncio.sleep(reconnect_delay)
                reconnect_delay = min(reconnect_delay * 2, 60)

    def _to_trade_event(self, msg_type: str, data: dict) -> MempoolTradeEvent | None:
        if not _is_pending(msg_type, data):
            return None
        trade_fields = _build_trade_fields(data)
        if trade_fields is None:
            return None
        return MempoolTradeEvent(**trade_fields)


class PolynodeSettledEventStream:
    """Stream confirmed trade fills from Polynode's settlements channel."""

    def __init__(
        self,
        api_key: str,
        ws_url: str = "wss://ws.polynode.dev/ws",
        *,
        on_status: StatusFn | None = None,
    ) -> None:
        self._api_key = api_key
        self._ws_url = ws_url
        self._on_status = on_status
        self._ws = None
        self._queue: asyncio.Queue[TradeEvent] = asyncio.Queue()
        self._listen_task: asyncio.Task | None = None
        self._ready = asyncio.Event()

    def _status(self, message: str) -> None:
        if self._on_status is not None:
            self._on_status(message)

    async def connect(self) -> None:
        if self._listen_task is None or self._listen_task.done():
            self._ready.clear()
            self._listen_task = asyncio.create_task(self._listen_loop())
        try:
            await asyncio.wait_for(self._ready.wait(), timeout=20)
        except TimeoutError as exc:
            raise ConnectionError(
                "polynode settled listen failed: subscribe did not complete"
            ) from exc

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
        ws_url_with_key = _with_api_key(self._ws_url, self._api_key)

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
                    await ws.send(json.dumps({"action": "subscribe", "type": "settlements"}))
                    self._ready.set()
                    self._status("connected listen=polynode settled")
                    async for raw in ws:
                        msg = _decode_message(raw)
                        if msg is None:
                            continue
                        for msg_type, data in _extract_trade_messages(msg):
                            event = self._to_trade_event(msg_type, data)
                            if event is not None:
                                await self._queue.put(event)

            except asyncio.CancelledError:
                raise

            except Exception as exc:
                self._status(
                    f"listen error: {type(exc).__name__}: {exc}  retry: {reconnect_delay:.0f}s  settled"
                )
                await asyncio.sleep(reconnect_delay)
                reconnect_delay = min(reconnect_delay * 2, 60)

    def _to_trade_event(self, msg_type: str, data: dict) -> TradeEvent | None:
        if not _is_confirmed(msg_type, data):
            return None
        block_number = _int_from_keys(data, "block_number", "blockNumber", "block")
        if block_number is None:
            return None
        trade_fields = _build_trade_fields(data)
        if trade_fields is None:
            return None
        return TradeEvent(
            **trade_fields,
            block_number=block_number,
            log_index=_int_from_keys(data, "log_index", "logIndex"),
        )


# Backward-compatible alias while callers migrate.
PolynodeEventStream = PolynodeMempoolEventStream
