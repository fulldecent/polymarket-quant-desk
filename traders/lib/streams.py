"""Build PUBLIC exec clients and listen/mempool streams from CLI flags."""

from __future__ import annotations

import os

from exchange_client.lib import trading_lib
from exchange_client.lib.event_stream import (
    MempoolTradeEvent,
    SettledTradeStream,
    TradeEvent,
    TradeMempoolStream,
    ws_url_from_env,
)
from exchange_client.lib.execution_client import (
    ClobExecutionClient,
    ExecutionClient,
    PolynodeExecutionClient,
)
from exchange_client.lib.polynode_event_stream import (
    PolynodeMempoolEventStream,
    PolynodeSettledEventStream,
)
from exchange_client.lib.rpc_event_stream import RpcSettledEventStream

LISTEN_CHOICES = ("rpc", "polynode")
EXEC_CHOICES = ("clob", "polynode")
SETTLEMENT_TIMEOUT_SECONDS = 60


def settled_stream(listen: str) -> SettledTradeStream:
    if listen == "rpc":
        ws_url = ws_url_from_env(
            os.environ.get("POLYGON_WS_URL", ""),
            os.environ.get("POLYGON_RPC_URL", ""),
        )
        return RpcSettledEventStream(ws_url)
    if listen == "polynode":
        api_key = trading_lib.require_env("POLYNODE_API_KEY")
        ws_url = os.environ.get("POLYNODE_WS_URL", "wss://ws.polynode.dev/ws")
        return PolynodeSettledEventStream(api_key=api_key, ws_url=ws_url)
    raise ValueError(f"unknown --listen {listen!r}")


def mempool_stream() -> TradeMempoolStream:
    api_key = trading_lib.require_env("POLYNODE_API_KEY")
    ws_url = os.environ.get("POLYNODE_WS_URL", "wss://ws.polynode.dev/ws")
    return PolynodeMempoolEventStream(api_key=api_key, ws_url=ws_url)


def execution_client(name: str) -> ExecutionClient:
    if name == "clob":
        return ClobExecutionClient()
    if name == "polynode":
        return PolynodeExecutionClient()
    raise ValueError(f"unknown --exec {name!r}")


def usdc_notional(event: TradeEvent | MempoolTradeEvent) -> float:
    try:
        if event.maker_asset_id == "0":
            return int(event.maker_amount) / 1e6
        if event.taker_asset_id == "0":
            return int(event.taker_amount) / 1e6
    except (TypeError, ValueError):
        return 0.0
    return 0.0


def outcome_token_id(event: TradeEvent | MempoolTradeEvent) -> str:
    if event.maker_asset_id not in {"", "0"}:
        return event.maker_asset_id
    if event.taker_asset_id not in {"", "0"}:
        return event.taker_asset_id
    return ""


def extract_buy_trigger(event: TradeEvent | MempoolTradeEvent) -> dict | None:
    if event.maker_asset_id == "0" and event.taker_asset_id not in {"", "0"}:
        buyer = event.maker
        buy_token = event.taker_asset_id
        raw_usdc = event.maker_amount
    elif event.taker_asset_id == "0" and event.maker_asset_id not in {"", "0"}:
        buyer = event.taker
        buy_token = event.maker_asset_id
        raw_usdc = event.taker_amount
    else:
        return None
    try:
        trade_value_usd = float(raw_usdc) / 1e6
    except (TypeError, ValueError):
        return None
    return {
        "buyer": buyer,
        "buy_token_id": buy_token,
        "trade_value_usd": trade_value_usd,
    }


def cleared_price(event: TradeEvent, token_id: str) -> float | None:
    if event.maker_asset_id == "0" and event.taker_asset_id == token_id:
        usdc_raw = int(event.maker_amount)
        token_raw = int(event.taker_amount)
    elif event.taker_asset_id == "0" and event.maker_asset_id == token_id:
        usdc_raw = int(event.taker_amount)
        token_raw = int(event.maker_amount)
    else:
        return None
    if token_raw <= 0:
        return None
    return usdc_raw / token_raw
