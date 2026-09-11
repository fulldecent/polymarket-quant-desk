"""Build PUBLIC exec clients and listen/mempool streams from CLI flags."""

from __future__ import annotations

import os
from collections.abc import Callable
from urllib.parse import urlparse

from exchange_client.lib import trading_lib
from exchange_client.lib.event_stream import (
    V1_CONTRACTS,
    V2_CONTRACTS,
    MempoolTradeEvent,
    SettledTradeStream,
    TradeEvent,
    TradeMempoolStream,
    ws_url_from_env,
)

_NEG_RISK_EXCHANGES = {
    V1_CONTRACTS["NegRiskCtfExchange"].lower(),
    V2_CONTRACTS["NegRiskCtfExchangeV2"].lower(),
    "0xe2222d002000ba0053cef3375333610f64600036",
}
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

StatusFn = Callable[[str], None]


def listen_host(listen: str) -> str:
    if listen == "rpc":
        raw = os.environ.get("POLYGON_WS_URL") or os.environ.get("POLYGON_RPC_URL") or ""
    else:
        raw = os.environ.get("POLYNODE_WS_URL") or "wss://ws.polynode.dev/ws"
    return urlparse(raw).hostname or "unset"


def settled_stream(listen: str, *, on_status: StatusFn | None = None) -> SettledTradeStream:
    if listen == "rpc":
        ws_url = ws_url_from_env(
            os.environ.get("POLYGON_WS_URL", ""),
            os.environ.get("POLYGON_RPC_URL", ""),
        )
        return RpcSettledEventStream(ws_url, on_status=on_status)
    if listen == "polynode":
        api_key = trading_lib.require_env("POLYNODE_API_KEY")
        ws_url = os.environ.get("POLYNODE_WS_URL", "wss://ws.polynode.dev/ws")
        return PolynodeSettledEventStream(
            api_key=api_key, ws_url=ws_url, on_status=on_status
        )
    raise ValueError(f"unknown --listen {listen!r}")


def mempool_stream(*, on_status: StatusFn | None = None) -> TradeMempoolStream:
    api_key = trading_lib.require_env("POLYNODE_API_KEY")
    ws_url = os.environ.get("POLYNODE_WS_URL", "wss://ws.polynode.dev/ws")
    return PolynodeMempoolEventStream(
        api_key=api_key, ws_url=ws_url, on_status=on_status
    )


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


def exchange_is_neg_risk(address: str) -> bool:
    """True if `address` is a NegRisk CTF exchange (v1 or v2)."""
    return address.lower() in _NEG_RISK_EXCHANGES


def event_is_neg_risk(event: TradeEvent | MempoolTradeEvent) -> bool:
    if bool(getattr(event, "neg_risk", False)):
        return True
    return exchange_is_neg_risk(getattr(event, "contract_address", "") or "")


def is_live_trigger(
    event: TradeEvent | MempoolTradeEvent, *, after_block: int
) -> bool:
    """True once warmup is over and this fill is from a newer block.

    Settled fills must have ``block_number > after_block`` (the high-water
    block observed during warmup). Mempool fills have no block yet; they
    are live as soon as warmup ends.
    """
    if isinstance(event, MempoolTradeEvent):
        return True
    return event.block_number > after_block


def extract_buy_trigger(event: TradeEvent | MempoolTradeEvent) -> dict | None:
    if event.maker_asset_id == "0" and event.taker_asset_id not in {"", "0"}:
        buy_token = event.taker_asset_id
        raw_usdc = event.maker_amount
        buyer = event.maker or event.taker
    elif event.taker_asset_id == "0" and event.maker_asset_id not in {"", "0"}:
        buy_token = event.maker_asset_id
        raw_usdc = event.taker_amount
        buyer = event.taker or event.maker
    else:
        return None
    if not buyer:
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
