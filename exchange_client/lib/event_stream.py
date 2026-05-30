"""Trade event streaming protocol and shared types.

Defines TradeEvent (the common data shape for on-chain trade fills) and
EventStream (the async iterator protocol that all event stream
implementations conform to).

The RPC implementation decodes raw EVM OrderFilled/OrdersMatched logs.
The Polynode implementation (future) consumes pre-decoded settlement or
trade events from the Polynode WebSocket API.
"""

from __future__ import annotations

from dataclasses import dataclass
from typing import AsyncIterator, Protocol

from eth_abi import decode as abi_decode


# ── Constants ────────────────────────────────────────────────────────────────

CONTRACTS = {
    "CTFExchange": "0x4bFb41d5B3570DeFd03C39a9A4D8dE6Bd8B8982E",
    "NegRiskCtfExchange": "0xC5d563A36AE78145C45a50134d48A1215220f80a",
}

ALL_ADDRESSES = list(CONTRACTS.values())

TOPIC0_MAP = {
    "0xd0a08e8c493f9c94f29311604c9de1b4e8c8d4c06bd0c789af57f2d65bfec0f6": "OrderFilled",
    "0x63bf4d16b7fa898ef4c4b2b6d90fd201e9c56313b65638af6088d149d2ce956c": "OrdersMatched",
}

WANTED_TOPICS = list(TOPIC0_MAP.keys())


# ── Data types ───────────────────────────────────────────────────────────────

@dataclass(frozen=True)
class TradeEvent:
    """A single Polymarket trade fill.

    This is the common representation produced by both RPC log decoding
    and Polynode settlement/trade events.  Field semantics follow the
    on-chain OrderFilled event:

    - maker/taker are lowercase 0x-prefixed addresses.
    - maker_asset_id/taker_asset_id are decimal-string token IDs.
      "0" means the USDC side.
    - amount fields are decimal-string raw uint256 values (6 decimals
      for USDC, 6 decimals for conditional tokens).
    """

    block_number: int | None  # None for Polynode pending settlements
    tx_hash: str
    log_index: int | None  # None for Polynode pending settlements
    contract_address: str
    event_type: str  # "OrderFilled" or "OrdersMatched"
    maker: str
    taker: str
    maker_asset_id: str
    taker_asset_id: str
    maker_amount: str
    taker_amount: str
    fee: str  # "0" when not available (e.g. OrdersMatched)


# ── Protocol ─────────────────────────────────────────────────────────────────

class EventStream(Protocol):
    """Async iterator of TradeEvent objects.

    Implementations handle connection, subscription, reconnection, and
    decoding internally.  Callers consume events with ``async for``.
    """

    async def connect(self) -> None: ...
    async def disconnect(self) -> None: ...
    def __aiter__(self) -> AsyncIterator[TradeEvent]: ...
    async def __anext__(self) -> TradeEvent: ...


# Backward-compatible alias while callers migrate.
TradeEventStream = EventStream


# ── Shared low-level helpers ─────────────────────────────────────────────────

def parse_hex(val: str) -> int:
    if not val or val == "0x":
        return 0
    return int(val, 16)


def hex_to_bytes(val: str) -> bytes:
    if val.startswith("0x"):
        val = val[2:]
    if len(val) % 2 != 0:
        val = "0" + val
    return bytes.fromhex(val)


def pad_address(topic: str) -> str:
    return ("0x" + topic[-40:]).lower()


def decode_log_data(types: list[str], data_hex: str) -> tuple:
    raw = hex_to_bytes(data_hex)
    if not raw:
        return tuple(0 for _ in types)
    return abi_decode(types, raw)


def decode_event(log: dict) -> TradeEvent | None:
    """Decode a raw EVM log dict into a TradeEvent.

    Returns None if the log is not an OrderFilled or OrdersMatched event,
    or if decoding fails.
    """
    topics = log.get("topics", [])
    if not topics:
        return None
    topic0 = topics[0].lower()
    event_name = TOPIC0_MAP.get(topic0)
    if event_name is None:
        return None

    block_number = parse_hex(log["blockNumber"])
    tx_hash = log["transactionHash"]
    log_index = parse_hex(log["logIndex"])
    contract_address = log["address"].lower()

    try:
        if event_name == "OrderFilled":
            maker = pad_address(topics[2])
            taker = pad_address(topics[3])
            (maker_asset_id, taker_asset_id, maker_amount,
             taker_amount, fee) = decode_log_data(
                ["uint256", "uint256", "uint256", "uint256", "uint256"],
                log["data"],
            )
            return TradeEvent(
                block_number=block_number,
                tx_hash=tx_hash,
                log_index=log_index,
                contract_address=contract_address,
                event_type=event_name,
                maker=maker,
                taker=taker,
                maker_asset_id=str(maker_asset_id),
                taker_asset_id=str(taker_asset_id),
                maker_amount=str(maker_amount),
                taker_amount=str(taker_amount),
                fee=str(fee),
            )

        elif event_name == "OrdersMatched":
            taker = pad_address(topics[2])
            (maker_asset_id, taker_asset_id, maker_amount,
             taker_amount) = decode_log_data(
                ["uint256", "uint256", "uint256", "uint256"],
                log["data"],
            )
            return TradeEvent(
                block_number=block_number,
                tx_hash=tx_hash,
                log_index=log_index,
                contract_address=contract_address,
                event_type=event_name,
                maker="",  # OrdersMatched has no maker in topics
                taker=taker,
                maker_asset_id=str(maker_asset_id),
                taker_asset_id=str(taker_asset_id),
                maker_amount=str(maker_amount),
                taker_amount=str(taker_amount),
                fee="0",
            )

    except Exception:
        return None

    return None


def ws_url_from_env(ws_url: str, http_url: str) -> str:
    """Derive a WebSocket URL from environment variables.

    Prefers ws_url if non-empty.  Falls back to converting http_url
    (with special handling for Infura endpoints).

    Raises SystemExit if neither is usable.
    """
    if ws_url:
        return ws_url
    if http_url:
        if "infura.io" in http_url:
            return (http_url
                    .replace("https://", "wss://")
                    .replace("http://", "ws://")
                    .replace("/v3/", "/ws/v3/"))
        return (http_url
                .replace("https://", "wss://")
                .replace("http://", "ws://"))
    import sys
    sys.exit("POLYGON_WS_URL or POLYGON_RPC_URL not set")
