"""Trade stream protocols and shared event types.

Settled trade streams yield confirmed trades with a required block number.
Mempool trade streams yield pre-confirmation trades that have not landed in
an on-chain block yet.

The RPC implementation decodes raw EVM OrderFilled/OrdersMatched logs into
settled TradeEvent objects. Polynode provides both pre-confirmation and
confirmed settlement feeds as separate stream implementations.

v1 and v2 exchanges use different topic0 hashes and ABIs. Do not mix v2
addresses into the v1 log filter (or the reverse). decode_event accepts both.
"""

from __future__ import annotations

from dataclasses import dataclass
from typing import AsyncIterator, Protocol

from eth_abi.abi import decode as abi_decode


# ── v1 exchanges (makerAssetId / takerAssetId) ───────────────────────────────

V1_CONTRACTS = {
    "CTFExchange": "0x4bFb41d5B3570DeFd03C39a9A4D8dE6Bd8B8982E",
    "NegRiskCtfExchange": "0xC5d563A36AE78145C45a50134d48A1215220f80a",
}

V1_TOPIC0_MAP = {
    "0xd0a08e8c493f9c94f29311604c9de1b4e8c8d4c06bd0c789af57f2d65bfec0f6": "OrderFilled",
    "0x63bf4d16b7fa898ef4c4b2b6d90fd201e9c56313b65638af6088d149d2ce956c": "OrdersMatched",
}

V1_ADDRESSES = list(V1_CONTRACTS.values())
V1_TOPICS = list(V1_TOPIC0_MAP.keys())

# ── v2 exchanges (side + tokenId) ────────────────────────────────────────────

V2_CONTRACTS = {
    "CTFExchangeV2": "0xE111180000d2663C0091e4f400237545B87B996B",
    "NegRiskCtfExchangeV2": "0xe2222d279d744050d28e00520010520000310F59",
}

V2_TOPIC0_MAP = {
    "0xd543adfd945773f1a62f74f0ee55a5e3b9b1a28262980ba90b1a89f2ea84d8ee": "OrderFilled",
    "0x174b3811690657c217184f89418266767c87e4805d09680c39fc9c031c0cab7c": "OrdersMatched",
}

V2_ADDRESSES = list(V2_CONTRACTS.values())
V2_TOPICS = list(V2_TOPIC0_MAP.keys())

# Combined views for callers that want every venue. RPC subscribe must still
# use the v1 pair and the v2 pair as two separate filters.
CONTRACTS = {**V1_CONTRACTS, **V2_CONTRACTS}
ALL_ADDRESSES = V1_ADDRESSES + V2_ADDRESSES
TOPIC0_MAP = {**V1_TOPIC0_MAP, **V2_TOPIC0_MAP}
WANTED_TOPICS = V1_TOPICS + V2_TOPICS


# ── Data types ───────────────────────────────────────────────────────────────

@dataclass(frozen=True)
class TradeFields:
    """Common trade fields shared by settled and mempool events."""

    tx_hash: str
    contract_address: str
    event_type: str
    maker: str
    taker: str
    maker_asset_id: str
    taker_asset_id: str
    maker_amount: str
    taker_amount: str
    fee: str
    condition_id: str = ""
    market_title: str = ""
    token_ids_csv: str = ""
    tokens_json: str = ""
    outcome: str = ""
    tick_size: str = ""
    neg_risk: bool = False


@dataclass(frozen=True)
class TradeEvent(TradeFields):
    """A confirmed Polymarket trade fill.

    Field semantics follow the on-chain OrderFilled event:

    - block_number is always present because settled streams are confirmed.
    - maker/taker are lowercase 0x-prefixed addresses when available.
    - maker_asset_id/taker_asset_id are decimal-string token IDs.
      "0" means the USDC side. maker_asset_id is the asset the maker provides.
    - amount fields are decimal-string raw uint256 values.
    """

    block_number: int = 0
    log_index: int | None = None


@dataclass(frozen=True)
class MempoolTradeEvent(TradeFields):
    """A pre-confirmation trade observed before block inclusion."""


# ── Protocol ─────────────────────────────────────────────────────────────────

class SettledTradeStream(Protocol):
    """Async iterator of confirmed TradeEvent objects."""

    async def connect(self) -> None: ...
    async def disconnect(self) -> None: ...
    def __aiter__(self) -> AsyncIterator[TradeEvent]: ...
    async def __anext__(self) -> TradeEvent: ...


class TradeMempoolStream(Protocol):
    """Async iterator of pre-confirmation MempoolTradeEvent objects."""

    async def connect(self) -> None: ...
    async def disconnect(self) -> None: ...
    def __aiter__(self) -> AsyncIterator[MempoolTradeEvent]: ...
    async def __anext__(self) -> MempoolTradeEvent: ...


# Backward-compatible aliases while callers migrate.
EventStream = SettledTradeStream
TradeEventStream = SettledTradeStream
MempoolEventStream = TradeMempoolStream


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


def v2_maker_taker_assets(side: int, token_id: str) -> tuple[str, str]:
    """Map v2 (side, tokenId) onto v1 maker_asset_id / taker_asset_id.

    maker_asset_id is the asset the maker provides (same as v1 OrderFilled).
    side 0 = BUY (maker pays USDC for tokens); side 1 = SELL (maker pays tokens).
    """
    if int(side) == 0:
        return "0", str(token_id)
    return str(token_id), "0"


def decode_event(log: dict) -> TradeEvent | None:
    """Decode a raw EVM log dict into a TradeEvent.

    Returns None if the log is not a known OrderFilled/OrdersMatched topic,
    or if decoding fails.
    """
    topics = log.get("topics", [])
    if not topics:
        return None
    topic0 = topics[0].lower()
    is_v2 = topic0 in V2_TOPIC0_MAP
    event_name = (V2_TOPIC0_MAP if is_v2 else V1_TOPIC0_MAP).get(topic0)
    if event_name is None:
        return None

    block_number = parse_hex(log["blockNumber"])
    tx_hash = log["transactionHash"]
    log_index = parse_hex(log["logIndex"])
    contract_address = log["address"].lower()

    try:
        if is_v2:
            return _decode_v2(
                event_name=event_name,
                topics=topics,
                data_hex=log.get("data", "0x"),
                block_number=block_number,
                tx_hash=tx_hash,
                log_index=log_index,
                contract_address=contract_address,
            )
        return _decode_v1(
            event_name=event_name,
            topics=topics,
            data_hex=log.get("data", "0x"),
            block_number=block_number,
            tx_hash=tx_hash,
            log_index=log_index,
            contract_address=contract_address,
        )
    except Exception:
        return None


def _decode_v1(
    *,
    event_name: str,
    topics: list[str],
    data_hex: str,
    block_number: int,
    tx_hash: str,
    log_index: int,
    contract_address: str,
) -> TradeEvent | None:
    if event_name == "OrderFilled":
        maker = pad_address(topics[2])
        taker = pad_address(topics[3])
        (
            maker_asset_id,
            taker_asset_id,
            maker_amount,
            taker_amount,
            fee,
        ) = decode_log_data(
            ["uint256", "uint256", "uint256", "uint256", "uint256"],
            data_hex,
        )
        return TradeEvent(
            tx_hash=tx_hash,
            contract_address=contract_address,
            event_type=event_name,
            maker=maker,
            taker=taker,
            maker_asset_id=str(maker_asset_id),
            taker_asset_id=str(taker_asset_id),
            maker_amount=str(maker_amount),
            taker_amount=str(taker_amount),
            fee=str(fee),
            block_number=block_number,
            log_index=log_index,
        )

    if event_name == "OrdersMatched":
        taker = pad_address(topics[2])
        maker_asset_id, taker_asset_id, maker_amount, taker_amount = decode_log_data(
            ["uint256", "uint256", "uint256", "uint256"],
            data_hex,
        )
        return TradeEvent(
            tx_hash=tx_hash,
            contract_address=contract_address,
            event_type=event_name,
            maker="",
            taker=taker,
            maker_asset_id=str(maker_asset_id),
            taker_asset_id=str(taker_asset_id),
            maker_amount=str(maker_amount),
            taker_amount=str(taker_amount),
            fee="0",
            block_number=block_number,
            log_index=log_index,
        )
    return None


def _decode_v2(
    *,
    event_name: str,
    topics: list[str],
    data_hex: str,
    block_number: int,
    tx_hash: str,
    log_index: int,
    contract_address: str,
) -> TradeEvent | None:
    if event_name == "OrderFilled":
        maker = pad_address(topics[2])
        taker = pad_address(topics[3])
        side, token_id, maker_amount, taker_amount, fee, _builder, _metadata = decode_log_data(
            ["uint8", "uint256", "uint256", "uint256", "uint256", "bytes32", "bytes32"],
            data_hex,
        )
        maker_asset_id, taker_asset_id = v2_maker_taker_assets(int(side), str(token_id))
        return TradeEvent(
            tx_hash=tx_hash,
            contract_address=contract_address,
            event_type=event_name,
            maker=maker,
            taker=taker,
            maker_asset_id=maker_asset_id,
            taker_asset_id=taker_asset_id,
            maker_amount=str(maker_amount),
            taker_amount=str(taker_amount),
            fee=str(fee),
            block_number=block_number,
            log_index=log_index,
        )

    if event_name == "OrdersMatched":
        taker = pad_address(topics[2])
        side, token_id, maker_amount, taker_amount = decode_log_data(
            ["uint8", "uint256", "uint256", "uint256"],
            data_hex,
        )
        maker_asset_id, taker_asset_id = v2_maker_taker_assets(int(side), str(token_id))
        return TradeEvent(
            tx_hash=tx_hash,
            contract_address=contract_address,
            event_type=event_name,
            maker="",
            taker=taker,
            maker_asset_id=maker_asset_id,
            taker_asset_id=taker_asset_id,
            maker_amount=str(maker_amount),
            taker_amount=str(taker_amount),
            fee="0",
            block_number=block_number,
            log_index=log_index,
        )
    return None


def ws_url_from_env(ws_url: str, http_url: str) -> str:
    """Resolve a Polygon JSON-RPC WebSocket URL.

    Uses POLYGON_WS_URL when set. Infura HTTP URLs can be rewritten to
    ``/ws/v3/``. Other HTTPS RPC endpoints (Chainstack, dRPC, …) are not
    WebSockets — do not flip https→wss.
    """
    import sys

    if ws_url:
        return ws_url
    if http_url:
        lowered = http_url.lower()
        if lowered.startswith("ws://") or lowered.startswith("wss://"):
            return http_url
        if "infura.io" in lowered:
            return (
                http_url.replace("https://", "wss://")
                .replace("http://", "ws://")
                .replace("/v3/", "/ws/v3/")
            )
        sys.exit(
            "POLYGON_WS_URL is required for --listen rpc. "
            f"POLYGON_RPC_URL ({http_url!r}) is HTTPS JSON-RPC, not a WebSocket "
            "(servers return HTTP 405 if you rewrite it to wss://)."
        )
    sys.exit("POLYGON_WS_URL or POLYGON_RPC_URL not set")
