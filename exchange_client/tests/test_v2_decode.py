"""v2 OrderFilled maps (side, tokenId) onto v1 maker/taker asset ids."""

from __future__ import annotations

import sys
from pathlib import Path

from eth_abi.abi import encode as abi_encode

_project_root = Path(__file__).resolve().parents[2]
sys.path.insert(0, str(_project_root))

from exchange_client.lib.event_stream import (  # noqa: E402
    V1_ADDRESSES,
    V1_TOPIC0_MAP,
    V2_ADDRESSES,
    V2_TOPIC0_MAP,
    decode_event,
    v2_maker_taker_assets,
)

V2_FILLED = "0xd543adfd945773f1a62f74f0ee55a5e3b9b1a28262980ba90b1a89f2ea84d8ee"
TOKEN = 123456789
MAKER = "0x" + "11" * 20
TAKER = "0x" + "22" * 20
V2_ADDR = V2_ADDRESSES[0]


def _topic_addr(addr: str) -> str:
    return "0x" + addr[2:].lower().rjust(64, "0")


def _v2_log(*, side: int) -> dict:
    data = abi_encode(
        ["uint8", "uint256", "uint256", "uint256", "uint256", "bytes32", "bytes32"],
        [side, TOKEN, 1_000_000, 2_000_000, 50_000, b"\x00" * 32, b"\x00" * 32],
    )
    return {
        "address": V2_ADDR,
        "topics": [
            V2_FILLED,
            "0x" + "ab" * 32,
            _topic_addr(MAKER),
            _topic_addr(TAKER),
        ],
        "data": "0x" + data.hex(),
        "blockNumber": "0x4a817c8",
        "transactionHash": "0x" + "cd" * 32,
        "logIndex": "0x3",
    }


def test_v1_and_v2_filters_are_disjoint():
    assert set(a.lower() for a in V1_ADDRESSES).isdisjoint(set(a.lower() for a in V2_ADDRESSES))
    assert set(V1_TOPIC0_MAP).isdisjoint(set(V2_TOPIC0_MAP))
    assert V2_FILLED in V2_TOPIC0_MAP
    assert V2_FILLED not in V1_TOPIC0_MAP


def test_buy_side_maker_provides_usdc():
    maker_asset, taker_asset = v2_maker_taker_assets(0, str(TOKEN))
    assert maker_asset == "0"
    assert taker_asset == str(TOKEN)


def test_sell_side_maker_provides_tokens():
    maker_asset, taker_asset = v2_maker_taker_assets(1, str(TOKEN))
    assert maker_asset == str(TOKEN)
    assert taker_asset == "0"


def test_decode_v2_buy():
    event = decode_event(_v2_log(side=0))
    assert event is not None
    assert event.maker == MAKER
    assert event.taker == TAKER
    assert event.maker_asset_id == "0"
    assert event.taker_asset_id == str(TOKEN)
    assert event.maker_amount == "1000000"
    assert event.taker_amount == "2000000"
    assert event.fee == "50000"
    assert event.block_number == 0x4A817C8
    assert event.event_type == "OrderFilled"


def test_decode_v2_sell():
    event = decode_event(_v2_log(side=1))
    assert event is not None
    assert event.maker_asset_id == str(TOKEN)
    assert event.taker_asset_id == "0"


def test_unknown_topic_is_rejected():
    log = _v2_log(side=0)
    log["topics"][0] = "0x" + "00" * 32
    assert decode_event(log) is None
