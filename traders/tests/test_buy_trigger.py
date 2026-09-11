"""Buy-trigger extraction used by follow_anything."""

from __future__ import annotations

import sys
from pathlib import Path

_project_root = Path(__file__).resolve().parents[2]
sys.path.insert(0, str(_project_root))

from exchange_client.lib.event_stream import TradeEvent  # noqa: E402
from exchange_client.lib.event_stream import MempoolTradeEvent  # noqa: E402
from traders.lib.streams import (  # noqa: E402
    event_is_neg_risk,
    exchange_is_neg_risk,
    extract_buy_trigger,
    is_live_trigger,
)

TOKEN = "123"


def _event(*, maker_asset: str, taker_asset: str, maker_amt: str, taker_amt: str) -> TradeEvent:
    return TradeEvent(
        tx_hash="0x" + "ab" * 32,
        contract_address="0x" + "11" * 20,
        event_type="OrderFilled",
        maker="0x" + "aa" * 20,
        taker="0x" + "bb" * 20,
        maker_asset_id=maker_asset,
        taker_asset_id=taker_asset,
        maker_amount=maker_amt,
        taker_amount=taker_amt,
        fee="0",
        block_number=1,
        log_index=0,
    )


def test_maker_pays_usdc_is_a_buy():
    trig = extract_buy_trigger(
        _event(maker_asset="0", taker_asset=TOKEN, maker_amt="2000000", taker_amt="4000000")
    )
    assert trig is not None
    assert trig["buyer"] == "0x" + "aa" * 20
    assert trig["buy_token_id"] == TOKEN
    assert trig["trade_value_usd"] == 2.0


def test_taker_pays_usdc_is_a_buy():
    trig = extract_buy_trigger(
        _event(maker_asset=TOKEN, taker_asset="0", maker_amt="4000000", taker_amt="2000000")
    )
    assert trig is not None
    assert trig["buyer"] == "0x" + "bb" * 20
    assert trig["buy_token_id"] == TOKEN


def test_empty_buyer_is_not_a_trigger():
    event = _event(maker_asset="0", taker_asset=TOKEN, maker_amt="0", taker_amt="1")
    object.__setattr__(event, "maker", "")
    object.__setattr__(event, "taker", "")
    assert extract_buy_trigger(event) is None


def test_event_neg_risk_flag_or_contract():
    event = _event(maker_asset="0", taker_asset=TOKEN, maker_amt="1", taker_amt="1")
    assert not event_is_neg_risk(event)
    object.__setattr__(event, "neg_risk", True)
    assert event_is_neg_risk(event)


def test_token_token_is_not_a_buy():
    assert (
        extract_buy_trigger(
            _event(maker_asset=TOKEN, taker_asset="999", maker_amt="1", taker_amt="1")
        )
        is None
    )


def test_settled_trigger_must_be_after_warmup_block():
    event = _event(maker_asset="0", taker_asset=TOKEN, maker_amt="1", taker_amt="1")
    assert not is_live_trigger(event, after_block=1)
    assert is_live_trigger(event, after_block=0)


def test_mempool_trigger_is_live_after_warmup():
    event = MempoolTradeEvent(
        tx_hash="0x" + "ab" * 32,
        contract_address="0x" + "11" * 20,
        event_type="OrderFilled",
        maker="0x" + "aa" * 20,
        taker="0x" + "bb" * 20,
        maker_asset_id="0",
        taker_asset_id=TOKEN,
        maker_amount="1",
        taker_amount="1",
        fee="0",
    )
    assert is_live_trigger(event, after_block=99_999_999)


def test_delayed_pending_settles_on_first_fill():
    from traders.follow_anything.main import _pending_is_settled, _pending_tx_label

    pending = {
        "expected": set(),
        "landed": set(),
        "order_id": "0xabc123def456",
    }
    assert not _pending_is_settled(pending)
    assert _pending_tx_label(pending) == "0xabc1…f456"
    pending["landed"].add("0x" + "ab" * 32)
    assert _pending_is_settled(pending)


def test_hashed_pending_needs_all_expected():
    from traders.follow_anything.main import _pending_is_settled, _pending_tx_label

    tx = "0x" + "cd" * 32
    pending = {"expected": {tx}, "landed": set(), "order_id": ""}
    assert not _pending_is_settled(pending)
    pending["landed"].add(tx)
    assert _pending_is_settled(pending)
    assert _pending_tx_label(pending).startswith("0xcdcd")


def test_timeout_label_empty_expected_no_order():
    from traders.follow_anything.main import _pending_tx_label

    assert _pending_tx_label({"expected": set(), "order_id": ""}) == "none"


def test_neg_risk_from_exchange_address():
    assert exchange_is_neg_risk("0xe2222d279d744050d28e00520010520000310F59")
    assert exchange_is_neg_risk("0xC5d563A36AE78145C45a50134d48A1215220f80a")
    assert not exchange_is_neg_risk("0xE111180000d2663C0091e4f400237545B87B996B")
    assert not exchange_is_neg_risk("0x4bFb41d5B3570DeFd03C39a9A4D8dE6Bd8B8982E")
