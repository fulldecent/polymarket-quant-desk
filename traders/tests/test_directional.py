"""Gate, formula, and 3-bet cap for directional."""

from __future__ import annotations

import json
import sys
from pathlib import Path

_ROOT = Path(__file__).resolve().parents[2]
sys.path.insert(0, str(_ROOT))
sys.path.insert(0, str(_ROOT / "explorations" / "directional-v1"))
sys.path.insert(0, str(_ROOT / "traders" / "directional"))

from book import LiveBook  # noqa: E402
from sim_lib import n_min  # noqa: E402
from stage_c import pick_ticket  # noqa: E402

FORMULA = json.loads((_ROOT / "traders" / "directional" / "formula.json").read_text())


def _fill(book: LiveBook, cid: str, block: int, px: float, *, maker: bool = True) -> None:
    book.ingest(
        cid=cid,
        block=block,
        yes_px=px,
        usdc=2.0,
        shares=4.0,
        is_taker=not maker,
        is_buy_yes=True,
        is_maker=maker,
        account="0xabc",
        fee=0.0,
        neg_risk=False,
        yes_token="yes1",
        no_token="no1",
    )


def test_formula_is_the_frozen_cell():
    a, c, p = FORMULA["stage_a"], FORMULA["stage_c"], FORMULA["portfolio"]
    assert a["need"] == 3 and a["window"] == 8 and a.get("criss_min") == 5
    assert c["in_frac"] == 1.0 and c["out_frac"] == 0.5 and c["double_k"] == 4
    assert c["size_mult"] == 1.1 and c["min_notional_usd"] == 2.0 and c["min_shares"] == 5
    assert c["skip_crypto"] and c["skip_fee"] and c["min_last"] == 0.10
    assert c["max_shares"] == 20
    assert p["max_open"] == 3 and p["warmup_blocks"] == 100
    assert p["max_loss_usd"] == 30.0


def test_gate_needs_three_of_eight_and_five_crosses():
    book = LiveBook()
    cid = "aa" * 32
    for i, b in enumerate([10, 11, 12, 13, 14, 15]):
        _fill(book, cid, b, 0.40)
    assert book.persist_count(cid, 15) == 6
    assert book.kris(cid, 15) == 0
    assert not book.gate(cid, 15, need=3, criss_min=5)

    book2 = LiveBook()
    for b in range(100, 108):
        for j in range(4):
            px = 0.40 + (0.02 if (b + j) % 2 == 0 else 0.00)
            _fill(book2, cid, b, px)
    x = 107
    assert book2.persist_count(cid, x) >= 3
    assert book2.kris(cid, x) >= 5
    assert book2.gate(cid, x, need=3, criss_min=5)


def test_sizing_floor_is_five_shares_and_two_dollars():
    n = max(5.0, 1.1 * n_min(0.50))
    assert n >= 5
    assert n * 0.50 >= 2.0
    spent = max(2.0, n * 0.50)
    assert spent >= 2.0
    cheap = max(5.0, 1.1 * n_min(0.80))
    assert max(2.0, cheap * 0.80) >= 2.0


def test_pick_ticket_always_on_from_b_heads():
    ev = {
        "last": 0.50,
        "tick": 0.01,
        "b_h1_hi": 0.04,
        "b_h1_lo": -0.01,
        "b_h130_hi": 0.06,
        "b_h130_lo": -0.02,
    }
    t = pick_ticket(ev, in_frac=1.0, out_frac=0.5, double_k=4, size_mult=1.1)
    assert t is not None
    assert t["side"] == "yes"
    assert t["p_in"] > 0
    assert t["p_out"] > t["p_in"]
    assert t["n"] >= 5


def test_yes_px_parses_polynode_decimals():
    from exchange_client.lib.event_stream import TradeEvent
    from main import _yes_px

    ev = TradeEvent(
        tx_hash="0x" + "ab" * 32,
        contract_address="0x" + "11" * 20,
        event_type="OrderFilled",
        maker="0x" + "aa" * 20,
        taker="0x" + "bb" * 20,
        maker_asset_id="0",
        taker_asset_id="999",
        maker_amount="2.0",
        taker_amount="5.0",
        fee="0",
        condition_id="0x" + "cd" * 32,
        outcome="Yes",
        block_number=10,
        log_index=0,
    )
    parsed = _yes_px(ev)
    assert parsed is not None
    cid, yes_px, usdc, shares, is_taker, is_buy_yes, yes_tok, no_tok = parsed
    assert usdc == 2.0
    assert shares == 5.0
    assert abs(yes_px - 0.4) < 1e-9
    assert is_buy_yes
    assert not is_taker


def test_crypto_updown_is_banned():
    from main import is_crypto_market, _is_our_sell
    from exchange_client.lib.event_stream import TradeEvent

    assert is_crypto_market("Bitcoin Up or Down - September 18, 6:10PM-6:15PM ET")
    assert is_crypto_market("ETH Up or Down 15m")
    assert not is_crypto_market("Will the Chargers win the 2027 NFL league championship?")
    ev = TradeEvent(
        tx_hash="0x" + "ab" * 32,
        contract_address="0x" + "11" * 20,
        event_type="OrderFilled",
        maker="0x" + "aa" * 20,
        taker="0x" + "bb" * 20,
        maker_asset_id="999",
        taker_asset_id="0",
        maker_amount="5.0",
        taker_amount="2.0",
        fee="0",
        block_number=1,
        log_index=0,
    )
    ours = {"0x" + "aa" * 20}
    assert _is_our_sell(ev, "999", ours)
    assert not _is_our_sell(ev, "999", {"0x" + "cc" * 20})
    ev_buy = TradeEvent(
        tx_hash="0x" + "ab" * 32,
        contract_address="0x" + "11" * 20,
        event_type="OrderFilled",
        maker="0x" + "aa" * 20,
        taker="0x" + "bb" * 20,
        maker_asset_id="0",
        taker_asset_id="999",
        maker_amount="2.0",
        taker_amount="5.0",
        fee="0",
        block_number=1,
        log_index=0,
    )
    assert not _is_our_sell(ev_buy, "999", ours)


def test_max_three_open_slots():
    open_pos = []
    max_open = 3

    def try_add(cid: str) -> bool:
        if len(open_pos) >= max_open:
            return False
        if any(p["cid"] == cid for p in open_pos):
            return False
        open_pos.append({"cid": cid})
        return True

    assert try_add("a") and try_add("b") and try_add("c")
    assert not try_add("d")
    assert len(open_pos) == 3
