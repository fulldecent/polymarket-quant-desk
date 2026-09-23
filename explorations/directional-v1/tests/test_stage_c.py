"""Stage C extras: account dominance, selective 30-block ticket pick."""

from __future__ import annotations

import sys
from pathlib import Path

import pandas as pd

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))

from sim_lib import Level, simulate_roundtrip  # noqa: E402
from datetime import date  # noqa: E402

from stage_c import (  # noqa: E402
    WHALE_MODES,
    account_stats,
    block_to_date,
    passes_whale,
    pick_ticket,
    week_shift_pairs,
)


def test_account_stats_one_whale_vs_crowd():
    sl = pd.DataFrame(
        {
            "block_number": [98, 99, 100, 100],
            "account": ["aa", "aa", "aa", "bb"],
            "usdc": [9.0, 9.0, 8.0, 2.0],
        }
    )
    out = account_stats(sl, 100)
    assert out["n_acct"] == 2
    assert abs(out["top_share"] - 26.0 / 28.0) < 1e-12
    assert out["n_acct_x"] == 2
    assert abs(out["top_share_x"] - 8.0 / 10.0) < 1e-12
    assert out["hhi"] > 0.8


def test_account_stats_empty():
    sl = pd.DataFrame(columns=["block_number", "account", "usdc"])
    out = account_stats(sl, 100)
    assert out["n_acct"] == 0
    assert out["top_share"] == 0.0


def test_whale_modes():
    assert "any" in WHALE_MODES
    assert passes_whale({"top_share": 0.9, "n_acct": 2}, "any")
    assert not passes_whale({"top_share": 0.9, "n_acct": 2}, "crowd70")
    assert passes_whale({"top_share": 0.6, "n_acct": 4}, "crowd70")


def _ev(**kw):
    base = {
        "p_act_1": 0.90,
        "p_act_30": 0.80,
        "fee_market": False,
        "top_share": 0.3,
        "n_acct": 6,
        "last": 0.50,
        "tick": 0.01,
        "p_bar": {(2, 30): 0.55, (-2, 30): 0.10, (3, 30): 0.40, (4, 30): 0.20},
    }
    base.update(kw)
    return base


def test_pick_requires_fok_liq_and_range():
    ev = _ev()
    kw = dict(
        t_fill=0.70,
        t_liq=0.60,
        t_range=0.40,
        k_need=2,
        price="passive",
        whale="any",
        skip_fee=False,
        size_mult=1.1,
        exit_end=30,
    )
    got = pick_ticket(ev, **kw)
    assert got is not None
    assert got["side"] == "yes"
    assert got["din"] == 0 and got["dout"] == 2
    assert pick_ticket(ev, **{**kw, "t_fill": 0.95}) is None
    assert pick_ticket(ev, **{**kw, "t_liq": 0.90}) is None
    assert pick_ticket(ev, **{**kw, "t_range": 0.90}) is None


def test_pay2_needs_k_gt_2():
    ev = _ev()
    kw = dict(
        t_fill=0.70,
        t_liq=0.60,
        t_range=0.15,
        k_need=2,
        price="pay2",
        whale="any",
        skip_fee=False,
        size_mult=1.1,
        exit_end=30,
    )
    assert pick_ticket(ev, **kw) is None
    got = pick_ticket(ev, **{**kw, "k_need": 3, "price": "pay2"})
    assert got is not None and got["din"] == 2 and got["dout"] == 3


def test_week_shift_pairs_same_weekday():
    knots = [(100, date(2026, 1, 5)), (100 + 7 * 40000, date(2026, 1, 12))]  # Mon→Mon
    # 5 Jan 2026 is a Monday
    events = (
        [{"x_block": 100, "i": i} for i in range(250)]
        + [{"x_block": 100 + 7 * 40000, "i": i} for i in range(250)]
        + [{"x_block": 100 + 3 * 40000, "i": i} for i in range(250)]  # Thu, no +7
    )
    pairs = week_shift_pairs(events, shift_days=7, min_n=200, knots=knots)
    assert len(pairs) == 1
    d, d7, tr, te = pairs[0]
    assert d.weekday() == d7.weekday() == 0
    assert (d7 - d).days == 7


def test_block_to_date_knots():
    knots = [(80_282_490, date(2025, 12, 14)), (93_789_879, date(2026, 9, 14))]
    assert block_to_date(80_282_490, knots) == date(2025, 12, 14)
    assert block_to_date(93_789_879, knots) == date(2026, 9, 14)


def test_simulate_30_block_exit_then_dump():
    # GTC ends at 30; dump at 31 must count when liq_start=31.
    out = simulate_roundtrip(
        n=5.0,
        p_entry=0.50,
        p_exit=0.52,
        entry_asks=[Level(0.50, 10.0)],
        exit_by_block=[],
        liq_by_block=[(131, [Level(0.49, 10.0)])],
        x_block=100,
        fee_market=False,
        exit_end=30,
        liq_start=31,
        liq_end=90,
    )
    assert out["entry_ok"]
    assert abs(out["q_liq"] - 5.0) < 1e-12
    assert out["q_zero"] < 1e-12
