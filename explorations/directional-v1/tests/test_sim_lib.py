"""Directional FOK fill-window tests."""

from __future__ import annotations

import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))

from sim_lib import (  # noqa: E402
    COOLDOWN_BLOCKS,
    Level,
    apply_cooldown,
    fold_resolve,
    infer_tick,
    naive_caps,
    n_min,
    settlement_yes,
    simulate_roundtrip,
    walk_fok_buy,
)


def test_offset_caps_skip_when_exit_not_below_entry():
    from sim_lib import long_caps, offset_caps

    assert offset_caps(0.50, 0, 0) == (0.50, 0.50)
    assert long_caps(0.50, 0, 0) is None
    assert long_caps(0.50, 1, 1) is None
    got = long_caps(0.50, 0, 1)
    assert got == (0.50, 0.51)
    got = offset_caps(0.50, 2, -1)
    assert got == (0.52, 0.49)


def test_prefilter_3_of_last_8():
    from sim_lib import prefilter_alive
    assert prefilter_alive({100, 99, 97}, 100) is True
    assert prefilter_alive({100, 97}, 100) is False  # only 2
    assert prefilter_alive({100}, 100) is False
    assert prefilter_alive({100, 99, 90}, 100) is False  # 90 is 10 back


def test_infer_tick_legal_set_and_sparse_01_fallback():
    assert infer_tick([0.51, 0.52, 0.53]) == 0.01
    assert infer_tick([0.501, 0.502]) == 0.001
    assert infer_tick([0.50, 0.60, 0.70, 0.80]) == 0.1
    # only 0.50 and 0.60 → do not coarsen to 0.1
    assert infer_tick([0.50, 0.60]) == 0.01
    assert infer_tick([]) == 0.01


def test_n_min_and_naive_caps():
    assert n_min(0.50) == 5
    assert n_min(0.10) == 12
    caps = naive_caps(0.50)
    assert caps == (0.51, 0.49)
    assert naive_caps(0.01) is not None
    # clipped extremes can collapse
    assert naive_caps(0.005) is None or naive_caps(0.005)[1] < naive_caps(0.005)[0]


def test_fok_cap_touch_counts_and_needs_n():
    levels = [Level(0.50, 5.0)]
    assert walk_fok_buy(levels, cap=0.50, n=5) is not None
    assert walk_fok_buy(levels, cap=0.49, n=5) is None
    assert walk_fok_buy([Level(0.40, 4.0)], cap=0.50, n=5) is None
    got = walk_fok_buy([Level(0.40, 3.0), Level(0.45, 4.0)], cap=0.50, n=5)
    assert got is not None
    usdc, vwap = got
    assert abs(usdc - (3 * 0.40 + 2 * 0.45)) < 1e-12


def test_x2_requires_strictly_better_exit():
    asks = [Level(0.50, 10.0)]
    # At P_exit=0.52, a 0.52 print on X+2 must NOT fill; 0.53 must.
    out = simulate_roundtrip(
        n=5.0,
        p_entry=0.51,
        p_exit=0.52,
        entry_asks=asks,
        exit_by_block=[(102, [Level(0.52, 10.0)])],  # X=100 → X+2
        liq_by_block=[],
        x_block=100,
        fee_market=False,
    )
    assert out["entry_ok"]
    assert out["q_exit"] == 0.0
    assert out["q_zero"] == 5.0

    out2 = simulate_roundtrip(
        n=5.0,
        p_entry=0.51,
        p_exit=0.52,
        entry_asks=asks,
        exit_by_block=[(102, [Level(0.53, 10.0)])],
        liq_by_block=[],
        x_block=100,
        fee_market=False,
    )
    assert abs(out2["q_exit"] - 5.0) < 1e-12
    assert out2["state"] == "exit_full"
    assert out2["pnl"] > 0  # sold 0.53, bought 0.50


def test_x3_matches_at_floor():
    asks = [Level(0.50, 10.0)]
    out = simulate_roundtrip(
        n=5.0,
        p_entry=0.51,
        p_exit=0.52,
        entry_asks=asks,
        exit_by_block=[(103, [Level(0.52, 10.0)])],  # X+3
        liq_by_block=[],
        x_block=100,
        fee_market=False,
    )
    assert abs(out["q_exit"] - 5.0) < 1e-12


def test_partial_then_dump_then_zero():
    asks = [Level(0.40, 10.0)]
    out = simulate_roundtrip(
        n=10.0,
        p_entry=0.50,
        p_exit=0.45,
        entry_asks=asks,
        exit_by_block=[(103, [Level(0.45, 3.0)])],
        liq_by_block=[(161, [Level(0.20, 4.0)])],  # X+61
        x_block=100,
        fee_market=False,
    )
    assert abs(out["q_exit"] - 3.0) < 1e-12
    assert abs(out["q_liq"] - 4.0) < 1e-12
    assert abs(out["q_zero"] - 3.0) < 1e-12
    assert out["state"] == "mixed"
    # in 4.0, out 3*0.45 + 4*0.20 = 1.35+0.80=2.15, zero 0
    assert abs(out["pnl"] - (2.15 - 4.0)) < 1e-9


def test_settlement_yes_parses_spaced_and_compact():
    assert settlement_yes('["1","0"]') == 1.0
    assert settlement_yes('["1", "0"]') == 1.0
    assert settlement_yes('["0","1"]') == 0.0
    assert settlement_yes('["1","1"]') == 0.5
    assert settlement_yes(None) is None


def test_fold_resolve_only_inside_horizon():
    # empty window sentinels h=0, l=1; YES wins at X+30
    a, h, l = fold_resolve(0, 0.0, 1.0, 102, 160, 130, 1.0)
    assert a == 1 and h == 1.0 and l == 1.0
    # after horizon: no change
    a2, h2, l2 = fold_resolve(0, 0.0, 1.0, 102, 160, 200, 1.0)
    assert a2 == 0 and h2 == 0.0 and l2 == 1.0
    # NO wins: -k barriers
    a3, h3, l3 = fold_resolve(0, 0.0, 1.0, 102, 160, 130, 0.0)
    assert a3 == 1 and h3 == 0.0 and l3 == 0.0
    # fills already there, YES resolve lifts high only
    a4, h4, l4 = fold_resolve(1, 0.60, 0.40, 102, 160, 130, 1.0)
    assert a4 == 1 and h4 == 1.0 and l4 == 0.40


def test_cooldown_skips_until_plus_180():
    rows = [(10, "a"), (20, "a"), (10 + COOLDOWN_BLOCKS, "a"), (11, "b")]
    got = apply_cooldown(rows)
    assert got == [(10, "a"), (10 + COOLDOWN_BLOCKS, "a"), (11, "b")]
