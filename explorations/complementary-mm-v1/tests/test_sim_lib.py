"""Fill-model unit tests. No parquet."""

from __future__ import annotations

import sys
from pathlib import Path

_HERE = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(_HERE))

from sim_lib import (  # noqa: E402
    CUTOVER_BLOCKS,
    HORIZON_BLOCKS,
    Level,
    caps_from_close,
    floor_walk_vwap,
    merge_and_inventory,
    n_min,
    neutralize_position,
    taker_fee_usdc,
    walk_buy,
    walk_buy_window,
    walk_dump,
    yes_payout,
)


def test_n_min_binds_on_cheap_and_expensive():
    assert n_min(0.10) == 12
    assert n_min(0.24) == 5
    assert n_min(0.50) == 5
    assert n_min(0.90) == 5
    assert n_min(0.24) * 0.24 >= 1.20 - 1e-9


def test_caps_from_close_are_tick_worse_not_from_next_block():
    p_yes, p_no = caps_from_close(0.55)
    assert p_yes == 0.56
    assert abs(p_no - 0.46) < 1e-12


def test_walk_buy_requires_strictly_better_than_cap_and_strictly_more_shares():
    levels = [Level(0.40, 5.0), Level(0.50, 5.0)]
    # Cap at the touch 0.40: those shares do not count.
    assert walk_buy(levels, cap=0.40, n=5) is None
    # Exactly 5 shares at 0.40: not *strictly more* than 5.
    assert walk_buy([Level(0.40, 5.0)], cap=0.50, n=5) is None
    fill = walk_buy([Level(0.40, 6.0)], cap=0.50, n=5)
    assert fill is not None
    assert fill.shares == 5
    assert abs(fill.vwap - 0.40) < 1e-12
    assert abs(fill.usdc - 2.0) < 1e-12


def test_walk_buy_window_is_block_order_not_global_best():
    # Block 1 is expensive, block 2 is cheap. Complement must eat block 1 first.
    window = [
        (100, [Level(0.60, 10.0)]),
        (101, [Level(0.10, 10.0)]),
    ]
    fill = walk_buy_window(window, cap=0.90, n=5)
    assert fill is not None
    assert abs(fill.vwap - 0.60) < 1e-12
    assert fill.block == 100


def test_floor_walk_needs_five_shares_and_dollar_twenty():
    assert floor_walk_vwap([Level(0.10, 4.0)]) is None
    assert floor_walk_vwap([Level(0.10, 11.0)]) is None  # 11*0.10 = 1.10 < 1.20
    got = floor_walk_vwap([Level(0.10, 12.0)])
    assert got is not None
    vwap, sh = got
    assert sh == 12.0
    assert abs(vwap - 0.10) < 1e-12


def test_dump_takes_legal_prefix_not_illegal_full_q():
    # 5 @ 0.50 is legal; adding 100 @ 0.01 makes n*p_worst illegal.
    levels = [Level(0.50, 5.0), Level(0.01, 100.0)]
    dumped = walk_dump(levels, q=105.0)
    assert dumped is not None
    shares, proceeds, vwap = dumped
    assert abs(shares - 5.0) < 1e-12
    assert abs(proceeds - 2.5) < 1e-12
    assert abs(vwap - 0.50) < 1e-12


def test_dump_fails_below_five_shares_or_no_bids():
    assert walk_dump([Level(0.90, 10.0)], q=4.0) is None
    assert walk_dump([], q=10.0) is None


def test_yes_payout_parses_spaced_json():
    assert yes_payout('["1", "0"]') == 1.0
    assert yes_payout('["0", "1"]') == 0.0
    assert yes_payout('["1", "1"]') == 0.5
    assert yes_payout(None) is None


def _empty_neutralize(**kwargs):
    defaults = dict(
        q=5.0,
        entry_usdc=2.5,
        entry_fees=0.0,
        side="yes",
        p_comp=0.50,
        fill_block=1000,
        complement_by_block=[],
        dump_by_block=[],
        resolved_block=None,
        payout_numerators=None,
    )
    defaults.update(kwargs)
    return neutralize_position(**defaults)


def test_inventory_resolve_inside_3d_not_after():
    fill_block = 1000
    mark = _empty_neutralize(
        fill_block=fill_block,
        resolved_block=fill_block + 100,
        payout_numerators='["1", "0"]',
    )
    assert mark.kind == "resolve"
    assert abs(mark.pnl - (5.0 - 2.5)) < 1e-12

    too_late = _empty_neutralize(
        fill_block=fill_block,
        resolved_block=fill_block + HORIZON_BLOCKS + 1,
        payout_numerators='["1", "0"]',
    )
    assert too_late.kind == "zero"
    assert abs(too_late.pnl - (-2.5)) < 1e-12


def test_inventory_complement_rest_beats_dump():
    fill_block = 1000
    comp_blk = fill_block + 50
    mark = _empty_neutralize(
        p_comp=0.55,
        fill_block=fill_block,
        complement_by_block=[(comp_blk, [Level(0.45, 10.0)])],
        dump_by_block=[(fill_block + CUTOVER_BLOCKS + 10, [Level(0.90, 10.0)])],
    )
    assert mark.kind == "complement"
    assert abs(mark.pnl - (5.0 - 2.25 - 2.5)) < 1e-9


def test_inventory_dump_after_cutover_not_before():
    fill_block = 1000
    early = fill_block + 10
    late = fill_block + CUTOVER_BLOCKS + 10
    mark = _empty_neutralize(
        fill_block=fill_block,
        complement_by_block=[],
        dump_by_block=[
            (early, [Level(0.80, 10.0)]),
            (late, [Level(0.40, 10.0)]),
        ],
    )
    assert mark.kind == "dump"
    assert abs(mark.dump_vwap - 0.40) < 1e-12


def test_inventory_dump_miss_is_zero_at_3d():
    mark = _empty_neutralize()
    assert mark.kind == "zero"
    assert abs(mark.pnl - (-2.5)) < 1e-12


def test_inventory_no_side_inverts_payout():
    mark = _empty_neutralize(
        q=5.0,
        entry_usdc=1.0,
        side="no",
        fill_block=1,
        resolved_block=10,
        payout_numerators='["1", "0"]',
    )
    assert mark.kind == "resolve"
    assert abs(mark.recover_usdc) < 1e-12
    assert abs(mark.pnl - (-1.0)) < 1e-12


def test_cutover_is_between_grace_and_twelve_hours():
    from sim_lib import BLOCKS_PER_HOUR, GRACE_BLOCKS
    assert CUTOVER_BLOCKS > GRACE_BLOCKS
    assert CUTOVER_BLOCKS < 12 * BLOCKS_PER_HOUR


def test_merge_both_legs_locks_the_dollar():
    from sim_lib import Fill

    yes = Fill(shares=5.0, usdc=2.0, vwap=0.40, block=10)
    no = Fill(shares=5.0, usdc=2.5, vwap=0.50, block=12)
    out = merge_and_inventory(
        filled_yes=yes,
        filled_no=no,
        fee_yes=0.0,
        fee_no=0.0,
        fill_block_yes=10,
        fill_block_no=12,
        resolved_block=None,
        payout_numerators=None,
        dump_yes=[],
        dump_no=[],
    )
    assert out["state"] == "both"
    assert abs(out["pair_cost"] - 0.90) < 1e-12
    assert abs(out["merge_pnl"] - 0.5) < 1e-12  # 5 * (1-0.90)
    assert abs(out["pnl"] - 0.5) < 1e-12
    assert out["inventory_q"] == 0.0


def test_yes_only_uses_inventory_recipe():
    from sim_lib import Fill

    yes = Fill(shares=5.0, usdc=2.5, vwap=0.50, block=10)
    out = merge_and_inventory(
        filled_yes=yes,
        filled_no=None,
        fee_yes=0.0,
        fee_no=0.0,
        fill_block_yes=10,
        fill_block_no=None,
        resolved_block=20,
        payout_numerators='["0", "1"]',
        dump_yes=[],
        dump_no=[],
    )
    assert out["state"] == "yes_only"
    assert out["merge_pnl"] == 0.0
    assert out["inventory_kind"] == "resolve"
    assert abs(out["pnl"] - (-2.5)) < 1e-12  # YES pays 0


def test_taker_fee_is_zero_on_fee_free_and_scales_with_lopsidedness():
    assert taker_fee_usdc(5.0, 0.50, False) == 0.0
    fee_mid = taker_fee_usdc(5.0, 0.50, True)
    fee_lopsided = taker_fee_usdc(5.0, 0.90, True)
    assert fee_mid > fee_lopsided
    assert abs(fee_mid - 0.0135 * 0.5 * 5.0) < 1e-12
