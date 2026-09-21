"""Shared FOK / GTC / one-sided / two-sided semantics."""

from __future__ import annotations

from explorations.frame import (
    CapTouch,
    Intent,
    Level,
    Outcome,
    ShareNeed,
    TicketKind,
    Tif,
    n_min,
    one_sided_fok_then_gtc,
    two_sided_make_pair,
    two_sided_take_pair,
    walk_dump,
    walk_rest_sell,
    walk_take_buy,
    walk_take_buy_window,
    yes_px,
)


def test_n_min_and_yes_px():
    assert n_min(0.10) == 12
    assert n_min(0.50) == 5
    assert yes_px(40.0, 100.0) == 0.40
    assert abs(yes_px(-40.0, -100.0) - 0.40) < 1e-12
    assert abs(yes_px(60.0, -100.0) - 0.40) < 1e-12  # buy NO at 0.60 → YES 0.40


def test_fok_inclusive_vs_cmm_strict_are_the_same_walk():
    levels = [Level(0.50, 5.0)]
    fok = walk_take_buy(
        levels, cap=0.50, n=5, cap_touch=CapTouch.INCLUSIVE, share_need=ShareNeed.AT_LEAST,
    )
    cmm = walk_take_buy(
        levels, cap=0.50, n=5, cap_touch=CapTouch.STRICT, share_need=ShareNeed.STRICTLY_MORE,
    )
    assert fok is not None
    assert abs(fok.vwap - 0.50) < 1e-12
    assert cmm is None
    cmm_ok = walk_take_buy(
        [Level(0.40, 6.0)], cap=0.50, n=5,
        cap_touch=CapTouch.STRICT, share_need=ShareNeed.STRICTLY_MORE,
    )
    assert cmm_ok is not None
    assert abs(cmm_ok.vwap - 0.40) < 1e-12


def test_take_window_is_block_order_not_global_best():
    window = [
        (100, [Level(0.60, 10.0)]),
        (101, [Level(0.10, 10.0)]),
    ]
    fill = walk_take_buy_window(
        window, cap=0.90, n=5,
        cap_touch=CapTouch.STRICT, share_need=ShareNeed.STRICTLY_MORE,
    )
    assert fill is not None
    assert abs(fill.vwap - 0.60) < 1e-12
    assert fill.block == 100


def test_gtc_sell_first_block_strict_then_inclusive():
    prints = [Level(0.52, 10.0)]
    taken_strict, _ = walk_rest_sell(prints, floor=0.52, remaining=5, cap_touch=CapTouch.STRICT)
    taken_incl, usdc = walk_rest_sell(prints, floor=0.52, remaining=5, cap_touch=CapTouch.INCLUSIVE)
    assert taken_strict == 0.0
    assert abs(taken_incl - 5.0) < 1e-12
    assert abs(usdc - 2.60) < 1e-12


def test_dump_legal_prefix():
    levels = [Level(0.50, 5.0), Level(0.01, 100.0)]
    got = walk_dump(levels, q=105.0)
    assert got is not None
    sh, _proc, vwap = got
    assert abs(sh - 5.0) < 1e-12
    assert abs(vwap - 0.50) < 1e-12


def test_one_sided_and_two_sided_tickets_share_leg_shape():
    one = one_sided_fok_then_gtc(
        signal_block=10, condition_id="c", outcome=Outcome.YES,
        n=5, p_entry=0.51, p_exit=0.52,
    )
    pair = two_sided_take_pair(
        signal_block=10, condition_id="c", n_yes=5, n_no=5, p_yes=0.51, p_no=0.51,
    )
    make = two_sided_make_pair(
        signal_block=10, condition_id="c", n_yes=5, n_no=5, p_yes=0.49, p_no=0.48,
    )
    assert one.kind is TicketKind.ONE_SIDED
    assert pair.kind is TicketKind.TWO_SIDED
    assert make.kind is TicketKind.TWO_SIDED
    assert one.entry_legs()[0].tif is Tif.FOK
    assert one.exit_legs()[0].intent is Intent.MAKE
    assert all(lg.action.value == "buy" for lg in pair.legs)
    assert all(lg.intent is Intent.MAKE for lg in make.legs)
    assert {lg.outcome for lg in pair.legs} == {Outcome.YES, Outcome.NO}
