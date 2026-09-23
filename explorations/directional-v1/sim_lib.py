"""Directional FOK / GTC fill mechanics. Shares and USDC as float, prices in (0, 1).

Walks and clip live in `explorations.frame` (see FRAME.md). This module
keeps the one-sided windows, Stage A persist constants, and round-trip.
"""

from __future__ import annotations

import json
import sys
from pathlib import Path

_ROOT = Path(__file__).resolve().parents[2]
if str(_ROOT) not in sys.path:
    sys.path.insert(0, str(_ROOT))

from explorations.frame.clip import (  # noqa: E402
    BLOCKS_PER_DAY,
    BLOCK_SEC,
    COOLDOWN_BLOCKS,
    LEGAL_TICKS,
    LOOKBACK_BLOCKS,
    MIN_SHARES,
    MIN_WORST_NOTIONAL,
    TAKER_FEE_RATE,
    TICK,
    clip_px,
    infer_tick,
    n_min,
    taker_fee_usdc,
    yes_px,
)
from explorations.frame.ticket import CapTouch, ShareNeed  # noqa: E402
from explorations.frame.walk import Level, walk_rest_sell, walk_take_buy  # noqa: E402

K_MAX = 5  # C shortlist uses k in {1,3,4,5}
# H1 = X+1 (FOK bar). H60 = X+2..X+60 (GTC window, excludes X+1).
HORIZONS = (1, 60)
# Stage A persist + Kris Kross. Live formula.json is 3-of-8 / criss 5.
# STAGE_A.md freeze card is 6-of-8 / Kris 10. See explorations/README.md.
PREFILTER_WINDOW = 8
PREFILTER_NEED = 3
PREFILTER_KRIS_MIN = 5
ENTRY_LAG = 1
EXIT_START = 2
EXIT_END = 60
LIQ_START = 61
LIQ_END = 120
BUCKETS = (1, 5, 15, 30, 60, 100)
MIN_PRICE = TICK
MAX_PRICE = 1.0 - TICK


def prefilter_alive(fill_blocks: set[int], x: int,
                    window: int = PREFILTER_WINDOW,
                    need: int = PREFILTER_NEED) -> bool:
    """True if `need` of `{x, x-1, ..., x-window+1}` have a fill on this condition."""
    n = 0
    for d in range(window):
        if (x - d) in fill_blocks:
            n += 1
            if n >= need:
                return True
    return False


def kris_count(px) -> int:
    """Kris Kross zigzag count on maker YES prices, oldest → newest.

    Skip equals of the first print. First ≠ first-price sets direction and
    count=1. Stay while equal-or-with direction vs the previous fill; first
    move against flips direction and increments count. Empty or all-equal → 0.
    """
    n = len(px)
    if n == 0:
        return 0
    first = float(px[0])
    i = 1
    while i < n and float(px[i]) == first:
        i += 1
    if i >= n:
        return 0
    direction = 1 if float(px[i]) > first else -1
    count = 1
    prev = float(px[i])
    i += 1
    while i < n:
        p = float(px[i])
        if direction == 1:
            if p < prev:
                direction = -1
                count += 1
        elif p > prev:
            direction = 1
            count += 1
        prev = p
        i += 1
    return count


def settlement_yes(payout) -> float | None:
    """YES-equivalent settlement: 1.0 YES wins, 0.0 NO wins, 0.5 draw/void."""
    if payout is None:
        return None
    if isinstance(payout, float) and payout != payout:
        return None
    if isinstance(payout, str):
        try:
            arr = json.loads(payout)
        except json.JSONDecodeError:
            return None
    else:
        arr = payout
    try:
        nums = [float(x) for x in arr]
    except (TypeError, ValueError):
        return None
    if len(nums) < 2:
        return None
    s = nums[0] + nums[1]
    if s <= 0:
        return None
    return nums[0] / s


def fold_resolve(a, h, l, lo: int, hi: int, resolved_block, settle_yes: float | None):
    """If resolve is inside [lo, hi], redeem is infinite liquidity at settle.

    Does not use anything after `hi`. Empty-window sentinels (h=0, l=1)
    still work: max/min with 1.0 or 0.0 is the right high/low.
    """
    if resolved_block is None or settle_yes is None:
        return int(a), float(h), float(l)
    rb = int(resolved_block)
    if lo <= rb <= hi:
        sy = float(settle_yes)
        return 1, max(float(h), sy), min(float(l), sy)
    return int(a), float(h), float(l)





def offset_caps(
    close_side: float, d_entry: int, d_exit: int, tick: float = TICK
) -> tuple[float, float] | None:
    """P_entry = close + d_entry ticks, P_exit = close + d_exit ticks.

    Does not require P_exit < P_entry. A long (buy then sell higher) needs
    P_exit > P_entry. Callers that want a locked-loss MM ticket must pass
    d_exit < d_entry themselves.
    """
    p_in = clip_px(close_side + d_entry * tick, tick)
    p_out = clip_px(close_side + d_exit * tick, tick)
    if p_in <= 0 or p_out <= 0:
        return None
    return p_in, p_out


def long_caps(
    close_side: float, d_entry: int, d_exit: int, tick: float = TICK
) -> tuple[float, float] | None:
    """Buy cap and sell floor with at least 1 tick of upside (P_exit > P_entry)."""
    caps = offset_caps(close_side, d_entry, d_exit, tick)
    if caps is None or caps[1] <= caps[0]:
        return None
    return caps


def naive_caps(close_side: float, tick: float = TICK) -> tuple[float, float] | None:
    """P_entry = close+tick, P_exit = close-tick. None if illegal."""
    return offset_caps(close_side, 1, -1, tick)


def walk_fok_buy(levels: list[Level], *, cap: float, n: float) -> tuple[float, float] | None:
    """FOK buy: inclusive cap-touch, at least n shares. Returns (usdc, vwap)."""
    fill = walk_take_buy(
        levels, cap=cap, n=n,
        cap_touch=CapTouch.INCLUSIVE, share_need=ShareNeed.AT_LEAST,
    )
    if fill is None:
        return None
    return fill.usdc, fill.vwap


def simulate_roundtrip(
    *,
    n: float,
    p_entry: float,
    p_exit: float,
    entry_asks: list[Level],
    exit_by_block: list[tuple[int, list[Level]]],
    liq_by_block: list[tuple[int, list[Level]]],
    x_block: int,
    fee_market: bool,
    exit_end: int = EXIT_END,
    liq_start: int = LIQ_START,
    liq_end: int = LIQ_END,
) -> dict:
    """Entry FOK at X+1; GTC sell X+2..exit_end; dump liq_start..liq_end; rest $0."""
    empty = {
        "entry_ok": False,
        "q": 0.0,
        "usdc_in": 0.0,
        "q_exit": 0.0,
        "usdc_exit": 0.0,
        "q_liq": 0.0,
        "usdc_liq": 0.0,
        "q_zero": 0.0,
        "fees_usdc": 0.0,
        "pnl": 0.0,
        "state": "miss",
    }
    got = walk_fok_buy(entry_asks, cap=p_entry, n=n)
    if got is None:
        return empty
    usdc_in, vwap_in = got
    fee_in = taker_fee_usdc(n, vwap_in, fee_market)
    remaining = n
    q_exit = 0.0
    usdc_exit = 0.0
    for blk, levels in sorted(exit_by_block, key=lambda r: r[0]):
        if remaining <= 1e-12:
            break
        rel = blk - x_block
        if rel == 2:
            touch = CapTouch.STRICT
        elif 3 <= rel <= exit_end:
            touch = CapTouch.INCLUSIVE
        else:
            continue
        taken, usd = walk_rest_sell(
            levels, floor=p_exit, remaining=remaining, cap_touch=touch,
        )
        q_exit += taken
        usdc_exit += usd
        remaining -= taken

    q_liq = 0.0
    usdc_liq = 0.0
    if remaining > 1e-12:
        for blk, levels in sorted(liq_by_block, key=lambda r: r[0]):
            if remaining <= 1e-12:
                break
            rel = blk - x_block
            if rel < liq_start or rel > liq_end:
                continue
            taken, usd = walk_rest_sell(
                levels, floor=0.0, remaining=remaining, cap_touch=CapTouch.INCLUSIVE,
            )
            q_liq += taken
            usdc_liq += usd
            remaining -= taken

    q_zero = max(0.0, remaining)
    fee_liq = taker_fee_usdc(q_liq, (usdc_liq / q_liq) if q_liq > 1e-12 else 0.0, fee_market)
    fees = fee_in + fee_liq
    pnl = usdc_exit + usdc_liq - usdc_in - fees
    if q_zero <= 1e-12 and q_liq <= 1e-12:
        state = "exit_full"
    elif q_exit <= 1e-12 and q_zero <= 1e-12:
        state = "liq_full"
    elif q_exit <= 1e-12 and q_liq <= 1e-12:
        state = "zero"
    else:
        state = "mixed"
    return {
        "entry_ok": True,
        "q": n,
        "usdc_in": usdc_in,
        "q_exit": q_exit,
        "usdc_exit": usdc_exit,
        "q_liq": q_liq,
        "usdc_liq": usdc_liq,
        "q_zero": q_zero,
        "fees_usdc": fees,
        "pnl": pnl,
        "state": state,
        "vwap_in": vwap_in,
    }


def apply_cooldown(
    rows: list[tuple[int, str]], cooldown: int = COOLDOWN_BLOCKS
) -> list[tuple[int, str]]:
    """rows are (block, cid) sorted by cid, block. Keep first then +cooldown."""
    last: dict[str, int] = {}
    out: list[tuple[int, str]] = []
    for blk, cid in rows:
        prev = last.get(cid)
        if prev is None or blk >= prev + cooldown:
            out.append((blk, cid))
            last[cid] = blk
    return out
