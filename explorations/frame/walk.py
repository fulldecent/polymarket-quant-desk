"""Tape walks: TAKE a buy, REST a sell, DUMP at any bid.

Interpretation C: buy-take walks maker-sell (asks). Sell-make walks
taker-buy volume (the tape that hits a resting sell). Dump walks
maker-buy (bids). Always best-first inside a block; windows keep
block order (do not globally sort).
"""

from __future__ import annotations

from dataclasses import dataclass

from explorations.frame.clip import MIN_SHARES, MIN_WORST_NOTIONAL, legal_clip
from explorations.frame.ticket import CapTouch, ShareNeed


@dataclass(frozen=True)
class Level:
    px: float
    tokens: float


@dataclass(frozen=True)
class Fill:
    shares: float
    usdc: float
    vwap: float
    block: int = 0


def _buy_ok(px: float, cap: float, touch: CapTouch) -> bool:
    if touch is CapTouch.INCLUSIVE:
        return px <= cap + 1e-15
    return px < cap - 1e-15


def _sell_ok(px: float, floor: float, touch: CapTouch) -> bool:
    if touch is CapTouch.INCLUSIVE:
        return px + 1e-15 >= floor
    return px > floor + 1e-15


def walk_take_buy(
    levels: list[Level],
    *,
    cap: float,
    n: float,
    cap_touch: CapTouch,
    share_need: ShareNeed = ShareNeed.AT_LEAST,
    kappa: float = 1.0,
    block: int = 0,
    order: str = "best",
) -> Fill | None:
    """FOK/FAK-style buy against asks. All-or-nothing for n.

    `order="best"` sorts lowest px first (single-block FOK).
    `order="given"` keeps caller order (block-concatenated windows).
    """
    if not legal_clip(n, cap):
        return None
    eligible = [
        lv for lv in levels if lv.tokens > 0 and _buy_ok(lv.px, cap, cap_touch)
    ]
    ordered = sorted(eligible, key=lambda lv: lv.px) if order == "best" else eligible
    avail = sum(lv.tokens for lv in ordered)
    need = n * kappa
    if share_need is ShareNeed.STRICTLY_MORE:
        if avail <= need + 1e-12:
            return None
    elif avail + 1e-12 < need:
        return None
    left, cost = n, 0.0
    for lv in ordered:
        take = min(left, lv.tokens)
        cost += take * lv.px
        left -= take
        if left <= 1e-12:
            break
    if left > 1e-12:
        return None
    return Fill(shares=n, usdc=cost, vwap=cost / n, block=block)


def walk_take_buy_window(
    by_block: list[tuple[int, list[Level]]],
    *,
    cap: float,
    n: float,
    cap_touch: CapTouch,
    share_need: ShareNeed = ShareNeed.AT_LEAST,
    kappa: float = 1.0,
) -> Fill | None:
    """Complement / delayed take: block order, best-first inside each block."""
    flat: list[Level] = []
    block_of: list[int] = []
    for blk, levels in by_block:
        for lv in sorted(levels, key=lambda x: x.px):
            flat.append(lv)
            block_of.append(blk)
    fill = walk_take_buy(
        flat, cap=cap, n=n, cap_touch=cap_touch, share_need=share_need, kappa=kappa,
        order="given",
    )
    if fill is None:
        return None
    left = n
    fill_block = by_block[0][0] if by_block else 0
    for lv, blk in zip(flat, block_of):
        if not _buy_ok(lv.px, cap, cap_touch) or lv.tokens <= 0:
            continue
        take = min(left, lv.tokens)
        left -= take
        fill_block = blk
        if left <= 1e-9:
            break
    return Fill(shares=fill.shares, usdc=fill.usdc, vwap=fill.vwap, block=fill_block)


def walk_rest_sell(
    levels: list[Level],
    *,
    floor: float,
    remaining: float,
    cap_touch: CapTouch,
) -> tuple[float, float]:
    """GTC sell against qualifying prints. Best (highest) first. Partials OK.

    Returns (shares_taken, usdc). Caller sets STRICT on the first rest
    block (no time priority) and INCLUSIVE after.
    """
    if remaining <= 1e-12:
        return 0.0, 0.0
    ordered = sorted(
        (lv for lv in levels if lv.tokens > 0 and _sell_ok(lv.px, floor, cap_touch)),
        key=lambda lv: -lv.px,
    )
    taken, usdc, left = 0.0, 0.0, remaining
    for lv in ordered:
        take = min(left, lv.tokens)
        taken += take
        usdc += take * lv.px
        left -= take
        if left <= 1e-12:
            break
    return taken, usdc


def walk_dump(levels: list[Level], q: float) -> tuple[float, float, float] | None:
    """Forced sell, best bid first. Largest legal prefix with n ≤ q."""
    if q < MIN_SHARES:
        return None
    ordered = sorted((lv for lv in levels if lv.tokens > 0), key=lambda lv: -lv.px)
    taken = 0.0
    proceeds = 0.0
    worst = None
    best_legal: tuple[float, float, float] | None = None
    for lv in ordered:
        if taken >= q:
            break
        take = min(q - taken, lv.tokens)
        taken += take
        proceeds += take * lv.px
        worst = lv.px if worst is None else min(worst, lv.px)
        if taken + 1e-12 >= MIN_SHARES and taken * worst >= MIN_WORST_NOTIONAL:
            best_legal = (taken, proceeds, proceeds / taken)
    return best_legal
