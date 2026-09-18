"""Directional FOK / GTC fill mechanics. Shares and USDC as float, prices in (0, 1)."""

from __future__ import annotations

import json
import math
from dataclasses import dataclass

MIN_SHARES = 5.0
MIN_WORST_NOTIONAL = 1.20
TICK = 0.01  # fallback only; infer_tick() is the Stage B step size
LEGAL_TICKS = (0.1, 0.01, 0.005, 0.0025, 0.001, 0.0001)  # coarsest → finest
K_MAX = 5  # C shortlist uses k in {1,3,4,5}
# H1 = X+1 (FOK bar). H60 = X+2..X+60 (GTC window, excludes X+1).
HORIZONS = (1, 60)
TAKER_FEE_RATE = 0.0135
COOLDOWN_BLOCKS = 180
LOOKBACK_BLOCKS = 100
# Stage A persist. Need this many of the last N blocks (including X)
# to have a fill. Frozen in STAGE_A.md. Do not retune.
PREFILTER_WINDOW = 8
PREFILTER_NEED = 5
ENTRY_LAG = 1
EXIT_START = 2
EXIT_END = 60
LIQ_START = 61
LIQ_END = 120
BUCKETS = (1, 5, 15, 30, 60, 100)
MIN_PRICE = TICK
MAX_PRICE = 1.0 - TICK
BLOCKS_PER_DAY = 41136
BLOCK_SEC = 2.1


@dataclass(frozen=True)
class Level:
    px: float
    tokens: float


def n_min(p: float) -> int:
    if p <= 0:
        raise ValueError("p must be positive")
    return max(5, math.ceil(MIN_WORST_NOTIONAL / p - 1e-12))


def yes_px(gross_usdc: float, net_yes_tokens: float) -> float | None:
    """YES-equivalent price of a fill. Complementary: NO at p ↔ YES at 1-p."""
    if net_yes_tokens == 0:
        return None
    g, n = float(gross_usdc), float(net_yes_tokens)
    if (g > 0 and n > 0) or (g < 0 and n < 0):
        return g / n
    return 1.0 + g / n


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


def infer_tick(prices: list[float]) -> float:
    """Coarsest documented tick that fits every price in the last-100-block window.

    Legal set: 0.1, 0.01, 0.005, 0.0025, 0.001, 0.0001
    (docs.polymarket.com/market-data/market-details).

    Sparse prints that all lie on 0.10 (e.g. only 0.50 and 0.60) must not
    coarsen past 0.01 — that is a 1¢ book until a 10¢-only grid is obvious.
    """
    uniq = sorted({round(float(p), 8) for p in prices if p is not None and 0 < p < 1})
    if not uniq:
        return 0.01

    def _fits(t: float) -> bool:
        return all(abs(round(p / t) * t - p) <= max(1e-9, t * 1e-6) for p in uniq)

    for t in LEGAL_TICKS:
        if not _fits(t):
            continue
        if t == 0.1 and len(uniq) < 3:
            continue
        return t
    return 0.0001


def clip_px(p: float, tick: float | None = None) -> float:
    t = TICK if tick is None else tick
    return min(1.0 - t, max(t, p))


def taker_fee_usdc(shares: float, px: float, fee_market: bool) -> float:
    if not fee_market or shares <= 0:
        return 0.0
    return TAKER_FEE_RATE * min(px, 1.0 - px) * shares


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
    """FOK buy: px <= cap, need at least n shares. Cap-touch counts. Returns (usdc, vwap)."""
    if n < MIN_SHARES or n * cap < MIN_WORST_NOTIONAL:
        return None
    ordered = sorted((lv for lv in levels if lv.tokens > 0 and lv.px <= cap + 1e-15), key=lambda lv: lv.px)
    avail = sum(lv.tokens for lv in ordered)
    if avail + 1e-12 < n:
        return None
    left = n
    cost = 0.0
    for lv in ordered:
        take = min(left, lv.tokens)
        cost += take * lv.px
        left -= take
        if left <= 1e-12:
            break
    if left > 1e-12:
        return None
    return cost, cost / n


def _consume_sell(
    levels: list[Level], remaining: float, ok
) -> tuple[float, float]:
    """Hit highest qualifying px first. Returns (shares_taken, usdc)."""
    if remaining <= 1e-12:
        return 0.0, 0.0
    ordered = sorted((lv for lv in levels if lv.tokens > 0 and ok(lv.px)), key=lambda lv: -lv.px)
    taken = 0.0
    usdc = 0.0
    left = remaining
    for lv in ordered:
        take = min(left, lv.tokens)
        taken += take
        usdc += take * lv.px
        left -= take
        if left <= 1e-12:
            break
    return taken, usdc


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
            pred = lambda px, pe=p_exit: px > pe + 1e-15
        elif 3 <= rel <= exit_end:
            pred = lambda px, pe=p_exit: px + 1e-15 >= pe
        else:
            continue
        taken, usd = _consume_sell(levels, remaining, pred)
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
            taken, usd = _consume_sell(levels, remaining, lambda px: True)
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
