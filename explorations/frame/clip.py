"""Desk clip, YES-price, fees, ticks. Shared by every exploration and trader."""

from __future__ import annotations

import math

MIN_SHARES = 5.0
MIN_WORST_NOTIONAL = 1.20
TICK = 0.01
LEGAL_TICKS = (0.1, 0.01, 0.005, 0.0025, 0.001, 0.0001)
TAKER_FEE_RATE = 0.0135
COOLDOWN_BLOCKS = 180
LOOKBACK_BLOCKS = 100
BLOCKS_PER_HOUR = 1714
BLOCKS_PER_DAY = 41136
BLOCK_SEC = 2.1


def n_min(p: float) -> int:
    """Smallest legal share count at worst price p: max(5, ceil(1.20 / p))."""
    if p <= 0:
        raise ValueError("p must be positive")
    return max(5, math.ceil(MIN_WORST_NOTIONAL / p - 1e-12))


def legal_clip(n: float, cap: float) -> bool:
    return n + 1e-12 >= MIN_SHARES and n * cap + 1e-12 >= MIN_WORST_NOTIONAL


def clip_px(p: float, tick: float | None = None) -> float:
    t = TICK if tick is None else tick
    return min(1.0 - t, max(t, p))


def yes_px(gross_usdc: float, net_yes_tokens: float) -> float | None:
    """YES-equivalent price. NO at p ↔ YES at 1−p."""
    if net_yes_tokens == 0:
        return None
    g, n = float(gross_usdc), float(net_yes_tokens)
    if (g > 0 and n > 0) or (g < 0 and n < 0):
        return g / n
    return 1.0 + g / n


def taker_fee_usdc(shares: float, px: float, fee_market: bool) -> float:
    if not fee_market or shares <= 0:
        return 0.0
    return TAKER_FEE_RATE * min(px, 1.0 - px) * shares


def infer_tick(prices: list[float]) -> float:
    """Coarsest documented tick that fits every price.

    Sparse 0.50/0.60 does not coarsen to 0.1.
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
