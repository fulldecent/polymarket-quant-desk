"""Pure fill-model helpers. Units: shares as float, USDC as float, prices in [0, 1].

See fill-model.md. Do not read this as CLOB law.
"""

from __future__ import annotations

import math
from dataclasses import dataclass

MIN_SHARES = 5.0
MIN_WORST_NOTIONAL = 1.20
TICK = 0.01
TAKER_FEE_RATE = 0.0135  # net ~135 bps of min(p, 1-p) per share, in USDC
COMPLEMENT_BLOCKS = 30
GRACE_BLOCKS = 15  # ~30s minimum time on the book
BLOCKS_PER_HOUR = 1714
BLOCKS_PER_DAY = 41136
# Cutover from fixed-price complement rest to market sale: 15 minutes.
# Floor is grace (30s); ceiling allowed is 12h. 15m is a session slice
# without sitting as a directional book for hours.
CUTOVER_BLOCKS = BLOCKS_PER_HOUR // 4  # 428
HORIZON_DAYS = 3
HORIZON_BLOCKS = BLOCKS_PER_DAY * HORIZON_DAYS  # 123_408
PAIR_COST_FIRE = 0.99
MIN_PRICE = TICK
MAX_PRICE = 1.0 - TICK


@dataclass(frozen=True)
class Level:
    px: float
    tokens: float


@dataclass(frozen=True)
class Fill:
    shares: float
    usdc: float
    vwap: float
    block: int


@dataclass(frozen=True)
class InventoryMark:
    kind: str  # complement | resolve | dump | zero
    pnl: float
    recover_usdc: float
    payout: float | None
    dump_shares: float
    dump_vwap: float | None
    at_block: int | None


def n_min(p: float) -> int:
    """Smallest legal share count at worst price p."""
    if p <= 0:
        raise ValueError("p must be positive")
    return max(5, math.ceil(MIN_WORST_NOTIONAL / p - 1e-12))


def clip_cap(p: float) -> float:
    return min(MAX_PRICE, max(MIN_PRICE, p))


def caps_from_close(close_yes: float, tick: float = TICK) -> tuple[float, float]:
    """P_yes, P_no from block-X close plus one tick. Not from X+1."""
    p_yes = clip_cap(close_yes + tick)
    p_no = clip_cap((1.0 - close_yes) + tick)
    return p_yes, p_no


def taker_fee_usdc(shares: float, px: float, fee_market: bool) -> float:
    if not fee_market or shares <= 0:
        return 0.0
    return TAKER_FEE_RATE * min(px, 1.0 - px) * shares


def walk_buy(
    levels: list[Level],
    *,
    cap: float,
    n: float,
    kappa: float = 1.0,
) -> Fill | None:
    """FOK buy: prices strictly below cap, need strictly more than n*kappa shares.

    `levels` are already in the required order (best-first within a block;
    block order for a multi-block window).
    """
    if n < MIN_SHARES or n * cap < MIN_WORST_NOTIONAL:
        return None
    need_avail = n * kappa
    available = 0.0
    for lv in levels:
        if lv.px >= cap:
            continue
        if lv.tokens <= 0:
            continue
        available += lv.tokens
    if available <= need_avail:
        return None
    remaining = n
    cost = 0.0
    for lv in levels:
        if remaining <= 0:
            break
        if lv.px >= cap or lv.tokens <= 0:
            continue
        take = min(remaining, lv.tokens)
        cost += take * lv.px
        remaining -= take
    if remaining > 1e-9:
        return None
    vwap = cost / n
    return Fill(shares=n, usdc=cost, vwap=vwap, block=0)


def walk_buy_window(
    by_block: list[tuple[int, list[Level]]],
    *,
    cap: float,
    n: float,
    kappa: float = 1.0,
) -> Fill | None:
    """Complement: blocks in order, best-first inside each block."""
    flat: list[Level] = []
    block_of: list[int] = []
    for blk, levels in by_block:
        ordered = sorted(levels, key=lambda lv: (lv.px, ))
        for lv in ordered:
            flat.append(lv)
            block_of.append(blk)
    fill = walk_buy(flat, cap=cap, n=n, kappa=kappa)
    if fill is None:
        return None
    remaining = n
    fill_block = by_block[0][0] if by_block else 0
    for lv, blk in zip(flat, block_of):
        if lv.px >= cap or lv.tokens <= 0:
            continue
        take = min(remaining, lv.tokens)
        remaining -= take
        fill_block = blk
        if remaining <= 1e-9:
            break
    return Fill(shares=fill.shares, usdc=fill.usdc, vwap=fill.vwap, block=fill_block)


def floor_walk_vwap(levels: list[Level]) -> tuple[float, float] | None:
    """Cheapest shares until N≥5 and N×p_marginal≥$1.20. Returns (vwap, shares)."""
    ordered = sorted((lv for lv in levels if lv.tokens > 0), key=lambda lv: lv.px)
    cum_sh = 0.0
    cum_usdc = 0.0
    for lv in ordered:
        cum_sh += lv.tokens
        cum_usdc += lv.tokens * lv.px
        if cum_sh >= MIN_SHARES and cum_sh * lv.px >= MIN_WORST_NOTIONAL:
            return cum_usdc / cum_sh, cum_sh
    return None


def walk_dump(levels: list[Level], q: float) -> tuple[float, float, float] | None:
    """Forced sell: best bid first. Largest legal prefix with n≤q.

    Returns (shares, proceeds, vwap) or None if no legal dump.
    """
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
        new_taken = taken + take
        new_proc = proceeds + take * lv.px
        new_worst = lv.px if worst is None else min(worst, lv.px)
        taken, proceeds, worst = new_taken, new_proc, new_worst
        if taken + 1e-12 >= MIN_SHARES and taken * worst >= MIN_WORST_NOTIONAL:
            best_legal = (taken, proceeds, proceeds / taken)
    return best_legal


def yes_payout(payout_numerators: str | None) -> float | None:
    if not payout_numerators:
        return None
    s = "".join(payout_numerators.split())
    if s == '["1","0"]':
        return 1.0
    if s == '["0","1"]':
        return 0.0
    if s == '["1","1"]':
        return 0.5
    return None


def first_dump_block_order(
    by_block: list[tuple[int, list[Level]]], q: float
) -> tuple[tuple[float, float, float], int] | None:
    """First legal dump, block order, best bid inside each block. Not the 3-day high."""
    for blk, levels in by_block:
        dumped = walk_dump(levels, q)
        if dumped is not None:
            return dumped, blk
    return None


def neutralize_position(
    *,
    q: float,
    entry_usdc: float,
    entry_fees: float,
    side: str,
    p_comp: float,
    fill_block: int,
    complement_by_block: list[tuple[int, list[Level]]],
    dump_by_block: list[tuple[int, list[Level]]],
    resolved_block: int | None,
    payout_numerators: str | None,
    fee_market: bool = False,
    kappa: float = 1.0,
    cutover_blocks: int = CUTOVER_BLOCKS,
    horizon_blocks: int = HORIZON_BLOCKS,
) -> InventoryMark:
    """Exit ladder from a one-sided fill.

    1. Rest a complement **buy** at fixed ``p_comp`` until cutover (15 min).
       Minimum time on the book is grace (~30s / 15 blocks); cutover is later.
    2. Then market-sell the held token at whatever bid is there, block order.
    3. Still open at 3 days → **$0**.

    Earliest event wins. Resolution inside 3 days counts if it beats the
    complement fill and the dump.
    """
    zero = InventoryMark(
        "zero", -entry_usdc - entry_fees, 0.0, None, 0.0, None, fill_block + horizon_blocks
    )
    cutover = fill_block + cutover_blocks
    horizon_end = fill_block + horizon_blocks
    events: list[tuple[int, int, str, object]] = []

    legal_comp = q + 1e-12 >= MIN_SHARES and q * p_comp >= MIN_WORST_NOTIONAL
    if legal_comp and complement_by_block:
        window = [(b, lv) for b, lv in complement_by_block if fill_block < b <= cutover]
        comp = walk_buy_window(window, cap=p_comp, n=q, kappa=kappa) if window else None
        if comp is not None and fill_block < comp.block <= cutover:
            events.append((comp.block, 0, "complement", comp))

    if resolved_block is not None and fill_block < resolved_block <= horizon_end:
        events.append((int(resolved_block), 1, "resolve", None))

    dump_window = [(b, lv) for b, lv in dump_by_block if cutover < b <= horizon_end]
    dumped = first_dump_block_order(dump_window, q)
    if dumped is not None:
        payload, blk = dumped
        events.append((blk, 2, "dump", payload))

    if not events:
        return zero
    events.sort()
    at, _, kind, payload = events[0]

    if kind == "complement":
        fill = payload
        assert isinstance(fill, Fill)
        fee_c = taker_fee_usdc(fill.shares, fill.vwap, fee_market)
        # Merge q YES+NO for $q; paid entry_usdc + complement usdc.
        recover = q * 1.0 - fill.usdc - fee_c
        pnl = recover - entry_usdc - entry_fees
        return InventoryMark("complement", pnl, recover, None, fill.shares, fill.vwap, fill.block)

    if kind == "resolve":
        pay = yes_payout(payout_numerators)
        if pay is None:
            return zero
        if side == "no":
            pay = 1.0 - pay
        recover = q * pay
        pnl = recover - entry_usdc - entry_fees
        return InventoryMark("resolve", pnl, recover, pay, 0.0, None, at)

    shares, proceeds, vwap = payload  # type: ignore[misc]
    recover = proceeds
    pnl = recover - entry_usdc - entry_fees
    return InventoryMark("dump", pnl, recover, None, shares, vwap, at)


def _neutralize_kwargs(
    *,
    side: str,
    p_yes: float,
    p_no: float,
    complement_yes: list[tuple[int, list[Level]]],
    complement_no: list[tuple[int, list[Level]]],
    dump_yes: list[tuple[int, list[Level]]],
    dump_no: list[tuple[int, list[Level]]],
) -> dict:
    if side == "yes":
        return {
            "p_comp": p_no,
            "complement_by_block": complement_no,
            "dump_by_block": dump_yes,
        }
    return {
        "p_comp": p_yes,
        "complement_by_block": complement_yes,
        "dump_by_block": dump_no,
    }


def merge_and_inventory(
    *,
    filled_yes: Fill | None,
    filled_no: Fill | None,
    fee_yes: float,
    fee_no: float,
    fill_block_yes: int | None,
    fill_block_no: int | None,
    resolved_block: int | None,
    payout_numerators: str | None,
    p_yes: float = 0.5,
    p_no: float = 0.5,
    complement_yes: list[tuple[int, list[Level]]] | None = None,
    complement_no: list[tuple[int, list[Level]]] | None = None,
    dump_yes: list[tuple[int, list[Level]]] | None = None,
    dump_no: list[tuple[int, list[Level]]] | None = None,
    fee_market: bool = False,
    kappa: float = 1.0,
) -> dict:
    """Headline vector for one attempt. Two buys; never a buy+sell ticket."""
    if filled_yes is None and filled_no is None:
        return {
            "state": "miss",
            "filled_yes": False,
            "filled_no": False,
            "q_yes": 0.0,
            "q_no": 0.0,
            "usdc_yes": 0.0,
            "usdc_no": 0.0,
            "pair_cost": None,
            "merge_pnl": 0.0,
            "inventory_q": 0.0,
            "inventory_side": None,
            "inventory_kind": None,
            "inventory_pnl": 0.0,
            "fees_usdc": 0.0,
            "pnl": 0.0,
            "capital_blocks": 0,
        }

    q_yes = filled_yes.shares if filled_yes else 0.0
    q_no = filled_no.shares if filled_no else 0.0
    u_yes = filled_yes.usdc if filled_yes else 0.0
    u_no = filled_no.usdc if filled_no else 0.0
    fees = fee_yes + fee_no

    if filled_yes is not None and filled_no is not None:
        pairs = min(q_yes, q_no)
        pair_cost = (u_yes / q_yes) + (u_no / q_no)
        # Cost basis of merged pairs; leftover keeps its pro-rata cost.
        merge_cost = pairs * (u_yes / q_yes) + pairs * (u_no / q_no)
        fee_pairs = fees * (2 * pairs) / (q_yes + q_no) if (q_yes + q_no) else fees
        merge_pnl = pairs * 1.0 - merge_cost - fee_pairs
        leftover = abs(q_yes - q_no)
        if leftover < 1e-9:
            return {
                "state": "both",
                "filled_yes": True,
                "filled_no": True,
                "q_yes": q_yes,
                "q_no": q_no,
                "usdc_yes": u_yes,
                "usdc_no": u_no,
                "pair_cost": pair_cost,
                "merge_pnl": merge_pnl,
                "inventory_q": 0.0,
                "inventory_side": None,
                "inventory_kind": None,
                "inventory_pnl": 0.0,
                "fees_usdc": fees,
                "pnl": merge_pnl,
                "capital_blocks": (filled_no.block - filled_yes.block)
                if filled_no.block and filled_yes.block
                else 0,
            }
        if q_yes > q_no:
            side = "yes"
            entry = u_yes * leftover / q_yes
            fee_left = fee_yes * leftover / q_yes
            fill_blk = fill_block_yes or filled_yes.block
        else:
            side = "no"
            entry = u_no * leftover / q_no
            fee_left = fee_no * leftover / q_no
            fill_blk = fill_block_no or filled_no.block
        mark = neutralize_position(
            q=leftover,
            entry_usdc=entry,
            entry_fees=fee_left,
            side=side,
            fill_block=fill_blk,
            resolved_block=resolved_block,
            payout_numerators=payout_numerators,
            fee_market=fee_market,
            kappa=kappa,
            **_neutralize_kwargs(
                side=side,
                p_yes=p_yes,
                p_no=p_no,
                complement_yes=complement_yes or [],
                complement_no=complement_no or [],
                dump_yes=dump_yes or [],
                dump_no=dump_no or [],
            ),
        )
        return {
            "state": "both",
            "filled_yes": True,
            "filled_no": True,
            "q_yes": q_yes,
            "q_no": q_no,
            "usdc_yes": u_yes,
            "usdc_no": u_no,
            "pair_cost": pair_cost,
            "merge_pnl": merge_pnl,
            "inventory_q": leftover,
            "inventory_side": side,
            "inventory_kind": mark.kind,
            "inventory_pnl": mark.pnl,
            "fees_usdc": fees,
            "pnl": merge_pnl + mark.pnl,
            "capital_blocks": HORIZON_BLOCKS if mark.kind != "resolve" else 0,
        }

    if filled_yes is not None:
        state = "yes_only"
        side = "yes"
        q, entry, fee_left, fill_blk = (
            q_yes, u_yes, fee_yes, fill_block_yes or filled_yes.block,
        )
    else:
        state = "no_only"
        side = "no"
        q, entry, fee_left, fill_blk = (
            q_no, u_no, fee_no, fill_block_no or filled_no.block,
        )
    mark = neutralize_position(
        q=q,
        entry_usdc=entry,
        entry_fees=fee_left,
        side=side,
        fill_block=fill_blk,
        resolved_block=resolved_block,
        payout_numerators=payout_numerators,
        fee_market=fee_market,
        kappa=kappa,
        **_neutralize_kwargs(
            side=side,
            p_yes=p_yes,
            p_no=p_no,
            complement_yes=complement_yes or [],
            complement_no=complement_no or [],
            dump_yes=dump_yes or [],
            dump_no=dump_no or [],
        ),
    )
    return {
        "state": state,
        "filled_yes": filled_yes is not None,
        "filled_no": filled_no is not None,
        "q_yes": q_yes,
        "q_no": q_no,
        "usdc_yes": u_yes,
        "usdc_no": u_no,
        "pair_cost": None,
        "merge_pnl": 0.0,
        "inventory_q": q,
        "inventory_side": side,
        "inventory_kind": mark.kind,
        "inventory_pnl": mark.pnl,
        "fees_usdc": fees,
        "pnl": mark.pnl,
        "capital_blocks": HORIZON_BLOCKS if mark.kind != "resolve" else 0,
    }
