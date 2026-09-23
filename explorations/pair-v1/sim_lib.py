"""Pure fill-model helpers. Units: shares as float, USDC as float, prices in [0, 1].

Walks and clip live in `explorations.frame` (see FRAME.md). Two-sided
windows, inventory recipe, and pair-cost fire stay here.
"""

from __future__ import annotations

import sys
from dataclasses import dataclass
from pathlib import Path

_ROOT = Path(__file__).resolve().parents[2]
if str(_ROOT) not in sys.path:
    sys.path.insert(0, str(_ROOT))

from explorations.frame.clip import (  # noqa: E402
    BLOCKS_PER_DAY,
    BLOCKS_PER_HOUR,
    MIN_SHARES,
    MIN_WORST_NOTIONAL,
    TAKER_FEE_RATE,
    TICK,
    n_min,
    taker_fee_usdc,
)
from explorations.frame.ticket import CapTouch, ShareNeed  # noqa: E402
from explorations.frame.walk import (  # noqa: E402
    Fill,
    Level,
    walk_dump,
    walk_take_buy,
    walk_take_buy_window as _walk_take_buy_window,
)

COMPLEMENT_BLOCKS = 30
GRACE_BLOCKS = 15  # ~30s minimum time on the book
# Cutover from fixed-price complement rest to market sale: 15 minutes.
CUTOVER_BLOCKS = BLOCKS_PER_HOUR // 4  # 428
HORIZON_DAYS = 3
HORIZON_BLOCKS = BLOCKS_PER_DAY * HORIZON_DAYS  # 123_408
PAIR_COST_FIRE = 0.99
MIN_PRICE = TICK
MAX_PRICE = 1.0 - TICK


@dataclass(frozen=True)
class InventoryMark:
    kind: str  # complement | resolve | dump | zero
    pnl: float
    recover_usdc: float
    payout: float | None
    dump_shares: float
    dump_vwap: float | None
    at_block: int | None


def clip_cap(p: float) -> float:
    return min(MAX_PRICE, max(MIN_PRICE, p))


def caps_from_close(close_yes: float, tick: float = TICK) -> tuple[float, float]:
    """P_yes, P_no from block-X close plus one tick. Not from X+1."""
    p_yes = clip_cap(close_yes + tick)
    p_no = clip_cap((1.0 - close_yes) + tick)
    return p_yes, p_no


def walk_buy(
    levels: list[Level],
    *,
    cap: float,
    n: float,
    kappa: float = 1.0,
) -> Fill | None:
    """FOK buy: STRICT cap-touch, strictly more than n*kappa shares."""
    return walk_take_buy(
        levels, cap=cap, n=n, kappa=kappa,
        cap_touch=CapTouch.STRICT, share_need=ShareNeed.STRICTLY_MORE,
    )


def walk_buy_window(
    by_block: list[tuple[int, list[Level]]],
    *,
    cap: float,
    n: float,
    kappa: float = 1.0,
) -> Fill | None:
    """Complement: blocks in order, best-first inside each block."""
    return _walk_take_buy_window(
        by_block, cap=cap, n=n, kappa=kappa,
        cap_touch=CapTouch.STRICT, share_need=ShareNeed.STRICTLY_MORE,
    )


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
