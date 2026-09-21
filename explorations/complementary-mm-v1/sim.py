#!/usr/bin/env python3
"""DEAD research line. Naive complementary-MM simulator (interpretation C, m=1).

yes_only + no_only was too large. See README.md.

Fires when block X has a floor walk on both asks with pair cost < 0.99.
Signs two buys from X close + 1 tick. Aggressive leg = more liquid ask
at X, fill window X+1. Complement = the other outcome, X+1..X+30.
One-sided exit: rest complement buy at the signed cap until 15 min
(grace 30s min on book), then market-sell the held token, $0 at 3 days.

Writes attempts parquet under SCRATCH_DIR and baseline-snapshot.md here.
Exploration: dirty git tree is fine. Not a catalog dataset.
"""

from __future__ import annotations

import argparse
import json
import os
import sys
import time
from collections import defaultdict
from dataclasses import replace
from datetime import datetime, timezone
from pathlib import Path

import duckdb
from dotenv import load_dotenv

_HERE = Path(__file__).resolve().parent
_PROJECT_ROOT = _HERE.parents[1]
sys.path.insert(0, str(_HERE))
sys.path.insert(0, str(_PROJECT_ROOT))
load_dotenv(_PROJECT_ROOT / ".env")

from lib.partition_utils import (  # noqa: E402
    PARTITION_10K_SIZE,
    partition_dir,
    partition_end,
    partition_start,
)
from sim_lib import (  # noqa: E402
    COMPLEMENT_BLOCKS,
    CUTOVER_BLOCKS,
    HORIZON_BLOCKS,
    Level,
    PAIR_COST_FIRE,
    caps_from_close,
    merge_and_inventory,
    n_min,
    taker_fee_usdc,
    walk_buy,
    walk_buy_window,
)

FILLS_DIR = os.environ.get("FILLS_V1_DIR", "")
C10K_DIR = os.environ.get("CONDITION_BY_10K_V1_DIR", "")
CBB_DIR = os.environ.get("CONDITION_BY_BLOCK_V1_DIR", "")
SCRATCH = os.environ.get("SCRATCH_DIR", "")
SNAP_PATH = _HERE / "baseline-snapshot.md"


def _fail(msg: str) -> None:
    sys.exit(msg)


def _parquet_in_range(root: Path, lo: int, hi: int) -> list[str]:
    files = []
    k = partition_start(lo)
    while k <= hi:
        p = root / partition_dir(k) / "data.parquet"
        if p.exists() and p.stat().st_size > 500:
            files.append(p.as_posix())
        k += PARTITION_10K_SIZE
    return files


def _sql_list(paths: list[str]) -> str:
    return "[" + ", ".join("'" + p.replace("'", "''") + "'" for p in paths) + "]"


def _frontier(root: Path) -> int:
    metas = sorted(root.glob("1M=*/10K=*/metadata.json"))
    if not metas:
        _fail(f"no partitions under {root}")
    last = json.loads(metas[-1].read_text())
    params = last.get("parameters") or {}
    return int(params.get("max_block") or 0)


def _connect() -> duckdb.DuckDBPyConnection:
    if not SCRATCH:
        _fail("SCRATCH_DIR is not set. Add it to .env.")
    Path(SCRATCH).mkdir(parents=True, exist_ok=True)
    con = duckdb.connect()
    con.execute(f"SET temp_directory = '{SCRATCH}'")
    con.execute("SET memory_limit = '10GB'")
    con.execute("SET threads = 4")
    return con


def _levels_from_rows(rows: list[tuple]) -> dict[tuple, list[Level]]:
    out: dict[tuple, list[Level]] = defaultdict(list)
    for block, cid, outcome, px, tokens in rows:
        if tokens and tokens > 0 and px and px > 0:
            out[(int(block), cid, outcome)].append(Level(float(px), float(tokens)))
    for key in out:
        out[key].sort(key=lambda lv: lv.px)
    return out


def run(
    trigger_days: float,
    interp: str,
    kappa: float,
    max_attempts: int | None,
    *,
    fee_free: bool = False,
) -> int:
    if interp != "C":
        _fail("v0 only implements interpretation C (maker-sell asks).")
    for name, val in (
        ("FILLS_V1_DIR", FILLS_DIR),
        ("CONDITION_BY_10K_V1_DIR", C10K_DIR),
        ("CONDITION_BY_BLOCK_V1_DIR", CBB_DIR),
    ):
        if not val or not Path(val).exists():
            _fail(f"{name} is not set or does not exist.")

    fills_root = Path(FILLS_DIR)
    frontier = _frontier(fills_root)
    # Trigger X must have fill+7d labels complete. Worst fill is X+30.
    x_hi = frontier - HORIZON_BLOCKS - COMPLEMENT_BLOCKS
    span = int(trigger_days * 41136)
    x_lo = x_hi - span + 1
    if x_lo < 0 or x_hi <= x_lo:
        _fail(f"not enough landed blocks for a 3-day complete window (frontier {frontier:,})")

    fill_lo = x_lo
    fill_hi = x_hi + COMPLEMENT_BLOCKS + HORIZON_BLOCKS
    started = datetime.now(timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ")
    snap_path = _HERE / ("fee-free-snapshot.md" if fee_free else "baseline-snapshot.md")
    print(
        f"naive baseline  interp=C  m=1  kappa={kappa:g}"
        + ("  fee-free" if fee_free else ""),
        flush=True,
    )
    print(f"  trigger X  {x_lo:,}–{x_hi:,}  ({trigger_days:g}d)", flush=True)
    print(f"  fills      {fill_lo:,}–{fill_hi:,}  frontier {frontier:,}", flush=True)

    fill_files = _parquet_in_range(fills_root, fill_lo, fill_hi)
    cbb_files = _parquet_in_range(Path(CBB_DIR), x_lo, x_hi)
    c10k_files = _parquet_in_range(Path(C10K_DIR), fill_lo, fill_hi)
    if not fill_files:
        _fail("no fills parquet in range")

    con = _connect()
    t0 = time.time()
    print(f"  loading {len(fill_files)} fills partitions …", flush=True)
    con.execute(
        f"""
        CREATE TABLE fills AS
        SELECT
            block_number,
            hex(condition_id) AS cid,
            is_taker,
            net_yes_tokens,
            gross_usdc,
            fee_usdc
        FROM read_parquet({_sql_list(fill_files)})
        WHERE block_number BETWEEN {fill_lo} AND {fill_hi}
        """
    )
    n_fills = con.execute("SELECT count(*) FROM fills").fetchone()[0]
    print(f"  fills rows {n_fills:,}  {time.time()-t0:.1f}s", flush=True)

    print("  ask/bid levels (interpretation C: maker sells / maker buys) …", flush=True)
    con.execute(
        """
        CREATE TABLE ask_levels AS
        SELECT
            block_number,
            cid,
            CASE
                WHEN net_yes_tokens < 0 THEN 'yes'
                ELSE 'no'
            END AS outcome,
            (-gross_usdc)::DOUBLE / abs(net_yes_tokens)::DOUBLE AS px,
            abs(net_yes_tokens)::DOUBLE / 1e6 AS tokens
        FROM fills
        WHERE NOT is_taker
          AND gross_usdc < 0
          AND net_yes_tokens <> 0
        """
    )
    con.execute(
        """
        CREATE TABLE bid_levels AS
        SELECT
            block_number,
            cid,
            CASE
                WHEN net_yes_tokens > 0 THEN 'yes'
                ELSE 'no'
            END AS outcome,
            (gross_usdc)::DOUBLE / abs(net_yes_tokens)::DOUBLE AS px,
            abs(net_yes_tokens)::DOUBLE / 1e6 AS tokens
        FROM fills
        WHERE NOT is_taker
          AND gross_usdc > 0
          AND net_yes_tokens <> 0
        """
    )
    con.execute(
        """
        CREATE TABLE ask_agg AS
        SELECT block_number, cid, outcome, px, sum(tokens) AS tokens
        FROM ask_levels
        GROUP BY 1, 2, 3, 4
        """
    )
    con.execute(
        """
        CREATE TABLE bid_agg AS
        SELECT block_number, cid, outcome, px, sum(tokens) AS tokens
        FROM bid_levels
        GROUP BY 1, 2, 3, 4
        """
    )

    print("  naive candidates (both ask floors, pair cost < 0.99) …", flush=True)
    con.execute(
        f"""
        CREATE TABLE floor_walks AS
        WITH ordered AS (
            SELECT
                block_number,
                cid,
                outcome,
                px,
                tokens,
                sum(tokens) OVER (
                    PARTITION BY block_number, cid, outcome
                    ORDER BY px
                    ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW
                ) AS cum_sh,
                sum(tokens * px) OVER (
                    PARTITION BY block_number, cid, outcome
                    ORDER BY px
                    ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW
                ) AS cum_usdc
            FROM ask_agg
            WHERE block_number BETWEEN {x_lo} AND {x_hi}
        ),
        flagged AS (
            SELECT
                *,
                (cum_sh >= 5 AND cum_sh * px >= 1.20) AS viable
            FROM ordered
        ),
        first_ok AS (
            SELECT
                block_number,
                cid,
                outcome,
                cum_usdc / cum_sh AS vwap,
                cum_sh AS shares,
                row_number() OVER (
                    PARTITION BY block_number, cid, outcome
                    ORDER BY px
                ) AS rn
            FROM flagged
            WHERE viable
        )
        SELECT block_number, cid, outcome, vwap, shares
        FROM first_ok
        WHERE rn = 1
        """
    )
    con.execute(
        f"""
        CREATE TABLE candidates AS
        SELECT
            y.block_number AS x_block,
            y.cid,
            y.vwap AS yes_vwap,
            n.vwap AS no_vwap,
            y.vwap + n.vwap AS pair_cost,
            y.shares AS yes_floor_sh,
            n.shares AS no_floor_sh
        FROM floor_walks y
        INNER JOIN floor_walks n
          ON y.block_number = n.block_number
         AND y.cid = n.cid
        WHERE y.outcome = 'yes' AND n.outcome = 'no'
          AND y.vwap + n.vwap < {PAIR_COST_FIRE}
        """
    )
    con.execute(
        """
        CREATE TABLE fee_cids AS
        SELECT cid
        FROM fills
        GROUP BY 1
        HAVING sum(fee_usdc) > 0
        """
    )
    if fee_free:
        con.execute(
            """
            CREATE OR REPLACE TABLE candidates AS
            SELECT c.*
            FROM candidates c
            LEFT JOIN fee_cids f ON f.cid = c.cid
            WHERE f.cid IS NULL
            """
        )
        print("  fee-free filter on", flush=True)
    n_cand = con.execute("SELECT count(*) FROM candidates").fetchone()[0]
    print(f"  candidates {n_cand:,}", flush=True)
    if n_cand == 0:
        _write_snapshot(
            started, x_lo, x_hi, frontier, interp, kappa, [], time.time() - t0, None,
            snap_path=snap_path, fee_free=fee_free,
        )
        print(f"wrote {snap_path}", flush=True)
        return 0

    if max_attempts is not None and n_cand > max_attempts:
        con.execute(
            f"""
            CREATE OR REPLACE TABLE candidates AS
            SELECT * FROM candidates
            ORDER BY x_block, cid
            LIMIT {int(max_attempts)}
            """
        )
        n_cand = max_attempts
        print(f"  capped to {n_cand:,} attempts", flush=True)

    close_sql = "NULL::DOUBLE"
    if cbb_files:
        con.execute(
            f"""
            CREATE TABLE closes AS
            SELECT
                block_number,
                hex(condition_id) AS cid,
                close_yes_price
            FROM read_parquet({_sql_list(cbb_files)})
            WHERE block_number BETWEEN {x_lo} AND {x_hi}
            """
        )
        close_sql = """(
            SELECT c.close_yes_price FROM closes c
            WHERE c.block_number = cand.x_block AND c.cid = cand.cid
        )"""

    if c10k_files:
        con.execute(
            f"""
            CREATE TABLE resolutions AS
            SELECT
                hex(condition_id) AS cid,
                min(resolved_block) AS resolved_block,
                any_value(payout_numerators) AS payout_numerators
            FROM read_parquet({_sql_list(c10k_files)})
            WHERE resolved_block IS NOT NULL
            GROUP BY 1
            """
        )
    else:
        con.execute(
            """
            CREATE TABLE resolutions AS
            SELECT NULL::VARCHAR AS cid, NULL::INTEGER AS resolved_block,
                   NULL::VARCHAR AS payout_numerators
            WHERE 1=0
            """
        )

    cand_rows = con.execute(
        f"""
        SELECT
            cand.x_block,
            cand.cid,
            cand.yes_vwap,
            cand.no_vwap,
            cand.pair_cost,
            cand.yes_floor_sh,
            cand.no_floor_sh,
            {close_sql} AS close_yes,
            fee_cids.cid IS NOT NULL AS fee_market,
            r.resolved_block,
            r.payout_numerators
        FROM candidates cand
        LEFT JOIN fee_cids ON fee_cids.cid = cand.cid
        LEFT JOIN resolutions r ON r.cid = cand.cid
        ORDER BY cand.x_block, cand.cid
        """
    ).fetchall()

    cids = tuple({row[1] for row in cand_rows})
    print(f"  pulling ask/bid levels for {len(cids):,} conditions …", flush=True)
    con.execute("CREATE TABLE cand_cids AS SELECT DISTINCT cid FROM candidates")
    ask_hi = x_hi + COMPLEMENT_BLOCKS + CUTOVER_BLOCKS
    ask_rows = con.execute(
        f"""
        SELECT a.block_number, a.cid, a.outcome, a.px, a.tokens
        FROM ask_agg a
        INNER JOIN cand_cids c ON c.cid = a.cid
        WHERE a.block_number BETWEEN {x_lo + 1} AND {ask_hi}
        """
    ).fetchall()
    bid_lo = x_lo + CUTOVER_BLOCKS
    bid_hi = x_hi + COMPLEMENT_BLOCKS + HORIZON_BLOCKS
    bid_rows = con.execute(
        f"""
        SELECT b.block_number, b.cid, b.outcome, b.px, b.tokens
        FROM bid_agg b
        INNER JOIN cand_cids c ON c.cid = b.cid
        WHERE b.block_number BETWEEN {bid_lo} AND {bid_hi}
        """
    ).fetchall()
    asks = _levels_from_rows(ask_rows)
    bids = _levels_from_rows(bid_rows)
    ask_idx: dict[tuple, list] = defaultdict(list)
    bid_idx: dict[tuple, list] = defaultdict(list)
    for (blk, cid, outcome), lv in asks.items():
        ask_idx[(cid, outcome)].append((blk, lv))
    for (blk, cid, outcome), lv in bids.items():
        bid_idx[(cid, outcome)].append((blk, lv))
    for idx in (ask_idx, bid_idx):
        for key in idx:
            idx[key].sort()
    print(
        f"  ask level-rows {len(ask_rows):,}  bid level-rows {len(bid_rows):,}",
        flush=True,
    )

    attempts = []
    n = len(cand_rows)
    t_sim = time.time()
    for i, row in enumerate(cand_rows, 1):
        (
            x_block, cid, yes_vwap, no_vwap, pair_cost, yes_sh, no_sh,
            close_yes, fee_market, resolved_block, payout_numerators,
        ) = row
        if close_yes is None or not (0 < close_yes < 1):
            continue
        p_yes, p_no = caps_from_close(float(close_yes))
        n_yes = float(n_min(p_yes))
        n_no = float(n_min(p_no))
        # Aggressive = more liquid floor at X (more shares in the floor walk).
        if (yes_sh or 0) >= (no_sh or 0):
            agg, comp = "yes", "no"
            p_agg, p_comp = p_yes, p_no
            n_agg, n_comp = n_yes, n_no
        else:
            agg, comp = "no", "yes"
            p_agg, p_comp = p_no, p_yes
            n_agg, n_comp = n_no, n_yes

        agg_levels = asks.get((x_block + 1, cid, agg), [])
        fill_agg = walk_buy(sorted(agg_levels, key=lambda lv: lv.px), cap=p_agg, n=n_agg, kappa=kappa)
        if fill_agg is not None:
            fill_agg = replace(fill_agg, block=x_block + 1)

        window = []
        for blk in range(x_block + 1, x_block + 1 + COMPLEMENT_BLOCKS):
            lv = asks.get((blk, cid, comp), [])
            if lv:
                window.append((blk, lv))
        fill_comp = walk_buy_window(window, cap=p_comp, n=n_comp, kappa=kappa) if window else None

        if agg == "yes":
            filled_yes, filled_no = fill_agg, fill_comp
        else:
            filled_yes, filled_no = fill_comp, fill_agg

        fee_yes = taker_fee_usdc(
            filled_yes.shares if filled_yes else 0.0,
            filled_yes.vwap if filled_yes else 0.0,
            bool(fee_market),
        )
        fee_no = taker_fee_usdc(
            filled_no.shares if filled_no else 0.0,
            filled_no.vwap if filled_no else 0.0,
            bool(fee_market),
        )

        fill_blk_yes = filled_yes.block if filled_yes else None
        fill_blk_no = filled_no.block if filled_no else None

        def _win(idx, outcome, lo, hi):
            return [(b, lv) for b, lv in idx.get((cid, outcome), []) if lo <= b <= hi]

        # Complement rest and dump windows are measured from the one-sided fill.
        fill_ref = fill_blk_yes or fill_blk_no or (x_block + 1)
        rest_lo, rest_hi = fill_ref + 1, fill_ref + CUTOVER_BLOCKS
        dump_lo, dump_hi = fill_ref + CUTOVER_BLOCKS + 1, fill_ref + HORIZON_BLOCKS

        vec = merge_and_inventory(
            filled_yes=filled_yes,
            filled_no=filled_no,
            fee_yes=fee_yes,
            fee_no=fee_no,
            fill_block_yes=fill_blk_yes,
            fill_block_no=fill_blk_no,
            resolved_block=int(resolved_block) if resolved_block is not None else None,
            payout_numerators=payout_numerators,
            p_yes=p_yes,
            p_no=p_no,
            complement_yes=_win(ask_idx, "yes", rest_lo, rest_hi),
            complement_no=_win(ask_idx, "no", rest_lo, rest_hi),
            dump_yes=_win(bid_idx, "yes", dump_lo, dump_hi),
            dump_no=_win(bid_idx, "no", dump_lo, dump_hi),
            fee_market=bool(fee_market),
            kappa=kappa,
        )
        vec.update(
            {
                "x_block": int(x_block),
                "cid": cid,
                "pair_cost_x": float(pair_cost),
                "close_yes": float(close_yes),
                "p_yes": p_yes,
                "p_no": p_no,
                "n_yes": n_yes,
                "n_no": n_no,
                "aggressive": agg,
                "fee_market": bool(fee_market),
                "size_multiple": 1.0,
            }
        )
        attempts.append(vec)
        if i % 2000 == 0 or i == n:
            print(f"  simulated {i:,}/{n:,}  {time.time()-t_sim:.1f}s", flush=True)

    out_dir = Path(SCRATCH) / "complementary-mm-v1"
    out_dir.mkdir(parents=True, exist_ok=True)
    out_parq = out_dir / (
        "naive_attempts_feefree.parquet" if fee_free else "naive_attempts.parquet"
    )
    if attempts:
        import pandas as pd

        df = pd.DataFrame(attempts)
        con.register("attempts_df", df)
        con.execute(
            f"COPY (SELECT * FROM attempts_df ORDER BY x_block, cid) TO '{out_parq.as_posix()}' (FORMAT PARQUET)"
        )
        print(f"  attempts parquet  {out_parq}  rows {len(df):,}", flush=True)

    elapsed = time.time() - t0
    _write_snapshot(
        started, x_lo, x_hi, frontier, interp, kappa, attempts, elapsed,
        out_parq if attempts else None,
        snap_path=snap_path, fee_free=fee_free,
    )
    print(f"wrote {snap_path}", flush=True)
    print(f"run complete  time: {elapsed:.1f}s", flush=True)
    con.close()
    return 0


def _write_snapshot(
    started: str,
    x_lo: int,
    x_hi: int,
    frontier: int,
    interp: str,
    kappa: float,
    attempts: list[dict],
    elapsed: float,
    parquet: Path | None = None,
    snap_path: Path | None = None,
    fee_free: bool = False,
) -> None:
    n = len(attempts)
    by_state = defaultdict(int)
    by_inv = defaultdict(int)
    pnl_state = defaultdict(float)
    pnl = merge = inv = fees = 0.0
    n_fee = n_free = 0
    pnl_fee = pnl_free = 0.0
    both_pair = []
    for a in attempts:
        st = a["state"]
        by_state[st] += 1
        pnl_state[st] += a["pnl"]
        if a.get("inventory_kind"):
            by_inv[a["inventory_kind"]] += 1
        pnl += a["pnl"]
        merge += a["merge_pnl"]
        inv += a["inventory_pnl"]
        fees += a["fees_usdc"]
        if a.get("fee_market"):
            n_fee += 1
            pnl_fee += a["pnl"]
        else:
            n_free += 1
            pnl_free += a["pnl"]
        if st == "both" and a.get("pair_cost") is not None:
            both_pair.append(a["pair_cost"])

    def pct(k: int) -> str:
        return f"{k:,} ({100.0 * k / n:.1f}%)" if n else "0"

    lines = [
        "# Naive baseline snapshot",
        "",
        f"Generated {started} by `explorations/complementary-mm-v1/sim.py`.",
        "",
        "Interpretation **C** (maker-sell asks). m=1. Fire if block X floor-walk "
        f"pair cost < {PAIR_COST_FIRE} on both outcomes. Caps = close_yes ± tick. "
        "Aggressive = more liquid ask at X. One-sided exit: complement buy at "
        f"signed cap until {CUTOVER_BLOCKS} blocks (~15 min, after 30s grace), "
        "then market-sell, **$0 at 3 days**.",
        "",
        f"- trigger X: {x_lo:,}–{x_hi:,}",
        f"- data frontier: {frontier:,}",
        f"- kappa: {kappa:g}",
        f"- elapsed: {elapsed:.1f}s",
        f"- parquet: `{parquet}`" if parquet else "- parquet: (none)",
        "",
        f"Attempts: **{n:,}**",
        "",
        "| state | n | P&L |",
        "| --- | --- | --- |",
        f"| both | {pct(by_state['both'])} | {pnl_state['both']:,.2f} |",
        f"| yes_only | {pct(by_state['yes_only'])} | {pnl_state['yes_only']:,.2f} |",
        f"| no_only | {pct(by_state['no_only'])} | {pnl_state['no_only']:,.2f} |",
        f"| miss | {pct(by_state['miss'])} | {pnl_state['miss']:,.2f} |",
        "",
        "| inventory kind (when leftover) | n |",
        "| --- | --- |",
        f"| complement rest | {by_inv['complement']:,} |",
        f"| resolve | {by_inv['resolve']:,} |",
        f"| dump | {by_inv['dump']:,} |",
        f"| zero (3d miss) | {by_inv['zero']:,} |",
        "",
        "| P&L | USDC |",
        "| --- | --- |",
        f"| merge | {merge:,.2f} |",
        f"| inventory | {inv:,.2f} |",
        f"| fees (included in the marks) | {fees:,.2f} |",
        f"| **headline** | **{pnl:,.2f}** |",
        "",
        f"Fee-market attempts: {n_fee:,} (P&L {pnl_fee:,.2f}). "
        f"Fee-free: {n_free:,} (P&L {pnl_free:,.2f}).",
        "",
    ]
    if n:
        lines.append(f"Headline $/attempt: {pnl / n:,.4f}")
        lines.append("")
    if both_pair:
        both_pair.sort()
        mid = both_pair[len(both_pair) // 2]
        lines.append(
            f"Filled-both pair cost p50: {mid:.4f}  "
            f"share < 1.00: {sum(1 for p in both_pair if p < 1.0) / len(both_pair):.1%}"
        )
        lines.append("")
    lines.append(
        "This is the naive dual-floor baseline, not a trained model. "
        "Go/no-go still requires interpretation C to stay green after "
        "inventory, including dump-miss = $0."
    )
    lines.append("")
    out = snap_path or SNAP_PATH
    header = "# Naive baseline snapshot"
    if fee_free:
        header = "# Fee-free complementary-MM snapshot"
        lines[0] = header
        lines.insert(
            2,
            "Fee-charging conditions dropped (5m crypto factories). "
            "Pair-cost fire `< 0.99` is Stage A.",
        )
    out.write_text("\n".join(lines))


def main() -> int:
    p = argparse.ArgumentParser(description="Naive complementary MM simulator")
    p.add_argument("--trigger-days", type=float, default=1.0,
                   help="width of the trigger window ending at the 3-day-complete frontier")
    p.add_argument("--interp", default="C", choices=["C"],
                   help="fill interpretation (v0: C only)")
    p.add_argument("--kappa", type=float, default=1.0,
                   help="competition multiple on required ask depth (1=C, 2=B-style)")
    p.add_argument("--max-attempts", type=int, default=None,
                   help="optional cap on naive candidates (deterministic first N by block)")
    p.add_argument(
        "--fee-free",
        action="store_true",
        help="drop conditions that ever charged taker fee (skip 5m crypto factories)",
    )
    args = p.parse_args()
    return run(
        args.trigger_days,
        args.interp,
        args.kappa,
        args.max_attempts,
        fee_free=args.fee_free,
    )


if __name__ == "__main__":
    raise SystemExit(main())
