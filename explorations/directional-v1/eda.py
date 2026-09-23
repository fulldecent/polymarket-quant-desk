#!/usr/bin/env python3
"""How often X+1 has takeable asks, and how often the exit/dump windows print.

Writes eda-snapshot.md. Dirty git tree is fine.
"""

from __future__ import annotations

import json
import os
import sys
import time
from datetime import datetime, timezone
from pathlib import Path

import duckdb
from dotenv import load_dotenv

_PROJECT_ROOT = Path(__file__).resolve().parents[2]
load_dotenv(_PROJECT_ROOT / ".env")

FILLS_DIR = os.environ.get("FILLS_V1_DIR", "")
SCRATCH = os.environ.get("SCRATCH_DIR", "")
OUT = Path(__file__).resolve().parent / "eda-snapshot.md"
MIN_SHARES = 5.0
MIN_USDC = 1.20
COOLDOWN_BLOCKS = 180
BLOCKS_PER_DAY = 41136
BLOCK_SEC = 2.1


def main() -> int:
    if not FILLS_DIR or not Path(FILLS_DIR).exists():
        sys.exit("FILLS_V1_DIR is not set or does not exist.")
    fills = Path(FILLS_DIR)
    metas = sorted(fills.glob("1M=*/10K=*/metadata.json"))
    nonempty = []
    for p in metas:
        m = json.loads(p.read_text())
        if int(m.get("row_count") or 0) > 0:
            nonempty.append(p.parent / "data.parquet")
    recent = [str(p) for p in nonempty[-24:]]
    if not recent:
        sys.exit("no fills partitions")
    if not SCRATCH:
        sys.exit("SCRATCH_DIR is not set.")
    Path(SCRATCH).mkdir(parents=True, exist_ok=True)
    con = duckdb.connect()
    con.execute(f"SET temp_directory = '{SCRATCH}'")
    con.execute("SET memory_limit = '8GB'")
    lst = "[" + ",".join("'" + p.replace("'", "''") + "'" for p in recent) + "]"
    print(f"scanning {len(recent)} fills partitions …", flush=True)
    t0 = time.time()
    con.execute(
        f"""
        CREATE TABLE fills AS
        SELECT block_number, hex(condition_id) AS cid,
               is_taker, net_yes_tokens, gross_usdc
        FROM read_parquet({lst})
        """
    )
    lo, hi, n = con.execute(
        "SELECT min(block_number), max(block_number), count(*) FROM fills"
    ).fetchone()
    # Maker-sell asks (entry take). Taker-buy (exit tape). Maker-buy (dump bids).
    stats = con.execute(
        f"""
        WITH asks AS (
            SELECT block_number, cid,
                   CASE WHEN net_yes_tokens < 0 THEN 'yes' ELSE 'no' END AS outcome,
                   abs(net_yes_tokens)::DOUBLE / 1e6 AS tokens,
                   (-gross_usdc)::DOUBLE / 1e6 AS usdc
            FROM fills
            WHERE NOT is_taker AND gross_usdc < 0 AND net_yes_tokens <> 0
        ),
        ask_bars AS (
            SELECT block_number, cid, outcome,
                   sum(tokens) AS tokens, sum(usdc) AS usdc
            FROM asks GROUP BY 1, 2, 3
        ),
        floors AS (
            SELECT * FROM ask_bars
            WHERE tokens >= {MIN_SHARES} AND usdc >= {MIN_USDC}
        ),
        tbuy AS (
            SELECT block_number, cid,
                   CASE WHEN net_yes_tokens > 0 THEN 'yes' ELSE 'no' END AS outcome
            FROM fills
            WHERE is_taker AND gross_usdc > 0 AND net_yes_tokens <> 0
            GROUP BY 1, 2, 3
        ),
        mbuy AS (
            SELECT block_number, cid,
                   CASE WHEN net_yes_tokens > 0 THEN 'yes' ELSE 'no' END AS outcome
            FROM fills
            WHERE NOT is_taker AND gross_usdc > 0 AND net_yes_tokens <> 0
            GROUP BY 1, 2, 3
        )
        SELECT
            (SELECT count(*) FROM ask_bars) AS ask_bars,
            (SELECT count(*) FROM floors) AS floor_bars,
            (SELECT count(*) FROM tbuy) AS taker_buy_bars,
            (SELECT count(*) FROM mbuy) AS maker_buy_bars
        """
    ).fetchone()
    # Of floor bars at X, does X+1 also have a floor on the same (cid, outcome)?
    follow = con.execute(
        f"""
        WITH asks AS (
            SELECT block_number, cid,
                   CASE WHEN net_yes_tokens < 0 THEN 'yes' ELSE 'no' END AS outcome,
                   abs(net_yes_tokens)::DOUBLE / 1e6 AS tokens,
                   (-gross_usdc)::DOUBLE / 1e6 AS usdc
            FROM fills
            WHERE NOT is_taker AND gross_usdc < 0 AND net_yes_tokens <> 0
        ),
        floors AS (
            SELECT block_number, cid, outcome
            FROM asks
            GROUP BY 1, 2, 3
            HAVING sum(tokens) >= {MIN_SHARES} AND sum(usdc) >= {MIN_USDC}
        ),
        tbuy AS (
            SELECT DISTINCT block_number, cid,
                   CASE WHEN net_yes_tokens > 0 THEN 'yes' ELSE 'no' END AS outcome
            FROM fills
            WHERE is_taker AND gross_usdc > 0 AND net_yes_tokens <> 0
        )
        SELECT
            count(*) AS n_x,
            sum(CASE WHEN f2.cid IS NOT NULL THEN 1 ELSE 0 END) AS x1_also_floor,
            sum(CASE WHEN EXISTS (
                SELECT 1 FROM tbuy t
                WHERE t.cid = f.cid AND t.outcome = f.outcome
                  AND t.block_number BETWEEN f.block_number + 2 AND f.block_number + 60
            ) THEN 1 ELSE 0 END) AS exit_window_has_taker_buy,
            sum(CASE WHEN EXISTS (
                SELECT 1 FROM tbuy t
                WHERE t.cid = f.cid AND t.outcome = f.outcome
                  AND t.block_number BETWEEN f.block_number + 61 AND f.block_number + 120
            ) THEN 1 ELSE 0 END) AS liq_window_has_taker_buy
        FROM floors f
        LEFT JOIN floors f2
          ON f2.cid = f.cid AND f2.outcome = f.outcome
         AND f2.block_number = f.block_number + 1
        """
    ).fetchone()
    dt = time.time() - t0
    n_x, x1, ex, liq = follow

    print("  naive signals with 180-block cooldown …", flush=True)
    opps = con.execute(
        f"""
        WITH asks AS (
            SELECT block_number, cid,
                   CASE WHEN net_yes_tokens < 0 THEN 'yes' ELSE 'no' END AS outcome,
                   abs(net_yes_tokens)::DOUBLE / 1e6 AS tokens,
                   (-gross_usdc)::DOUBLE / 1e6 AS usdc
            FROM fills
            WHERE NOT is_taker AND gross_usdc < 0 AND net_yes_tokens <> 0
        ),
        floors AS (
            SELECT block_number, cid, outcome
            FROM asks
            GROUP BY 1, 2, 3
            HAVING sum(tokens) >= {MIN_SHARES} AND sum(usdc) >= {MIN_USDC}
        )
        SELECT block_number, cid
        FROM floors
        GROUP BY 1, 2
        ORDER BY cid, block_number
        """
    ).fetchall()
    last_fire: dict[str, int] = {}
    gated: list[tuple[int, str]] = []
    for blk, cid in opps:
        prev = last_fire.get(cid)
        if prev is None or int(blk) >= prev + COOLDOWN_BLOCKS:
            gated.append((int(blk), cid))
            last_fire[cid] = int(blk)
    span_blocks = int(hi) - int(lo) + 1
    span_days = span_blocks * BLOCK_SEC / 86400.0
    raw_n = len(opps)
    gated_n = len(gated)
    raw_per_day = raw_n / span_days if span_days else 0
    gated_per_day = gated_n / span_days if span_days else 0
    n_cond = len({c for _, c in gated})

    text = f"""# Directional FOK — EDA snapshot

Generated {datetime.now(timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ")} by `eda.py`.

Last {len(recent)} non-empty `fills_v1` partitions, blocks {lo:,}–{hi:,}, {n:,} legs, scan {dt:.1f}s.

Maker-sell bars are the entry-take tape (interpretation C). Taker-buy bars are the resting-sell tape. Maker-buy bars are dump bids.

| metric | count |
| --- | --- |
| (block, condition, outcome) maker-sell bars | {stats[0]:,} |
| of which legal floor (≥5 sh and ≥$1.20) | {stats[1]:,} |
| taker-buy bars | {stats[2]:,} |
| maker-buy bars | {stats[3]:,} |

Among floor bars at block X (same condition and outcome):

| follow-on | count | share of X floors |
| --- | --- | --- |
| X+1 also has a floor | {x1:,} | {100*x1/n_x:.1f}% |
| some taker-buy in X+2..X+60 (exit window) | {ex:,} | {100*ex/n_x:.1f}% |
| some taker-buy in X+61..X+120 (liq window) | {liq:,} | {100*liq/n_x:.1f}% |

X floors: {n_x:,}. This is unconditional tape, not a model trigger. A print in the window is not a fill at a signed `P_exit`.

## Naive signals per day (180-block cooldown)

A naive opportunity is a `(block, condition)` with a legal maker-sell floor on at least one outcome (the baseline then picks the deeper side). After a fire on a condition at `X`, the next fire on that condition is `X+180` (~6.3 min). The cooldown starts at the signal, including FOK misses.

Window {span_blocks:,} blocks ≈ {span_days:.2f} days.

| | count | per day |
| --- | --- | --- |
| opportunities (no cooldown) | {raw_n:,} | {raw_per_day:,.0f} |
| signals after 180-block cooldown | {gated_n:,} | {gated_per_day:,.0f} |
| distinct conditions that fired | {n_cond:,} | — |

Cooldown factor: {raw_n / gated_n:.1f}× fewer tickets than ungated tape.
"""
    OUT.write_text(text)
    print(text)
    print(f"wrote {OUT}", flush=True)
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
