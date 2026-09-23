#!/usr/bin/env python3
"""How often does a Stage A (3-of-8) event resolve in the next 30/60/120 blocks?

Resolution in that span is infinite liquidity at 1.0 / 0.0. This counts
it; it does not use anything after the horizon as a feature.
"""

from __future__ import annotations

import os
import sys
import time
from datetime import datetime, timezone
from pathlib import Path

import numpy as np
from dotenv import load_dotenv

_HERE = Path(__file__).resolve().parent
_ROOT = _HERE.parents[1]
sys.path.insert(0, str(_HERE))
sys.path.insert(0, str(_ROOT))
load_dotenv(_ROOT / ".env")

import run as R  # noqa: E402
from sim_lib import (  # noqa: E402
    BLOCKS_PER_DAY,
    LIQ_END,
    LOOKBACK_BLOCKS,
    prefilter_alive,
)

C10K = Path(os.environ.get("CONDITION_BY_10K_V1_DIR", ""))
OUT = _HERE / "resolve-in-horizon.md"


def _pct(n: int, d: int) -> str:
    if d <= 0:
        return "—"
    return f"{100.0 * n / d:.3f}%"


def _quantiles(xs: list[int]) -> str:
    if not xs:
        return "—"
    a = np.array(xs, dtype=float)
    qs = np.quantile(a, [0.5, 0.9, 0.99])
    return f"n={len(xs):,}  min={int(a.min())}  p50={qs[0]:.0f}  p90={qs[1]:.0f}  p99={qs[2]:.0f}  max={int(a.max())}"


def main() -> int:
    ap_days = 4.0
    if len(sys.argv) > 1:
        ap_days = float(sys.argv[1])
    if not R.FILLS_DIR or not Path(R.FILLS_DIR).exists():
        R._fail("FILLS_V1_DIR missing")
    if not C10K.exists():
        R._fail("CONDITION_BY_10K_V1_DIR missing")
    t0 = time.time()
    fills_root = Path(R.FILLS_DIR)
    frontier = R._frontier(fills_root)
    x_hi = frontier - LIQ_END
    span = int(ap_days * BLOCKS_PER_DAY)
    x_lo = x_hi - span + 1
    fill_lo = x_lo - LOOKBACK_BLOCKS
    fill_hi = x_hi + 120
    print(f"resolve-in-horizon  trigger {x_lo:,}–{x_hi:,}", flush=True)

    con = R._connect()
    fill_files = R._files(fills_root, fill_lo, fill_hi)
    c10k_files = R._files(C10K, fill_lo, fill_hi)
    if not c10k_files:
        R._fail("no condition_by_10k files in range")
    con.execute(
        f"""
        CREATE TABLE fills AS
        SELECT block_number, hex(condition_id) AS cid
        FROM read_parquet({R._sql_list(fill_files)})
        WHERE block_number BETWEEN {fill_lo} AND {fill_hi}
          AND net_yes_tokens <> 0 AND gross_usdc <> 0
        """
    )
    con.execute(
        f"""
        CREATE TABLE res AS
        SELECT hex(condition_id) AS cid,
               resolved_block,
               payout_numerators
        FROM read_parquet({R._sql_list(c10k_files)})
        WHERE resolved_block IS NOT NULL
        """
    )
    n_res = con.execute("SELECT count(*) FROM res").fetchone()[0]
    print(f"  resolutions in loaded 10Ks {n_res:,}", flush=True)

    ev = con.execute(
        f"""
        SELECT DISTINCT block_number, cid
        FROM fills
        WHERE block_number BETWEEN {x_lo} AND {x_hi}
        ORDER BY 1, 2
        """
    ).fetchall()
    print(f"  candidate (cid,X) {len(ev):,}", flush=True)

    blocks = con.execute(
        "SELECT cid, list(DISTINCT block_number ORDER BY block_number) FROM fills GROUP BY 1"
    ).fetchall()
    fill_blocks = {cid: set(lst) for cid, lst in blocks}
    last_fill = {cid: max(s) for cid, s in fill_blocks.items()}

    res_rows = con.execute("SELECT cid, resolved_block FROM res").fetchall()
    resolved = {}
    for cid, rb in res_rows:
        if rb is None:
            continue
        rb = int(rb)
        prev = resolved.get(cid)
        if prev is None or rb < prev:
            resolved[cid] = rb

    n_cand = n_alive = 0
    n_has_res = 0
    hit = {1: 0, 30: 0, 60: 0, 120: 0}
    same_block = 0
    gaps_alive = []
    for blk, cid in ev:
        n_cand += 1
        x = int(blk)
        fb = fill_blocks.get(cid)
        if fb is None or not prefilter_alive(fb, x):
            continue
        n_alive += 1
        r = resolved.get(cid)
        if r is None:
            continue
        n_has_res += 1
        d = r - x
        if d == 0:
            same_block += 1
        if d >= 0:
            gaps_alive.append(d)
        for h in (1, 30, 60, 120):
            if 1 <= d <= h:
                hit[h] += 1

    # Last print on a condition → resolve, regardless of Stage A.
    last_gaps = []
    last_in_60 = 0
    last_in_120 = 0
    n_last = 0
    for cid, r in resolved.items():
        lf = last_fill.get(cid)
        if lf is None:
            continue
        n_last += 1
        g = r - int(lf)
        last_gaps.append(g)
        if 1 <= g <= 60:
            last_in_60 += 1
        if 1 <= g <= 120:
            last_in_120 += 1

    started = datetime.now(timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ")
    lines = [
        "# Stage A events vs resolution in the P&L span",
        "",
        f"Generated {started}. Wall {time.time()-t0:.1f}s.",
        f"Trigger `{x_lo:,}`–`{x_hi:,}` (~{ap_days:g} days). Stage A gate **3 of last 8**.",
        "Resolution from `condition_by_10k_v1.resolved_block` (on-chain",
        "`ConditionResolution`). A resolve inside `X+1..X+H` is infinite",
        "liquidity at 1.0 / 0.0 for P&L over that horizon. Nothing after `X+H`",
        "is used.",
        "",
        f"Resolutions loaded in nearby 10Ks: {n_res:,}.",
        f"Candidate fill-blocks: {n_cand:,}. After 3-of-8: **{n_alive:,}**.",
        f"Of those, condition has a known resolve (any future in loaded files): {n_has_res:,} ({_pct(n_has_res, n_alive)}).",
        "",
        "## Stage A events with resolve in the next H blocks",
        "",
        "| horizon | n | of Stage A events |",
        "| --- | --- | --- |",
        f"| same block (`d=0`) | {same_block:,} | {_pct(same_block, n_alive)} |",
        f"| `X+1` | {hit[1]:,} | {_pct(hit[1], n_alive)} |",
        f"| `X+1..X+30` | {hit[30]:,} | {_pct(hit[30], n_alive)} |",
        f"| `X+1..X+60` | {hit[60]:,} | {_pct(hit[60], n_alive)} |",
        f"| `X+1..X+120` | {hit[120]:,} | {_pct(hit[120], n_alive)} |",
        "",
        "Gap `resolved_block − X` among Stage A events whose condition resolves",
        f"at or after X (loaded files): {_quantiles(gaps_alive)}.",
        "",
        "## Does the CLOB go quiet before resolve?",
        "",
        "For every resolution in the loaded 10Ks that has at least one fill",
        "in the lookback/trigger/horizon files: gap from **that condition's",
        "last fill** to `resolved_block`. `fills_v1` already drops fills after",
        "resolution, so this is last print → oracle.",
        "",
        f"Conditions with last-fill and resolve: {n_last:,}.",
        f"Last fill → resolve in 1..60 blocks: {last_in_60:,} ({_pct(last_in_60, n_last)}).",
        f"Last fill → resolve in 1..120 blocks: {last_in_120:,} ({_pct(last_in_120, n_last)}).",
        f"Gap last-fill → resolve: {_quantiles(last_gaps)}.",
        "",
        "If those percentages are tiny, redeem-at-1.0 inside a 60-block GTC",
        "is rare on this tape. If they are not, Stage B/C must treat resolve",
        "in-horizon as a touch at 1.0 / 0.0.",
        "",
    ]
    OUT.write_text("\n".join(lines))
    print(f"wrote {OUT}  alive {n_alive:,}  in60 {hit[60]:,}  {time.time()-t0:.1f}s", flush=True)
    con.close()
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
