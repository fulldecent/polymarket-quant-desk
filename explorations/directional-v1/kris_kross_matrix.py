#!/usr/bin/env python3
"""Last-7-day Stage A persist (k-of-8) × Kris Kross maker-zigzag count.

Universe: every (X, cid) with a fill at X in the last 7 calendar days.
Persist k-of-8 = at least k of {X-7..X} have a fill (Stage A).
Kris Kross: maker YES prices in that window, oldest→newest; count = number
of directional legs (equals ignored; first ≠ first-price starts count=1).
"""

from __future__ import annotations

import os
import sys
import time
from pathlib import Path

import numpy as np
from dotenv import load_dotenv

_HERE = Path(__file__).resolve().parent
_ROOT = _HERE.parents[1]
sys.path.insert(0, str(_HERE))
sys.path.insert(0, str(_ROOT))
load_dotenv(_ROOT / ".env")

import run as R  # noqa: E402
from sim_lib import LIQ_END, kris_count  # noqa: E402

DAYS = 7
SEC_PER_BLOCK = 1.5
TRIGGER_BLOCKS = int(DAYS * 86400 / SEC_PER_BLOCK)
WINDOW = 8


def _fmt_row(label: str, vals: list[int], width: int = 12) -> str:
    cells = "".join(f"{v:>{width},}" for v in vals)
    return f"{label:<8}{cells}"


def main() -> int:
    t0 = time.time()
    fills_root = Path(R.FILLS_DIR)
    frontier = R._frontier(fills_root)
    x_hi = frontier - LIQ_END
    x_lo = x_hi - TRIGGER_BLOCKS + 1
    fill_lo = x_lo - (WINDOW - 1)
    fill_hi = x_hi
    print(
        f"kris-kross matrix  last {DAYS}d  {x_lo:,}–{x_hi:,}  "
        f"fills {fill_lo:,}–{fill_hi:,}",
        flush=True,
    )
    ypx = R._yes_px_sql()
    files = R._files(fills_root, fill_lo, fill_hi)
    con = R._connect()
    con.execute("SET memory_limit = '8GB'")
    con.execute(
        f"""
        CREATE TABLE legs AS
        SELECT block_number, logical_fill_index, hex(condition_id) AS cid,
               is_taker, round(({ypx})::DOUBLE, 6) AS px
        FROM read_parquet({R._sql_list(files)})
        WHERE block_number BETWEEN {fill_lo} AND {fill_hi}
          AND net_yes_tokens <> 0 AND gross_usdc <> 0
        """
    )
    n_legs = con.execute("SELECT count(*) FROM legs").fetchone()[0]
    print(f"  legs {n_legs:,}  {time.time()-t0:.1f}s", flush=True)
    blocks = con.execute(
        """
        SELECT cid, block_number
        FROM (SELECT DISTINCT cid, block_number FROM legs)
        ORDER BY cid, block_number
        """
    ).fetchdf()
    makers = con.execute(
        """
        SELECT cid, block_number, logical_fill_index, px
        FROM legs
        WHERE NOT is_taker AND px IS NOT NULL
        ORDER BY cid, block_number, logical_fill_index
        """
    ).fetchdf()
    con.close()
    print(
        f"  fill-blocks {len(blocks):,}  maker legs {len(makers):,}  "
        f"{time.time()-t0:.1f}s",
        flush=True,
    )

    # exact[persist_k, count_bucket]  persist 1..8 → row 0..7
    # count_bucket: 0..9 exact, 10 = 10+
    COUNT_MAX = 10
    exact = np.zeros((8, COUNT_MAX + 1), dtype=np.int64)
    n_sig = 0
    g_blocks = {cid: g.block_number.to_numpy(dtype=np.int64) for cid, g in blocks.groupby("cid", sort=False)}
    g_m_blk = {}
    g_m_px = {}
    for cid, g in makers.groupby("cid", sort=False):
        g_m_blk[cid] = g.block_number.to_numpy(dtype=np.int64)
        g_m_px[cid] = g.px.to_numpy(dtype=np.float64)

    for cid, bn in g_blocks.items():
        m_blk = g_m_blk.get(cid)
        m_px = g_m_px.get(cid)
        # triggers: fill blocks inside [x_lo, x_hi]
        i0 = int(np.searchsorted(bn, x_lo, side="left"))
        i1 = int(np.searchsorted(bn, x_hi, side="right"))
        xs = bn[i0:i1]
        for X in xs:
            lo = X - (WINDOW - 1)
            p0 = int(np.searchsorted(bn, lo, side="left"))
            p1 = int(np.searchsorted(bn, X, side="right"))
            persist = p1 - p0  # unique fill blocks in window; X included → 1..8
            if persist < 1:
                continue
            if persist > 8:
                persist = 8
            if m_blk is None:
                kk = 0
            else:
                a = int(np.searchsorted(m_blk, lo, side="left"))
                b = int(np.searchsorted(m_blk, X, side="right"))
                kk = kris_count(m_px[a:b])
            kb = COUNT_MAX if kk >= COUNT_MAX else kk
            exact[persist - 1, kb] += 1
            n_sig += 1
        if n_sig and n_sig % 2_000_000 == 0:
            print(f"  … {n_sig:,}  {time.time()-t0:.1f}s", flush=True)

    # cell[p, c] = persist >= p+1 of 8  AND  kris count >= c
    cum = np.zeros_like(exact)
    for p in range(8):
        for c in range(COUNT_MAX + 1):
            cum[p, c] = int(exact[p:, c:].sum())

    def dump(title: str, mat: np.ndarray) -> None:
        print(title, flush=True)
        hdr = f"{'count':<8}" + "".join(f"{c:>12}" for c in (f"{k}+ of 8" for k in range(1, 9)))
        print(hdr, flush=True)
        for r in range(COUNT_MAX + 1):
            lab = f"{r}+"
            print(_fmt_row(lab, [int(mat[c, r]) for c in range(8)]), flush=True)
        print(flush=True)

    print(f"  signals {n_sig:,}  {time.time()-t0:.1f}s", flush=True)
    print(flush=True)
    dump("CUMULATIVE  persist k+ of 8  ×  Kris Kross count c+", cum)
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
