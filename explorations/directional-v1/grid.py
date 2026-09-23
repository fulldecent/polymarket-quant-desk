#!/usr/bin/env python3
"""Cap grid: P_entry = close+{0,1,2,3} ticks, P_exit = close+{-2,-1,0,+1} ticks.

Same gated opportunities as run.py (180-block cooldown, deeper-ask direction).
Writes cap-grid.md. Dirty git tree is fine.
"""

from __future__ import annotations

import argparse
import os
import sys
import time
from collections import defaultdict
from datetime import datetime, timezone
from pathlib import Path

import pandas as pd
from dotenv import load_dotenv

_HERE = Path(__file__).resolve().parent
_PROJECT_ROOT = _HERE.parents[1]
sys.path.insert(0, str(_HERE))
sys.path.insert(0, str(_PROJECT_ROOT))
load_dotenv(_PROJECT_ROOT / ".env")

import run as R  # noqa: E402
from sim_lib import (  # noqa: E402
    BLOCK_SEC,
    COOLDOWN_BLOCKS,
    LIQ_END,
    LOOKBACK_BLOCKS,
    MIN_SHARES,
    TICK,
    apply_cooldown,
    n_min,
    offset_caps,
    simulate_roundtrip,
)

ENTRY_OFF = (0, 1, 2, 3)
EXIT_OFF = (-2, -1, 0, 1)
OUT = _HERE / "cap-grid.md"


def _summarize(sims: list[dict]) -> dict:
    n = len(sims)
    if n == 0:
        return {
            "n": 0, "n_ok": 0, "hit": 0.0, "pnl": 0.0, "pnl_ok": 0.0,
            "q_exit": 0.0, "q_liq": 0.0, "q_zero": 0.0, "q_tot": 0.0,
        }
    n_ok = sum(1 for s in sims if s["entry_ok"])
    pnl = sum(s["pnl"] for s in sims)
    pnl_ok = sum(s["pnl"] for s in sims if s["entry_ok"])
    qe = sum(s["q_exit"] for s in sims)
    ql = sum(s["q_liq"] for s in sims)
    qz = sum(s["q_zero"] for s in sims)
    qt = qe + ql + qz
    return {
        "n": n,
        "n_ok": n_ok,
        "hit": n_ok / n,
        "pnl": pnl,
        "pnl_ok": pnl_ok,
        "q_exit": qe / qt if qt else 0.0,
        "q_liq": ql / qt if qt else 0.0,
        "q_zero": qz / qt if qt else 0.0,
        "q_tot": qt,
    }


def main() -> int:
    p = argparse.ArgumentParser()
    p.add_argument("--trigger-days", type=float, default=3.0)
    p.add_argument("--max-attempts", type=int, default=None)
    args = p.parse_args()

    if not R.FILLS_DIR or not Path(R.FILLS_DIR).exists():
        R._fail("FILLS_V1_DIR missing")
    fills_root = Path(R.FILLS_DIR)
    frontier = R._frontier(fills_root)
    x_hi = frontier - LIQ_END
    span = int(args.trigger_days * 41136)
    x_lo = x_hi - span + 1
    fill_lo = x_lo - LOOKBACK_BLOCKS
    fill_hi = x_hi + LIQ_END
    started = datetime.now(timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ")
    print(f"cap grid  trigger {x_lo:,}–{x_hi:,}", flush=True)

    fill_files = R._files(fills_root, fill_lo, fill_hi)
    if not fill_files:
        R._fail("no fills")
    con = R._connect()
    t0 = time.time()
    con.execute(
        f"""
        CREATE TABLE fills AS
        SELECT block_number, logical_fill_index, hex(condition_id) AS cid,
               is_taker, net_yes_tokens, gross_usdc, fee_usdc, market_id
        FROM read_parquet({R._sql_list(fill_files)})
        WHERE block_number BETWEEN {fill_lo} AND {fill_hi}
          AND net_yes_tokens <> 0 AND gross_usdc <> 0
        """
    )
    print(f"  fills {con.execute('SELECT count(*) FROM fills').fetchone()[0]:,}", flush=True)
    con.execute(
        f"""
        CREATE TABLE bars AS
        SELECT
          block_number, cid,
          CASE WHEN {R._same_sign_yes_book()} THEN 'yes' ELSE 'no' END AS outcome,
          sum(abs(net_yes_tokens)) FILTER (
            WHERE NOT is_taker AND gross_usdc < 0)::DOUBLE / 1e6 AS ask_tokens,
          sum(-gross_usdc) FILTER (
            WHERE NOT is_taker AND gross_usdc < 0)::DOUBLE / 1e6 AS ask_usdc,
          arg_max({R._yes_px_sql()}, logical_fill_index) AS close_yes,
          max(fee_usdc) AS fee_usdc
        FROM fills
        GROUP BY 1, 2, 3
        """
    )
    con.execute(
        """
        CREATE TABLE bars2 AS
        SELECT *,
               (coalesce(ask_tokens,0) >= 5 AND coalesce(ask_usdc,0) >= 1.20) AS ask_floor
        FROM bars
        """
    )
    opps = con.execute(
        f"""
        SELECT block_number, cid FROM bars2
        WHERE ask_floor AND block_number BETWEEN {x_lo} AND {x_hi}
        GROUP BY 1, 2
        ORDER BY cid, block_number
        """
    ).fetchall()
    gated = apply_cooldown([(int(b), c) for b, c in opps], COOLDOWN_BLOCKS)
    if args.max_attempts:
        gated = gated[: args.max_attempts]
    print(f"  gated {len(gated):,}", flush=True)
    gdf = pd.DataFrame(gated, columns=["x_block", "cid"])
    con.register("gated", gdf)
    con.execute(
        """
        CREATE TABLE cand0 AS
        SELECT g.x_block, g.cid,
               y.ask_tokens AS yes_ask, n.ask_tokens AS no_ask,
               coalesce(y.close_yes, n.close_yes) AS close_yes,
               coalesce(y.fee_usdc, n.fee_usdc, 0) AS fee_usdc
        FROM gated g
        LEFT JOIN bars2 y
          ON y.block_number = g.x_block AND y.cid = g.cid AND y.outcome = 'yes'
        LEFT JOIN bars2 n
          ON n.block_number = g.x_block AND n.cid = g.cid AND n.outcome = 'no'
        """
    )
    base = []
    for x_block, cid, yes_ask, no_ask, close_yes, fee_usdc in con.execute("SELECT * FROM cand0").fetchall():
        if close_yes is None or not (0 < float(close_yes) < 1):
            continue
        ya, na = float(yes_ask or 0), float(no_ask or 0)
        if ya >= na and ya >= MIN_SHARES:
            direction, close_side = "yes", float(close_yes)
        elif na >= MIN_SHARES:
            direction, close_side = "no", 1.0 - float(close_yes)
        else:
            continue
        base.append(
            {
                "x_block": int(x_block),
                "cid": cid,
                "direction": direction,
                "close_side": close_side,
                "fee_market": bool(fee_usdc and fee_usdc > 0),
            }
        )
    bdf = pd.DataFrame(base)
    con.register("base", bdf)
    pers = con.execute(
        """
        SELECT r.x_block, r.cid, r.direction,
               sum(CASE WHEN b.ask_floor THEN 1 ELSE 0 END) AS persist_5
        FROM base r
        LEFT JOIN bars2 b
          ON b.cid = r.cid AND b.outcome = r.direction
         AND b.block_number BETWEEN r.x_block - 4 AND r.x_block
        GROUP BY 1, 2, 3
        """
    ).fetchdf()
    bdf = bdf.merge(pers, on=["x_block", "cid", "direction"], how="left")
    bdf["persist_5"] = bdf["persist_5"].fillna(0).astype(int)
    print(f"  base tickets {len(bdf):,}  persist>=3 {int((bdf.persist_5 >= 3).sum()):,}", flush=True)

    print("  loading levels …", flush=True)
    ask_rows = con.execute(
        f"""
        SELECT block_number, cid,
               CASE WHEN net_yes_tokens < 0 THEN 'yes' ELSE 'no' END AS outcome,
               {R._outcome_px_sql()} AS px,
               abs(net_yes_tokens)::DOUBLE / 1e6 AS tokens
        FROM fills
        WHERE NOT is_taker AND gross_usdc < 0
          AND block_number BETWEEN {x_lo + 1} AND {x_hi + 1}
        """
    ).fetchall()
    tbuy_rows = con.execute(
        f"""
        SELECT block_number, cid,
               CASE WHEN net_yes_tokens > 0 THEN 'yes' ELSE 'no' END AS outcome,
               {R._outcome_px_sql()} AS px,
               abs(net_yes_tokens)::DOUBLE / 1e6 AS tokens
        FROM fills
        WHERE is_taker AND gross_usdc > 0
          AND block_number BETWEEN {x_lo + 2} AND {x_hi + 60}
        """
    ).fetchall()
    bid_rows = con.execute(
        f"""
        SELECT block_number, cid,
               CASE WHEN net_yes_tokens > 0 THEN 'yes' ELSE 'no' END AS outcome,
               {R._outcome_px_sql()} AS px,
               abs(net_yes_tokens)::DOUBLE / 1e6 AS tokens
        FROM fills
        WHERE NOT is_taker AND gross_usdc > 0
          AND block_number BETWEEN {x_lo + 61} AND {x_hi + 120}
        """
    ).fetchall()

    def _idx(rows):
        d = defaultdict(list)
        for blk, cid, outc, px, tok in rows:
            if px and tok and tok > 0:
                d[(cid, outc, int(blk))].append((float(px), float(tok)))
        return d

    from sim_lib import Level

    asks_i, tbuy_i, bids_i = _idx(ask_rows), _idx(tbuy_rows), _idx(bid_rows)

    def _lv(pairs):
        return [Level(p, t) for p, t in pairs]

    rows = bdf.to_dict("records")
    # Pre-build per-ticket windows once
    windows = []
    for rec in rows:
        x, cid, d = rec["x_block"], rec["cid"], rec["direction"]
        windows.append(
            (
                rec,
                _lv(asks_i.get((cid, d, x + 1), [])),
                [(b, _lv(tbuy_i[(cid, d, b)])) for b in range(x + 2, x + 61) if (cid, d, b) in tbuy_i],
                [(b, _lv(bids_i[(cid, d, b)])) for b in range(x + 61, x + 121) if (cid, d, b) in bids_i],
            )
        )
    print(f"  windows ready  {time.time()-t0:.1f}s", flush=True)

    cells = []
    for din in ENTRY_OFF:
        for dout in EXIT_OFF:
            sims_all = []
            sims_p = []
            skipped = 0
            for rec, entry, exb, liqb in windows:
                caps = offset_caps(rec["close_side"], din, dout)
                if caps is None or caps[1] >= caps[0]:
                    skipped += 1
                    continue
                p_in, p_out = caps
                sim = simulate_roundtrip(
                    n=float(n_min(p_in)),
                    p_entry=p_in,
                    p_exit=p_out,
                    entry_asks=entry,
                    exit_by_block=exb,
                    liq_by_block=liqb,
                    x_block=rec["x_block"],
                    fee_market=rec["fee_market"],
                )
                sims_all.append(sim)
                if rec["persist_5"] >= 3:
                    sims_p.append(sim)
            cells.append(
                {
                    "d_in": din,
                    "d_out": dout,
                    "skipped": skipped,
                    "all": _summarize(sims_all),
                    "persist3": _summarize(sims_p),
                }
            )
            a = cells[-1]["all"]
            print(
                f"  entry{din:+d} exit{dout:+d}  n={a['n']:,} hit={100*a['hit']:.1f}%  "
                f"pnl={a['pnl']:,.1f}  zero={100*a['q_zero']:.0f}%",
                flush=True,
            )

    elapsed = time.time() - t0
    span_d = (x_hi - x_lo + 1) * BLOCK_SEC / 86400.0
    lines = [
        "# Cap grid",
        "",
        f"Generated {started} by `grid.py`. Same gated universe as `run.py` "
        f"(cooldown {COOLDOWN_BLOCKS}, deeper-ask direction). "
        f"`P_entry = close + d_in·tick`, `P_exit = close + d_out·tick`, tick={TICK}. "
        "Illegal cells (`P_exit ≥ P_entry` after clip) drop that ticket.",
        "",
        f"- trigger X: {x_lo:,}–{x_hi:,} (~{span_d:.2f} d)",
        f"- frontier: {frontier:,}",
        f"- gated tickets: {len(rows):,}",
        f"- persist_floors_5 ≥ 3: {int((bdf.persist_5 >= 3).sum()):,}",
        f"- elapsed: {elapsed:.1f}s",
        "",
        "## All gated tickets",
        "",
        "| d_in | d_out | n | hit | P&L | P&L/attempt | exit q | dump q | zero q |",
        "| --- | --- | --- | --- | --- | --- | --- | --- | --- |",
    ]
    for c in cells:
        a = c["all"]
        lines.append(
            f"| {c['d_in']:+d} | {c['d_out']:+d} | {a['n']:,} | {100*a['hit']:.1f}% | "
            f"{a['pnl']:,.2f} | {a['pnl']/a['n'] if a['n'] else 0:,.4f} | "
            f"{100*a['q_exit']:.1f}% | {100*a['q_liq']:.1f}% | {100*a['q_zero']:.1f}% |"
        )
    lines += [
        "",
        "## persist_floors_5 ≥ 3 only",
        "",
        "| d_in | d_out | n | hit | P&L | P&L/attempt | exit q | dump q | zero q |",
        "| --- | --- | --- | --- | --- | --- | --- | --- | --- |",
    ]
    for c in cells:
        a = c["persist3"]
        lines.append(
            f"| {c['d_in']:+d} | {c['d_out']:+d} | {a['n']:,} | {100*a['hit']:.1f}% | "
            f"{a['pnl']:,.2f} | {a['pnl']/a['n'] if a['n'] else 0:,.4f} | "
            f"{100*a['q_exit']:.1f}% | {100*a['q_liq']:.1f}% | {100*a['q_zero']:.1f}% |"
        )
    lines += [
        "",
        "Naive cell is `d_in=+1`, `d_out=−1`. Hit = FOK fill at X+1. "
        "exit/dump/zero shares are of **entered** tokens only.",
        "",
    ]
    OUT.write_text("\n".join(lines))
    print(f"wrote {OUT}  {elapsed:.1f}s", flush=True)
    con.close()
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
