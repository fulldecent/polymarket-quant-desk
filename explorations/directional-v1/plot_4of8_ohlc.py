#!/usr/bin/env python3
"""Count 4-of-8 Stage A triggers in the last 7 calendar days; plot 20 random
OHLC paths aligned at X, last price = 0. SVG to Desktop."""

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
from sim_lib import LIQ_END, LOOKBACK_BLOCKS, kris_count, prefilter_alive  # noqa: E402

# Recent Polygon ~1.5 s/block → 7 calendar days.
DAYS = 7
SEC_PER_BLOCK = 1.5
TRIGGER_BLOCKS = int(DAYS * 86400 / SEC_PER_BLOCK)
NEED = 4
WINDOW = 8
FWD = 60
N_PLOT = 20
SEED = 0
OUT = Path.home() / "Desktop" / "stage-a-4of8-ohlc-20.svg"

# Short-horizon crypto up-down / coin-price factories. Identified from
# ConditionalTokens/condition_preparation.oracle on cold events:
# dense tape, fill span ~5–30 min (4h/1d on the last two), taker fees,
# reportPayouts on a 5/15m clock. Not UmaCtfAdapter / NegRiskAdapter
# (politics, sports, crypto-policy).
_CRYPTO_ORACLES = frozenset(
    {
        "58e1745bedda7312c4cddb72618923da1b90efde",  # 5m/15m (~2.8k cid/day)
        "e4ff29ae397712a8bd250be92f06e8208134d580",  # 5m/15m
        "29ae8827af7fec3660aa76c1ee4f6bc84e46c590",  # ~1h
        "1a78eda82e34b446f296f6ed9e0847e5a7c2acd0",  # 4h
    }
)


def _norm_cid(s: str) -> str:
    return (s or "").lower().replace("0x", "")


def crypto_cids_from_prep(cids: set[str], lo: int, hi: int) -> set[str]:
    """condition_ids whose preparing oracle is a crypto price factory."""
    events = os.environ.get("POLYGON_CONTRACT_EVENTS_V3_DIR") or ""
    root = Path(events) / "ConditionalTokens" / "condition_preparation"
    if not events or not root.exists():
        raise SystemExit("POLYGON_CONTRACT_EVENTS_V3_DIR missing")
    slack = int(4 * 86400 / SEC_PER_BLOCK)
    files = R._files(root, lo - slack, hi)
    if not files:
        return set()
    oracles = ",".join(f"'{o}'" for o in sorted(_CRYPTO_ORACLES))
    con = R._connect()
    rows = con.execute(
        f"""
        SELECT hex(condition_id)
        FROM read_parquet({R._sql_list(files)})
        WHERE lower(hex(oracle)) IN ({oracles})
        """
    ).fetchall()
    con.close()
    crypto = {_norm_cid(r[0]) for r in rows if r and r[0]}
    return {c for c in cids if _norm_cid(c) in crypto}


def main() -> int:
    import argparse

    global NEED, OUT
    ap = argparse.ArgumentParser()
    ap.add_argument("--need", type=int, default=NEED)
    ap.add_argument("--kk-min", type=int, default=0, help="Kris Kross count >= this (0 = off)")
    ap.add_argument("--n-plot", type=int, default=N_PLOT)
    ap.add_argument("--seed", type=int, default=SEED)
    ap.add_argument("--out", type=str, default="")
    ap.add_argument("--exclude-crypto", action="store_true")
    args = ap.parse_args()
    NEED = int(args.need)
    n_plot = int(args.n_plot)
    kk_min = int(args.kk_min)
    suffix = f"{NEED}of8"
    if kk_min:
        suffix += f"-kk{kk_min}"
    if args.exclude_crypto:
        suffix += "-nocrrypto"
    OUT = (
        Path(args.out)
        if args.out
        else Path.home() / "Desktop" / f"stage-a-{suffix}-ohlc-{n_plot}.svg"
    )
    t0 = time.time()
    fills_root = Path(R.FILLS_DIR)
    frontier = R._frontier(fills_root)
    x_hi = frontier - LIQ_END
    x_lo = x_hi - TRIGGER_BLOCKS + 1
    fill_lo = x_lo - LOOKBACK_BLOCKS
    fill_hi = x_hi + FWD
    print(
        f"{NEED}-of-8 triggers  last {DAYS}d (~{TRIGGER_BLOCKS:,} blocks @ {SEC_PER_BLOCK}s)  "
        f"{x_lo:,}–{x_hi:,}",
        flush=True,
    )
    ypx = R._yes_px_sql()
    con = R._connect()
    con.execute("SET memory_limit = '8GB'")
    files = R._files(fills_root, fill_lo, fill_hi)
    con.execute(
        f"""
        CREATE TABLE cbar AS
        SELECT block_number, cid,
               arg_min(yes_px, logical_fill_index) AS open_yes,
               max(yes_px) AS high_yes,
               min(yes_px) AS low_yes,
               arg_max(yes_px, logical_fill_index) AS close_yes
        FROM (
          SELECT block_number, logical_fill_index, hex(condition_id) AS cid,
                 {ypx} AS yes_px
          FROM read_parquet({R._sql_list(files)})
          WHERE block_number BETWEEN {fill_lo} AND {fill_hi}
            AND net_yes_tokens <> 0 AND gross_usdc <> 0
        )
        GROUP BY 1, 2
        """
    )
    cbar = con.execute(
        "SELECT block_number, cid, open_yes, high_yes, low_yes, close_yes FROM cbar"
    ).fetchdf()
    con.close()
    cbar = cbar.sort_values(["cid", "block_number"])
    print(f"  cbar {len(cbar):,}  {time.time()-t0:.1f}s", flush=True)

    grouped = {cid: g.reset_index(drop=True) for cid, g in cbar.groupby("cid", sort=False)}
    fill_blocks = {cid: set(g.block_number.astype(int)) for cid, g in grouped.items()}
    g_blocks = {cid: g.block_number.to_numpy(dtype=np.int64) for cid, g in grouped.items()}

    g_m_blk, g_m_px = {}, {}
    if kk_min > 0:
        mk_lo = x_lo - (WINDOW - 1)
        con = R._connect()
        con.execute("SET memory_limit = '8GB'")
        makers = con.execute(
            f"""
            SELECT cid, block_number, logical_fill_index, px
            FROM (
              SELECT block_number, logical_fill_index, hex(condition_id) AS cid,
                     is_taker, round(({ypx})::DOUBLE, 6) AS px
              FROM read_parquet({R._sql_list(files)})
              WHERE block_number BETWEEN {mk_lo} AND {x_hi}
                AND net_yes_tokens <> 0 AND gross_usdc <> 0
            )
            WHERE NOT is_taker AND px IS NOT NULL
            ORDER BY cid, block_number, logical_fill_index
            """
        ).fetchdf()
        con.close()
        print(f"  maker legs {len(makers):,}  {time.time()-t0:.1f}s", flush=True)
        for cid, g in makers.groupby("cid", sort=False):
            g_m_blk[cid] = g.block_number.to_numpy(dtype=np.int64)
            g_m_px[cid] = g.px.to_numpy(dtype=np.float64)

    ev = cbar[(cbar.block_number >= x_lo) & (cbar.block_number <= x_hi)][
        ["block_number", "cid"]
    ].drop_duplicates()
    alive = []
    for row in ev.itertuples(index=False):
        x, cid = int(row.block_number), row.cid
        if not prefilter_alive(fill_blocks[cid], x, window=WINDOW, need=NEED):
            continue
        if kk_min > 0:
            m_blk = g_m_blk.get(cid)
            if m_blk is None:
                continue
            lo = x - (WINDOW - 1)
            a = int(np.searchsorted(m_blk, lo, side="left"))
            b = int(np.searchsorted(m_blk, x, side="right"))
            if kris_count(g_m_px[cid][a:b]) < kk_min:
                continue
        alive.append((x, cid))
    n_all = len(alive)
    n_crypto = 0
    if args.exclude_crypto:
        unique = {cid for _, cid in alive}
        print(
            f"  classifying {len(unique):,} trigger cids via condition_preparation.oracle …",
            flush=True,
        )
        crypto = crypto_cids_from_prep(unique, fill_lo, fill_hi)
        print(f"  crypto-oracle cids {len(crypto):,}", flush=True)
        crypto_n = {_norm_cid(c) for c in crypto}
        kept = []
        for x, cid in alive:
            if _norm_cid(cid) in crypto_n:
                n_crypto += 1
            else:
                kept.append((x, cid))
        alive = kept
        print(
            f"  {NEED}-of-8 all {n_all:,}  crypto {n_crypto:,}  "
            f"**non-crypto {len(alive):,}**",
            flush=True,
        )
    kk_bit = f"  Kris Kross {kk_min}+" if kk_min else ""
    print(
        f"  candidates {len(ev):,}  {NEED}+ of 8{kk_bit}  **{len(alive):,}**",
        flush=True,
    )
    if len(alive) < n_plot:
        print("  not enough triggers to plot", flush=True)
        return 1

    rng = np.random.RandomState(int(args.seed))
    pick = [alive[i] for i in rng.choice(len(alive), n_plot, replace=False)]
    pick.sort()

    series = []
    ymin, ymax = 0.0, 0.0
    for x, cid in pick:
        g = grouped[cid]
        bn = g_blocks[cid]
        i0 = int(np.searchsorted(bn, x - (LOOKBACK_BLOCKS - 1), side="left"))
        i1 = int(np.searchsorted(bn, x + FWD, side="right"))
        sl = g.iloc[i0:i1]
        at = sl[sl.block_number.to_numpy() == x]
        if at.empty or at["close_yes"].isna().all():
            continue
        last = float(at["close_yes"].iloc[-1])
        bars = []
        for r in sl.itertuples(index=False):
            rel = int(r.block_number) - x
            o = float(r.open_yes) - last if r.open_yes == r.open_yes else 0.0
            h = float(r.high_yes) - last if r.high_yes == r.high_yes else 0.0
            l = float(r.low_yes) - last if r.low_yes == r.low_yes else 0.0
            c = float(r.close_yes) - last if r.close_yes == r.close_yes else 0.0
            bars.append((rel, o, h, l, c))
            ymin = min(ymin, l, c, o)
            ymax = max(ymax, h, c, o)
        series.append({"x": x, "cid": cid, "last": last, "bars": bars})
    print(f"  plotted {len(series)}  y∈[{ymin:.4f},{ymax:.4f}]", flush=True)

    W, H = 2400, 1100
    pad_l, pad_r, pad_t, pad_b = 70, 30, 50, 50
    x0, x1 = -(LOOKBACK_BLOCKS - 1), FWD
    ypad = max(0.01, 0.08 * (ymax - ymin if ymax > ymin else 0.05))
    y0, y1 = ymin - ypad, ymax + ypad

    def sx(rel: float) -> float:
        return pad_l + (rel - x0) / (x1 - x0) * (W - pad_l - pad_r)

    def sy(p: float) -> float:
        return pad_t + (y1 - p) / (y1 - y0) * (H - pad_t - pad_b)

    hues = [
        f"hsl({int(i * 360 / max(1, len(series)))},70%,40%)" for i in range(len(series))
    ]
    parts = [
        f'<svg xmlns="http://www.w3.org/2000/svg" width="{W}" height="{H}" '
        f'viewBox="0 0 {W} {H}">',
        "<style>",
        "  text { font-family: ui-monospace, Menlo, monospace; font-size: 12px; fill: #222; }",
        "  .axis { stroke: #111; stroke-width: 1.2; }",
        "  .grid { stroke: #ddd; stroke-width: 0.6; }",
        "  .ohlc { fill: none; stroke-width: 1.05; opacity: 0.7; stroke-linecap: square; }",
        "  .link { fill: none; stroke-width: 1.0; opacity: 0.5; stroke-linecap: butt; }",
        "</style>",
        f'<rect width="{W}" height="{H}" fill="#fafafa"/>',
        f'<text x="{pad_l}" y="22">{NEED}+ of 8'
        + (f", Kris Kross {kk_min}+" if kk_min else "")
        + (" non-crypto" if args.exclude_crypto else "")
        + f"  last {DAYS} calendar days  "
        f"n={len(alive):,}  sample {len(series)}  "
        f"x=block−X  y=YES−last(X)  lookback {LOOKBACK_BLOCKS}  forward {FWD}</text>",
        f'<line class="axis" x1="{sx(0):.1f}" y1="{pad_t}" x2="{sx(0):.1f}" y2="{H-pad_b}"/>',
        f'<line class="axis" x1="{pad_l}" y1="{sy(0):.1f}" x2="{W-pad_r}" y2="{sy(0):.1f}"/>',
        f'<text x="{sx(0)+4:.1f}" y="{pad_t+14}">X</text>',
        f'<text x="{pad_l+4}" y="{sy(0)-4:.1f}">last</text>',
    ]
    for rel in range(x0, x1 + 1, 20):
        parts.append(
            f'<line class="grid" x1="{sx(rel):.1f}" y1="{pad_t}" x2="{sx(rel):.1f}" y2="{H-pad_b}"/>'
        )
        parts.append(
            f'<text x="{sx(rel):.1f}" y="{H-pad_b+16}" text-anchor="middle">{rel}</text>'
        )
    for i, s in enumerate(series):
        color = hues[i]
        step = sx(1) - sx(0)
        tick = max(2.0, step * 0.38)
        link = []
        prev_c = None
        for rel, o, h, l, c in s["bars"]:
            xmid = sx(rel)
            yo, yh, yl, yc = sy(o), sy(h), sy(l), sy(c)
            o_left, c_right = xmid - tick, xmid + tick
            # Classic OHLC: O tick left of stem, HL vertical, C tick right of stem.
            # All three share xmid so O/C meet HL.
            parts.append(
                f'<path class="ohlc" stroke="{color}" d="'
                f"M{xmid:.2f},{yh:.2f} L{xmid:.2f},{yl:.2f} "
                f"M{o_left:.2f},{yo:.2f} L{xmid:.2f},{yo:.2f} "
                f"M{xmid:.2f},{yc:.2f} L{c_right:.2f},{yc:.2f}"
                f'"/>'
            )
            if prev_c is not None:
                px, py = prev_c
                link.append(f"M{px:.2f},{py:.2f} L{o_left:.2f},{yo:.2f}")
            prev_c = (c_right, yc)
        if link:
            parts.append(f'<path class="link" stroke="{color}" d="{" ".join(link)}"/>')
    # legend
    ly = 38
    for i, s in enumerate(series[:20]):
        parts.append(
            f'<rect x="{W-420}" y="{ly+i*16-8}" width="10" height="10" fill="{hues[i]}" opacity="0.7"/>'
        )
        parts.append(
            f'<text x="{W-406}" y="{ly+i*16}">{s["cid"][:10]}… X={s["x"]} last={s["last"]:.3f}</text>'
        )
    parts.append("</svg>")
    OUT.write_text("\n".join(parts))
    print(f"wrote {OUT}  {time.time()-t0:.1f}s", flush=True)
    tag = f"TRIGGERS_7D_{NEED}OF8" + (f"_KK{kk_min}" if kk_min else "")
    print(f"{tag} {len(alive)}", flush=True)
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
