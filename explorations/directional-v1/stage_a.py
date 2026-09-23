#!/usr/bin/env python3
"""Deprecated Stage B freeze. Use stage_b.py.

Filename is historical. Stage A persist is STAGE_A.md. The live
tape-touch freeze is stage_b.py / stage-b-snapshot.md.


H1 = X+1. H60 = X+2..X+60 (not X+1). Two HGBs. Per-head AUC/Brier and
hard-subset (lookback range < |k| ticks). Time split by event. No Stage B.

Writes stage-a-snapshot.md and scratch/directional-v1/stage_a_*.joblib
"""

from __future__ import annotations

import argparse
import json
import os
import sys
import time
from collections import Counter, defaultdict
from datetime import datetime, timezone
from pathlib import Path

import joblib
import numpy as np
import pandas as pd
from dotenv import load_dotenv
from sklearn.ensemble import HistGradientBoostingClassifier
from sklearn.metrics import brier_score_loss, roc_auc_score

_HERE = Path(__file__).resolve().parent
_PROJECT_ROOT = _HERE.parents[1]
sys.path.insert(0, str(_HERE))
sys.path.insert(0, str(_PROJECT_ROOT))
load_dotenv(_PROJECT_ROOT / ".env")

import run as R  # noqa: E402
from sim_lib import (  # noqa: E402
    BLOCKS_PER_DAY,
    BUCKETS,
    K_MAX,
    LIQ_END,
    LOOKBACK_BLOCKS,
    infer_tick,
    prefilter_alive,
)

KS_BAR = tuple(k for k in range(-K_MAX, K_MAX + 1) if k != 0)
LOG_DIR = Path(os.environ.get("SCRATCH_DIR", "/tmp")) / "directional-v1"
SNAP = _HERE / "stage-a-snapshot.md"
FEAT_KEYS: list[str] = []


def _auc(y, p) -> float:
    y = np.asarray(y)
    p = np.asarray(p)
    if y.size < 20 or y.sum() == 0 or y.sum() == len(y):
        return float("nan")
    try:
        return float(roc_auc_score(y, p))
    except Exception:
        return float("nan")


def _brier(y, p) -> float:
    y = np.asarray(y, dtype=float)
    p = np.asarray(p, dtype=float)
    if y.size < 20:
        return float("nan")
    try:
        return float(brier_score_loss(y, p))
    except Exception:
        return float("nan")


def _feat_window(sl: pd.DataFrame, x: int, last: float, tick: float) -> dict:
    out = {
        "last": last,
        "tick": tick,
        "min_p_1mp": float(min(last, 1.0 - last)),
        "hour_slot": float(x % BLOCKS_PER_DAY),
        "n_print_blocks": float(len(sl)),
        "fee_bit": 0.0,
        "neg_risk": 0.0,
        "ask_x": 0.0,
        "persist_floors_5": 0.0,
        "ret_1": 0.0,
        "ret_15": 0.0,
        "dist_high_ticks": 0.0,
        "dist_low_ticks": 0.0,
    }
    if sl.empty:
        for b in BUCKETS:
            out[f"n_{b}"] = 0.0
            out[f"range_{b}"] = 0.0
            out[f"imbalance_{b}"] = 0.0
            out[f"up_usdc_{b}"] = 0.0
        return out
    out["fee_bit"] = float((sl["fee_usdc"].fillna(0) > 0).any())
    out["neg_risk"] = float(sl["neg_risk"].fillna(0).max())
    at = sl[sl.block_number == x]
    if len(at):
        out["ask_x"] = float(at["ask_usdc"].fillna(0).sum())
    last5 = sl[sl.block_number >= x - 4]
    out["persist_floors_5"] = float(last5["ask_floor"].fillna(False).sum())
    hi100 = sl["high_yes"].max()
    lo100 = sl["low_yes"].min()
    if pd.notna(hi100) and tick > 0:
        out["dist_high_ticks"] = float((hi100 - last) / tick)
    if pd.notna(lo100) and tick > 0:
        out["dist_low_ticks"] = float((last - lo100) / tick)
    prev1 = sl[sl.block_number == x - 1]
    if len(prev1) and pd.notna(prev1["close_yes"].iloc[-1]):
        out["ret_1"] = last - float(prev1["close_yes"].iloc[-1])
    prev15 = sl[sl.block_number <= x - 15]
    if len(prev15) and pd.notna(prev15["close_yes"].iloc[-1]):
        out["ret_15"] = last - float(prev15["close_yes"].iloc[-1])
    for b in BUCKETS:
        w = sl[sl.block_number >= x - (b - 1)]
        out[f"n_{b}"] = float(w["n_fills"].fillna(0).sum())
        hi, lo = w["high_yes"].max(), w["low_yes"].min()
        out[f"range_{b}"] = float(hi - lo) if pd.notna(hi) and pd.notna(lo) else 0.0
        up = float(w["up_usdc"].fillna(0).sum())
        dn = float(w["dn_usdc"].fillna(0).sum())
        s = up + dn
        out[f"up_usdc_{b}"] = up
        out[f"imbalance_{b}"] = (up - dn) / s if s > 0 else 0.0
    return out


def _vec(feat: dict, extra: list[float]) -> np.ndarray:
    return np.array([feat[k] for k in FEAT_KEYS] + extra, dtype=float)


def _hgb():
    return HistGradientBoostingClassifier(
        max_depth=5,
        max_iter=80,
        learning_rate=0.08,
        min_samples_leaf=50,
        l2_regularization=0.1,
        random_state=0,
    )


def main() -> int:
    global FEAT_KEYS
    ap = argparse.ArgumentParser()
    ap.add_argument("--trigger-days", type=float, default=2.0)
    ap.add_argument("--max-events", type=int, default=None)
    args = ap.parse_args()

    if not R.FILLS_DIR or not Path(R.FILLS_DIR).exists():
        R._fail("FILLS_V1_DIR missing")
    LOG_DIR.mkdir(parents=True, exist_ok=True)
    fills_root = Path(R.FILLS_DIR)
    frontier = R._frontier(fills_root)
    x_hi = frontier - LIQ_END
    span = int(args.trigger_days * BLOCKS_PER_DAY)
    x_lo = x_hi - span + 1
    fill_lo = x_lo - LOOKBACK_BLOCKS
    fill_hi = x_hi + 60
    t0 = time.time()
    started = datetime.now(timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ")
    print(f"stage A freeze  trigger {x_lo:,}–{x_hi:,}", flush=True)

    ypx = """
      CASE
        WHEN (gross_usdc > 0 AND net_yes_tokens > 0) OR (gross_usdc < 0 AND net_yes_tokens < 0)
          THEN gross_usdc::DOUBLE / net_yes_tokens::DOUBLE
        ELSE 1.0 + gross_usdc::DOUBLE / net_yes_tokens::DOUBLE
      END
    """
    con = R._connect()
    fill_files = R._files(fills_root, fill_lo, fill_hi)
    con.execute(
        f"""
        CREATE TABLE fills AS
        SELECT block_number, logical_fill_index, hex(condition_id) AS cid,
               is_taker, net_yes_tokens, gross_usdc, fee_usdc, market_id,
               {ypx} AS yes_px
        FROM read_parquet({R._sql_list(fill_files)})
        WHERE block_number BETWEEN {fill_lo} AND {fill_hi}
          AND net_yes_tokens <> 0 AND gross_usdc <> 0
        """
    )
    print(f"  fills {con.execute('SELECT count(*) FROM fills').fetchone()[0]:,}", flush=True)
    con.execute(
        """
        CREATE TABLE cbar AS
        SELECT
          block_number, cid,
          arg_max(yes_px, logical_fill_index) AS close_yes,
          max(yes_px) AS high_yes,
          min(yes_px) AS low_yes,
          count(*) AS n_fills,
          sum(gross_usdc) FILTER (
            WHERE is_taker AND gross_usdc > 0 AND net_yes_tokens > 0
          )::DOUBLE / 1e6 AS up_usdc,
          sum(gross_usdc) FILTER (
            WHERE is_taker AND gross_usdc > 0 AND net_yes_tokens < 0
          )::DOUBLE / 1e6 AS dn_usdc,
          sum(-gross_usdc) FILTER (WHERE NOT is_taker AND gross_usdc < 0)::DOUBLE / 1e6 AS ask_usdc,
          (sum(abs(net_yes_tokens)) FILTER (WHERE NOT is_taker AND gross_usdc < 0)::DOUBLE / 1e6 >= 5
           AND sum(-gross_usdc) FILTER (WHERE NOT is_taker AND gross_usdc < 0)::DOUBLE / 1e6 >= 1.20
          ) AS ask_floor,
          max(fee_usdc) AS fee_usdc,
          max(CASE WHEN market_id IS NOT NULL THEN 1 ELSE 0 END) AS neg_risk
        FROM fills
        GROUP BY 1, 2
        """
    )
    con.execute(
        """
        CREATE TABLE cpx AS
        SELECT DISTINCT cid, block_number, round(yes_px, 8) AS px
        FROM fills WHERE yes_px > 0 AND yes_px < 1
        """
    )
    cbar = con.execute("SELECT * FROM cbar").fetchdf()
    cpx = con.execute("SELECT cid, block_number, px FROM cpx").fetchdf()
    hl = {
        (r.cid, int(r.block_number)): (int(r.n_fills), float(r.high_yes), float(r.low_yes))
        for r in cbar.itertuples(index=False)
    }
    cbar = cbar.sort_values(["cid", "block_number"])
    grouped = {cid: g.reset_index(drop=True) for cid, g in cbar.groupby("cid", sort=False)}
    pgrouped = {cid: g.reset_index(drop=True) for cid, g in cpx.groupby("cid", sort=False)}
    fill_blocks = {cid: set(g.block_number.astype(int)) for cid, g in grouped.items()}

    ev_keys = (
        cbar[(cbar.block_number >= x_lo) & (cbar.block_number <= x_hi)][["block_number", "cid"]]
        .drop_duplicates()
        .sort_values(["block_number", "cid"])
    )
    if args.max_events:
        ev_keys = ev_keys.head(int(args.max_events))
    print(f"  events {len(ev_keys):,}", flush=True)

    def window_hl(cid: str, lo: int, hi: int) -> tuple[int, float, float]:
        n = 0
        h, l = 0.0, 1.0
        anyp = 0
        for b in range(lo, hi + 1):
            hit = hl.get((cid, b))
            if not hit:
                continue
            nn, hh, ll = hit
            anyp = 1
            n += nn
            if hh > h:
                h = hh
            if ll < l:
                l = ll
        return anyp, h, l

    events = []
    t_feat = time.time()
    ticks = Counter()
    for i, row in enumerate(ev_keys.itertuples(index=False), 1):
        x, cid = int(row.block_number), row.cid
        g = grouped.get(cid)
        if g is None:
            continue
        if not prefilter_alive(fill_blocks[cid], x):
            continue
        sl = g[(g.block_number >= x - (LOOKBACK_BLOCKS - 1)) & (g.block_number <= x)]
        at = sl[sl.block_number == x]
        if at.empty or pd.isna(at["close_yes"].iloc[-1]):
            continue
        last = float(at["close_yes"].iloc[-1])
        if not (0 < last < 1):
            continue
        pg = pgrouped.get(cid)
        if pg is None:
            tick = 0.01
        else:
            wpx = pg[(pg.block_number >= x - (LOOKBACK_BLOCKS - 1)) & (pg.block_number <= x)]
            tick = infer_tick(wpx["px"].tolist())
        ticks[tick] += 1
        feat = _feat_window(sl, x, last, tick)
        if not FEAT_KEYS:
            FEAT_KEYS.extend(feat.keys())
        a1, h1, l1 = window_hl(cid, x + 1, x + 1)
        a60, h60, l60 = window_hl(cid, x + 2, x + 60)
        events.append(
            {
                "x_block": x,
                "cid": cid,
                "last": last,
                "tick": tick,
                "feat": feat,
                "range_100": feat["range_100"],
                "y_act_1": a1,
                "y_act_60": a60,
                "h1": h1,
                "l1": l1,
                "h60": h60,
                "l60": l60,
            }
        )
        if i % 20000 == 0:
            print(f"  features {i:,}/{len(ev_keys):,}  {time.time()-t_feat:.1f}s", flush=True)

    print(f"  events kept {len(events):,}  {time.time()-t0:.1f}s  ticks {dict(ticks)}", flush=True)
    xs = sorted({e["x_block"] for e in events})
    span_b = xs[-1] - xs[0]
    emb = min(120, max(0, span_b // 25))
    cut1 = xs[int(len(xs) * 0.55)]
    cut2 = xs[int(len(xs) * 0.78)]
    train_ev = [e for e in events if e["x_block"] <= cut1]
    val_ev = [e for e in events if cut1 + emb <= e["x_block"] <= cut2]
    test_ev = [e for e in events if e["x_block"] >= cut2 + emb]
    print(f"  split train {len(train_ev):,} val {len(val_ev):,} test {len(test_ev):,}", flush=True)

    def pack_act(evs):
        X, y, meta = [], [], []
        for e in evs:
            for z, key in ((1, "y_act_1"), (60, "y_act_60")):
                X.append(_vec(e["feat"], [float(z)]))
                y.append(e[key])
                meta.append((e, z, 0))
        return np.vstack(X), np.array(y, dtype=int), meta

    def pack_bar(evs):
        X, y, meta = [], [], []
        for e in evs:
            last, tick = e["last"], e["tick"]
            for z, h, l, a in (
                (1, e["h1"], e["l1"], e["y_act_1"]),
                (60, e["h60"], e["l60"], e["y_act_60"]),
            ):
                for k in KS_BAR:
                    if a:
                        barrier = last + k * tick
                        lab = int(h >= barrier - 1e-15) if k > 0 else int(l <= barrier + 1e-15)
                    else:
                        lab = 0
                    X.append(_vec(e["feat"], [float(k), float(z)]))
                    y.append(lab)
                    meta.append((e, z, k))
        return np.vstack(X), np.array(y, dtype=int), meta

    Xa_tr, ya_tr, _ = pack_act(train_ev)
    Xa_va, ya_va, ma_va = pack_act(val_ev)
    Xa_te, ya_te, ma_te = pack_act(test_ev)
    Xb_tr, yb_tr, _ = pack_bar(train_ev)
    Xb_va, yb_va, mb_va = pack_bar(val_ev)
    Xb_te, yb_te, mb_te = pack_bar(test_ev)

    print(f"  fit activity n={len(ya_tr):,} pos={ya_tr.mean():.3f}", flush=True)
    est_act = _hgb()
    est_act.fit(Xa_tr, ya_tr)
    print(f"  fit barrier n={len(yb_tr):,} pos={yb_tr.mean():.3f}", flush=True)
    est_bar = _hgb()
    est_bar.fit(Xb_tr, yb_tr)

    pa_va = est_act.predict_proba(Xa_va)[:, 1]
    pa_te = est_act.predict_proba(Xa_te)[:, 1]
    pb_va = est_bar.predict_proba(Xb_va)[:, 1]
    pb_te = est_bar.predict_proba(Xb_te)[:, 1]

    def split_act(y, p, meta):
        out = {}
        for z in (1, 60):
            m = np.array([t[1] == z for t in meta])
            out[z] = (y[m], p[m])
        return out

    def split_bar(y, p, meta):
        out = {}
        hard = {}
        for z in (1, 60):
            for k in KS_BAR:
                m = np.array([t[1] == z and t[2] == k for t in meta])
                out[(k, z)] = (y[m], p[m])
                hm = []
                hy = []
                hp = []
                for i, (e, zz, kk) in enumerate(meta):
                    if zz != z or kk != k:
                        continue
                    if e["range_100"] + 1e-15 < abs(k) * e["tick"]:
                        hy.append(y[i])
                        hp.append(p[i])
                hard[(k, z)] = (np.array(hy), np.array(hp))
        return out, hard

    act_va = split_act(ya_va, pa_va, ma_va)
    act_te = split_act(ya_te, pa_te, ma_te)
    bar_va, hard_va = split_bar(yb_va, pb_va, mb_va)
    bar_te, hard_te = split_bar(yb_te, pb_te, mb_te)

    # stability: val first vs second half by time
    if val_ev:
        mid = val_ev[len(val_ev) // 2]["x_block"]
        val_a = [e for e in val_ev if e["x_block"] <= mid]
        val_b = [e for e in val_ev if e["x_block"] > mid]
        X1, y1, m1m = pack_act(val_a)
        X2, y2, m2m = pack_act(val_b)
        p1 = est_act.predict_proba(X1)[:, 1]
        p2 = est_act.predict_proba(X2)[:, 1]
        stab_h1 = (
            _auc(*split_act(y1, p1, m1m)[1]),
            _auc(*split_act(y2, p2, m2m)[1]),
        )
        Xb1, yb1, mb1 = pack_bar(val_a)
        Xb2, yb2, mb2 = pack_bar(val_b)
        pb1 = est_bar.predict_proba(Xb1)[:, 1]
        pb2 = est_bar.predict_proba(Xb2)[:, 1]
        # +1 H60 as a representative barrier
        def pick(y, p, meta, k, z):
            m = np.array([t[1] == z and t[2] == k for t in meta])
            return y[m], p[m]
        stab_up = (
            _auc(*pick(yb1, pb1, mb1, 1, 60)),
            _auc(*pick(yb2, pb2, mb2, 1, 60)),
        )
    else:
        stab_h1 = (float("nan"), float("nan"))
        stab_up = (float("nan"), float("nan"))

    joblib.dump({"act": est_act, "bar": est_bar, "feat_keys": FEAT_KEYS}, LOG_DIR / "stage_a.joblib")
    metrics = {
        "started": started,
        "n_train": len(train_ev),
        "n_val": len(val_ev),
        "n_test": len(test_ev),
        "ticks": dict(ticks),
        "stab_act_h1_val_halves": stab_h1,
        "stab_bar_p1_h60_val_halves": stab_up,
    }
    (LOG_DIR / "stage_a_metrics.json").write_text(json.dumps(metrics, default=str, indent=2))

    def row(name, y, p):
        return (
            f"| {name} | {len(y):,} | {np.mean(y):.3f} | {_auc(y, p):.3f} | {_brier(y, p):.4f} |"
            if len(y) else f"| {name} | 0 | — | — | — |"
        )

    lines = [
        "# Stage A freeze",
        "",
        f"Generated {started}. Wall {time.time()-t0:.1f}s. "
        f"Trigger {x_lo:,}–{x_hi:,}.",
        "",
        "Two HGBs. **activity** (`k=0`) and **barrier** (`k≠0`). "
        "`H1` = block `X+1`. `H60` = `X+2..X+60` (GTC window, not the FOK bar). "
        "Tick inferred from last 100 blocks. Both directions via `±k` on one last YES price.",
        "",
        f"Split: train {len(train_ev):,}  val {len(val_ev):,}  test {len(test_ev):,} "
        f"(event-level, embargo {emb} blocks).",
        "",
        "Inferred ticks: " + ", ".join(f"{t:g}×{c}" for t, c in sorted(ticks.items(), key=lambda kv: -kv[1])),
        "",
        f"Val stability (first vs second half): activity H1 AUC {stab_h1[0]:.3f} / {stab_h1[1]:.3f}; "
        f"barrier +1 H60 AUC {stab_up[0]:.3f} / {stab_up[1]:.3f}.",
        "",
        "## Activity (`k=0`)",
        "",
        "| split/head | n | pos | AUC | Brier |",
        "| --- | --- | --- | --- | --- |",
        row("val H1", *act_va[1]),
        row("val H60", *act_va[60]),
        row("test H1", *act_te[1]),
        row("test H60", *act_te[60]),
        "",
        "## Barrier (all val)",
        "",
        "| k | H1 pos | H1 AUC | H1 Brier | H60 pos | H60 AUC | H60 Brier |",
        "| --- | --- | --- | --- | --- | --- | --- |",
    ]
    for k in KS_BAR:
        y1, p1 = bar_va[(k, 1)]
        y6, p6 = bar_va[(k, 60)]
        lines.append(
            f"| {k:+d} | {np.mean(y1):.3f} | {_auc(y1, p1):.3f} | {_brier(y1, p1):.4f} | "
            f"{np.mean(y6):.3f} | {_auc(y6, p6):.3f} | {_brier(y6, p6):.4f} |"
        )
    lines += [
        "",
        "## Barrier hard subset (val): lookback range < |k| ticks",
        "",
        "If the last 100 blocks already swung more than `k` ticks, `+k` is easy. "
        "This slice is the breakout case.",
        "",
        "| k | H1 n | H1 pos | H1 AUC | H60 n | H60 pos | H60 AUC |",
        "| --- | --- | --- | --- | --- | --- | --- |",
    ]
    for k in KS_BAR:
        y1, p1 = hard_va[(k, 1)]
        y6, p6 = hard_va[(k, 60)]
        lines.append(
            f"| {k:+d} | {len(y1):,} | {np.mean(y1) if len(y1) else float('nan'):.3f} | {_auc(y1, p1):.3f} | "
            f"{len(y6):,} | {np.mean(y6) if len(y6) else float('nan'):.3f} | {_auc(y6, p6):.3f} |"
        )
    lines += [
        "",
        "## Test confirmation (not used to pick the freeze)",
        "",
        "| k | H1 AUC | H60 AUC | H60 hard AUC |",
        "| --- | --- | --- | --- |",
    ]
    for k in KS_BAR:
        y1, p1 = bar_te[(k, 1)]
        y6, p6 = bar_te[(k, 60)]
        yh, ph = hard_te[(k, 60)]
        lines.append(
            f"| {k:+d} | {_auc(y1, p1):.3f} | {_auc(y6, p6):.3f} | {_auc(yh, ph):.3f} |"
        )
    lines += [
        "",
        f"Models: `{LOG_DIR / 'stage_a.joblib'}`.",
        "",
        "Stage A is **frozen** on this card. Stage B is not run here.",
        "",
    ]
    SNAP.write_text("\n".join(lines))
    print(f"wrote {SNAP}  {time.time()-t0:.1f}s", flush=True)
    con.close()
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
