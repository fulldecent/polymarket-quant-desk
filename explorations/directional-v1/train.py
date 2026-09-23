#!/usr/bin/env python3
"""Stage C sequential policy on Stage B tape-touch (after Stage A persist).

Every (condition, block) with fills. Last price is YES-equivalent; +k is
buy-YES, −k is buy-NO. Tick from last 100 blocks among documented legal
steps. Horizons Z=1 (X+1) and Z=60 (X+1..X+60).

Train proposes, val promotes (≥1 activation/hour else hard fail), test reports.
"""

from __future__ import annotations

import argparse
import csv
import os
import sys
import time
from collections import defaultdict
from datetime import datetime, timezone
from pathlib import Path

import numpy as np
import pandas as pd
from dotenv import load_dotenv
from sklearn.ensemble import HistGradientBoostingClassifier
from sklearn.metrics import roc_auc_score

_HERE = Path(__file__).resolve().parent
_PROJECT_ROOT = _HERE.parents[1]
sys.path.insert(0, str(_HERE))
sys.path.insert(0, str(_PROJECT_ROOT))
load_dotenv(_PROJECT_ROOT / ".env")

import run as R  # noqa: E402
from sim_lib import (  # noqa: E402
    BLOCKS_PER_DAY,
    BUCKETS,
    COOLDOWN_BLOCKS,
    HORIZONS,
    K_MAX,
    LIQ_END,
    LOOKBACK_BLOCKS,
    MIN_SHARES,
    Level,
    infer_tick,
    long_caps,
    n_min,
    simulate_roundtrip,
)

BLOCKS_PER_HOUR = 1714
SIZE_MULT = 1.1
LOG_DIR = Path(os.environ.get("SCRATCH_DIR", "/tmp")) / "directional-v1"
SNAP = _HERE / "train-snapshot.md"
KS = tuple(range(-K_MAX, K_MAX + 1))  # -4 .. +4
FEAT_KEYS: list[str] = []


def _hours(lo: int, hi: int) -> float:
    return (hi - lo + 1) / float(BLOCKS_PER_HOUR)


def _vec(feat: dict, k: float, z: float) -> np.ndarray:
    return np.array([feat[key] for key in FEAT_KEYS] + [k, z], dtype=float)


def _feat_window(sl: pd.DataFrame, x: int, last: float, tick: float) -> dict:
    out = {
        "last": last,
        "tick": tick,
        "min_p_1mp": float(min(last, 1.0 - last)),
        "hour_slot": float(x % BLOCKS_PER_DAY),
        "n_print_blocks": float(len(sl)),
        "fee_bit": float((sl["fee_usdc"].fillna(0) > 0).any()) if len(sl) else 0.0,
        "neg_risk": float(sl["neg_risk"].fillna(0).max()) if len(sl) else 0.0,
        "ask_x": 0.0,
        "persist_floors_5": 0.0,
    }
    if sl.empty:
        for b in BUCKETS:
            out[f"n_{b}"] = 0.0
            out[f"range_{b}"] = 0.0
            out[f"imbalance_{b}"] = 0.0
            out[f"up_usdc_{b}"] = 0.0
        return out
    at = sl[sl.block_number == x]
    if len(at):
        out["ask_x"] = float(at["ask_usdc"].fillna(0).sum())
    last5 = sl[sl.block_number >= x - 4]
    out["persist_floors_5"] = float(last5["ask_floor"].fillna(False).sum())
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


def _proba(est, X: np.ndarray) -> np.ndarray:
    if est is None or len(X) == 0:
        return np.zeros(len(X))
    return est.predict_proba(X)[:, 1]


def _touch(hi: float, lo: float, n: int, last: float, tick: float, k: int) -> int:
    if n <= 0:
        return 0
    if k == 0:
        return 1
    barrier = last + k * tick
    if k > 0:
        return int(hi >= barrier - 1e-15)
    return int(lo <= barrier + 1e-15)


def sequential_eval(events, *, t0p, t_up, k_min, asks_i, tbuy_i, bids_i) -> dict:
    cool: dict[str, int] = {}
    n_part = pnl = n_ok = 0
    qe = ql = qz = 0.0
    for ev in events:
        x, cid = ev["x_block"], ev["cid"]
        if x < cool.get(cid, -1):
            continue
        last, tick = ev["last"], ev["tick"]
        pmap = ev.get("p", {})
        best = None
        best_sc = 0.0
        if pmap.get((0, 1), 0.0) < t0p:
            continue
        for side, sign in (("yes", 1), ("no", -1)):
            close_side = last if side == "yes" else 1.0 - last
            for din in range(0, K_MAX):
                for dout in range(din + 1, K_MAX + 1):
                    caps = long_caps(close_side, din, dout, tick)
                    if caps is None:
                        continue
                    p_in, p_out = caps
                    if (p_out - p_in) + 1e-15 < k_min * tick:
                        continue
                    # +dout on YES-price for buy YES; −dout for buy NO
                    k_exit = sign * dout
                    pe = pmap.get((k_exit, 60), 0.0)
                    if pe < t_up:
                        continue
                    sc = pe * (p_out - p_in)
                    if sc > best_sc:
                        best_sc = sc
                        n = max(MIN_SHARES, SIZE_MULT * n_min(p_in))
                        best = (side, p_in, p_out, n)
        if best is None:
            continue
        side, p_in, p_out, n = best
        n_part += 1
        cool[cid] = x + COOLDOWN_BLOCKS
        entry = asks_i.get((cid, side, x + 1), [])
        exb = [(b, tbuy_i[(cid, side, b)]) for b in range(x + 2, x + 61) if (cid, side, b) in tbuy_i]
        liqb = [(b, bids_i[(cid, side, b)]) for b in range(x + 61, x + 121) if (cid, side, b) in bids_i]
        sim = simulate_roundtrip(
            n=float(n),
            p_entry=p_in,
            p_exit=p_out,
            entry_asks=entry,
            exit_by_block=exb,
            liq_by_block=liqb,
            x_block=x,
            fee_market=bool(ev["fee_market"]),
        )
        pnl += sim["pnl"]
        n_ok += int(sim["entry_ok"])
        qe += sim["q_exit"]
        ql += sim["q_liq"]
        qz += sim["q_zero"]
    if events:
        hours = _hours(events[0]["x_block"], events[-1]["x_block"])
    else:
        hours = 0.0
    rate = n_part / hours if hours > 0 else 0.0
    qt = qe + ql + qz
    return {
        "n_part": n_part,
        "per_hour": rate,
        "hard_fail": rate < 1.0,
        "pnl": None if rate < 1.0 else pnl,
        "hit": (n_ok / n_part) if n_part else 0.0,
        "q_zero": (qz / qt) if qt else 0.0,
    }


def main() -> int:
    global FEAT_KEYS
    ap = argparse.ArgumentParser()
    ap.add_argument("--trigger-days", type=float, default=2.0)
    ap.add_argument("--b-iters", type=int, default=10)
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
    fill_hi = x_hi + LIQ_END
    t0 = time.time()
    started = datetime.now(timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ")
    print(f"stage A tape-touch  trigger {x_lo:,}–{x_hi:,}", flush=True)

    fill_files = R._files(fills_root, fill_lo, fill_hi)
    con = R._connect()
    ypx = """
      CASE
        WHEN (gross_usdc > 0 AND net_yes_tokens > 0) OR (gross_usdc < 0 AND net_yes_tokens < 0)
          THEN gross_usdc::DOUBLE / net_yes_tokens::DOUBLE
        ELSE 1.0 + gross_usdc::DOUBLE / net_yes_tokens::DOUBLE
      END
    """
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
        FROM fills
        WHERE yes_px > 0 AND yes_px < 1
        """
    )
    cbar = con.execute("SELECT * FROM cbar").fetchdf()
    cpx = con.execute("SELECT cid, block_number, px FROM cpx").fetchdf()
    print(f"  cbars {len(cbar):,}  distinct px rows {len(cpx):,}", flush=True)

    # per (cid, block) high/low/n for labels
    hl = {
        (r.cid, int(r.block_number)): (int(r.n_fills), float(r.high_yes), float(r.low_yes))
        for r in cbar.itertuples(index=False)
    }

    print("  FOK/exit/dump levels …", flush=True)

    def _idx(q):
        d = defaultdict(list)
        for blk, cid, outc, px, tok in q:
            if px and tok and tok > 0:
                d[(cid, outc, int(blk))].append(Level(float(px), float(tok)))
        return d

    asks_i = _idx(
        con.execute(
            f"""
            SELECT block_number, cid,
                   CASE WHEN net_yes_tokens < 0 THEN 'yes' ELSE 'no' END,
                   abs(gross_usdc)::DOUBLE / abs(net_yes_tokens)::DOUBLE,
                   abs(net_yes_tokens)::DOUBLE / 1e6
            FROM fills
            WHERE NOT is_taker AND gross_usdc < 0
              AND block_number BETWEEN {x_lo + 1} AND {x_hi + 1}
            """
        ).fetchall()
    )
    tbuy_i = _idx(
        con.execute(
            f"""
            SELECT block_number, cid,
                   CASE WHEN net_yes_tokens > 0 THEN 'yes' ELSE 'no' END,
                   abs(gross_usdc)::DOUBLE / abs(net_yes_tokens)::DOUBLE,
                   abs(net_yes_tokens)::DOUBLE / 1e6
            FROM fills
            WHERE is_taker AND gross_usdc > 0
              AND block_number BETWEEN {x_lo + 2} AND {x_hi + 60}
            """
        ).fetchall()
    )
    bids_i = _idx(
        con.execute(
            f"""
            SELECT block_number, cid,
                   CASE WHEN net_yes_tokens > 0 THEN 'yes' ELSE 'no' END,
                   abs(gross_usdc)::DOUBLE / abs(net_yes_tokens)::DOUBLE,
                   abs(net_yes_tokens)::DOUBLE / 1e6
            FROM fills
            WHERE NOT is_taker AND gross_usdc > 0
              AND block_number BETWEEN {x_lo + 61} AND {x_hi + 120}
            """
        ).fetchall()
    )

    cbar = cbar.sort_values(["cid", "block_number"])
    grouped = {cid: g.reset_index(drop=True) for cid, g in cbar.groupby("cid", sort=False)}
    pgrouped = {cid: g.reset_index(drop=True) for cid, g in cpx.groupby("cid", sort=False)}

    ev_keys = (
        cbar[(cbar.block_number >= x_lo) & (cbar.block_number <= x_hi)][["block_number", "cid"]]
        .drop_duplicates()
        .sort_values(["block_number", "cid"])
    )
    if args.max_events:
        ev_keys = ev_keys.head(int(args.max_events))
    print(f"  events {len(ev_keys):,}", flush=True)

    events: list[dict] = []
    Xs: list[np.ndarray] = []
    ys: list[int] = []
    t_feat = time.time()
    for i, row in enumerate(ev_keys.itertuples(index=False), 1):
        x, cid = int(row.block_number), row.cid
        g = grouped.get(cid)
        if g is None:
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
        feat = _feat_window(sl, x, last, tick)
        if not FEAT_KEYS:
            FEAT_KEYS.extend(feat.keys())
        # window high/low/n for each horizon
        def _agg(z: int):
            n = hi = 0.0
            lo = 1.0
            anyp = 0
            for b in range(x + 1, x + z + 1):
                hit = hl.get((cid, b))
                if not hit:
                    continue
                nn, h, l = hit
                anyp = 1
                n += nn
                if h > hi:
                    hi = h
                if l < lo:
                    lo = l
            return anyp, hi, lo

        a1, h1, l1 = _agg(1)
        a60, h60, l60 = _agg(60)
        for z, a, h, l in ((1, a1, h1, l1), (60, a60, h60, l60)):
            for k in KS:
                y = _touch(h, l, a, last, tick, k) if a else 0
                Xs.append(_vec(feat, float(k), float(z)))
                ys.append(y)
        events.append(
            {
                "x_block": x,
                "cid": cid,
                "last": last,
                "tick": tick,
                "feat": feat,
                "fee_market": bool(at["fee_usdc"].fillna(0).max() > 0),
            }
        )
        if i % 20000 == 0:
            print(f"  features {i:,}/{len(ev_keys):,}  {time.time()-t_feat:.1f}s", flush=True)

    X = np.vstack(Xs) if Xs else np.zeros((0, 1))
    y = np.array(ys, dtype=int)
    print(f"  events {len(events):,}  rows {len(y):,}  pos {y.mean():.3f}  {time.time()-t0:.1f}s", flush=True)

    xs = sorted({e["x_block"] for e in events})
    span_b = xs[-1] - xs[0] if xs else 0
    emb = min(120, max(0, span_b // 25))
    cut1 = xs[int(len(xs) * 0.55)]
    cut2 = xs[int(len(xs) * 0.78)]
    train_hi = cut1
    val_lo, val_hi = cut1 + emb, cut2
    test_lo = cut2 + emb
    if val_hi <= val_lo:
        val_lo, val_hi = cut1, max(cut1 + 1, cut2)
        test_lo = val_hi + 1
    train_ev = [e for e in events if e["x_block"] <= train_hi]
    val_ev = [e for e in events if val_lo <= e["x_block"] <= val_hi]
    test_ev = [e for e in events if e["x_block"] >= test_lo]
    print(f"  split train {len(train_ev):,} val {len(val_ev):,} test {len(test_ev):,}", flush=True)

    nX = len(y)
    tr_cut = int(nX * 0.60)
    est = HistGradientBoostingClassifier(
        max_depth=5,
        max_iter=80,
        learning_rate=0.08,
        min_samples_leaf=50,
        l2_regularization=0.1,
        random_state=0,
    )
    est.fit(X[:tr_cut], y[:tr_cut])
    try:
        auc = float(roc_auc_score(y[tr_cut:], est.predict_proba(X[tr_cut:])[:, 1]))
    except Exception:
        auc = float("nan")
    print(f"  Stage A AUC {auc:.3f} (pooled k,Z held-out rows)", flush=True)
    # Per-head AUC on held-out rows (order: event × Z × k)
    ho = slice(tr_cut, None)
    yho, pho = y[ho], est.predict_proba(X[ho])[:, 1]
    n_head = len(KS) * len(HORIZONS)
    # rows after tr_cut may not align to event boundary
    start = tr_cut + (n_head - (tr_cut % n_head)) % n_head
    head_auc = {}
    if start < len(y):
        for zi, z in enumerate(HORIZONS):
            for ki, k in enumerate(KS):
                off = zi * len(KS) + ki
                sl = np.arange(start + off, len(y), n_head)
                yy = y[sl]
                pp = est.predict_proba(X[sl])[:, 1]
                if yy.sum() == 0 or yy.sum() == len(yy):
                    head_auc[(k, z)] = float("nan")
                else:
                    try:
                        head_auc[(k, z)] = float(roc_auc_score(yy, pp))
                    except Exception:
                        head_auc[(k, z)] = float("nan")
                print(f"    AUC k={k:+d} Z={z}  pos={yy.mean():.3f}  auc={head_auc[(k, z)]:.3f}", flush=True)

    def attach(evs):
        rows = []
        ptr = []
        for i, ev in enumerate(evs):
            ev["p"] = {}
            for z in HORIZONS:
                for k in KS:
                    rows.append(_vec(ev["feat"], float(k), float(z)))
                    ptr.append((i, k, z))
        if not rows:
            return
        pr = _proba(est, np.vstack(rows))
        for p, (i, k, z) in zip(pr, ptr):
            evs[i]["p"][(k, z)] = float(p)

    attach(events)

    def evl(evs, t0p, t_up, k_min):
        return sequential_eval(
            evs, t0p=t0p, t_up=t_up, k_min=k_min,
            asks_i=asks_i, tbuy_i=tbuy_i, bids_i=bids_i,
        )

    rng = np.random.default_rng(1)
    t0p, t_up, k_min = 0.20, 0.25, 1.0
    log_rows = []
    best = None
    best_val = -1e18
    parent = "init"

    def _log(cid, parent, params, tr, va, te, note):
        rec = {
            "ts": datetime.now(timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ"),
            "id": cid,
            "parent": parent,
            "t0": params[0],
            "t_up": params[1],
            "k_min": params[2],
            "train_per_hour": tr["per_hour"],
            "train_hard_fail": tr["hard_fail"],
            "train_pnl": tr["pnl"],
            "val_per_hour": va["per_hour"],
            "val_hard_fail": va["hard_fail"],
            "val_pnl": va["pnl"],
            "test_per_hour": te["per_hour"],
            "test_hard_fail": te["hard_fail"],
            "test_pnl": te["pnl"],
            "wall_s": round(time.time() - t0, 1),
            "note": note,
        }
        log_rows.append(rec)
        print(
            f"  {cid} val fail={va['hard_fail']} /h={va['per_hour']:.2f} pnl={va['pnl']}  {note}",
            flush=True,
        )
        return rec

    for it in range(args.b_iters + 1):
        if it == 0:
            params = (t0p, t_up, k_min)
        else:
            params = (
                float(np.clip(t0p + rng.normal(0, 0.08), 0.05, 0.9)),
                float(np.clip(t_up + rng.normal(0, 0.08), 0.05, 0.9)),
                float(np.clip(k_min + rng.normal(0, 0.4), 1.0, 4.0)),
            )
        tr = evl(train_ev, *params)
        va = evl(val_ev, *params)
        te = evl(test_ev, *params)
        name = f"b{it}"
        _log(name, parent, params, tr, va, te, "init" if it == 0 else "mutate")
        if not va["hard_fail"] and va["pnl"] is not None and va["pnl"] > best_val:
            best_val = va["pnl"]
            best = (name, params, va, te)
            t0p, t_up, k_min = params
            parent = name
            print(f"  PROMOTE {name} val_pnl={va['pnl']:.2f}", flush=True)

    log_path = LOG_DIR / "train_log.csv"
    with log_path.open("w", newline="") as f:
        w = csv.DictWriter(f, fieldnames=list(log_rows[0].keys()))
        w.writeheader()
        w.writerows(log_rows)

    lines = [
        "# Train snapshot (Stage A tape-touch ±k)",
        "",
        f"Generated {started}. Wall {time.time()-t0:.1f}s. "
        f"Trigger {x_lo:,}–{x_hi:,}.",
        "",
        "Stage A: inferred legal tick, last YES-equivalent, heads `k ∈ [-4,+4]`, "
        f"`Z ∈ {HORIZONS}`. Pooled held-out AUC **{auc:.3f}**.",
        "",
        "Val: **< 1 activation/hour ⇒ hard fail, P&L not considered.**",
        "",
        "| id | t0 | t_up | k_min | val /h | val fail | val P&L | test /h | test fail | test P&L |",
        "| --- | --- | --- | --- | --- | --- | --- | --- | --- | --- |",
    ]
    for r in log_rows:
        vp = "—" if r["val_hard_fail"] else f"{r['val_pnl']:.2f}"
        tp = "—" if r["test_hard_fail"] else f"{r['test_pnl']:.2f}"
        lines.append(
            f"| {r['id']} | {r['t0']:.2f} | {r['t_up']:.2f} | {r['k_min']:.1f} | "
            f"{r['val_per_hour']:.2f} | {r['val_hard_fail']} | {vp} | "
            f"{r['test_per_hour']:.2f} | {r['test_hard_fail']} | {tp} |"
        )
    if best:
        name, params, va, te = best
        tpn = te["pnl"] if not te["hard_fail"] else "HARD FAIL"
        lines += [
            "",
            f"**best_model** `{name}` t0={params[0]:.2f} t_up={params[1]:.2f} "
            f"k_min={params[2]:.1f}  val P&L {va['pnl']:.2f}  test P&L {tpn}.",
        ]
    else:
        lines += ["", "**No promotion.** Every val candidate hard-failed or had non-positive P&L."]
    lines += ["", f"Log: `{log_path}`", ""]
    SNAP.write_text("\n".join(lines))
    print(f"wrote {SNAP}  {time.time()-t0:.1f}s", flush=True)
    con.close()
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
