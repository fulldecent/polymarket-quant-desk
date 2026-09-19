#!/usr/bin/env python3
"""Walk-forward B/C. Concatenated test P&L is the only edge number.

See WALKFORWARD.md. The weekend sliver is not a pass of this protocol.
"""

from __future__ import annotations

import json
import os
import sys
import time
from datetime import datetime, timezone
from itertools import product
from pathlib import Path

import joblib
import numpy as np
from dotenv import load_dotenv
from sklearn.isotonic import IsotonicRegression
from sklearn.metrics import brier_score_loss

_HERE = Path(__file__).resolve().parent
_ROOT = _HERE.parents[1]
sys.path.insert(0, str(_HERE))
sys.path.insert(0, str(_ROOT))
load_dotenv(_ROOT / ".env")

import run as R  # noqa: E402
from heat import HEAT_DAYS, HEAT_KEYS, attach_heat  # noqa: E402
from sim_lib import (  # noqa: E402
    BLOCKS_PER_DAY,
    COOLDOWN_BLOCKS,
    K_MAX,
    LIQ_END,
    LOOKBACK_BLOCKS,
    Level,
)
from stage_b import (  # noqa: E402
    FEAT_KEYS,
    HORIZONS,
    LOG_DIR,
    _hgb,
    pack_hl,
    stride_take,
    weighted_mse,
)
from stage_c import (  # noqa: E402
    naive_eval,
    pick_ticket,
    sequential_eval,
)

B_LOOKBACK = 60 * BLOCKS_PER_DAY
CALIB_TAIL = 14 * BLOCKS_PER_DAY
EMBARGO = 280
C_VAL = 14 * BLOCKS_PER_DAY
C_TEST = 14 * BLOCKS_PER_DAY
SLIDE = 14 * BLOCKS_PER_DAY
MIN_PART_VAL = 100
MIN_OOS_PART = 500
MAX_C_EVENTS = 8_000  # stride C val/test; 20k + 28d fills OOMs
NEIGHBOR_ABS = 5.0
NEIGHBOR_MULT = 3.0
EPS_P = 1e-4
EVENTS_JOB = LOG_DIR / "stage_b_span_events.joblib"
HEAT_JOB = LOG_DIR / "events_with_heat.joblib"
OUT_MD = _HERE / "walkforward-results.md"
OUT_JSON = LOG_DIR / "walkforward.json"
PARTIAL = LOG_DIR / "walkforward_partial.json"

IN_FRACS = (0.0, 0.5, 1.0)
OUT_FRACS = (0.5, 1.0, 1.5)
DOUBLE_KS = (0, 1, 2, 4)


def shortlist():
    cells = []
    for in_frac, out_frac, double_k in product(IN_FRACS, OUT_FRACS, DOUBLE_KS):
        cells.append(
            {
                "in_frac": in_frac,
                "out_frac": out_frac,
                "double_k": double_k,
                "size_mult": 1.1,
                "exit_end": 30,
            }
        )
    return cells


def _iso(raw, y):
    raw = np.asarray(raw, dtype=float)
    y = np.asarray(y, dtype=float)
    if len(y) < 80 or y.min() == y.max():
        return None
    m = np.isfinite(raw) & np.isfinite(y)
    if m.sum() < 80:
        return None
    iso = IsotonicRegression(out_of_bounds="clip", y_min=EPS_P, y_max=1.0 - EPS_P)
    try:
        iso.fit(raw[m], y[m])
    except Exception:
        return None
    return iso


def _apply_iso(iso, p):
    if iso is None:
        return np.asarray(p, dtype=float)
    return iso.transform(np.asarray(p, dtype=float))


def fit_b(train_ev):
    X, y, w, _ = pack_hl(train_ev)
    est = _hgb()
    est.fit(X, y, sample_weight=w)
    return est


def score_b(evs, est):
    from stage_c import attach_stage_b

    attach_stage_b(evs, est, list(FEAT_KEYS))


def neighbors(cell):
    out = []
    for inf in IN_FRACS:
        if inf != cell["in_frac"]:
            out.append({**cell, "in_frac": inf})
    for of in OUT_FRACS:
        if of != cell["out_frac"]:
            out.append({**cell, "out_frac": of})
    for dk in DOUBLE_KS:
        if dk != cell["double_k"]:
            out.append({**cell, "double_k": dk})
    return out


def promote(ranked, naive_pnl):
    """ranked: list of (cell, eval) on val, best-first by pnl."""
    def _sc(ev):
        s = ev.get("score")
        if s is None:
            s = ev.get("pnl")
        return s

    ok = [
        r
        for r in ranked
        if not r[1]["hard_fail"]
        and _sc(r[1]) is not None
        and _sc(r[1]) > 0
        and r[1]["n_part"] >= MIN_PART_VAL
        and r[1]["per_hour"] >= 1.0
    ]
    if naive_pnl is not None:
        ok = [r for r in ok if (r[1].get("pnl") or 0) > naive_pnl]
    for cell, ev in ok:
        floor = -max(NEIGHBOR_ABS, NEIGHBOR_MULT * abs(ev.get("pnl") or 0))
        disaster = False
        by = {(c["in_frac"], c["out_frac"], c["double_k"]): e for c, e in ranked}
        for nb in neighbors(cell):
            key = (nb["in_frac"], nb["out_frac"], nb["double_k"])
            nev = by.get(key)
            if nev is None or nev["pnl"] is None:
                continue
            if nev["pnl"] < floor:
                disaster = True
                break
        if not disaster:
            return cell, ev
    return None, None


def load_cbar(x_lo: int, x_hi: int):
    fills_root = Path(R.FILLS_DIR)
    fill_lo = x_lo - HEAT_DAYS * BLOCKS_PER_DAY - LOOKBACK_BLOCKS
    fill_hi = x_hi
    files = R._files(fills_root, fill_lo, fill_hi)
    if not files:
        return None
    con = R._connect()
    con.execute("SET memory_limit = '6GB'")
    con.execute("SET threads = 2")
    con.execute("SET preserve_insertion_order = false")
    ypx = R._yes_px_sql()
    con.execute(
        f"""
        CREATE TABLE cbar AS
        SELECT block_number, hex(condition_id) AS cid,
               sum(abs(gross_usdc))::DOUBLE / 1e6 AS all_usdc
        FROM read_parquet({R._sql_list(files)})
        WHERE block_number BETWEEN {fill_lo} AND {fill_hi}
          AND net_yes_tokens <> 0 AND gross_usdc <> 0
        GROUP BY 1, 2
        """
    )
    df = con.execute("SELECT * FROM cbar ORDER BY block_number").fetchdf()
    con.close()
    return df


def _do_sim(e, ticket, asks_i, tbuy_i, bids_i):
    x, cid = e["x_block"], e["cid"]
    side = ticket["side"]
    x_end = int(ticket.get("exit_end", 60))
    entry = asks_i.get((cid, side, x + 1), [])
    exb = [
        (b, tbuy_i[(cid, side, b)])
        for b in range(x + 2, x + x_end + 1)
        if (cid, side, b) in tbuy_i
    ]
    liqb = [
        (b, bids_i[(cid, side, b)])
        for b in range(x + x_end + 1, x + x_end + 61)
        if (cid, side, b) in bids_i
    ]
    from sim_lib import simulate_roundtrip

    return simulate_roundtrip(
        n=ticket["n"],
        p_entry=ticket["p_in"],
        p_exit=ticket["p_out"],
        entry_asks=entry,
        exit_by_block=exb,
        liq_by_block=liqb,
        x_block=x,
        fee_market=bool(e.get("fee_market", 0)),
        exit_end=x_end,
        liq_start=x_end + 1,
        liq_end=x_end + 60,
    )


def precompute_fires(evs, cells, asks_i, tbuy_i, bids_i):
    """One sim per unique ticket; cooldown passes are cheap."""
    sim_cache = {}
    fires = [[] for _ in cells]
    n_sim = 0
    for i, e in enumerate(evs):
        for ci, p in enumerate(cells):
            ticket = pick_ticket(e, **p)
            if ticket is None:
                continue
            skey = (
                e["x_block"],
                e["cid"],
                ticket["side"],
                round(float(ticket["p_in"]), 8),
                round(float(ticket["p_out"]), 8),
                round(float(ticket["n"]), 6),
                int(ticket.get("exit_end", 30)),
                int(bool(e.get("fee_market"))),
            )
            if skey not in sim_cache:
                sim_cache[skey] = _do_sim(e, ticket, asks_i, tbuy_i, bids_i)
                n_sim += 1
            fires[ci].append(
                (
                    e["cid"],
                    e["x_block"],
                    sim_cache[skey],
                    int(ticket.get("should_bet_double", 0)),
                )
            )
    return fires, n_sim


def seq_from_fires(fires_one, lo, hi):
    from stage_c import _hours

    cool: dict[str, int] = {}
    n_part = pnl = n_ok = 0
    qe = ql = qz = buy = score = 0.0
    for row in fires_one:
        cid, x, sim = row[0], row[1], row[2]
        dbl = int(row[3]) if len(row) > 3 else 0
        if x < cool.get(cid, -1):
            continue
        n_part += 1
        cool[cid] = x + COOLDOWN_BLOCKS
        raw = float(sim["pnl"])
        pnl += raw
        score += raw * (2.0 if dbl else 1.0)
        n_ok += int(sim["entry_ok"])
        if sim.get("entry_ok"):
            buy += float(sim.get("usdc_in", 0.0))
        qe += sim["q_exit"]
        ql += sim["q_liq"]
        qz += sim["q_zero"]
    hours = _hours(lo, hi)
    rate = n_part / hours
    qt = qe + ql + qz
    hard = rate < 1.0
    days = hours / 24.0
    return {
        "n_part": n_part,
        "per_hour": rate,
        "hard_fail": hard,
        "pnl": None if hard else pnl,
        "score": None if hard else score,
        "hit": (n_ok / n_part) if n_part else 0.0,
        "q_zero": (qz / qt) if qt else 0.0,
        "q_liq": (ql / qt) if qt else 0.0,
        "hours": hours,
        "days": days,
        "n_fill": n_ok,
        "buy_usdc": buy,
    }


def load_sim(x_lo: int, x_hi: int, cids: set[str] | None = None):
    fills_root = Path(R.FILLS_DIR)
    files = R._files(fills_root, x_lo, x_hi + LIQ_END)
    if not files:
        return {}, {}, {}
    con = R._connect()
    con.execute("SET memory_limit = '6GB'")
    con.execute("SET threads = 2")
    ypx = R._yes_px_sql()
    cid_sql = ""
    if cids:
        # keep the join small — only names C might actually trade
        sample = list(cids)
        if len(sample) > 4000:
            sample = sample[:4000]
        lst = ",".join("'" + c.replace("'", "") + "'" for c in sample)
        cid_sql = f"AND hex(condition_id) IN ({lst})"
    con.execute(
        f"""
        CREATE TABLE fills AS
        SELECT block_number, hex(condition_id) AS cid,
               is_taker, net_yes_tokens, gross_usdc, {ypx} AS yes_px
        FROM read_parquet({R._sql_list(files)})
        WHERE block_number BETWEEN {x_lo} AND {x_hi + LIQ_END}
          AND net_yes_tokens <> 0 AND gross_usdc <> 0
          {cid_sql}
        """
    )

    def idx(sql):
        d = {}
        for blk, cid, outc, px, tok in con.execute(sql).fetchall():
            if px and tok and tok > 0:
                d.setdefault((cid, outc, int(blk)), []).append(Level(float(px), float(tok)))
        return d

    asks = idx(
        f"""
        SELECT block_number, cid,
               CASE WHEN net_yes_tokens < 0 THEN 'yes' ELSE 'no' END,
               abs(gross_usdc)::DOUBLE / abs(net_yes_tokens)::DOUBLE,
               abs(net_yes_tokens)::DOUBLE / 1e6
        FROM fills WHERE NOT is_taker AND gross_usdc < 0
          AND block_number BETWEEN {x_lo + 1} AND {x_hi + 1}
        """
    )
    tbuy = idx(
        f"""
        SELECT block_number, cid,
               CASE WHEN net_yes_tokens > 0 THEN 'yes' ELSE 'no' END,
               abs(gross_usdc)::DOUBLE / abs(net_yes_tokens)::DOUBLE,
               abs(net_yes_tokens)::DOUBLE / 1e6
        FROM fills WHERE is_taker AND gross_usdc > 0
          AND block_number BETWEEN {x_lo + 2} AND {x_hi + 60}
        """
    )
    bids = idx(
        f"""
        SELECT block_number, cid,
               CASE WHEN net_yes_tokens > 0 THEN 'yes' ELSE 'no' END,
               abs(gross_usdc)::DOUBLE / abs(net_yes_tokens)::DOUBLE,
               abs(net_yes_tokens)::DOUBLE / 1e6
        FROM fills WHERE NOT is_taker AND gross_usdc > 0
          AND block_number BETWEEN {x_lo + 31} AND {x_hi + 120}
        """
    )
    con.close()
    return asks, tbuy, bids


def attach_heat_events(events):
    if not events:
        return
    xs = sorted(e["x_block"] for e in events)
    batch = 4 * BLOCKS_PER_DAY
    i = 0
    n = len(xs)
    print("  attach heat …", flush=True)
    t0 = time.time()
    by_x = {}
    for e in events:
        by_x.setdefault(e["x_block"], []).append(e)
    ux = sorted(by_x)
    start = ux[0]
    while start <= ux[-1]:
        end = start + batch - 1
        chunk = [e for x, es in by_x.items() if start <= x <= end for e in es]
        if chunk:
            cbar = load_cbar(start, end)
            if cbar is not None and not cbar.empty:
                attach_heat(chunk, cbar)
        start = end + 1
        i += 1
        if i % 4 == 0:
            print(f"    heat batch through {end:,}  {time.time()-t0:.1f}s", flush=True)


def brier_slice(evs, key="p_act_1", ykey="y_act_1"):
    if not evs or "b_h1_hi" not in evs[0]:
        return float("nan")
    y, p, w = [], [], []
    for e in evs:
        y.extend(
            [
                float(e.get("y_h1_hi", 0.0)),
                float(e.get("y_h1_lo", 0.0)),
                float(e.get("y_h130_hi", 0.0)),
                float(e.get("y_h130_lo", 0.0)),
            ]
        )
        p.extend(
            [
                float(e.get("b_h1_hi", 0.0)),
                float(e.get("b_h1_lo", 0.0)),
                float(e.get("b_h130_hi", 0.0)),
                float(e.get("b_h130_lo", 0.0)),
            ]
        )
        w.extend([2.0, 2.0, 1.0, 1.0])
    return weighted_mse(y, p, w)


def main() -> int:
    t0 = time.time()
    started = datetime.now(timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ")
    if not EVENTS_JOB.exists():
        R._fail(f"missing {EVENTS_JOB}; run stage_b_span.py first")
    blob = joblib.load(EVENTS_JOB)
    events = blob["events"]
    events.sort(key=lambda e: e["x_block"])
    print(f"walk-forward  n={len(events):,}  {started}", flush=True)
    if HEAT_JOB.exists() and HEAT_JOB.stat().st_mtime >= EVENTS_JOB.stat().st_mtime:
        print(f"  load heat cache {HEAT_JOB}", flush=True)
        events = joblib.load(HEAT_JOB)
        events.sort(key=lambda e: e["x_block"])
    else:
        for e in events:
            feat = e.get("feat") or {}
            e["feat"] = feat
            for k in HEAT_KEYS:
                feat.setdefault(k, 0.0)
            for k in FEAT_KEYS:
                feat.setdefault(k, 0.0)
            e.setdefault("fee_market", bool(feat.get("fee_bit", 0)))
            e.setdefault("imbalance_15", float(feat.get("imbalance_15", 0.0)))
            e.setdefault("top_share", 0.0)
            e.setdefault("n_acct", 0.0)
            e.setdefault("hhi", 1.0)
        attach_heat_events(events)
        joblib.dump(events, HEAT_JOB)
        print(f"  wrote heat cache {HEAT_JOB}", flush=True)
    x0, x1 = events[0]["x_block"], events[-1]["x_block"]
    cells = shortlist()
    t = x0 + B_LOOKBACK
    folds = []
    concat = []
    fold_i = 0
    if PARTIAL.exists():
        prev = json.loads(PARTIAL.read_text())
        folds = prev.get("folds") or []
        concat = prev.get("concat") or []
        fold_i = prev.get("fold_i") or len(concat)
        t = int(prev.get("next_t") or t)
        print(f"  resume after fold {fold_i}  next t={t:,}", flush=True)
    while t + EMBARGO + C_VAL + EMBARGO + C_TEST <= x1:
        fold_i += 1
        b_lo, b_hi = t - B_LOOKBACK, t - EMBARGO
        v_lo, v_hi = t + EMBARGO, t + EMBARGO + C_VAL
        s_lo, s_hi = v_hi + EMBARGO, v_hi + EMBARGO + C_TEST
        train = [e for e in events if b_lo <= e["x_block"] < b_hi]
        val_e = [e for e in events if v_lo <= e["x_block"] < v_hi]
        test_e = [e for e in events if s_lo <= e["x_block"] < s_hi]
        print(
            f"fold {fold_i}  B {b_lo:,}–{b_hi:,} n={len(train):,}  "
            f"val n={len(val_e):,}  test n={len(test_e):,}",
            flush=True,
        )
        if len(val_e) > MAX_C_EVENTS:
            val_e = [val_e[i] for i in stride_take(len(val_e), MAX_C_EVENTS)]
        if len(test_e) > MAX_C_EVENTS:
            test_e = [test_e[i] for i in stride_take(len(test_e), MAX_C_EVENTS)]
        if len(train) < 2000 or len(val_e) < 200 or len(test_e) < 200:
            print("    skip thin fold", flush=True)
            t += SLIDE
            PARTIAL.write_text(
                json.dumps({"fold_i": fold_i, "next_t": t, "concat": concat, "folds": folds}, default=str)
            )
            continue
        est = fit_b(train)
        score_b(val_e, est)
        score_b(test_e, est)
        print("    load sim fills …", flush=True)
        cids = {e["cid"] for e in val_e} | {e["cid"] for e in test_e}
        asks, tbuy, bids = load_sim(v_lo, s_hi, cids)
        print(f"    sim indexes asks={len(asks):,} tbuy={len(tbuy):,} bids={len(bids):,}", flush=True)
        print("    precompute val fires …", flush=True)
        val_fires, n_sim_v = precompute_fires(val_e, cells, asks, tbuy, bids)
        print(f"    val unique sims {n_sim_v:,}", flush=True)
        ranked = []
        for ci, p in enumerate(cells):
            ev = seq_from_fires(val_fires[ci], v_lo, v_hi - 1)
            ranked.append((p, ev))
        ranked.sort(
            key=lambda r: (
                r[1].get("score") is not None,
                r[1].get("score") if r[1].get("score") is not None else -1e99,
            ),
            reverse=True,
        )
        print("    naive val/test …", flush=True)
        naive_v = naive_eval(val_e, lo=v_lo, hi=v_hi - 1, asks_i=asks, tbuy_i=tbuy, bids_i=bids)
        naive_t = naive_eval(test_e, lo=s_lo, hi=s_hi - 1, asks_i=asks, tbuy_i=tbuy, bids_i=bids)
        print(
            f"    naive val n={naive_v['n_part']:,} test n={naive_t['n_part']:,}",
            flush=True,
        )
        nv = 0.0 if naive_v["hard_fail"] or naive_v["pnl"] is None else float(naive_v["pnl"])
        cell, val_ev = promote(ranked, nv)
        if cell is None:
            test_ev = {
                "n_part": 0,
                "per_hour": 0.0,
                "hard_fail": True,
                "pnl": 0.0,
                "hit": 0.0,
                "q_zero": 0.0,
                "q_liq": 0.0,
                "skipped": True,
            }
            print("    val skip (promotion failed)", flush=True)
        else:
            test_ev = sequential_eval(
                test_e, params=cell, lo=s_lo, hi=s_hi - 1, asks_i=asks, tbuy_i=tbuy, bids_i=bids
            )
            test_ev["skipped"] = False
            print(
                f"    ticket in={cell['in_frac']} out={cell['out_frac']} "
                f"dbl_k={cell['double_k']} val ${val_ev['pnl']:.2f} "
                f"score ${val_ev.get('score') or 0:.2f}  test "
                + ("hard-fail" if test_ev["hard_fail"] else f"${test_ev['pnl']:.2f}"),
                flush=True,
            )
        test_pnl = 0.0 if test_ev["hard_fail"] or test_ev["pnl"] is None else float(test_ev["pnl"])
        naive_tp = 0.0 if naive_t["hard_fail"] or naive_t["pnl"] is None else float(naive_t["pnl"])
        test_days = float(test_ev.get("days") or ((s_hi - s_lo) / BLOCKS_PER_DAY))
        concat.append(
            {
                "fold": fold_i,
                "test_lo": s_lo,
                "test_hi": s_hi,
                "test_pnl": test_pnl,
                "naive_pnl": naive_tp,
                "n_part": 0 if test_ev["hard_fail"] else test_ev["n_part"],
                "n_fill": 0 if test_ev["hard_fail"] else int(test_ev.get("n_fill", 0)),
                "buy_usdc": 0.0 if test_ev["hard_fail"] else float(test_ev.get("buy_usdc", 0.0)),
                "days": test_days,
                "naive_fill": 0 if naive_t["hard_fail"] else int(naive_t.get("n_fill", 0)),
                "naive_buy": 0.0 if naive_t["hard_fail"] else float(naive_t.get("buy_usdc", 0.0)),
                "ticket": None
                if cell is None
                else {k: cell[k] for k in ("in_frac", "out_frac", "double_k")},
                "brier_act1": brier_slice(test_e),
            }
        )
        folds.append(
            {
                "fold": fold_i,
                "b_lo": b_lo,
                "val_lo": v_lo,
                "test_lo": s_lo,
                "n_train": len(train),
                "n_val": len(val_e),
                "n_test": len(test_e),
                "ticket": None if cell is None else cell,
                "val": val_ev,
                "test": test_ev,
                "naive_val": naive_v,
                "naive_test": naive_t,
                "brier_test_act1": concat[-1]["brier_act1"],
            }
        )
        t += SLIDE
        PARTIAL.write_text(
            json.dumps({"fold_i": fold_i, "next_t": t, "concat": concat, "folds": folds}, default=str)
        )

    oos_n = sum(c["n_part"] for c in concat)
    oos_fill = sum(c.get("n_fill", 0) for c in concat)
    oos_pnl = sum(c["test_pnl"] for c in concat)
    oos_buy = sum(c.get("buy_usdc", 0.0) for c in concat)
    oos_naive = sum(c["naive_pnl"] for c in concat)
    oos_naive_fill = sum(c.get("naive_fill", 0) for c in concat)
    oos_naive_buy = sum(c.get("naive_buy", 0.0) for c in concat)
    oos_days = sum(c.get("days", 14.0) for c in concat) or 1.0
    n_test_slices = len(concat)
    # Scale: last 30 protocol days of tape. Edge: all OOS $/fill.
    recent_lo = x1 - 30 * BLOCKS_PER_DAY
    recent = [c for c in concat if c.get("test_hi", 0) >= recent_lo] or concat[-2:] or concat
    recent_days = sum(c.get("days", 14.0) for c in recent) or 1.0
    scale_tr = sum(c.get("n_fill", 0) for c in recent) / recent_days
    scale_naive_tr = sum(c.get("naive_fill", 0) for c in recent) / recent_days
    pnl_pf = oos_pnl / oos_fill if oos_fill else 0.0
    buy_pf = oos_buy / oos_fill if oos_fill else 0.0
    npnl_pf = oos_naive / oos_naive_fill if oos_naive_fill else 0.0
    nbuy_pf = oos_naive_buy / oos_naive_fill if oos_naive_fill else 0.0
    d_tr, d_buy, d_pnl = scale_tr, scale_tr * buy_pf, scale_tr * pnl_pf
    nd_tr, nd_buy, nd_pnl = scale_naive_tr, scale_naive_tr * nbuy_pf, scale_naive_tr * npnl_pf
    prints = (
        n_test_slices >= 3
        and oos_fill >= MIN_OOS_PART
        and oos_pnl > 0
        and oos_pnl > oos_naive
    )
    def _day_line(tr, buy, pnl):
        return f"{tr:.1f} trades, ${buy:.2f} total buys, {pnl:+.2f} PnL"

    lines = [
        "# Walk-forward results",
        "",
        f"Generated {started}. Wall {time.time()-t0:.1f}s. Protocol [`WALKFORWARD.md`](WALKFORWARD.md).",
        "Weekend sliver is **not** a pass of this protocol.",
        "",
        f"**Expected one-day next ~30d** (recent-scale fills × OOS $/fill, "
        f"{recent_days:.1f} recent test days): {_day_line(d_tr, d_buy, d_pnl)}",
        "",
        f"Dummy one-day: {_day_line(nd_tr, nd_buy, nd_pnl)}",
        "",
        f"Folds with a test slice: **{n_test_slices}**. Concatenated test P&L "
        f"**${oos_pnl:.2f}** vs dummy **${oos_naive:.2f}**. OOS fills {oos_fill:,} "
        f"(floor {MIN_OOS_PART}).",
        "",
        f"**Prints money under this protocol: {'yes' if prints else 'no'}.**",
        "",
        "| fold | test P&L | dummy | fills | buys $ | ticket | Brier H1 |",
        "| --- | --- | --- | --- | --- | --- | --- |",
    ]
    for c in concat:
        tk = c["ticket"]
        tks = "skip" if not tk else f"in={tk['in_frac']} out={tk['out_frac']} dbl={tk['double_k']}"
        lines.append(
            f"| {c['fold']} | {c['test_pnl']:.2f} | {c['naive_pnl']:.2f} | "
            f"{c.get('n_fill', 0):,} | {c.get('buy_usdc', 0):.2f} | {tks} | {c['brier_act1']:.4f} |"
        )
    lines += [
        "",
        "Go-live checklist (pre-registered): 3+ slices, ≥500 OOS fills, concat P&L>0 "
        "and beats dummy, neighbors, latest-slice Brier, dump not most of P&L, "
        "shadow week. This run is research OOS, not live.",
        "",
    ]
    OUT_MD.write_text("\n".join(lines))
    OUT_JSON.write_text(json.dumps({"folds": folds, "concat": concat, "prints": prints}, default=str, indent=2))
    print(f"wrote {OUT_MD}  concat ${oos_pnl:.2f} dummy ${oos_naive:.2f}  prints={prints}", flush=True)
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
