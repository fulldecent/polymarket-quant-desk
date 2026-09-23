#!/usr/bin/env python3
"""Stage B cross-era generalization.

Families stay frozen (selected 67 cols). Re-fit the two HGBs in each
required era (election, Super Bowl, FIFA, US military before/after,
quiet controls, freeze weekend) and transfer. Train never sees the
held-out era.

Writes stage-b-eras.md and scratch/directional-v1/stage_b_eras.json
"""

from __future__ import annotations

import argparse
import json
import os
import sys
import time
from datetime import datetime, timezone
from pathlib import Path

import joblib
import numpy as np
import pandas as pd
from dotenv import load_dotenv

_HERE = Path(__file__).resolve().parent
_ROOT = _HERE.parents[1]
sys.path.insert(0, str(_HERE))
sys.path.insert(0, str(_ROOT))
load_dotenv(_ROOT / ".env")

import run as R  # noqa: E402
from eras import ERAS  # noqa: E402
from sim_lib import (  # noqa: E402
    LIQ_END,
    LOOKBACK_BLOCKS,
    infer_tick,
    prefilter_alive,
)
from stage_b import (  # noqa: E402
    FAMILIES,
    FEAT_KEYS,
    LOG_DIR,
    _auc,
    _hgb,
    feat_window,
    pack_act,
    pack_bar,
    primary_auc,
    split_act,
    split_bar,
    stride_take,
)

FROZEN_FAMS = ["level", "persist", "depth", "counts", "range", "flow", "adverse"]
FROZEN_KEYS = [c for fam in FROZEN_FAMS for c in FAMILIES[fam]]
BLOCKS_PATH = _HERE / "era-blocks.json"
SNAP = _HERE / "stage-b-eras.md"
CACHE = LOG_DIR / "era_events"
MIN_EVENTS = 1_500  # below this, 3-of-8 tape is too thin for a two-HGB fit


def _f3(x: float) -> str:
    return "—" if x != x else f"{x:.3f}"


MAX_SPAN = 120_000  # ~2 calendar days at 1.5 s/block; dense 2026 tape OOMs larger chunks


def collect_events(x_lo: int, x_hi: int, max_events: int) -> tuple[list[dict], dict]:
    span = x_hi - x_lo + 1
    if span <= MAX_SPAN:
        return _collect_one(x_lo, x_hi, max_events)
    n = min(6, int(np.ceil(span / MAX_SPAN)))
    width = min(MAX_SPAN, span)
    starts = np.linspace(x_lo, x_hi - width + 1, n).astype(int)
    per = max(1, max_events // n) if max_events else 0
    all_evs: list[dict] = []
    stats = {"n_cand": 0, "n_gate": 0, "n_kept": 0, "chunks": n}
    for s in starts:
        evs, st = _collect_one(int(s), int(s) + width - 1, per)
        all_evs.extend(evs)
        stats["n_cand"] += st["n_cand"]
        stats["n_gate"] += st["n_gate"]
    if max_events > 0 and len(all_evs) > max_events:
        all_evs = [all_evs[i] for i in stride_take(len(all_evs), max_events)]
    stats["n_kept"] = len(all_evs)
    return all_evs, stats


def _collect_one(x_lo: int, x_hi: int, max_events: int) -> tuple[list[dict], dict]:
    fills_root = Path(R.FILLS_DIR)
    fill_lo = x_lo - LOOKBACK_BLOCKS
    fill_hi = x_hi + 60
    frontier = R._frontier(fills_root)
    x_hi = min(x_hi, frontier - LIQ_END)
    if x_hi < x_lo:
        return [], {"n_cand": 0, "n_gate": 0, "n_kept": 0}
    ypx = R._yes_px_sql()
    con = R._connect()
    con.execute("SET memory_limit = '6GB'")
    con.execute("SET threads = 2")
    con.execute("SET preserve_insertion_order = false")
    fill_files = R._files(fills_root, fill_lo, fill_hi)
    if not fill_files:
        con.close()
        return [], {"n_cand": 0, "n_gate": 0, "n_kept": 0}
    con.execute(
        f"""
        CREATE TABLE fills AS
        SELECT block_number, logical_fill_index, transaction_index,
               hex(condition_id) AS cid,
               hex(account) AS account,
               is_taker, net_yes_tokens, gross_usdc, fee_usdc, market_id,
               {ypx} AS yes_px
        FROM read_parquet({R._sql_list(fill_files)})
        WHERE block_number BETWEEN {fill_lo} AND {fill_hi}
          AND net_yes_tokens <> 0 AND gross_usdc <> 0
        """
    )
    con.execute(
        """
        CREATE TABLE match_taker AS
        SELECT cid, block_number, transaction_index,
               max(yes_px) FILTER (WHERE is_taker) AS tpx
        FROM fills
        GROUP BY 1, 2, 3
        HAVING max(yes_px) FILTER (WHERE is_taker) IS NOT NULL
        """
    )
    con.execute(
        """
        CREATE TABLE mt AS
        SELECT m.cid, m.block_number,
               median(abs(t.tpx - m.yes_px)) AS mt_abs_med
        FROM fills m
        JOIN match_taker t
          ON m.cid = t.cid AND m.block_number = t.block_number
         AND m.transaction_index = t.transaction_index
        WHERE NOT m.is_taker
        GROUP BY 1, 2
        """
    )
    con.execute(
        """
        CREATE TABLE cbar AS
        SELECT
          f.block_number, f.cid,
          arg_max(f.yes_px, f.logical_fill_index) AS close_yes,
          max(f.yes_px) AS high_yes,
          min(f.yes_px) AS low_yes,
          count(*) AS n_fills,
          count(DISTINCT f.account) AS n_accounts,
          count(DISTINCT f.account) FILTER (WHERE NOT f.is_taker) AS n_makers,
          sum(f.gross_usdc) FILTER (
            WHERE f.is_taker AND f.gross_usdc > 0 AND f.net_yes_tokens > 0
          )::DOUBLE / 1e6 AS up_usdc,
          sum(f.gross_usdc) FILTER (
            WHERE f.is_taker AND f.gross_usdc > 0 AND f.net_yes_tokens < 0
          )::DOUBLE / 1e6 AS dn_usdc,
          sum(-f.gross_usdc) FILTER (
            WHERE NOT f.is_taker AND f.gross_usdc < 0
          )::DOUBLE / 1e6 AS ask_usdc,
          sum(f.gross_usdc) FILTER (
            WHERE NOT f.is_taker AND f.gross_usdc > 0
          )::DOUBLE / 1e6 AS bid_usdc,
          sum(abs(f.gross_usdc)) FILTER (WHERE f.is_taker)::DOUBLE / 1e6 AS taker_usdc,
          sum(abs(f.gross_usdc))::DOUBLE / 1e6 AS all_usdc,
          max(f.gross_usdc) FILTER (
            WHERE f.is_taker AND f.gross_usdc > 0
          )::DOUBLE / 1e6 AS max_taker_buy_usdc,
          sum(abs(f.net_yes_tokens)) FILTER (
            WHERE NOT f.is_taker AND f.gross_usdc < 0
          )::DOUBLE / 1e6 AS ask_shares,
          (sum(abs(f.net_yes_tokens)) FILTER (
             WHERE NOT f.is_taker AND f.gross_usdc < 0
           )::DOUBLE / 1e6 >= 5
           AND sum(-f.gross_usdc) FILTER (
             WHERE NOT f.is_taker AND f.gross_usdc < 0
           )::DOUBLE / 1e6 >= 1.20
          ) AS ask_floor,
          sum(abs(f.gross_usdc))::DOUBLE
            / nullif(sum(abs(f.net_yes_tokens)), 0)::DOUBLE AS vwap_yes,
          max(f.fee_usdc) AS fee_usdc,
          max(CASE WHEN f.market_id IS NOT NULL THEN 1 ELSE 0 END) AS neg_risk,
          any_value(mt.mt_abs_med) AS mt_abs_med
        FROM fills f
        LEFT JOIN mt ON f.cid = mt.cid AND f.block_number = mt.block_number
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
    con.close()
    if cbar.empty:
        return [], {"n_cand": 0, "n_gate": 0, "n_kept": 0}

    cbar = cbar.sort_values(["cid", "block_number"])
    grouped = {cid: g.reset_index(drop=True) for cid, g in cbar.groupby("cid", sort=False)}
    pgrouped = {cid: g.reset_index(drop=True) for cid, g in cpx.groupby("cid", sort=False)}
    fill_blocks = {cid: set(g.block_number.astype(int)) for cid, g in grouped.items()}
    g_blocks = {cid: g.block_number.to_numpy(dtype=np.int64) for cid, g in grouped.items()}
    p_blocks = {cid: g.block_number.to_numpy(dtype=np.int64) for cid, g in pgrouped.items()}
    ev_keys = (
        cbar[(cbar.block_number >= x_lo) & (cbar.block_number <= x_hi)][["block_number", "cid"]]
        .drop_duplicates()
        .sort_values(["block_number", "cid"])
    )
    alive: list[tuple[int, str]] = []
    n_skip = 0
    for row in ev_keys.itertuples(index=False):
        x, cid = int(row.block_number), row.cid
        fb = fill_blocks.get(cid)
        if fb is None or not prefilter_alive(fb, x):
            n_skip += 1
            continue
        alive.append((x, cid))
    if max_events > 0 and len(alive) > max_events:
        alive = [alive[i] for i in stride_take(len(alive), max_events)]

    def window_hl(cid: str, lo: int, hi: int) -> tuple[int, float, float]:
        bn = g_blocks.get(cid)
        if bn is None:
            return 0, 0.0, 1.0
        i0 = int(np.searchsorted(bn, lo, side="left"))
        i1 = int(np.searchsorted(bn, hi, side="right"))
        if i1 <= i0:
            return 0, 0.0, 1.0
        w = grouped[cid].iloc[i0:i1]
        return 1, float(w["high_yes"].max()), float(w["low_yes"].min())

    events = []
    for x, cid in alive:
        g = grouped.get(cid)
        if g is None:
            continue
        bn = g_blocks[cid]
        lo = int(np.searchsorted(bn, x - (LOOKBACK_BLOCKS - 1), side="left"))
        hi = int(np.searchsorted(bn, x, side="right"))
        sl = g.iloc[lo:hi]
        at = sl[sl.block_number.to_numpy() == x] if len(sl) else sl
        if at.empty or pd.isna(at["close_yes"].iloc[-1]):
            continue
        last = float(at["close_yes"].iloc[-1])
        if not (0 < last < 1):
            continue
        pg = pgrouped.get(cid)
        if pg is None:
            tick = 0.01
        else:
            pbn = p_blocks[cid]
            plo = int(np.searchsorted(pbn, x - (LOOKBACK_BLOCKS - 1), side="left"))
            phi = int(np.searchsorted(pbn, x, side="right"))
            tick = infer_tick(pg.iloc[plo:phi]["px"].tolist())
        feat = feat_window(sl, x, last, tick)
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
    stats = {"n_cand": int(len(ev_keys)), "n_gate": n_skip, "n_kept": len(events)}
    return events, stats


def time_split(evs: list[dict], frac: float = 0.7):
    if len(evs) < 20:
        return evs, []
    xs = sorted(e["x_block"] for e in evs)
    cut = xs[int(len(xs) * frac)]
    emb = 120
    tr = [e for e in evs if e["x_block"] <= cut]
    va = [e for e in evs if e["x_block"] >= cut + emb]
    return tr, va


def fit_pair(train_evs: list[dict]):
    Xa, ya, _ = pack_act(train_evs, FROZEN_KEYS)
    Xb, yb, _ = pack_bar(train_evs, FROZEN_KEYS)
    if len(ya) < 50 or ya.min() == ya.max():
        return None, None
    if len(yb) < 50 or yb.min() == yb.max():
        return None, None
    act = _hgb()
    bar = _hgb()
    try:
        act.fit(Xa, ya)
        bar.fit(Xb, yb)
    except Exception:
        return None, None
    return act, bar


def score_pair(act, bar, evs: list[dict]) -> dict:
    if act is None or bar is None or len(evs) < 50:
        return {"n": len(evs), "act_h1": float("nan"), "primary": float("nan")}
    Xa, ya, ma = pack_act(evs, FROZEN_KEYS)
    Xb, yb, mb = pack_bar(evs, FROZEN_KEYS)
    pa = act.predict_proba(Xa)[:, 1]
    pb = bar.predict_proba(Xb)[:, 1]
    a = split_act(ya, pa, ma)
    _, hard = split_bar(yb, pb, mb)
    return {
        "n": len(evs),
        "act_h1": _auc(*a[1]),
        "act_h1_pos": float(np.mean(a[1][0])) if len(a[1][0]) else float("nan"),
        "primary": primary_auc(hard),
        "hard_p1_n": int(len(hard[(1, 60)][0])),
        "hard_p1_pos": float(np.mean(hard[(1, 60)][0])) if len(hard[(1, 60)][0]) else float("nan"),
    }


def subsample(evs: list[dict], k: int) -> list[dict]:
    if k <= 0 or len(evs) <= k:
        return evs
    idx = stride_take(len(evs), k)
    return [evs[i] for i in idx]


def main() -> int:
    ap = argparse.ArgumentParser()
    ap.add_argument("--max-events", type=int, default=40_000)
    ap.add_argument("--loo-each", type=int, default=8_000)
    ap.add_argument("--only", type=str, default="")
    args = ap.parse_args()

    if not BLOCKS_PATH.exists():
        R._fail(f"missing {BLOCKS_PATH}; run resolve_era_blocks.py first")
    eras = json.loads(BLOCKS_PATH.read_text())
    if args.only:
        want = {x.strip() for x in args.only.split(",") if x.strip()}
        eras = [e for e in eras if e["id"] in want]
    LOG_DIR.mkdir(parents=True, exist_ok=True)
    CACHE.mkdir(parents=True, exist_ok=True)
    t0 = time.time()
    started = datetime.now(timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ")
    print(
        f"stage B cross-era  n_eras {len(eras)}  frozen {len(FROZEN_KEYS)} cols  "
        f"gate 3-of-8",
        flush=True,
    )

    bundle: dict[str, list[dict]] = {}
    stats: dict[str, dict] = {}
    for era in eras:
        eid = era["id"]
        cache_p = CACHE / f"{eid}.joblib"
        x_lo, x_hi = int(era["start_block"]), int(era["end_block"])
        print(f"  collect {eid}  {era['start']}–{era['end']}  {x_lo:,}–{x_hi:,}", flush=True)
        if cache_p.exists():
            evs = joblib.load(cache_p)
            st = {"n_cand": -1, "n_gate": -1, "n_kept": len(evs), "cached": True}
        else:
            try:
                evs, st = collect_events(x_lo, x_hi, args.max_events)
            except Exception as exc:
                print(f"    FAILED collect {eid}: {exc}", flush=True)
                evs, st = [], {"n_cand": 0, "n_gate": 0, "n_kept": 0, "error": str(exc)}
            st["cached"] = False
            joblib.dump(evs, cache_p)
        bundle[eid] = evs
        stats[eid] = st
        print(f"    kept {len(evs):,}  {time.time()-t0:.1f}s", flush=True)

    usable = [e for e in eras if len(bundle[e["id"]]) >= MIN_EVENTS]
    skipped = [e["id"] for e in eras if len(bundle[e["id"]]) < MIN_EVENTS]
    print(f"  usable {len(usable)}  skipped {skipped}", flush=True)

    in_era = {}
    models = {}
    for era in usable:
        eid = era["id"]
        tr, va = time_split(bundle[eid], 0.7)
        print(f"  fit {eid}  train {len(tr):,} val {len(va):,}", flush=True)
        act, bar = fit_pair(tr if len(tr) >= MIN_EVENTS else bundle[eid])
        models[eid] = (act, bar)
        in_era[eid] = score_pair(act, bar, va if len(va) >= 80 else bundle[eid])
        print(
            f"    in-era actH1 {_f3(in_era[eid]['act_h1'])}  "
            f"primary {_f3(in_era[eid]['primary'])}",
            flush=True,
        )

    # Full-era models for transfer (fit on all of the era).
    full_models = {}
    for era in usable:
        eid = era["id"]
        full_models[eid] = fit_pair(bundle[eid])

    ids = [e["id"] for e in usable]
    cross = {a: {} for a in ids}
    for a in ids:
        act, bar = full_models[a]
        for b in ids:
            if a == b:
                cross[a][b] = in_era[a]
                continue
            cross[a][b] = score_pair(act, bar, bundle[b])
        print(f"  scored transfers from {a}  {time.time()-t0:.1f}s", flush=True)

    loo = {}
    for held in usable:
        hid = held["id"]
        pool = []
        for e in usable:
            if e["id"] == hid:
                continue
            pool.extend(subsample(bundle[e["id"]], args.loo_each))
        print(f"  LOO hold {hid}  train {len(pool):,}", flush=True)
        act, bar = fit_pair(pool)
        loo[hid] = score_pair(act, bar, bundle[hid])
        print(
            f"    LOO actH1 {_f3(loo[hid]['act_h1'])}  primary {_f3(loo[hid]['primary'])}",
            flush=True,
        )

    chrono = {}
    ordered = sorted(usable, key=lambda e: e["start"])
    for i, held in enumerate(ordered):
        hid = held["id"]
        prior = ordered[:i]
        if not prior:
            chrono[hid] = {"n": 0, "act_h1": float("nan"), "primary": float("nan"), "note": "first"}
            continue
        pool = []
        for e in prior:
            pool.extend(subsample(bundle[e["id"]], args.loo_each))
        act, bar = fit_pair(pool)
        chrono[hid] = score_pair(act, bar, bundle[hid])
        print(
            f"  chrono {hid} from {len(prior)} prior  "
            f"actH1 {_f3(chrono[hid]['act_h1'])}  primary {_f3(chrono[hid]['primary'])}",
            flush=True,
        )

    payload = {
        "started": started,
        "frozen_families": FROZEN_FAMS,
        "n_features": len(FROZEN_KEYS),
        "stats": stats,
        "skipped": skipped,
        "in_era": in_era,
        "loo": loo,
        "chrono": chrono,
        "cross_primary": {a: {b: cross[a][b].get("primary") for b in ids} for a in ids},
        "cross_act_h1": {a: {b: cross[a][b].get("act_h1") for b in ids} for a in ids},
    }
    (LOG_DIR / "stage_b_eras.json").write_text(json.dumps(payload, default=str, indent=2))

    def row_era(eid, d):
        return (
            f"| `{eid}` | {d.get('n', 0):,} | {_f3(d.get('act_h1', float('nan')))} | "
            f"{_f3(d.get('primary', float('nan')))} | {d.get('hard_p1_n', 0):,} |"
        )

    lines = [
        "# Stage B cross-era generalization",
        "",
        f"Generated {started}. Wall {time.time()-t0:.1f}s.",
        "",
        "Families **frozen** at the selected 67 columns (no `return`, no `venue`).",
        "Two HGBs re-fit per era. Held-out eras are never used to select features.",
        "Stage A 3-of-8 still applies. Events strided inside each window.",
        "",
        "Polymarket got busier and more automated across this span. The question",
        "is whether tape-touch **principles** transfer, not whether Sep 2026",
        "weights are a universal model.",
        "",
        "Skipped (too few events after 3-of-8): "
        + (", ".join(skipped) if skipped else "(none)"),
        "",
        "## In-era (train first 70% of the window, score the rest)",
        "",
        "| era | n val | act H1 AUC | hard ±1 H60 | hard +1 n |",
        "| --- | --- | --- | --- | --- |",
    ]
    for e in usable:
        lines.append(row_era(e["id"], in_era[e["id"]]))
    lines += [
        "",
        "## Leave-one-era-out (train on the others, score the held-out era)",
        "",
        "| era | n | act H1 AUC | hard ±1 H60 | hard +1 n |",
        "| --- | --- | --- | --- | --- |",
    ]
    for e in usable:
        lines.append(row_era(e["id"], loo[e["id"]]))
    lines += [
        "",
        "## Walk-forward (train only on earlier eras)",
        "",
        "| era | n | act H1 AUC | hard ±1 H60 | hard +1 n |",
        "| --- | --- | --- | --- | --- |",
    ]
    for e in ordered:
        lines.append(row_era(e["id"], chrono[e["id"]]))
    lines += [
        "",
        "## Transfer matrix — hard ±1 H60 AUC (row trains, column tests)",
        "",
        "| train \\ test | " + " | ".join(f"`{i}`" for i in ids) + " |",
        "| --- | " + " | ".join("---" for _ in ids) + " |",
    ]
    for a in ids:
        cells = " | ".join(_f3(cross[a][b]["primary"]) for b in ids)
        lines.append(f"| `{a}` | {cells} |")
    lines += [
        "",
        "## Transfer matrix — activity H1 AUC",
        "",
        "| train \\ test | " + " | ".join(f"`{i}`" for i in ids) + " |",
        "| --- | " + " | ".join("---" for _ in ids) + " |",
    ]
    for a in ids:
        cells = " | ".join(_f3(cross[a][b]["act_h1"]) for b in ids)
        lines.append(f"| `{a}` | {cells} |")
    lines += [
        "",
        "Era catalog and block bounds: [`eras.py`](eras.py), [`era-blocks.json`](era-blocks.json).",
        "The Sep 2026 freeze card remains [`STAGE_B.md`](STAGE_B.md); this file is the",
        "generalization test that card did not have.",
        "",
    ]
    SNAP.write_text("\n".join(lines))
    print(f"wrote {SNAP}  {time.time()-t0:.1f}s", flush=True)
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
