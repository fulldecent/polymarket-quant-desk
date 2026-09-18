#!/usr/bin/env python3
"""Naive directional FOK baseline + last-100-block features + HGB P&L model.

Writes model-snapshot.md here and attempts parquet under SCRATCH_DIR.
Dirty git tree is fine.
"""

from __future__ import annotations

import argparse
import json
import math
import os
import sys
import time
from collections import defaultdict
from datetime import datetime, timezone
from pathlib import Path

import duckdb
import numpy as np
import pandas as pd
from dotenv import load_dotenv
from sklearn.ensemble import HistGradientBoostingRegressor

_HERE = Path(__file__).resolve().parent
_PROJECT_ROOT = _HERE.parents[1]
sys.path.insert(0, str(_HERE))
sys.path.insert(0, str(_PROJECT_ROOT))
load_dotenv(_PROJECT_ROOT / ".env")

from lib.partition_utils import (  # noqa: E402
    PARTITION_10K_SIZE,
    partition_dir,
    partition_start,
)
from sim_lib import (  # noqa: E402
    BLOCKS_PER_DAY,
    BLOCK_SEC,
    BUCKETS,
    COOLDOWN_BLOCKS,
    LIQ_END,
    LOOKBACK_BLOCKS,
    Level,
    MIN_SHARES,
    MIN_WORST_NOTIONAL,
    TICK,
    apply_cooldown,
    n_min,
    naive_caps,
    simulate_roundtrip,
)

FILLS_DIR = os.environ.get("FILLS_V1_DIR", "")
CBB_DIR = os.environ.get("CONDITION_BY_BLOCK_V1_DIR", "")
SCRATCH = os.environ.get("SCRATCH_DIR", "")
SNAP = _HERE / "model-snapshot.md"

FEATURE_COLS = []  # filled after we know names


def _fail(msg: str) -> None:
    sys.exit(msg)


def _files(root: Path, lo: int, hi: int) -> list[str]:
    out = []
    k = partition_start(lo)
    while k <= hi:
        p = root / partition_dir(k) / "data.parquet"
        if p.exists() and p.stat().st_size > 500:
            out.append(p.as_posix())
        k += PARTITION_10K_SIZE
    return out


def _sql_list(paths: list[str]) -> str:
    return "[" + ", ".join("'" + p.replace("'", "''") + "'" for p in paths) + "]"


def _frontier(root: Path) -> int:
    metas = sorted(root.glob("1M=*/10K=*/metadata.json"))
    if not metas:
        _fail(f"no partitions under {root}")
    last = json.loads(metas[-1].read_text())
    return int((last.get("parameters") or {}).get("max_block") or 0)


def _connect() -> duckdb.DuckDBPyConnection:
    if not SCRATCH:
        _fail("SCRATCH_DIR is not set.")
    Path(SCRATCH).mkdir(parents=True, exist_ok=True)
    con = duckdb.connect()
    con.execute(f"SET temp_directory = '{SCRATCH}'")
    con.execute("SET memory_limit = '10GB'")
    con.execute("SET threads = 4")
    return con


def _same_sign_yes_book() -> str:
    return "( (gross_usdc > 0 AND net_yes_tokens > 0) OR (gross_usdc < 0 AND net_yes_tokens < 0) )"


def _yes_px_sql() -> str:
    return f"""
    CASE
      WHEN net_yes_tokens = 0 THEN NULL
      WHEN {_same_sign_yes_book()} THEN gross_usdc::DOUBLE / net_yes_tokens::DOUBLE
      ELSE 1.0 + gross_usdc::DOUBLE / net_yes_tokens::DOUBLE
    END
    """


def _outcome_px_sql() -> str:
    return "abs(gross_usdc)::DOUBLE / abs(net_yes_tokens)::DOUBLE"


def run(trigger_days: float, max_attempts: int | None) -> int:
    if not FILLS_DIR or not Path(FILLS_DIR).exists():
        _fail("FILLS_V1_DIR missing")
    fills_root = Path(FILLS_DIR)
    frontier = _frontier(fills_root)
    x_hi = frontier - LIQ_END
    span = int(trigger_days * BLOCKS_PER_DAY)
    x_lo = x_hi - span + 1
    fill_lo = x_lo - LOOKBACK_BLOCKS
    fill_hi = x_hi + LIQ_END
    started = datetime.now(timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ")
    print(f"directional FOK  trigger {x_lo:,}–{x_hi:,}  fills {fill_lo:,}–{fill_hi:,}", flush=True)

    fill_files = _files(fills_root, fill_lo, fill_hi)
    cbb_files = _files(Path(CBB_DIR), x_lo, x_hi) if CBB_DIR else []
    if not fill_files:
        _fail("no fills")
    con = _connect()
    t0 = time.time()
    con.execute(
        f"""
        CREATE TABLE fills AS
        SELECT block_number, logical_fill_index, hex(condition_id) AS cid,
               is_taker, net_yes_tokens, gross_usdc, fee_usdc, market_id
        FROM read_parquet({_sql_list(fill_files)})
        WHERE block_number BETWEEN {fill_lo} AND {fill_hi}
          AND net_yes_tokens <> 0 AND gross_usdc <> 0
        """
    )
    n_fills = con.execute("SELECT count(*) FROM fills").fetchone()[0]
    print(f"  fills {n_fills:,}  {time.time()-t0:.1f}s", flush=True)

    print("  bars + maker-taker distance …", flush=True)
    con.execute(
        f"""
        CREATE TABLE bars AS
        SELECT
          block_number, cid,
          CASE WHEN {_same_sign_yes_book()} THEN 'yes' ELSE 'no' END AS outcome,
          sum(abs(net_yes_tokens)) FILTER (
            WHERE NOT is_taker AND gross_usdc < 0)::DOUBLE / 1e6 AS ask_tokens,
          sum(-gross_usdc) FILTER (
            WHERE NOT is_taker AND gross_usdc < 0)::DOUBLE / 1e6 AS ask_usdc,
          sum(abs(net_yes_tokens)) FILTER (
            WHERE NOT is_taker AND gross_usdc > 0)::DOUBLE / 1e6 AS bid_tokens,
          sum(gross_usdc) FILTER (
            WHERE NOT is_taker AND gross_usdc > 0)::DOUBLE / 1e6 AS bid_usdc,
          sum(abs(net_yes_tokens)) FILTER (
            WHERE is_taker AND gross_usdc > 0)::DOUBLE / 1e6 AS tbuy_tokens,
          sum(gross_usdc) FILTER (
            WHERE is_taker AND gross_usdc > 0)::DOUBLE / 1e6 AS tbuy_usdc,
          sum(abs(net_yes_tokens)) FILTER (
            WHERE is_taker AND gross_usdc < 0)::DOUBLE / 1e6 AS tsell_tokens,
          sum(-gross_usdc) FILTER (
            WHERE is_taker AND gross_usdc < 0)::DOUBLE / 1e6 AS tsell_usdc,
          sum(CASE WHEN is_taker THEN 1 ELSE 0 END) AS taker_legs,
          count(*) AS legs,
          count(DISTINCT CASE WHEN is_taker THEN 1 ELSE 0 END) AS dummy,
          max({_yes_px_sql()}) AS high_yes,
          min({_yes_px_sql()}) AS low_yes,
          arg_max({_yes_px_sql()}, logical_fill_index) AS close_yes,
          max(fee_usdc) AS fee_usdc,
          max(CASE WHEN market_id IS NOT NULL THEN 1 ELSE 0 END) AS neg_risk
        FROM fills
        GROUP BY 1, 2, 3
        """
    )
    # unique accounts: skip DISTINCT account (not in SELECT). Use legs as activity.
    con.execute(
        f"""
        CREATE TABLE mt_raw AS
        WITH tagged AS (
          SELECT
            block_number, logical_fill_index, cid, is_taker,
            CASE WHEN {_same_sign_yes_book()} THEN 'yes' ELSE 'no' END AS book,
            {_outcome_px_sql()} AS px,
            sum(CASE WHEN is_taker THEN 1 ELSE 0 END) OVER (
              PARTITION BY block_number ORDER BY logical_fill_index
            ) AS match_id
          FROM fills
        )
        SELECT m.block_number, m.cid, m.book,
               abs(m.px - t.px) AS dist
        FROM tagged m
        INNER JOIN tagged t
          ON t.block_number = m.block_number
         AND t.match_id = m.match_id
         AND t.is_taker AND NOT m.is_taker
         AND t.cid = m.cid AND t.book = m.book
        """
    )
    con.execute(
        """
        CREATE TABLE mt_bar AS
        SELECT block_number, cid, book AS outcome,
               median(dist) AS mt_abs_med
        FROM mt_raw
        GROUP BY 1, 2, 3
        """
    )
    con.execute(
        """
        CREATE TABLE bars2 AS
        SELECT b.*, m.mt_abs_med,
               (coalesce(b.ask_tokens,0) >= 5 AND coalesce(b.ask_usdc,0) >= 1.20) AS ask_floor
        FROM bars b
        LEFT JOIN mt_bar m
          ON m.block_number = b.block_number AND m.cid = b.cid AND m.outcome = b.outcome
        """
    )

    print("  naive opportunities + cooldown …", flush=True)
    opps = con.execute(
        f"""
        SELECT block_number, cid
        FROM bars2
        WHERE ask_floor AND block_number BETWEEN {x_lo} AND {x_hi}
        GROUP BY 1, 2
        ORDER BY cid, block_number
        """
    ).fetchall()
    gated = apply_cooldown([(int(b), c) for b, c in opps], COOLDOWN_BLOCKS)
    if max_attempts is not None:
        gated = gated[: int(max_attempts)]
    print(f"  opps {len(opps):,}  gated {len(gated):,}", flush=True)
    if not gated:
        _write_snap(started, x_lo, x_hi, frontier, [], None, time.time() - t0, {})
        return 0

    gdf = pd.DataFrame(gated, columns=["x_block", "cid"])
    con.register("gated", gdf)

    # Direction = deeper ask at X; close from bars2.
    con.execute(
        """
        CREATE TABLE cand0 AS
        SELECT g.x_block, g.cid,
               y.ask_tokens AS yes_ask, n.ask_tokens AS no_ask,
               coalesce(y.close_yes, n.close_yes) AS close_yes,
               coalesce(y.fee_usdc, n.fee_usdc, 0) AS fee_usdc,
               greatest(coalesce(y.neg_risk,0), coalesce(n.neg_risk,0)) AS neg_risk
        FROM gated g
        LEFT JOIN bars2 y
          ON y.block_number = g.x_block AND y.cid = g.cid AND y.outcome = 'yes'
        LEFT JOIN bars2 n
          ON n.block_number = g.x_block AND n.cid = g.cid AND n.outcome = 'no'
        """
    )
    cand_rows = con.execute("SELECT * FROM cand0").fetchall()
    records = []
    for x_block, cid, yes_ask, no_ask, close_yes, fee_usdc, neg_risk in cand_rows:
        if close_yes is None or not (0 < float(close_yes) < 1):
            continue
        ya = float(yes_ask or 0)
        na = float(no_ask or 0)
        if ya >= na and ya >= MIN_SHARES:
            direction = "yes"
            close_side = float(close_yes)
        elif na >= MIN_SHARES:
            direction = "no"
            close_side = 1.0 - float(close_yes)
        else:
            continue
        caps = naive_caps(close_side)
        if caps is None:
            continue
        p_entry, p_exit = caps
        n = float(n_min(p_entry))
        records.append(
            {
                "x_block": int(x_block),
                "cid": cid,
                "direction": direction,
                "close_side": close_side,
                "p_entry": p_entry,
                "p_exit": p_exit,
                "n": n,
                "fee_market": bool(fee_usdc and fee_usdc > 0),
                "neg_risk": bool(neg_risk),
            }
        )
    print(f"  tickets with caps {len(records):,}", flush=True)

    # Level indexes for sim
    print("  loading ask/tbuy/bid levels …", flush=True)
    ask_rows = con.execute(
        f"""
        SELECT block_number, cid,
               CASE WHEN net_yes_tokens < 0 THEN 'yes' ELSE 'no' END AS outcome,
               {_outcome_px_sql()} AS px,
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
               {_outcome_px_sql()} AS px,
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
               {_outcome_px_sql()} AS px,
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
                d[(cid, outc, int(blk))].append(Level(float(px), float(tok)))
        return d

    asks_i, tbuy_i, bids_i = _idx(ask_rows), _idx(tbuy_rows), _idx(bid_rows)

    t_sim = time.time()
    attempts = []
    for i, rec in enumerate(records, 1):
        x, cid, d = rec["x_block"], rec["cid"], rec["direction"]
        entry = asks_i.get((cid, d, x + 1), [])
        exit_b = [(b, tbuy_i[(cid, d, b)]) for b in range(x + 2, x + 61) if (cid, d, b) in tbuy_i]
        liq_b = [(b, bids_i[(cid, d, b)]) for b in range(x + 61, x + 121) if (cid, d, b) in bids_i]
        sim = simulate_roundtrip(
            n=rec["n"],
            p_entry=rec["p_entry"],
            p_exit=rec["p_exit"],
            entry_asks=entry,
            exit_by_block=exit_b,
            liq_by_block=liq_b,
            x_block=x,
            fee_market=rec["fee_market"],
        )
        rec.update(sim)
        attempts.append(rec)
        if i % 5000 == 0 or i == len(records):
            print(f"  simulated {i:,}/{len(records):,}  {time.time()-t_sim:.1f}s", flush=True)

    adf = pd.DataFrame(attempts)
    print("  features last 100 blocks …", flush=True)
    con.register("att", adf[["x_block", "cid", "direction", "close_side", "n"]])
    feat_sql_parts = [
        "a.x_block",
        "a.cid",
        "a.direction",
        "a.close_side",
        "a.n",
        "max(b.neg_risk) AS neg_risk_w",
        "max(CASE WHEN b.fee_usdc > 0 THEN 1 ELSE 0 END) AS fee_bit",
        "sum(CASE WHEN b.block_number = a.x_block THEN coalesce(b.ask_tokens,0) ELSE 0 END) AS ask_x",
        "sum(CASE WHEN b.block_number = a.x_block - 1 THEN coalesce(b.ask_tokens,0) ELSE 0 END) AS ask_xm1",
        "sum(CASE WHEN b.ask_floor AND b.block_number >= a.x_block - 4 THEN 1 ELSE 0 END) AS persist_floors_5",
        "max(CASE WHEN b.block_number = a.x_block THEN b.close_yes END) AS close_yes_x",
        "min(b.low_yes) AS low_100",
        "max(b.high_yes) AS high_100",
        "count(DISTINCT b.block_number) AS n_print_blocks",
    ]
    for k in BUCKETS:
        feat_sql_parts += [
            f"sum(coalesce(b.tbuy_usdc,0)) FILTER (WHERE b.block_number >= a.x_block - {k-1}) AS tbuy_{k}",
            f"sum(coalesce(b.tsell_usdc,0)) FILTER (WHERE b.block_number >= a.x_block - {k-1}) AS tsell_{k}",
            f"sum(coalesce(b.bid_usdc,0)) FILTER (WHERE b.block_number >= a.x_block - {k-1}) AS bid_{k}",
            f"sum(coalesce(b.ask_usdc,0)) FILTER (WHERE b.block_number >= a.x_block - {k-1}) AS ask_{k}",
            f"median(b.mt_abs_med) FILTER (WHERE b.block_number >= a.x_block - {k-1}) AS mt_abs_med_{k}",
            f"min(b.low_yes) FILTER (WHERE b.block_number >= a.x_block - {k-1}) AS low_{k}",
            f"max(b.high_yes) FILTER (WHERE b.block_number >= a.x_block - {k-1}) AS high_{k}",
            f"sum(CASE WHEN b.tbuy_tokens > 0 AND b.block_number >= a.x_block - {k-1} THEN 1 ELSE 0 END) AS tbuy_blocks_{k}",
        ]
    con.execute(
        f"""
        CREATE TABLE feat AS
        SELECT {", ".join(feat_sql_parts)}
        FROM att a
        LEFT JOIN bars2 b
          ON b.cid = a.cid AND b.outcome = a.direction
         AND b.block_number BETWEEN a.x_block - {LOOKBACK_BLOCKS - 1} AND a.x_block
        GROUP BY a.x_block, a.cid, a.direction, a.close_side, a.n
        """
    )
    fdf = con.execute("SELECT * FROM feat").fetchdf()
    df = adf.merge(fdf, on=["x_block", "cid", "direction"], how="left", suffixes=("", "_f"))

    def _imb(buy, sell):
        s = buy.fillna(0) + sell.fillna(0)
        return np.where(s > 0, (buy.fillna(0) - sell.fillna(0)) / s, 0.0)

    df["depth_over_nmin"] = df["ask_x"].fillna(0) / df["n"].replace(0, np.nan)
    df["min_p_1mp"] = np.minimum(df["close_side"], 1.0 - df["close_side"])
    df["hour_slot"] = df["x_block"] % BLOCKS_PER_DAY
    df["tick_size"] = TICK
    df["range_100"] = df["high_100"] - df["low_100"]
    for k in BUCKETS:
        df[f"imbalance_{k}"] = _imb(df[f"tbuy_{k}"], df[f"tsell_{k}"])
        df[f"range_{k}"] = df[f"high_{k}"] - df[f"low_{k}"]
        # return vs close_side: (close_side - low) as loc; ret uses high-low already
        df[f"ret_proxy_{k}"] = df[f"range_{k}"] * np.sign(df[f"imbalance_{k}"])
    df["close_loc_last"] = np.where(
        df["range_1"].fillna(0) > 0,
        (df["close_side"] - df["low_1"]) / df["range_1"].replace(0, np.nan),
        0.5,
    )
    df["blocks_since_tbuy"] = np.where(df["tbuy_blocks_100"] > 0, 0, 100)

    feat_cols = [
        "persist_floors_5",
        "ask_x",
        "ask_xm1",
        "depth_over_nmin",
        "fee_bit",
        "neg_risk_w",
        "min_p_1mp",
        "hour_slot",
        "n_print_blocks",
        "close_loc_last",
        "blocks_since_tbuy",
    ]
    for k in BUCKETS:
        feat_cols += [
            f"tbuy_{k}",
            f"imbalance_{k}",
            f"bid_{k}",
            f"mt_abs_med_{k}",
            f"range_{k}",
            f"ret_proxy_{k}",
            f"tbuy_blocks_{k}",
        ]
    for c in feat_cols:
        if c not in df.columns:
            df[c] = 0.0
        df[c] = pd.to_numeric(df[c], errors="coerce")

    # Time split: train first 70%, embargo 120, test rest
    xs = np.sort(df["x_block"].unique())
    cut = xs[int(len(xs) * 0.70)]
    train = df[df["x_block"] < cut - 120].copy()
    test = df[df["x_block"] >= cut].copy()
    print(f"  train {len(train):,}  test {len(test):,}  cut block {cut:,}", flush=True)

    model_metrics = {}
    if len(train) >= 200 and len(test) >= 50:
        Xtr = train[feat_cols].to_numpy(dtype=float)
        ytr = train["pnl"].to_numpy(dtype=float)
        Xte = test[feat_cols].to_numpy(dtype=float)
        est = HistGradientBoostingRegressor(
            loss="absolute_error",
            max_depth=6,
            max_iter=150,
            learning_rate=0.08,
            min_samples_leaf=40,
            l2_regularization=0.1,
            random_state=0,
        )
        est.fit(Xtr, ytr)
        test = test.copy()
        test["pnl_hat"] = est.predict(Xte)
        fired = test[test["pnl_hat"] > 0]
        model_metrics = {
            "train_n": len(train),
            "test_n": len(test),
            "test_pnl_all": float(test["pnl"].sum()),
            "test_pnl_model": float(fired["pnl"].sum()) if len(fired) else 0.0,
            "test_n_model": int(len(fired)),
            "test_hit_all": float(test["entry_ok"].mean()),
            "test_hit_model": float(fired["entry_ok"].mean()) if len(fired) else 0.0,
        }
        # importances: permutation is slow; skip. Record mean |pnl_hat| corr per col
        corrs = []
        for c in feat_cols:
            v = test[c].to_numpy(dtype=float)
            if np.nanstd(v) < 1e-12:
                continue
            mask = np.isfinite(v) & np.isfinite(test["pnl"].to_numpy())
            if mask.sum() < 30:
                continue
            r = np.corrcoef(v[mask], test["pnl"].to_numpy()[mask])[0, 1]
            if np.isfinite(r):
                corrs.append((abs(r), r, c))
        corrs.sort(reverse=True)
        model_metrics["top_corr"] = corrs[:12]
        df.loc[test.index, "pnl_hat"] = test["pnl_hat"]
    else:
        fired = test.iloc[0:0]
        model_metrics["top_corr"] = []

    def _slice_pnl(mask, name):
        sub = test[mask] if len(test) else test
        return {
            "name": name,
            "n": int(len(sub)),
            "pnl": float(sub["pnl"].sum()) if len(sub) else 0.0,
            "entry_ok": float(sub["entry_ok"].mean()) if len(sub) else 0.0,
            "q_zero_share": float(
                (sub["q_zero"] / sub["q"].replace(0, np.nan)).mean()
            ) if len(sub) else 0.0,
        }

    ablations = []
    if len(test):
        ablations.append(_slice_pnl(np.ones(len(test), dtype=bool), "naive all test"))
        ablations.append(_slice_pnl(test["persist_floors_5"].fillna(0) >= 3, "persist_floors_5 >= 3"))
        ablations.append(_slice_pnl(test["imbalance_15"].fillna(0) > 0, "imbalance_15 > 0"))
        ablations.append(_slice_pnl(test["imbalance_100"].fillna(0) > 0, "imbalance_100 > 0"))
        ablations.append(_slice_pnl(test["range_15"].fillna(0) >= 0.03, "range_15 >= 3 ticks"))
        ablations.append(_slice_pnl(test["range_100"].fillna(0) >= 0.03, "range_100 >= 3 ticks"))
        ablations.append(_slice_pnl(test["tbuy_15"].fillna(0) > 0, "tbuy_15 > 0"))
        ablations.append(_slice_pnl(test["fee_bit"].fillna(0) == 0, "fee-free (last 100)"))
        ablations.append(
            _slice_pnl(
                (test["persist_floors_5"].fillna(0) >= 3)
                & (test["imbalance_15"].fillna(0) > 0)
                & (test["tbuy_15"].fillna(0) > 0),
                "intersection persist+imb15+tbuy15",
            )
        )
        if "pnl_hat" in test.columns:
            ablations.append(_slice_pnl(test["pnl_hat"] > 0, "HGB pnl_hat > 0"))
        # mt distance: tight vs wide (median split on train)
        if test["mt_abs_med_15"].notna().any():
            med = float(train["mt_abs_med_15"].median()) if len(train) else float(test["mt_abs_med_15"].median())
            ablations.append(_slice_pnl(test["mt_abs_med_15"] <= med, f"mt_abs_med_15 <= {med:.4f}"))
            ablations.append(_slice_pnl(test["mt_abs_med_15"] > med, f"mt_abs_med_15 > {med:.4f}"))
            med100 = float(train["mt_abs_med_100"].median()) if len(train) else 0.0
            ablations.append(_slice_pnl(test["mt_abs_med_100"] <= med100, f"mt_abs_med_100 <= {med100:.4f}"))

    out_dir = Path(SCRATCH) / "directional-fok-v1"
    out_dir.mkdir(parents=True, exist_ok=True)
    parq = out_dir / "attempts.parquet"
    keep = [c for c in df.columns if c in set(adf.columns) | set(feat_cols) | {"pnl_hat", "state", "entry_ok"}]
    df.to_parquet(parq, index=False)
    elapsed = time.time() - t0
    _write_snap(started, x_lo, x_hi, frontier, attempts, parq, elapsed, model_metrics, ablations, feat_cols)
    print(f"wrote {SNAP}  {elapsed:.1f}s", flush=True)
    con.close()
    return 0


def _write_snap(started, x_lo, x_hi, frontier, attempts, parq, elapsed, model_metrics, ablations=None, feat_cols=None):
    n = len(attempts)
    by = defaultdict(int)
    pnl = 0.0
    n_ok = 0
    qe = ql = qz = 0.0
    for a in attempts:
        by[a.get("state", "miss")] += 1
        pnl += float(a.get("pnl") or 0)
        n_ok += int(bool(a.get("entry_ok")))
        qe += float(a.get("q_exit") or 0)
        ql += float(a.get("q_liq") or 0)
        qz += float(a.get("q_zero") or 0)
    qtot = qe + ql + qz
    span_d = (x_hi - x_lo + 1) * BLOCK_SEC / 86400.0
    lines = [
        "# Directional FOK — model snapshot",
        "",
        f"Generated {started} by `run.py`. Features: last **100 blocks** only. "
        "No cooldown recency. Buckets are views, not family-killers.",
        "",
        f"- trigger X: {x_lo:,}–{x_hi:,} (~{span_d:.2f} d)",
        f"- frontier: {frontier:,}",
        f"- cooldown: {COOLDOWN_BLOCKS} blocks",
        f"- elapsed: {elapsed:.1f}s",
        f"- parquet: `{parq}`" if parq else "- parquet: (none)",
        "",
        f"Attempts: **{n:,}** ({n / span_d:,.0f}/day). Entry hit {n_ok:,} ({100*n_ok/n:.1f}%)." if n else "Attempts: 0",
        "",
        "| state | n |",
        "| --- | --- |",
        f"| miss | {by['miss']:,} |",
        f"| exit_full | {by['exit_full']:,} |",
        f"| liq_full | {by['liq_full']:,} |",
        f"| mixed | {by['mixed']:,} |",
        f"| zero | {by['zero']:,} |",
        "",
        f"Headline P&L (all naive tickets): **{pnl:,.2f}** USDC"
        + (f" ({pnl/n:,.4f}/attempt)" if n else ""),
        "",
    ]
    if qtot > 0:
        lines += [
            "| entered shares | share of q |",
            "| --- | --- |",
            f"| GTC exit X+2..60 | {100*qe/qtot:.1f}% |",
            f"| dump X+61..120 | {100*ql/qtot:.1f}% |",
            f"| worthless | {100*qz/qtot:.1f}% |",
            "",
        ]
    if ablations:
        lines += [
            "## Test-window slices (not family obituaries)",
            "",
            "A weak 15-block cut is not proof the feature is useless. "
            "Compare nearby buckets.",
            "",
            "| slice | n | P&L | entry hit | mean q_zero/q |",
            "| --- | --- | --- | --- | --- |",
        ]
        for a in ablations:
            z = a["q_zero_share"]
            ztxt = f"{100*z:.1f}%" if z == z else "—"
            lines.append(
                f"| {a['name']} | {a['n']:,} | {a['pnl']:,.2f} | {100*a['entry_ok']:.1f}% | {ztxt} |"
            )
        lines.append("")
    mm = model_metrics or {}
    if mm.get("test_n"):
        lines += [
            "## HGB (absolute error, predict pnl, fire if ŷ > 0, flat m=1)",
            "",
            f"Train n={mm['train_n']:,}  test n={mm['test_n']:,}. "
            f"Naive test P&L {mm['test_pnl_all']:,.2f}. "
            f"Model fire n={mm['test_n_model']:,} P&L {mm['test_pnl_model']:,.2f}.",
            "",
        ]
        if mm.get("top_corr"):
            lines += ["| |corr| vs pnl (test) | signed r | feature |", "| --- | --- | --- |"]
            for ar, r, c in mm["top_corr"]:
                lines.append(f"| {ar:.3f} | {r:+.3f} | `{c}` |")
            lines.append("")
    lines.append(
        "Kelly is not applied (flat m=1). Cap grid: `python explorations/directional-fok-v1/grid.py`."
    )
    SNAP.write_text("\n".join(lines) + "\n")


def main() -> int:
    p = argparse.ArgumentParser()
    p.add_argument("--trigger-days", type=float, default=3.0)
    p.add_argument("--max-attempts", type=int, default=None)
    args = p.parse_args()
    return run(args.trigger_days, args.max_attempts)


if __name__ == "__main__":
    raise SystemExit(main())
