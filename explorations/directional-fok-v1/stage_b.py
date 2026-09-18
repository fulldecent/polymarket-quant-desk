#!/usr/bin/env python3
"""Stage B freeze (tape-touch), after Stage A persist 5-of-8.

Modeling: two binary HistGradientBoosting classifiers (sklearn).
  activity (k=0): features + horizon Z → P(any fill)
  barrier  (k≠0): features + signed k + Z → P(touch last ± k ticks)
Not a net. Not one mushy Z. k and Z are inputs, not separate models.

Features: every last-100-block condition-level family we can build from
fills_v1. No 10K account tables, no cooldown recency, no block>X.
Labels: CLOB prints in the horizon, plus on-chain resolve **inside**
that same horizon (redeem = infinite liquidity at 1.0 / 0.0). Resolve
after the horizon is unused.
Fit ALL families on train, then SELECT families on val (permutation
importance + a pre-declared drop rule). Test never selects.

Diagnostics (not the freeze): simple-rule rankers, a linear model.
Primary val metric: mean hard-subset H60 AUC at k=±1 (lookback range
has not already cleared the barrier).

Writes stage-b-snapshot.md and scratch/directional-fok-v1/stage_b.joblib
"""

from __future__ import annotations

import argparse
import json
import os
import sys
import time
import warnings
from collections import Counter
from datetime import datetime, timezone
from pathlib import Path

import joblib
import numpy as np
import pandas as pd
from dotenv import load_dotenv
from sklearn.ensemble import HistGradientBoostingClassifier
from sklearn.linear_model import LogisticRegression
from sklearn.metrics import brier_score_loss, roc_auc_score
from sklearn.pipeline import Pipeline
from sklearn.preprocessing import StandardScaler

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
    PREFILTER_NEED,
    PREFILTER_WINDOW,
    fold_resolve,
    infer_tick,
    kris_count,
    n_min,
    prefilter_alive,
    settlement_yes,
)

KS_BAR = tuple(k for k in range(-K_MAX, K_MAX + 1) if k != 0)
HORIZONS = (1, 30, 60)  # H1 FOK bar; H30 GTC short; H60 GTC long. Z is an input.
LOG_DIR = Path(os.environ.get("SCRATCH_DIR", "/tmp")) / "directional-fok-v1"
SNAP = _HERE / "stage-b-snapshot.md"

# Drop a family on val if both of these AUC drops are below the floor.
PERM_FLOOR = 0.002
# Prefer the smaller (selected) set if it is within this of all-features.
SELECT_SLACK = 0.003
# Primary: hard-subset H60 AUC, averaged over the 1-tick barriers.
PRIMARY_KS = (-1, 1)

# Families are the unit of selection. Buckets inside a family are views.
FAMILIES: dict[str, list[str]] = {
    "level": ["last", "tick", "min_p_1mp", "hour_slot"],
    "regime": ["notional_pctile", "log_venue_vs_7d", "log_venue_vs_t7"],
    "persist": [
        "n_print_blocks",
        "persist_floors_5",
        "persist_8",
        "kris_count_8",
        "blocks_since_floor",
        "blocks_since_taker_buy",
    ],
    "depth": [
        "ask_x",
        "ask_xm1",
        "bid_x",
        "ask_shares_x",
        "depth_over_nmin",
        "max_taker_buy_x",
        "n_makers_x",
    ],
    "counts": [f"n_{b}" for b in BUCKETS],
    "range": (
        [f"range_{b}" for b in BUCKETS]
        + [f"range_ticks_{b}" for b in BUCKETS]
        + ["dist_high_ticks", "dist_low_ticks"]
    ),
    "flow": (
        [f"up_usdc_{b}" for b in BUCKETS]
        + [f"imbalance_{b}" for b in BUCKETS]
        + [f"maker_buy_usdc_{b}" for b in BUCKETS]
        + [f"taker_share_{b}" for b in BUCKETS]
    ),
    "return": [f"ret_{b}" for b in BUCKETS]
    + ["close_minus_vwap_15", "close_loc_last", "close_loc_15"],
    "adverse": [f"mt_abs_med_{b}" for b in BUCKETS] + ["n_accounts_100"],
    "venue": ["fee_bit", "neg_risk"],
}

# Always kept even if permutation is flat: tick/last are the barrier geometry.
KEEP_ALWAYS = ("level",)

CBAR_COLS = (
    "block_number",
    "close_yes",
    "high_yes",
    "low_yes",
    "n_fills",
    "n_accounts",
    "n_makers",
    "up_usdc",
    "dn_usdc",
    "ask_usdc",
    "bid_usdc",
    "taker_usdc",
    "all_usdc",
    "max_taker_buy_usdc",
    "ask_shares",
    "ask_floor",
    "vwap_yes",
    "fee_usdc",
    "neg_risk",
    "mt_abs_med",
)


def feat_keys() -> list[str]:
    keys: list[str] = []
    seen: set[str] = set()
    for cols in FAMILIES.values():
        for c in cols:
            if c in seen:
                raise ValueError(f"duplicate feature {c}")
            seen.add(c)
            keys.append(c)
    return keys


FEAT_KEYS = feat_keys()


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


def _f3(x: float) -> str:
    return "—" if x != x else f"{x:.3f}"


def _f4(x: float) -> str:
    return "—" if x != x else f"{x:.4f}"


def feat_window(sl: pd.DataFrame, x: int, last: float, tick: float) -> dict:
    """Last-100-block condition-level features. `sl` is bars with block ≤ X."""
    out = {k: 0.0 for k in FEAT_KEYS}
    out["last"] = float(last)
    out["tick"] = float(tick)
    out["min_p_1mp"] = float(min(last, 1.0 - last))
    out["hour_slot"] = float(x % BLOCKS_PER_DAY)
    out["blocks_since_floor"] = float(LOOKBACK_BLOCKS)
    out["blocks_since_taker_buy"] = float(LOOKBACK_BLOCKS)
    if sl is None or sl.empty:
        return out
    bn = sl["block_number"].to_numpy(dtype=np.int64)
    out["n_print_blocks"] = float(len(sl))
    out["fee_bit"] = float((sl["fee_usdc"].fillna(0).to_numpy() > 0).any())
    out["neg_risk"] = float(sl["neg_risk"].fillna(0).max())
    at = sl[bn == x]
    if len(at):
        out["ask_x"] = float(at["ask_usdc"].fillna(0).sum())
        out["bid_x"] = float(at["bid_usdc"].fillna(0).sum())
        out["ask_shares_x"] = float(at["ask_shares"].fillna(0).sum())
        out["max_taker_buy_x"] = float(at["max_taker_buy_usdc"].fillna(0).max())
        out["n_makers_x"] = float(at["n_makers"].fillna(0).max())
        hi_x, lo_x = at["high_yes"].max(), at["low_yes"].min()
        if pd.notna(hi_x) and pd.notna(lo_x) and hi_x > lo_x:
            out["close_loc_last"] = float((last - lo_x) / (hi_x - lo_x))
        else:
            out["close_loc_last"] = 0.5
    if tick > 0 and out["ask_shares_x"] > 0 and 0 < last < 1:
        out["depth_over_nmin"] = out["ask_shares_x"] / float(n_min(last))
    xm1 = sl[bn == x - 1]
    if len(xm1):
        out["ask_xm1"] = float(xm1["ask_usdc"].fillna(0).sum())
    last5 = sl[bn >= x - 4]
    if len(last5):
        out["persist_floors_5"] = float(last5["ask_floor"].fillna(False).sum())
    last8 = sl[bn >= x - 7]
    out["persist_8"] = float(len(last8))
    floors = sl[sl["ask_floor"].fillna(False).to_numpy()]
    if len(floors):
        out["blocks_since_floor"] = float(x - int(floors["block_number"].iloc[-1]))
    taker_buy = (sl["up_usdc"].fillna(0).to_numpy() + sl["dn_usdc"].fillna(0).to_numpy()) > 0
    if taker_buy.any():
        out["blocks_since_taker_buy"] = float(x - int(bn[taker_buy][-1]))
    hi100 = sl["high_yes"].max()
    lo100 = sl["low_yes"].min()
    if pd.notna(hi100) and tick > 0:
        out["dist_high_ticks"] = float((hi100 - last) / tick)
    if pd.notna(lo100) and tick > 0:
        out["dist_low_ticks"] = float((last - lo100) / tick)
    out["n_accounts_100"] = float(sl["n_accounts"].fillna(0).sum())
    close = sl["close_yes"].to_numpy()
    for b in BUCKETS:
        wmask = bn >= x - (b - 1)
        w = sl[wmask]
        out[f"n_{b}"] = float(w["n_fills"].fillna(0).sum()) if len(w) else 0.0
        if len(w):
            hi, lo = w["high_yes"].max(), w["low_yes"].min()
            rng = float(hi - lo) if pd.notna(hi) and pd.notna(lo) else 0.0
            out[f"range_{b}"] = rng
            out[f"range_ticks_{b}"] = rng / tick if tick > 0 else 0.0
            up = float(w["up_usdc"].fillna(0).sum())
            dn = float(w["dn_usdc"].fillna(0).sum())
            s = up + dn
            out[f"up_usdc_{b}"] = up
            out[f"imbalance_{b}"] = (up - dn) / s if s > 0 else 0.0
            out[f"maker_buy_usdc_{b}"] = float(w["bid_usdc"].fillna(0).sum())
            tak = float(w["taker_usdc"].fillna(0).sum())
            allu = float(w["all_usdc"].fillna(0).sum())
            out[f"taker_share_{b}"] = tak / allu if allu > 0 else 0.0
            mt = w["mt_abs_med"].dropna()
            out[f"mt_abs_med_{b}"] = float(mt.median()) if len(mt) else 0.0
        prev_mask = bn <= x - b
        if prev_mask.any() and pd.notna(close[prev_mask][-1]):
            out[f"ret_{b}"] = last - float(close[prev_mask][-1])
    w15 = sl[bn >= x - 14]
    if len(w15):
        hi, lo = w15["high_yes"].max(), w15["low_yes"].min()
        if pd.notna(hi) and pd.notna(lo) and hi > lo:
            out["close_loc_15"] = float((last - lo) / (hi - lo))
        else:
            out["close_loc_15"] = 0.5
        usdc = w15["all_usdc"].fillna(0).to_numpy()
        vw = w15["vwap_yes"].to_numpy()
        ok = (usdc > 0) & pd.notna(vw)
        if ok.any():
            vwap = float(np.dot(vw[ok], usdc[ok]) / usdc[ok].sum())
            out["close_minus_vwap_15"] = last - vwap
    return out


def _vec(feat: dict, extra: list[float]) -> np.ndarray:
    return np.array([feat[k] for k in FEAT_KEYS] + extra, dtype=np.float32)


def pack_act(evs, keys: list[str] | None = None):
    keys = keys or FEAT_KEYS
    X, y, meta = [], [], []
    for e in evs:
        base = [e["feat"][k] for k in keys]
        for z in HORIZONS:
            X.append(base + [float(z)])
            y.append(e[f"y_act_{z}"])
            meta.append((e, z, 0))
    if not X:
        return np.zeros((0, len(keys) + 1), dtype=np.float32), np.array([], dtype=int), []
    return np.asarray(X, dtype=np.float32), np.array(y, dtype=int), meta


def pack_bar(evs, keys: list[str] | None = None):
    keys = keys or FEAT_KEYS
    X, y, meta = [], [], []
    for e in evs:
        last, tick = e["last"], e["tick"]
        base = [e["feat"][k] for k in keys]
        for z, h, l, a in (
            (1, e["h1"], e["l1"], e["y_act_1"]),
            (30, e["h30"], e["l30"], e["y_act_30"]),
            (60, e["h60"], e["l60"], e["y_act_60"]),
        ):
            for k in KS_BAR:
                if a:
                    barrier = last + k * tick
                    lab = int(h >= barrier - 1e-15) if k > 0 else int(l <= barrier + 1e-15)
                else:
                    lab = 0
                X.append(base + [float(k), float(z)])
                y.append(lab)
                meta.append((e, z, k))
    if not X:
        return np.zeros((0, len(keys) + 2), dtype=np.float32), np.array([], dtype=int), []
    return np.asarray(X, dtype=np.float32), np.array(y, dtype=int), meta


def split_act(y, p, meta):
    out = {}
    for z in HORIZONS:
        m = np.array([t[1] == z for t in meta])
        out[z] = (y[m], p[m])
    return out


def split_bar(y, p, meta):
    out = {}
    hard = {}
    if not meta:
        for z in HORIZONS:
            for k in KS_BAR:
                out[(k, z)] = (np.array([]), np.array([]))
                hard[(k, z)] = (np.array([]), np.array([]))
        return out, hard
    r100 = np.array([t[0]["range_100"] for t in meta])
    tick = np.array([t[0]["tick"] for t in meta])
    zz = np.array([t[1] for t in meta])
    kk = np.array([t[2] for t in meta])
    y = np.asarray(y)
    p = np.asarray(p)
    for z in HORIZONS:
        for k in KS_BAR:
            m = (zz == z) & (kk == k)
            out[(k, z)] = (y[m], p[m])
            hm = m & hard_mask(r100, tick, k)
            hard[(k, z)] = (y[hm], p[hm])
    return out, hard


def _hgb():
    # Binary heads (activity, barrier). Bernoulli log-loss / binary
    # cross-entropy; early stopping on that same loss, not accuracy.
    # Features are covariates, not separate objectives.
    return HistGradientBoostingClassifier(
        loss="log_loss",
        scoring="loss",
        max_depth=6,
        max_iter=200,
        learning_rate=0.06,
        min_samples_leaf=80,
        l2_regularization=0.15,
        early_stopping=True,
        validation_fraction=0.1,
        n_iter_no_change=15,
        random_state=0,
    )


def _linear():
    return Pipeline(
        [
            ("sc", StandardScaler()),
            (
                "lr",
                LogisticRegression(
                    max_iter=400,
                    solver="lbfgs",
                    random_state=0,
                ),
            ),
        ]
    )


def family_indices(names: list[str] | None = None) -> dict[str, np.ndarray]:
    keys = FEAT_KEYS
    out = {}
    for fam, cols in FAMILIES.items():
        if names is not None and fam not in names:
            continue
        out[fam] = np.array([keys.index(c) for c in cols], dtype=int)
    return out


def hard_mask(range_100: np.ndarray, tick: np.ndarray, k: int) -> np.ndarray:
    return range_100 + 1e-15 < abs(k) * tick


def primary_auc(hard_by_k: dict) -> float:
    vals = [_auc(*hard_by_k.get((k, 60), (np.array([]), np.array([])))) for k in PRIMARY_KS]
    arr = np.array(vals, dtype=float)
    if not np.all(np.isnan(arr)):
        return float(np.nanmean(arr))
    more = [_auc(*hard_by_k.get((k, 60), (np.array([]), np.array([])))) for k in KS_BAR]
    arr = np.array(more, dtype=float)
    if np.all(np.isnan(arr)):
        return float("nan")
    return float(np.nanmean(arr))


def stride_take(n: int, k: int) -> np.ndarray:
    """Evenly spaced indices in `[0, n)` — not a time prefix."""
    if k <= 0 or k >= n:
        return np.arange(n)
    return np.unique(np.linspace(0, n - 1, k).astype(int))


def collect_window(
    x_lo: int,
    x_hi: int,
    keep: int,
    rng: np.random.RandomState | None = None,
) -> tuple[list[dict], dict]:
    """5-of-8 events in [x_lo, x_hi] with resolve-in-horizon labels.

    `keep`: max events. `rng` None → time-stride; else random sample.
    """
    fills_root = Path(R.FILLS_DIR)
    fill_lo = x_lo - LOOKBACK_BLOCKS
    fill_hi = x_hi + 60
    ypx = R._yes_px_sql()
    con = R._connect()
    con.execute("SET memory_limit = '6GB'")
    con.execute("SET threads = 2")
    con.execute("SET preserve_insertion_order = false")
    fill_files = R._files(fills_root, fill_lo, fill_hi)
    if not fill_files:
        con.close()
        return [], {"n_cand": 0, "n_alive": 0, "n_kept": 0, "n_res_h1": 0, "n_res_h60": 0}
    c10k_dir = Path(os.environ.get("CONDITION_BY_10K_V1_DIR", ""))
    c10k_files = R._files(c10k_dir, fill_lo, fill_hi) if c10k_dir.exists() else []
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
    mk_lo = x_lo - (PREFILTER_WINDOW - 1)
    makers = con.execute(
        f"""
        SELECT cid, block_number, logical_fill_index, round(yes_px, 6) AS px
        FROM fills
        WHERE NOT is_taker AND yes_px IS NOT NULL
          AND block_number BETWEEN {mk_lo} AND {x_hi}
        ORDER BY cid, block_number, logical_fill_index
        """
    ).fetchdf()
    resolved: dict[str, tuple[int, float]] = {}
    if c10k_files:
        con.execute(
            f"""
            CREATE TABLE res AS
            SELECT hex(condition_id) AS cid, resolved_block, payout_numerators
            FROM read_parquet({R._sql_list(c10k_files)})
            WHERE resolved_block IS NOT NULL
            """
        )
        for cid, rb, po in con.execute(
            "SELECT cid, resolved_block, payout_numerators FROM res"
        ).fetchall():
            sy = settlement_yes(po)
            if sy is None or rb is None:
                continue
            rb = int(rb)
            prev = resolved.get(cid)
            if prev is None or rb < prev[0]:
                resolved[cid] = (rb, sy)
    con.close()
    if cbar.empty:
        return [], {"n_cand": 0, "n_alive": 0, "n_kept": 0, "n_res_h1": 0, "n_res_h60": 0}

    cbar = cbar.sort_values(["cid", "block_number"])
    grouped = {cid: g.reset_index(drop=True) for cid, g in cbar.groupby("cid", sort=False)}
    pgrouped = {cid: g.reset_index(drop=True) for cid, g in cpx.groupby("cid", sort=False)}
    g_m_blk, g_m_px = {}, {}
    if makers is not None and not makers.empty:
        for cid, g in makers.groupby("cid", sort=False):
            g_m_blk[cid] = g.block_number.to_numpy(dtype=np.int64)
            g_m_px[cid] = g.px.to_numpy(dtype=np.float64)
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
    n_alive = len(alive)
    if keep > 0 and n_alive > keep:
        if rng is not None:
            idx = np.sort(rng.choice(n_alive, keep, replace=False))
            alive = [alive[i] for i in idx]
        else:
            alive = [alive[i] for i in stride_take(n_alive, keep)]

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
    n_res_h1 = n_res_h60 = 0
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
        m_blk = g_m_blk.get(cid)
        if m_blk is not None:
            a = int(np.searchsorted(m_blk, x - (PREFILTER_WINDOW - 1), side="left"))
            b = int(np.searchsorted(m_blk, x, side="right"))
            feat["kris_count_8"] = float(kris_count(g_m_px[cid][a:b]))
        a1, h1, l1 = window_hl(cid, x + 1, x + 1)
        a30, h30, l30 = window_hl(cid, x + 2, x + 30)
        a60, h60, l60 = window_hl(cid, x + 2, x + 60)
        rb_sy = resolved.get(cid)
        if rb_sy is not None:
            rb, sy = rb_sy
            a1b, a30b, a60b = a1, a30, a60
            a1, h1, l1 = fold_resolve(a1, h1, l1, x + 1, x + 1, rb, sy)
            a30, h30, l30 = fold_resolve(a30, h30, l30, x + 2, x + 30, rb, sy)
            a60, h60, l60 = fold_resolve(a60, h60, l60, x + 2, x + 60, rb, sy)
            if a1 and not a1b:
                n_res_h1 += 1
            if a60 and not a60b:
                n_res_h60 += 1
        events.append(
            {
                "x_block": x,
                "cid": cid,
                "last": last,
                "tick": tick,
                "feat": feat,
                "range_100": feat["range_100"],
                "y_act_1": a1,
                "y_act_30": a30,
                "y_act_60": a60,
                "h1": h1,
                "l1": l1,
                "h30": h30,
                "l30": l30,
                "h60": h60,
                "l60": l60,
            }
        )
    stats = {
        "n_cand": int(len(ev_keys)),
        "n_alive": n_alive,
        "n_kept": len(events),
        "n_res_h1": n_res_h1,
        "n_res_h60": n_res_h60,
        "n_skip": n_skip,
    }
    return events, stats


def calib_rows(y, p, n_bins: int = 10) -> list[tuple[float, float, int]]:
    y = np.asarray(y, dtype=float)
    p = np.asarray(p, dtype=float)
    if y.size < n_bins * 5:
        return []
    qs = np.quantile(p, np.linspace(0, 1, n_bins + 1))
    qs[0], qs[-1] = -np.inf, np.inf
    rows = []
    for i in range(n_bins):
        m = (p >= qs[i]) & (p < qs[i + 1]) if i < n_bins - 1 else (p >= qs[i])
        if m.sum() == 0:
            continue
        rows.append((float(p[m].mean()), float(y[m].mean()), int(m.sum())))
    return rows


def main() -> int:
    ap = argparse.ArgumentParser()
    ap.add_argument("--trigger-days", type=float, default=4.0)
    ap.add_argument(
        "--max-events",
        type=int,
        default=250_000,
        help="max events after 5-of-8, strided across the trigger span (0 = all)",
    )
    ap.add_argument(
        "--from-events",
        type=str,
        default="",
        help="joblib of pre-collected events; skip fill scan",
    )
    args = ap.parse_args()

    LOG_DIR.mkdir(parents=True, exist_ok=True)
    t0 = time.time()
    started = datetime.now(timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ")
    n_feat = len(FEAT_KEYS)
    if args.from_events:
        blob = joblib.load(args.from_events)
        events = blob["events"]
        n_skip_gate = int(blob.get("n_skip_gate", 0))
        x_lo = int(blob.get("x_lo", events[0]["x_block"] if events else 0))
        x_hi = int(blob.get("x_hi", events[-1]["x_block"] if events else 0))
        print(
            f"stage B from-events {args.from_events}  n={len(events):,}  "
            f"span {x_lo:,}–{x_hi:,}  cols {n_feat}",
            flush=True,
        )
        ticks = Counter(e["tick"] for e in events)
        n_res_h1 = n_res_h60 = 0
        if len(events) < 500:
            R._fail(f"too few events: {len(events)}")
    else:
        if not R.FILLS_DIR or not Path(R.FILLS_DIR).exists():
            R._fail("FILLS_V1_DIR missing")
        fills_root = Path(R.FILLS_DIR)
        frontier = R._frontier(fills_root)
        x_hi = frontier - LIQ_END
        span = int(args.trigger_days * BLOCKS_PER_DAY)
        x_lo = x_hi - span + 1
        print(
            f"stage B tape-touch  trigger {x_lo:,}–{x_hi:,}  "
            f"families {len(FAMILIES)}  cols {n_feat}  "
            f"gate {PREFILTER_NEED}-of-{PREFILTER_WINDOW}",
            flush=True,
        )
        events, st = collect_window(
            x_lo, x_hi, int(args.max_events) if args.max_events else 0, rng=None
        )
        n_skip_gate = st["n_skip"]
        n_res_h1, n_res_h60 = st["n_res_h1"], st["n_res_h60"]
        ticks = Counter(e["tick"] for e in events)
        print(
            f"  events kept {len(events):,}  skipped gate {n_skip_gate:,}  "
            f"resolve-as-liq H1 {n_res_h1:,} H60 {n_res_h60:,}",
            flush=True,
        )
        if len(events) < 500:
            R._fail(
                f"too few events after {PREFILTER_NEED}-of-{PREFILTER_WINDOW}: {len(events)}"
            )
    print(
        "  model: two HGBs (activity k=0, barrier k≠0); "
        "all families then val-select; test never selects",
        flush=True,
    )
    xs = sorted({e["x_block"] for e in events})
    span_b = xs[-1] - xs[0]
    emb = min(120, max(0, span_b // 25))
    cut1 = xs[int(len(xs) * 0.55)]
    cut2 = xs[int(len(xs) * 0.78)]
    train_ev = [e for e in events if e["x_block"] <= cut1]
    val_ev = [e for e in events if cut1 + emb <= e["x_block"] <= cut2]
    test_ev = [e for e in events if e["x_block"] >= cut2 + emb]
    print(
        f"  split train {len(train_ev):,} val {len(val_ev):,} test {len(test_ev):,}  "
        f"embargo {emb}",
        flush=True,
    )

    Xa_tr, ya_tr, _ = pack_act(train_ev)
    Xa_va, ya_va, ma_va = pack_act(val_ev)
    Xa_te, ya_te, ma_te = pack_act(test_ev)
    Xb_tr, yb_tr, _ = pack_bar(train_ev)
    Xb_va, yb_va, mb_va = pack_bar(val_ev)
    Xb_te, yb_te, mb_te = pack_bar(test_ev)

    print(
        f"  fit activity n={len(ya_tr):,} pos={ya_tr.mean():.3f}  "
        f"barrier n={len(yb_tr):,} pos={yb_tr.mean():.3f}",
        flush=True,
    )
    est_act = _hgb()
    est_act.fit(Xa_tr, ya_tr)
    print(f"  activity iters {getattr(est_act, 'n_iter_', '?')}  {time.time()-t0:.1f}s", flush=True)
    est_bar = _hgb()
    est_bar.fit(Xb_tr, yb_tr)
    print(f"  barrier iters {getattr(est_bar, 'n_iter_', '?')}  {time.time()-t0:.1f}s", flush=True)

    pa_va = est_act.predict_proba(Xa_va)[:, 1]
    pa_te = est_act.predict_proba(Xa_te)[:, 1]
    pb_va = est_bar.predict_proba(Xb_va)[:, 1]
    pb_te = est_bar.predict_proba(Xb_te)[:, 1]

    act_va = split_act(ya_va, pa_va, ma_va)
    act_te = split_act(ya_te, pa_te, ma_te)
    bar_va, hard_va = split_bar(yb_va, pb_va, mb_va)
    bar_te, hard_te = split_bar(yb_te, pb_te, mb_te)
    primary_all = primary_auc(hard_va)
    yh, _ph = hard_va[(1, 60)]
    ym, _pm = hard_va[(-1, 60)]
    print(
        f"  all-features val primary (hard H60 ±1 AUC) {primary_all:.3f}  "
        f"+1 n={len(yh):,} pos={np.mean(yh) if len(yh) else float('nan'):.4f}  "
        f"-1 n={len(ym):,} pos={np.mean(ym) if len(ym) else float('nan'):.4f}",
        flush=True,
    )

    # Simple-rule rankers (no fit): activity uses n_15; barrier uses
    # lookback distance already traveled toward the barrier.
    def rule_act(meta):
        return np.array([e["feat"]["n_15"] for e, _, _ in meta], dtype=float)

    def rule_bar(meta):
        s = []
        for e, _, k in meta:
            s.append(e["feat"]["dist_high_ticks"] if k > 0 else e["feat"]["dist_low_ticks"])
        return np.array(s, dtype=float)

    ra_va = split_act(ya_va, rule_act(ma_va), ma_va)
    rb_va, rh_va = split_bar(yb_va, rule_bar(mb_va), mb_va)

    # Linear diagnostic on a train subsample. Not a freeze candidate.
    rng = np.random.RandomState(0)
    lin_act_auc = lin_bar_primary = float("nan")
    la_va = None
    lb_va = None
    try:
        n_lin = min(120_000, len(ya_tr))
        ia = rng.choice(len(ya_tr), n_lin, replace=False) if len(ya_tr) > n_lin else np.arange(len(ya_tr))
        n_lin_b = min(150_000, len(yb_tr))
        ib = (
            rng.choice(len(yb_tr), n_lin_b, replace=False)
            if len(yb_tr) > n_lin_b
            else np.arange(len(yb_tr))
        )
        with warnings.catch_warnings():
            warnings.simplefilter("ignore")
            lin_a = _linear()
            lin_a.fit(Xa_tr[ia], ya_tr[ia])
            lin_b = _linear()
            lin_b.fit(Xb_tr[ib], yb_tr[ib])
        la_va = split_act(ya_va, lin_a.predict_proba(Xa_va)[:, 1], ma_va)
        lb_va, lh_va = split_bar(yb_va, lin_b.predict_proba(Xb_va)[:, 1], mb_va)
        lin_act_auc = _auc(*la_va[1])
        lin_bar_primary = primary_auc(lh_va)
        print(
            f"  linear val activity H1 AUC {lin_act_auc:.3f}  "
            f"hard H60 ±1 {lin_bar_primary:.3f}",
            flush=True,
        )
    except Exception as exc:
        la_va = None
        print(f"  linear skipped: {exc}", flush=True)

    # Family permutation on val (barrier model; activity reported too).
    print("  family permutation on val …", flush=True)
    fam_idx = family_indices()
    perm_rows = []
    act_h1_base = _auc(*act_va[1])
    act_h60_base = _auc(*act_va[60])
    bar_p1_base = _auc(*bar_va[(1, 60)])
    for fam, idx in fam_idx.items():
        Xp_a = Xa_va.copy()
        Xp_b = Xb_va.copy()
        perm = rng.permutation(len(Xp_a))
        Xp_a[:, idx] = Xp_a[perm][:, idx]
        perm_b = rng.permutation(len(Xp_b))
        Xp_b[:, idx] = Xp_b[perm_b][:, idx]
        pa = est_act.predict_proba(Xp_a)[:, 1]
        pb = est_bar.predict_proba(Xp_b)[:, 1]
        a1 = split_act(ya_va, pa, ma_va)
        _, hh = split_bar(yb_va, pb, mb_va)
        d_h1 = act_h1_base - _auc(*a1[1])
        d_h60 = act_h60_base - _auc(*a1[60])
        d_pri = primary_all - primary_auc(hh)
        d_p1 = bar_p1_base - _auc(*split_bar(yb_va, pb, mb_va)[0][(1, 60)])
        perm_rows.append(
            {
                "fam": fam,
                "d_act_h1": d_h1,
                "d_act_h60": d_h60,
                "d_bar_p1_h60": d_p1,
                "d_primary": d_pri,
            }
        )
        print(
            f"    {fam:<8} ΔH1 {d_h1:+.3f}  ΔH60 {d_h60:+.3f}  "
            f"Δ+1H60 {d_p1:+.3f}  Δhard±1 {d_pri:+.3f}",
            flush=True,
        )

    def _below_floor(delta: float) -> bool:
        return delta == delta and delta < PERM_FLOOR

    selected = []
    dropped = []
    for row in perm_rows:
        fam = row["fam"]
        keep = fam in KEEP_ALWAYS or not (
            _below_floor(row["d_primary"]) and _below_floor(row["d_act_h1"])
        )
        if keep:
            selected.append(fam)
        else:
            dropped.append(fam)
    if not selected:
        selected = list(FAMILIES)
        dropped = []
    sel_keys = [c for fam in selected for c in FAMILIES[fam]]
    print(f"  selected families: {selected}", flush=True)
    print(f"  dropped families:  {dropped or '(none)'}", flush=True)

    # Refit selected if it is a proper subset.
    if dropped:
        print("  refit selected …", flush=True)
        Xa_tr_s, ya_tr_s, _ = pack_act(train_ev, sel_keys)
        Xa_va_s, ya_va_s, ma_va_s = pack_act(val_ev, sel_keys)
        Xa_te_s, ya_te_s, ma_te_s = pack_act(test_ev, sel_keys)
        Xb_tr_s, yb_tr_s, _ = pack_bar(train_ev, sel_keys)
        Xb_va_s, yb_va_s, mb_va_s = pack_bar(val_ev, sel_keys)
        Xb_te_s, yb_te_s, mb_te_s = pack_bar(test_ev, sel_keys)
        est_act_s = _hgb()
        est_act_s.fit(Xa_tr_s, ya_tr_s)
        est_bar_s = _hgb()
        est_bar_s.fit(Xb_tr_s, yb_tr_s)
        pa_va_s = est_act_s.predict_proba(Xa_va_s)[:, 1]
        pb_va_s = est_bar_s.predict_proba(Xb_va_s)[:, 1]
        act_va_s = split_act(ya_va_s, pa_va_s, ma_va_s)
        bar_va_s, hard_va_s = split_bar(yb_va_s, pb_va_s, mb_va_s)
        primary_sel = primary_auc(hard_va_s)
        print(f"  selected val primary {primary_sel:.3f}  all {primary_all:.3f}", flush=True)
        take_sel = primary_sel + 1e-15 >= primary_all - SELECT_SLACK
    else:
        take_sel = False
        primary_sel = primary_all
        est_act_s = est_act
        est_bar_s = est_bar
        sel_keys = list(FEAT_KEYS)
        act_va_s = act_va
        bar_va_s, hard_va_s = bar_va, hard_va
        Xa_te_s, ya_te_s, ma_te_s = Xa_te, ya_te, ma_te
        Xb_te_s, yb_te_s, mb_te_s = Xb_te, yb_te, mb_te
        ya_va_s, ma_va_s = ya_va, ma_va
        yb_va_s, mb_va_s = yb_va, mb_va
        pa_va_s, pb_va_s = pa_va, pb_va

    if take_sel:
        freeze_which = "selected"
        freeze_act, freeze_bar, freeze_keys = est_act_s, est_bar_s, sel_keys
        freeze_primary = primary_sel
    else:
        freeze_which = "all"
        freeze_act, freeze_bar, freeze_keys = est_act, est_bar, list(FEAT_KEYS)
        freeze_primary = primary_all

    Xa_va_f, ya_va_f, ma_va_f = pack_act(val_ev, freeze_keys)
    Xb_va_f, yb_va_f, mb_va_f = pack_bar(val_ev, freeze_keys)
    Xa_te_f, ya_te_f, ma_te_f = pack_act(test_ev, freeze_keys)
    Xb_te_f, yb_te_f, mb_te_f = pack_bar(test_ev, freeze_keys)
    pa_va_f = freeze_act.predict_proba(Xa_va_f)[:, 1]
    pb_va_f = freeze_bar.predict_proba(Xb_va_f)[:, 1]
    pa_te_f = freeze_act.predict_proba(Xa_te_f)[:, 1]
    pb_te_f = freeze_bar.predict_proba(Xb_te_f)[:, 1]
    freeze_act_va = split_act(ya_va_f, pa_va_f, ma_va_f)
    freeze_bar_va, freeze_hard_va = split_bar(yb_va_f, pb_va_f, mb_va_f)
    act_te_f = split_act(ya_te_f, pa_te_f, ma_te_f)
    bar_te_f, hard_te_f = split_bar(yb_te_f, pb_te_f, mb_te_f)
    freeze_primary = primary_auc(freeze_hard_va)

    print(f"  FREEZE {freeze_which}  val primary {freeze_primary:.3f}", flush=True)

    # Val half-stability on the frozen model.
    if val_ev:
        mid = val_ev[len(val_ev) // 2]["x_block"]
        val_a = [e for e in val_ev if e["x_block"] <= mid]
        val_b = [e for e in val_ev if e["x_block"] > mid]
        X1, y1, m1m = pack_act(val_a, freeze_keys)
        X2, y2, m2m = pack_act(val_b, freeze_keys)
        p1 = freeze_act.predict_proba(X1)[:, 1]
        p2 = freeze_act.predict_proba(X2)[:, 1]
        stab_h1 = (_auc(*split_act(y1, p1, m1m)[1]), _auc(*split_act(y2, p2, m2m)[1]))
        Xb1, yb1, mb1 = pack_bar(val_a, freeze_keys)
        Xb2, yb2, mb2 = pack_bar(val_b, freeze_keys)
        pb1 = freeze_bar.predict_proba(Xb1)[:, 1]
        pb2 = freeze_bar.predict_proba(Xb2)[:, 1]
        h1 = split_bar(yb1, pb1, mb1)[1]
        h2 = split_bar(yb2, pb2, mb2)[1]
        stab_pri = (primary_auc(h1), primary_auc(h2))
        def pick(y, p, meta, k, z):
            m = np.array([t[1] == z and t[2] == k for t in meta])
            return y[m], p[m]
        stab_up = (_auc(*pick(yb1, pb1, mb1, 1, 60)), _auc(*pick(yb2, pb2, mb2, 1, 60)))
    else:
        stab_h1 = (float("nan"), float("nan"))
        stab_up = (float("nan"), float("nan"))
        stab_pri = (float("nan"), float("nan"))

    # Tick-stratified on frozen val, activity H1 and barrier +1 H60.
    tick_rows = []
    for tval, _cnt in sorted(ticks.items(), key=lambda kv: -kv[1]):
        m_act = np.array([e["tick"] == tval and z == 1 for e, z, _ in ma_va_f])
        y_a, p_a = ya_va_f[m_act], pa_va_f[m_act]
        m_bar = np.array([e["tick"] == tval and z == 60 and k == 1 for e, z, k in mb_va_f])
        y_b, p_b = yb_va_f[m_bar], pb_va_f[m_bar]
        tick_rows.append((tval, int(m_act.sum()), _auc(y_a, p_a), int(m_bar.sum()), _auc(y_b, p_b)))

    cal_h1 = calib_rows(*freeze_act_va[1])
    cal_up = calib_rows(*freeze_bar_va[(1, 60)])

    joblib.dump(
        {
            "act": freeze_act,
            "bar": freeze_bar,
            "feat_keys": freeze_keys,
            "families": selected if take_sel else list(FAMILIES),
            "which": freeze_which,
            "primary_ks": PRIMARY_KS,
            "horizons": list(HORIZONS),
            "train_cut_block": int(cut1),
            "val_cut_block": int(cut2),
            "embargo": int(emb),
        },
        LOG_DIR / "stage_b.joblib",
    )
    metrics = {
        "started": started,
        "n_train": len(train_ev),
        "n_val": len(val_ev),
        "n_test": len(test_ev),
        "n_skip_gate": n_skip_gate,
        "ticks": {str(t): c for t, c in ticks.items()},
        "which": freeze_which,
        "primary_all": primary_all,
        "primary_sel": primary_sel,
        "dropped": dropped,
        "selected": selected,
        "stab_act_h1_val_halves": stab_h1,
        "stab_primary_val_halves": stab_pri,
        "n_features_all": len(FEAT_KEYS),
        "n_features_frozen": len(freeze_keys),
    }
    (LOG_DIR / "stage_b_metrics.json").write_text(json.dumps(metrics, default=str, indent=2))

    def row_act(name, y, p):
        if len(y) == 0:
            return f"| {name} | 0 | — | — | — |"
        return (
            f"| {name} | {len(y):,} | {np.mean(y):.3f} | {_f3(_auc(y, p))} | {_f4(_brier(y, p))} |"
        )

    lines = [
        "# Stage B freeze (tape-touch)",
        "",
        f"Generated {started}. Wall {time.time()-t0:.1f}s. "
        f"Trigger {x_lo:,}–{x_hi:,}. Stage A gate **3 of last 8**.",
        "",
        "## Modeling",
        "",
        "Two binary **HistGradientBoosting** classifiers (sklearn), log-loss,",
        "early stopping on a 10% train holdout. **activity** (`k=0`) and",
        "**barrier** (`k≠0`). `H1` = block `X+1`. `H60` = `X+2..X+60`",
        "(GTC window, not the FOK bar). Tick inferred from last 100 blocks.",
        "Both directions via `±k` on one last YES price. `k` and horizon `Z`",
        "are inputs. Not a neural net. Not a joint ordinal. Size / FOK stay",
        "in Stage C. On-chain resolve **inside** H1 / H60 is infinite",
        "liquidity at settlement (1.0 / 0.0 / 0.5). Resolve after the",
        "horizon is unused. CLOB-only card:",
        "[`stage-b-snapshot-clob-only.md`](stage-b-snapshot-clob-only.md).",
        "",
        "## Features: all, then select",
        "",
        f"All **{len(FEAT_KEYS)}** last-100-block condition-level columns in",
        f"**{len(FAMILIES)}** families, built from `fills_v1` only. No 10K",
        "account tables, no cooldown recency, no `block > X`. Fit all on",
        "train. Select families on val by permuting each family and dropping",
        f"it if Δ activity-H1 AUC **and** Δ hard-H60-±1 AUC are both `< {PERM_FLOOR}`.",
        "`level` (last, tick, min_p, hour) is never dropped. Frozen set is",
        f"**selected** if its val primary is within {SELECT_SLACK} of all-features,",
        "else **all**. Test is confirmation, not selection.",
        "",
        f"Primary val metric: mean hard-subset H60 AUC at `k=±1` "
        "(lookback range has not already cleared a 1-tick barrier).",
        "",
        f"Split: train {len(train_ev):,}  val {len(val_ev):,}  test {len(test_ev):,} "
        f"(event-level, embargo {emb} blocks). "
        f"Events {len(events):,}; {PREFILTER_NEED}-of-{PREFILTER_WINDOW} "
        f"skipped in collect {n_skip_gate:,}.",
        "",
        "Inferred ticks: "
        + ", ".join(f"{t:g}×{c}" for t, c in sorted(ticks.items(), key=lambda kv: -kv[1])),
        "",
        f"Val stability (first vs second half): activity H1 AUC "
        f"{_f3(stab_h1[0])} / {_f3(stab_h1[1])}; "
        f"barrier +1 H60 {_f3(stab_up[0])} / {_f3(stab_up[1])}; "
        f"primary (hard ±1 H60) {_f3(stab_pri[0])} / {_f3(stab_pri[1])}.",
        "",
        f"**Frozen:** `{freeze_which}` "
        f"({len(freeze_keys)} cols, families "
        f"{selected if take_sel else list(FAMILIES)}). "
        f"Val primary {_f3(freeze_primary)} "
        f"(all {_f3(primary_all)}, selected {_f3(primary_sel)}).",
        "",
        "Dropped families: " + (", ".join(dropped) if dropped else "(none)"),
        "",
        "## Capacity check (val, not used to freeze)",
        "",
        "Simple rule: activity ranked by `n_15`; barrier ranked by lookback",
        "ticks already traveled toward the barrier (`dist_high` / `dist_low`).",
        "Linear: `StandardScaler + LogisticRegression` on all columns, fit on",
        "a train subsample. If HGB is no better than the range rule on the",
        "**hard** subset, Stage B is not finding breakouts.",
        "",
        "| model | activity H1 AUC | +1 H60 AUC | hard ±1 H60 (primary) |",
        "| --- | --- | --- | --- |",
        f"| simple rule | {_f3(_auc(*ra_va[1]))} | {_f3(_auc(*rb_va[(1, 60)]))} | {_f3(primary_auc(rh_va))} |",
        f"| linear | {_f3(lin_act_auc)} | "
        + (
            f"{_f3(_auc(*lb_va[(1, 60)]))}" if la_va is not None else "—"
        )
        + f" | {_f3(lin_bar_primary)} |",
        f"| HGB all | {_f3(act_h1_base)} | {_f3(bar_p1_base)} | {_f3(primary_all)} |",
        f"| HGB selected | {_f3(_auc(*act_va_s[1]))} | {_f3(_auc(*bar_va_s[(1, 60)]))} | {_f3(primary_sel)} |",
        "",
        "## Family permutation (val ΔAUC; higher = family mattered)",
        "",
        "| family | Δ act H1 | Δ act H60 | Δ +1 H60 | Δ hard ±1 | keep |",
        "| --- | --- | --- | --- | --- | --- |",
    ]
    keep_set = set(selected)
    for row in perm_rows:
        fam = row["fam"]
        lines.append(
            f"| {fam} | {_f3(row['d_act_h1'])} | {_f3(row['d_act_h60'])} | "
            f"{_f3(row['d_bar_p1_h60'])} | {_f3(row['d_primary'])} | "
            f"{'yes' if fam in keep_set else 'no'} |"
        )
    lines += [
        "",
        "## Activity (`k=0`) — frozen",
        "",
        "| split/head | n | pos | AUC | Brier |",
        "| --- | --- | --- | --- | --- |",
        row_act("val H1", *freeze_act_va[1]),
        row_act("val H30", *freeze_act_va[30]),
        row_act("val H60", *freeze_act_va[60]),
        row_act("test H1", *act_te_f[1]),
        row_act("test H30", *act_te_f[30]),
        row_act("test H60", *act_te_f[60]),
        "",
        "## Barrier (all val) — frozen",
        "",
        "| k | H1 pos | H1 AUC | H1 Brier | H60 pos | H60 AUC | H60 Brier |",
        "| --- | --- | --- | --- | --- | --- | --- |",
    ]
    for k in KS_BAR:
        y1, p1 = freeze_bar_va[(k, 1)]
        y6, p6 = freeze_bar_va[(k, 60)]
        lines.append(
            f"| {k:+d} | {np.mean(y1):.3f} | {_f3(_auc(y1, p1))} | {_f4(_brier(y1, p1))} | "
            f"{np.mean(y6):.3f} | {_f3(_auc(y6, p6))} | {_f4(_brier(y6, p6))} |"
        )
    lines += [
        "",
        "## Barrier hard subset (val): lookback range < |k| ticks",
        "",
        "If the last 100 blocks already swung more than `k` ticks, `+k` is easy.",
        "This slice is the breakout case. **This is the Stage B question.**",
        "",
        "| k | H1 n | H1 pos | H1 AUC | H60 n | H60 pos | H60 AUC |",
        "| --- | --- | --- | --- | --- | --- | --- |",
    ]
    for k in KS_BAR:
        y1, p1 = freeze_hard_va[(k, 1)]
        y6, p6 = freeze_hard_va[(k, 60)]
        lines.append(
            f"| {k:+d} | {len(y1):,} | "
            f"{(np.mean(y1) if len(y1) else float('nan')):.3f} | {_f3(_auc(y1, p1))} | "
            f"{len(y6):,} | "
            f"{(np.mean(y6) if len(y6) else float('nan')):.3f} | {_f3(_auc(y6, p6))} |"
        )
    lines += [
        "",
        "## Tick strata (frozen val)",
        "",
        "| tick | n H1 | act H1 AUC | n +1 H60 | +1 H60 AUC |",
        "| --- | --- | --- | --- | --- |",
    ]
    for tval, n1, a1, n6, a6 in tick_rows:
        lines.append(f"| {tval:g} | {n1:,} | {_f3(a1)} | {n6:,} | {_f3(a6)} |")
    lines += [
        "",
        "## Calibration (frozen val, quantile bins)",
        "",
        "Activity H1:",
        "",
        "| mean p | mean y | n |",
        "| --- | --- | --- |",
    ]
    for mp, my, n in cal_h1:
        lines.append(f"| {mp:.3f} | {my:.3f} | {n:,} |")
    lines += [
        "",
        "Barrier +1 H60:",
        "",
        "| mean p | mean y | n |",
        "| --- | --- | --- |",
    ]
    for mp, my, n in cal_up:
        lines.append(f"| {mp:.3f} | {my:.3f} | {n:,} |")
    lines += [
        "",
        "## Test confirmation (not used to pick the freeze)",
        "",
        "| k | H1 AUC | H60 AUC | H60 hard AUC |",
        "| --- | --- | --- | --- |",
    ]
    for k in KS_BAR:
        y1, p1 = bar_te_f[(k, 1)]
        y6, p6 = bar_te_f[(k, 60)]
        yh, ph = hard_te_f[(k, 60)]
        lines.append(
            f"| {k:+d} | {_f3(_auc(y1, p1))} | {_f3(_auc(y6, p6))} | {_f3(_auc(yh, ph))} |"
        )
    lines += [
        "",
        f"Models: `{LOG_DIR / 'stage_b.joblib'}`.",
        "",
        "Stage B freeze lives on this card. Stage C is the sequential decoder.",
        "",
    ]
    SNAP.write_text("\n".join(lines))
    print(f"wrote {SNAP}  {time.time()-t0:.1f}s", flush=True)
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
