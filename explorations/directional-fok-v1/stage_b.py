#!/usr/bin/env python3
"""Stage B: four high/low YES-delta heads after Stage A 6-of-8 + Kris 10+.

One HistGradientBoostingRegressor. Heads are (z, is_high):
  X+1 high, X+1 low, X+1..X+30 high, X+1..X+30 low, all vs last(X).
Squared error; sample_weight 2 on X+1 rows, 1 on X+1..X+30.
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
from sklearn.ensemble import HistGradientBoostingRegressor
from sklearn.linear_model import Ridge
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
    PREFILTER_KRIS_MIN,
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
HORIZONS = (1, 30)
# Four B heads: (z, is_high, label_key). z=1 is X+1 only; z=30 is X+1..X+30.
HL_HEADS = (
    (1, 1, "y_h1_hi"),
    (1, 0, "y_h1_lo"),
    (30, 1, "y_h130_hi"),
    (30, 0, "y_h130_lo"),
)
H1_LOSS_WEIGHT = 2.0
H30_LOSS_WEIGHT = 1.0
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


def pack_hl(evs, keys: list[str] | None = None):
    """One row per (event, head). sample_weight is 2 on X+1 heads, 1 on X+1..X+30."""
    keys = keys or FEAT_KEYS
    X, y, w, meta = [], [], [], []
    for e in evs:
        feat = e.get("feat") or {}
        base = [float(feat.get(k, 0.0)) for k in keys]
        for z, is_hi, yk in HL_HEADS:
            X.append(base + [float(z), float(is_hi)])
            y.append(float(e.get(yk, 0.0)))
            w.append(H1_LOSS_WEIGHT if z == 1 else H30_LOSS_WEIGHT)
            meta.append((e, z, is_hi))
    if not X:
        return (
            np.zeros((0, len(keys) + 2), dtype=np.float32),
            np.array([], dtype=float),
            np.array([], dtype=float),
            [],
        )
    return (
        np.asarray(X, dtype=np.float32),
        np.array(y, dtype=float),
        np.array(w, dtype=float),
        meta,
    )


def weighted_mse(y, p, w) -> float:
    y = np.asarray(y, dtype=float)
    p = np.asarray(p, dtype=float)
    w = np.asarray(w, dtype=float)
    if y.size < 8:
        return float("nan")
    d = y - p
    return float(np.average(d * d, weights=w))


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
    # Four high/low delta heads, one tree model. Squared error; X+1 rows
    # get sample_weight 2 so they are twice as important as X+1..X+30.
    return HistGradientBoostingRegressor(
        loss="squared_error",
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
            ("rd", Ridge(alpha=1.0)),
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
        m_blk = g_m_blk.get(cid)
        if m_blk is None:
            n_skip += 1
            continue
        a = int(np.searchsorted(m_blk, x - (PREFILTER_WINDOW - 1), side="left"))
        b = int(np.searchsorted(m_blk, x, side="right"))
        if kris_count(g_m_px[cid][a:b]) < PREFILTER_KRIS_MIN:
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
        a130, h130, l130 = window_hl(cid, x + 1, x + 30)
        if not a1:
            h1, l1 = last, last
        if not a130:
            h130, l130 = last, last
        rb_sy = resolved.get(cid)
        if rb_sy is not None:
            rb, sy = rb_sy
            a1b, a130b = a1, a130
            a1, h1, l1 = fold_resolve(a1, h1, l1, x + 1, x + 1, rb, sy)
            a130, h130, l130 = fold_resolve(a130, h130, l130, x + 1, x + 30, rb, sy)
            if a1 and not a1b:
                n_res_h1 += 1
            if a130 and not a130b:
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
                "y_h1_hi": float(h1) - last,
                "y_h1_lo": float(l1) - last,
                "y_h130_hi": float(h130) - last,
                "y_h130_lo": float(l130) - last,
                "h1": h1,
                "l1": l1,
                "h130": h130,
                "l130": l130,
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
        "  model: one HGB regressor, 4 high/low delta heads; "
        "squared error, X+1 weight 2, X+1..X+30 weight 1",
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

    X_tr, y_tr, w_tr, _ = pack_hl(train_ev)
    X_va, y_va, w_va, _ = pack_hl(val_ev)
    X_te, y_te, w_te, _ = pack_hl(test_ev)
    print(f"  fit n={len(y_tr):,}  {time.time()-t0:.1f}s", flush=True)
    est = _hgb()
    est.fit(X_tr, y_tr, sample_weight=w_tr)
    p_va = est.predict(X_va)
    p_te = est.predict(X_te)
    mse_va = weighted_mse(y_va, p_va, w_va)
    mse_te = weighted_mse(y_te, p_te, w_te)
    print(
        f"  weighted MSE val {mse_va:.6f}  test {mse_te:.6f}  "
        f"iters {getattr(est, 'n_iter_', '?')}",
        flush=True,
    )
    freeze_keys = list(FEAT_KEYS)
    joblib.dump(
        {
            "hl": est,
            "feat_keys": freeze_keys,
            "families": list(FAMILIES),
            "which": "all",
            "horizons": list(HORIZONS),
            "h1_weight": H1_LOSS_WEIGHT,
            "h30_weight": H30_LOSS_WEIGHT,
            "train_cut_block": int(cut1),
            "val_cut_block": int(cut2),
            "embargo": int(emb),
            "weighted_mse_val": mse_va,
            "weighted_mse_test": mse_te,
        },
        LOG_DIR / "stage_b.joblib",
    )
    print(f"  wrote {LOG_DIR / 'stage_b.joblib'}", flush=True)
    SNAP.write_text(
        "\n".join(
            [
                "# Stage B freeze (high/low deltas)",
                "",
                f"Generated {started}. Wall {time.time()-t0:.1f}s.",
                f"Gate {PREFILTER_NEED}-of-{PREFILTER_WINDOW} and Kris Kross "
                f"{PREFILTER_KRIS_MIN}+. One HistGradientBoostingRegressor, "
                "squared error, sample_weight 2 on X+1 heads and 1 on X+1..X+30.",
                f"Val weighted MSE {mse_va:.6f}. Test {mse_te:.6f}.",
                f"Train {len(train_ev):,} val {len(val_ev):,} test {len(test_ev):,}.",
                "",
            ]
        )
    )
    print(f"wrote {SNAP}  {time.time()-t0:.1f}s", flush=True)
    return 0



if __name__ == "__main__":
    raise SystemExit(main())
