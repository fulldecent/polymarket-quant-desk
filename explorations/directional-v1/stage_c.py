#!/usr/bin/env python3
"""Stage C sequential policy. Stage B frozen heads are inputs, not the trader.

Extra last-100-block features Stage B does not emit (account dominance,
unique accounts, HHI, fee/venue). Sequential P&L after 180-block cooldown.
Train proposes a small decoder grid on calendar day D; test is D+7
(same weekday). <1 activation/hour is a hard fail (no dollar number).
"""

from __future__ import annotations

import argparse
import csv
import json
import os
import sys
import time
from collections import defaultdict
from datetime import date, datetime, timedelta, timezone
from itertools import product
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
from sim_lib import (  # noqa: E402
    BLOCKS_PER_DAY,
    COOLDOWN_BLOCKS,
    K_MAX,
    LIQ_END,
    LOOKBACK_BLOCKS,
    MIN_SHARES,
    PREFILTER_NEED,
    PREFILTER_WINDOW,
    Level,
    infer_tick,
    clip_px,
    long_caps,
    n_min,
    prefilter_alive,
    simulate_roundtrip,
    taker_fee_usdc,
)
from stage_b import (  # noqa: E402
    FEAT_KEYS,
    HL_HEADS,
    HORIZONS,
    feat_window,
    stride_take,
)

BLOCKS_PER_HOUR = 1714
LOG_DIR = Path(os.environ.get("SCRATCH_DIR", "/tmp")) / "directional-v1"
B_JOB = LOG_DIR / "stage_b.joblib"
SNAP = _HERE / "stage-c-snapshot.md"
SIZE_MULTS = (1.1,)
WHALE_MODES = ("any", "crowd70")
# Selective: 1/hour is a floor, not a target. Wait for a loud 30-block range.
T_FILL = (0.70, 0.85)
T_LIQ = (0.55, 0.75)
T_RANGE = (0.40, 0.60)
K_NEEDS = (2, 3, 4)
PRICE_MODES = ("passive", "pay1", "pay2")  # din = 0, 1, 2
PRICE_DIN = {"passive": 0, "pay1": 1, "pay2": 2}
EXIT_ENDS = (30, 60)
SKIP_FEES = (False, True)
CACHE = LOG_DIR / "stage_c_events.joblib"


def _hours(lo: int, hi: int) -> float:
    return max(1e-9, (hi - lo + 1) / float(BLOCKS_PER_HOUR))


def block_date_knots() -> list[tuple[int, date]]:
    """(block, UTC date) knots from era-blocks plus the 9-month span anchors."""
    knots = [
        (80_282_490, date(2025, 12, 14)),
        (93_789_879, date(2026, 9, 14)),
    ]
    p = _HERE / "era-blocks.json"
    if p.exists():
        for e in json.loads(p.read_text()):
            knots.append((int(e["start_block"]), date.fromisoformat(e["start"])))
    # unique by block, sorted
    by_b = {}
    for b, d in knots:
        by_b[int(b)] = d
    return sorted(by_b.items())


def block_to_date(x: int, knots: list[tuple[int, date]] | None = None) -> date:
    """UTC calendar date for a Polygon block (piecewise-linear on knots)."""
    knots = knots or block_date_knots()
    if not knots:
        raise ValueError("no block-date knots")
    if x <= knots[0][0]:
        return knots[0][1]
    if x >= knots[-1][0]:
        b0, d0 = knots[-2] if len(knots) > 1 else knots[0]
        b1, d1 = knots[-1]
        if b1 == b0:
            return d1
        t = (x - b0) / (b1 - b0)
        return d0 + timedelta(days=round(t * (d1 - d0).days))
    for i in range(len(knots) - 1):
        b0, d0 = knots[i]
        b1, d1 = knots[i + 1]
        if b0 <= x <= b1:
            if b1 == b0:
                return d0
            t = (x - b0) / (b1 - b0)
            return d0 + timedelta(days=round(t * (d1 - d0).days))
    return knots[-1][1]


def week_shift_pairs(
    events: list[dict],
    shift_days: int = 7,
    min_n: int = 200,
    knots: list[tuple[int, date]] | None = None,
) -> list[tuple[date, date, list[dict], list[dict]]]:
    """Train day D, test day D+shift (same weekday)."""
    knots = knots or block_date_knots()
    by: dict[date, list[dict]] = defaultdict(list)
    for e in events:
        by[block_to_date(int(e["x_block"]), knots)].append(e)
    out = []
    for d in sorted(by):
        d7 = d + timedelta(days=int(shift_days))
        if d7 not in by:
            continue
        if d.weekday() != d7.weekday():
            continue
        if len(by[d]) < min_n or len(by[d7]) < min_n:
            continue
        out.append((d, d7, by[d], by[d7]))
    return out


def _f3(x) -> str:
    if x is None:
        return "hard-fail"
    try:
        x = float(x)
    except (TypeError, ValueError):
        return "—"
    if x != x:
        return "—"
    return f"{x:.3f}"


def account_stats(sl: pd.DataFrame, x: int) -> dict:
    """Unique-account concentration in last 100 blocks, plus block X."""
    out = {
        "top_share": 0.0,
        "n_acct": 0.0,
        "hhi": 1.0,
        "top_share_x": 0.0,
        "n_acct_x": 0.0,
    }
    if sl is None or sl.empty:
        return out
    by = sl.groupby("account", sort=False)["usdc"].sum()
    tot = float(by.sum())
    if tot > 0:
        mx = float(by.max())
        out["top_share"] = mx / tot
        out["n_acct"] = float(len(by))
        shares = (by.to_numpy(dtype=float) / tot) ** 2
        out["hhi"] = float(shares.sum())
    at = sl[sl["block_number"].to_numpy() == x]
    if len(at):
        byx = at.groupby("account", sort=False)["usdc"].sum()
        tx = float(byx.sum())
        if tx > 0:
            out["top_share_x"] = float(byx.max()) / tx
            out["n_acct_x"] = float(len(byx))
    return out


def passes_whale(st: dict, mode: str) -> bool:
    top = float(st.get("top_share", 0.0))
    n = float(st.get("n_acct", 0.0))
    if mode == "any":
        return True
    if mode == "crowd50":
        return top <= 0.50 + 1e-15
    if mode == "crowd70":
        return top <= 0.70 + 1e-15
    if mode == "diverse5":
        return n + 1e-15 >= 5
    if mode == "dominate50":
        return top >= 0.50 - 1e-15
    return True


def pick_ticket(
    ev: dict,
    *,
    in_frac: float = 1.0,
    out_frac: float = 1.0,
    double_k: float = 1.0,
    size_mult: float = 1.1,
    exit_end: int = 30,
    **_ignore,
) -> dict | None:
    """Always-on decoder from Stage B high/low deltas.

    entry/exit are deltas to last(X) in YES space. Direction is the larger
    predicted excursion. should_bet_double if predicted edge ≥ double_k ticks.
    Size is 1× floor (double is an eval weight, not a skip).
    """
    last = float(ev["last"])
    tick = float(ev["tick"])
    if tick <= 0 or not (0 < last < 1):
        return None
    h1h = float(ev.get("b_h1_hi", 0.0))
    h1l = float(ev.get("b_h1_lo", 0.0))
    h30h = float(ev.get("b_h130_hi", 0.0))
    h30l = float(ev.get("b_h130_lo", 0.0))
    up = h1h + h30h
    dn = (-h1l) + (-h30l)
    if up >= dn:
        side = "yes"
        d_in = float(in_frac) * h1h
        d_out = float(out_frac) * h30h
        p_in = clip_px(last + d_in, tick)
        p_out = clip_px(last + d_out, tick)
    else:
        side = "no"
        d_in = float(in_frac) * h1l
        d_out = float(out_frac) * h30l
        p_in = clip_px(1.0 - (last + d_in), tick)
        p_out = clip_px(1.0 - (last + d_out), tick)
    if p_out <= p_in + 1e-15:
        p_out = clip_px(p_in + tick, tick)
    if p_out <= p_in + 1e-15:
        return None
    n = max(MIN_SHARES, float(size_mult) * n_min(p_in))
    edge = p_out - p_in
    double = int(edge + 1e-15 >= float(double_k) * tick)
    d_in_yes = (p_in if side == "yes" else 1.0 - p_in) - last
    d_out_yes = (p_out if side == "yes" else 1.0 - p_out) - last
    return {
        "side": side,
        "p_in": p_in,
        "p_out": p_out,
        "n": float(n),
        "din": d_in_yes,
        "dout": d_out_yes,
        "entry_delta": d_in_yes,
        "exit_delta": d_out_yes,
        "should_bet_double": double,
        "exit_end": int(exit_end),
    }


def sequential_eval(
    events,
    *,
    params: dict,
    lo: int,
    hi: int,
    asks_i,
    tbuy_i,
    bids_i,
    sims: dict | None = None,
) -> dict:
    cool: dict[str, int] = {}
    n_part = pnl = n_ok = 0
    qe = ql = qz = buy = score = 0.0
    for ev in events:
        x, cid = ev["x_block"], ev["cid"]
        if x < cool.get(cid, -1):
            continue
        ticket = pick_ticket(ev, **params)
        if ticket is None:
            continue
        n_part += 1
        cool[cid] = x + COOLDOWN_BLOCKS
        key = (
            id(ev),
            ticket["side"],
            round(float(ticket["din"]), 8),
            round(float(ticket["dout"]), 8),
            ticket["n"],
        )
        if sims is not None and key in sims:
            sim = sims[key]
        else:
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
            sim = simulate_roundtrip(
                n=ticket["n"],
                p_entry=ticket["p_in"],
                p_exit=ticket["p_out"],
                entry_asks=entry,
                exit_by_block=exb,
                liq_by_block=liqb,
                x_block=x,
                fee_market=bool(ev.get("fee_market", ev.get("feat", {}).get("fee_bit", 0))),
                exit_end=x_end,
                liq_start=x_end + 1,
                liq_end=x_end + 60,
            )
        raw = float(sim["pnl"])
        pnl += raw
        dbl = int(ticket.get("should_bet_double", 0))
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
    return {
        "n_part": n_part,
        "per_hour": rate,
        "hard_fail": hard,
        "pnl": None if hard else pnl,
        "score": None if hard else score,
        "hit": (n_ok / n_part) if n_part else 0.0,
        "q_zero": (qz / qt) if qt else 0.0,
        "q_exit": (qe / qt) if qt else 0.0,
        "q_liq": (ql / qt) if qt else 0.0,
        "hours": hours,
        "days": hours / 24.0,
        "n_fill": n_ok,
        "buy_usdc": buy,
    }


def naive_eval(events, *, lo, hi, asks_i, tbuy_i, bids_i) -> dict:
    """Every alive trigger after cooldown: buy the up-imbalance side, +0 / +1 tick."""
    cool: dict[str, int] = {}
    n_part = pnl = n_ok = 0
    qe = ql = qz = buy = 0.0
    for ev in events:
        x, cid = ev["x_block"], ev["cid"]
        if x < cool.get(cid, -1):
            continue
        last, tick = ev["last"], ev["tick"]
        imb = float(ev.get("imbalance_15", 0.0))
        side = "yes" if imb >= 0 else "no"
        close_side = last if side == "yes" else 1.0 - last
        caps = long_caps(close_side, 0, 1, tick)
        if caps is None:
            continue
        p_in, p_out = caps
        n = max(MIN_SHARES, 1.1 * n_min(p_in))
        n_part += 1
        cool[cid] = x + COOLDOWN_BLOCKS
        entry = asks_i.get((cid, side, x + 1), [])
        exb = [
            (b, tbuy_i[(cid, side, b)])
            for b in range(x + 2, x + 61)
            if (cid, side, b) in tbuy_i
        ]
        liqb = [
            (b, bids_i[(cid, side, b)])
            for b in range(x + 61, x + 121)
            if (cid, side, b) in bids_i
        ]
        sim = simulate_roundtrip(
            n=float(n),
            p_entry=p_in,
            p_exit=p_out,
            entry_asks=entry,
            exit_by_block=exb,
            liq_by_block=liqb,
            x_block=x,
            fee_market=bool(ev.get("fee_market", ev.get("feat", {}).get("fee_bit", 0))),
        )
        pnl += sim["pnl"]
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
    return {
        "n_part": n_part,
        "per_hour": rate,
        "hard_fail": hard,
        "pnl": None if hard else pnl,
        "hit": (n_ok / n_part) if n_part else 0.0,
        "q_zero": (qz / qt) if qt else 0.0,
        "hours": hours,
        "days": hours / 24.0,
        "n_fill": n_ok,
        "buy_usdc": buy,
        "name": "naive_imb_+0/+1",
    }


def attach_stage_b(events, est, keys: list[str] | None = None) -> None:
    """Score the four high/low delta heads onto each event."""
    keys = keys or list(FEAT_KEYS)
    n = len(events)
    if n == 0:
        return
    n_h = len(HL_HEADS)
    X = np.zeros((n * n_h, len(keys) + 2), dtype=np.float32)
    r = 0
    for e in events:
        feat = e.get("feat") or {}
        base = [float(feat.get(k, 0.0)) for k in keys]
        for z, is_hi, _yk in HL_HEADS:
            X[r, :-2] = base
            X[r, -2] = float(z)
            X[r, -1] = float(is_hi)
            r += 1
    pred = est.predict(X)
    r = 0
    for e in events:
        e["b_h1_hi"] = float(pred[r]); r += 1
        e["b_h1_lo"] = float(pred[r]); r += 1
        e["b_h130_hi"] = float(pred[r]); r += 1
        e["b_h130_lo"] = float(pred[r]); r += 1


def _idx(rows) -> dict:
    d = defaultdict(list)
    for blk, cid, outc, px, tok in rows:
        if px and tok and tok > 0:
            d[(cid, outc, int(blk))].append(Level(float(px), float(tok)))
    return d


def main() -> int:
    ap = argparse.ArgumentParser()
    ap.add_argument(
        "--trigger-days",
        type=float,
        default=21.0,
        help="C event span in 2.1s-days. Week-shift val needs multiple weeks.",
    )
    ap.add_argument("--max-events", type=int, default=80_000)
    ap.add_argument(
        "--week-shift",
        type=int,
        default=7,
        help="validate C by training on day D and testing D+N (0 = classic 55/23/22)",
    )
    args = ap.parse_args()

    if not B_JOB.exists():
        R._fail(f"frozen Stage B missing: {B_JOB}")
    blob = joblib.load(B_JOB)
    est_act, est_bar = blob["act"], blob["bar"]
    b_keys = list(blob["feat_keys"])
    b_horizons = set(int(z) for z in blob.get("horizons", ()))
    if 30 not in b_horizons:
        R._fail(
            "frozen Stage B has no H30 head. Refit B with Z in {1,30,60} "
            "on an earlier slice; C does not fit that head."
        )
    b_train_cut = blob.get("train_cut_block")
    b_embargo = int(blob.get("embargo", 120))
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
    print(
        f"stage C  trigger {x_lo:,}–{x_hi:,}  frozen B {len(b_keys)} cols  "
        f"gate {PREFILTER_NEED}-of-{PREFILTER_WINDOW}",
        flush=True,
    )

    ypx = R._yes_px_sql()
    con = R._connect()
    con.execute("SET memory_limit = '8GB'")
    con.execute("SET preserve_insertion_order = false")
    fill_files = R._files(fills_root, fill_lo, fill_hi)
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
    n_fills = con.execute("SELECT count(*) FROM fills").fetchone()[0]
    print(f"  fills {n_fills:,}  {time.time()-t0:.1f}s", flush=True)

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
    con.execute(
        """
        CREATE TABLE acct_bar AS
        SELECT cid, block_number, account,
               sum(abs(gross_usdc))::DOUBLE / 1e6 AS usdc
        FROM fills
        GROUP BY 1, 2, 3
        """
    )
    cbar = con.execute("SELECT * FROM cbar").fetchdf()
    cpx = con.execute("SELECT cid, block_number, px FROM cpx").fetchdf()
    acct = con.execute(
        "SELECT cid, block_number, account, usdc FROM acct_bar ORDER BY cid, block_number"
    ).fetchdf()
    print(f"  cbar {len(cbar):,}  acct {len(acct):,}  {time.time()-t0:.1f}s", flush=True)

    print("  FOK/exit/dump levels …", flush=True)
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
              AND block_number BETWEEN {x_lo + 31} AND {x_hi + 120}
            """
        ).fetchall()
    )
    con.close()

    cbar = cbar.sort_values(["cid", "block_number"])
    grouped = {cid: g.reset_index(drop=True) for cid, g in cbar.groupby("cid", sort=False)}
    pgrouped = {cid: g.reset_index(drop=True) for cid, g in cpx.groupby("cid", sort=False)}
    agrouped = {cid: g.reset_index(drop=True) for cid, g in acct.groupby("cid", sort=False)}
    fill_blocks = {cid: set(g.block_number.astype(int)) for cid, g in grouped.items()}
    g_blocks = {cid: g.block_number.to_numpy(dtype=np.int64) for cid, g in grouped.items()}
    p_blocks = {cid: g.block_number.to_numpy(dtype=np.int64) for cid, g in pgrouped.items()}
    a_blocks = {cid: g.block_number.to_numpy(dtype=np.int64) for cid, g in agrouped.items()}

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
    print(
        f"  after {PREFILTER_NEED}-of-{PREFILTER_WINDOW} {len(alive):,}  "
        f"dropped {n_skip:,}",
        flush=True,
    )
    cap = int(args.max_events)
    if cap > 0 and len(alive) > cap:
        alive = [alive[i] for i in stride_take(len(alive), cap)]
        print(f"  time-strided to {len(alive):,}", flush=True)

    def window_hl(cid: str, lo_b: int, hi_b: int) -> tuple[int, float, float]:
        bn = g_blocks.get(cid)
        if bn is None:
            return 0, 0.0, 1.0
        i0 = int(np.searchsorted(bn, lo_b, side="left"))
        i1 = int(np.searchsorted(bn, hi_b, side="right"))
        if i1 <= i0:
            return 0, 0.0, 1.0
        w = grouped[cid].iloc[i0:i1]
        return 1, float(w["high_yes"].max()), float(w["low_yes"].min())

    events: list[dict] = []
    t_feat = time.time()
    for i, (x, cid) in enumerate(alive, 1):
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
        ag = agrouped.get(cid)
        if ag is None:
            ast = account_stats(pd.DataFrame(columns=["block_number", "account", "usdc"]), x)
        else:
            abn = a_blocks[cid]
            alo = int(np.searchsorted(abn, x - (LOOKBACK_BLOCKS - 1), side="left"))
            ahi = int(np.searchsorted(abn, x, side="right"))
            ast = account_stats(ag.iloc[alo:ahi], x)
        a30, h30, l30 = window_hl(cid, x + 2, x + 30)
        events.append(
            {
                "x_block": x,
                "cid": cid,
                "last": last,
                "tick": tick,
                "feat": feat,
                "fee_market": bool(at["fee_usdc"].fillna(0).max() > 0),
                "imbalance_15": float(feat.get("imbalance_15", 0.0)),
                "y_act_30": a30,
                "h30": h30,
                "l30": l30,
                **ast,
            }
        )
        if i % 20000 == 0:
            print(f"  features {i:,}/{len(alive):,}  {time.time()-t_feat:.1f}s", flush=True)

    print(f"  events {len(events):,}  {time.time()-t0:.1f}s", flush=True)
    if len(events) < 500:
        R._fail(f"too few events: {len(events)}")

    print("  score frozen Stage B (H1/H30/H60) …", flush=True)
    attach_stage_b(events, est_act, est_bar, b_keys)
    print(f"  scored  {time.time()-t0:.1f}s", flush=True)

    # C must not search on the slice that trained B (including H30).
    if b_train_cut is not None:
        floor = int(b_train_cut) + b_embargo
        n_before = len(events)
        events = [e for e in events if e["x_block"] >= floor]
        print(
            f"  drop B-train overlap: {n_before:,} → {len(events):,}  "
            f"(x >= {floor:,})",
            flush=True,
        )
    if len(events) < 500:
        R._fail(
            f"too few events after B-train embargo: {len(events)}. "
            "Collect C on a later window than B's train cut."
        )

    grid = [
        {
            "t_fill": tf,
            "t_liq": tl,
            "t_range": tr,
            "k_need": kn,
            "price": pr,
            "whale": wh,
            "skip_fee": sf,
            "size_mult": sm,
            "exit_end": xe,
        }
        for tf, tl, tr, kn, pr, wh, sf, sm, xe in product(
            T_FILL, T_LIQ, T_RANGE, K_NEEDS, PRICE_MODES, WHALE_MODES, SKIP_FEES, SIZE_MULTS, EXIT_ENDS
        )
        if not (pr == "pay2" and kn <= 2)
    ]

    def _rank_key(item):
        _p, ev = item
        if ev["hard_fail"] or ev["pnl"] is None:
            return (-1e99, -ev["per_hour"])
        return (ev["pnl"], ev["per_hour"])

    if args.week_shift > 0:
        pairs = week_shift_pairs(events, shift_days=args.week_shift)
        print(
            f"  week-shift {args.week_shift}d  pairs {len(pairs)}  grid {len(grid)}",
            flush=True,
        )
        if not pairs:
            R._fail(
                "no D / D+7 pairs after B-train embargo. "
                "C needs multiple weeks of events, not a 4-day window."
            )
        folds = []
        for d, d7, tr, te in pairs:
            tr = sorted(tr, key=lambda e: e["x_block"])
            te = sorted(te, key=lambda e: e["x_block"])
            tr_lo, tr_hi = tr[0]["x_block"], tr[-1]["x_block"]
            te_lo, te_hi = te[0]["x_block"], te[-1]["x_block"]
            print(f"  fold {d.isoformat()} → {d7.isoformat()}  n {len(tr):,}/{len(te):,}", flush=True)
            ranked = []
            for p in grid:
                ev = sequential_eval(
                    tr, params=p, lo=tr_lo, hi=tr_hi, asks_i=asks_i, tbuy_i=tbuy_i, bids_i=bids_i
                )
                ranked.append((p, ev))
            ranked.sort(key=_rank_key, reverse=True)
            ok = [r for r in ranked if not r[1]["hard_fail"] and r[1]["pnl"] is not None and r[1]["pnl"] > 0]
            if not ok:
                folds.append(
                    {
                        "d": d.isoformat(),
                        "d7": d7.isoformat(),
                        "dow": d.strftime("%A"),
                        "n_d": len(tr),
                        "n_d7": len(te),
                        "promoted": None,
                        "d_eval": ranked[0][1] if ranked else None,
                        "d7_eval": None,
                    }
                )
                print("    no candidate on D", flush=True)
                continue
            p, d_ev = ok[0]
            d7_ev = sequential_eval(
                te, params=p, lo=te_lo, hi=te_hi, asks_i=asks_i, tbuy_i=tbuy_i, bids_i=bids_i
            )
            folds.append(
                {
                    "d": d.isoformat(),
                    "d7": d7.isoformat(),
                    "dow": d.strftime("%A"),
                    "n_d": len(tr),
                    "n_d7": len(te),
                    "promoted": p,
                    "d_eval": d_ev,
                    "d7_eval": d7_ev,
                }
            )
            d7_pnl = "hard-fail" if d7_ev["hard_fail"] else f"${d7_ev['pnl']:.2f}"
            print(
                f"    D ${d_ev['pnl']:.2f} {d_ev['per_hour']:.1f}/h → "
                f"D+7 {d7_pnl} {d7_ev['per_hour']:.1f}/h",
                flush=True,
            )
        n_prom = sum(1 for f in folds if f["promoted"] is not None)
        n_ok = sum(
            1
            for f in folds
            if f["d7_eval"]
            and not f["d7_eval"]["hard_fail"]
            and f["d7_eval"]["pnl"] is not None
            and f["d7_eval"]["pnl"] > 0
        )
        (LOG_DIR / "stage_c_weekshift.json").write_text(json.dumps(folds, default=str, indent=2))
        lines = [
            "# Stage C week-shift validation (D → D+7)",
            "",
            f"Generated {started}. Wall {time.time()-t0:.1f}s.",
            f"Train decoder on calendar day D, test on D+{args.week_shift} "
            f"(same weekday). Frozen B including H30. No C-internal H30 fit.",
            f"Folds {len(folds)}. D had a candidate: {n_prom}. "
            f"D+7 printed (>0 and ≥1/h): **{n_ok}**.",
            "",
            "| D | weekday | D+7 | D n | D+7 n | D P&L | D /h | D+7 P&L | D+7 /h |",
            "| --- | --- | --- | --- | --- | --- | --- | --- | --- |",
        ]
        for f in folds:
            de, te = f["d_eval"], f["d7_eval"]
            d_pnl = "—" if not de or de["hard_fail"] else f"{de['pnl']:.2f}"
            d_h = "—" if not de else f"{de['per_hour']:.1f}"
            if te is None:
                t_pnl, t_h = "no D cand", "—"
            elif te["hard_fail"]:
                t_pnl, t_h = "hard-fail", f"{te['per_hour']:.1f}"
            else:
                t_pnl, t_h = f"{te['pnl']:.2f}", f"{te['per_hour']:.1f}"
            lines.append(
                f"| {f['d']} | {f['dow']} | {f['d7']} | {f['n_d']:,} | {f['n_d7']:,} | "
                f"{d_pnl} | {d_h} | {t_pnl} | {t_h} |"
            )
        lines += [
            "",
            f"Success rule: D+7 P&L > 0 and ≥1/hour. **{n_ok}/{len(folds)} folds.**",
            "",
        ]
        SNAP.write_text("\n".join(lines))
        print(f"wrote {SNAP}  {n_ok}/{len(folds)} D+7 printed  {time.time()-t0:.1f}s", flush=True)
        return 0

    xs = sorted({e["x_block"] for e in events})
    span_b = xs[-1] - xs[0]
    emb = min(120, max(0, span_b // 25))
    cut1 = xs[int(len(xs) * 0.55)]
    cut2 = xs[int(len(xs) * 0.78)]
    train = [e for e in events if e["x_block"] <= cut1]
    val = [e for e in events if cut1 + emb <= e["x_block"] <= cut2]
    test = [e for e in events if e["x_block"] >= cut2 + emb]
    tr_lo, tr_hi = train[0]["x_block"], train[-1]["x_block"]
    va_lo, va_hi = (val[0]["x_block"], val[-1]["x_block"]) if val else (0, 0)
    te_lo, te_hi = (test[0]["x_block"], test[-1]["x_block"]) if test else (0, 0)
    print(
        f"  split train {len(train):,} val {len(val):,} test {len(test):,}  "
        f"embargo {emb}",
        flush=True,
    )

    print(f"  decoder grid {len(grid)} on train …", flush=True)
    ranked = []
    for i, p in enumerate(grid, 1):
        ev = sequential_eval(
            train, params=p, lo=tr_lo, hi=tr_hi, asks_i=asks_i, tbuy_i=tbuy_i, bids_i=bids_i
        )
        ranked.append((p, ev))
        if i % 30 == 0:
            print(f"    {i}/{len(grid)}  {time.time()-t0:.1f}s", flush=True)

    def _key(item):
        p, ev = item
        if ev["hard_fail"] or ev["pnl"] is None:
            return (-1e99, -ev["per_hour"])
        return (ev["pnl"], ev["per_hour"])

    ranked.sort(key=_key, reverse=True)
    top = [r for r in ranked if not r[1]["hard_fail"]][:12]
    if not top:
        top = ranked[:5]
    print(f"  val-select among {len(top)} train survivors …", flush=True)
    val_ranked = []
    for p, _tr in top:
        ev = sequential_eval(
            val, params=p, lo=va_lo, hi=va_hi, asks_i=asks_i, tbuy_i=tbuy_i, bids_i=bids_i
        )
        val_ranked.append((p, ev))
    val_ok = [r for r in val_ranked if not r[1]["hard_fail"] and r[1]["pnl"] is not None]
    val_ok.sort(key=lambda r: r[1]["pnl"], reverse=True)
    print("  val top:", flush=True)
    for p, ev in sorted(val_ranked, key=_key, reverse=True)[:8]:
        pnl = "hard-fail" if ev["hard_fail"] else f"{ev['pnl']:.2f}"
        print(
            f"    k={p['k_need']} {p['price']} z={p['exit_end']} "
            f"tf={p['t_fill']} tl={p['t_liq']} tr={p['t_range']} "
            f"/h={ev['per_hour']:.2f} pnl={pnl}",
            flush=True,
        )
    naive_tr = naive_eval(train, lo=tr_lo, hi=tr_hi, asks_i=asks_i, tbuy_i=tbuy_i, bids_i=bids_i)
    naive_va = naive_eval(val, lo=va_lo, hi=va_hi, asks_i=asks_i, tbuy_i=tbuy_i, bids_i=bids_i)
    naive_te = naive_eval(test, lo=te_lo, hi=te_hi, asks_i=asks_i, tbuy_i=tbuy_i, bids_i=bids_i)

    promoted = None
    if val_ok and val_ok[0][1]["pnl"] > 0:
        promoted = val_ok[0]
    test_ev = None
    if promoted is not None:
        test_ev = sequential_eval(
            test,
            params=promoted[0],
            lo=te_lo,
            hi=te_hi,
            asks_i=asks_i,
            tbuy_i=tbuy_i,
            bids_i=bids_i,
        )

    prints = (
        promoted is not None
        and test_ev is not None
        and not test_ev["hard_fail"]
        and test_ev["pnl"] is not None
        and test_ev["pnl"] > 0
        and (naive_te["pnl"] is None or test_ev["pnl"] > naive_te["pnl"])
    )

    logp = LOG_DIR / "stage_c_search.csv"
    with logp.open("w", newline="") as f:
        w = csv.writer(f)
        w.writerow(
            [
                "split",
                "t_fill",
                "t_liq",
                "t_range",
                "k_need",
                "price",
                "exit_end",
                "whale",
                "skip_fee",
                "size_mult",
                "n_part",
                "per_hour",
                "hard_fail",
                "pnl",
                "hit",
                "q_zero",
            ]
        )
        for split_name, pairs in (("train", ranked), ("val", val_ranked)):
            for p, ev in pairs:
                w.writerow(
                    [
                        split_name,
                        p["t_fill"],
                        p["t_liq"],
                        p["t_range"],
                        p["k_need"],
                        p["price"],
                        p["exit_end"],
                        p["whale"],
                        int(p["skip_fee"]),
                        p["size_mult"],
                        ev["n_part"],
                        f"{ev['per_hour']:.4f}",
                        int(ev["hard_fail"]),
                        "" if ev["pnl"] is None else f"{ev['pnl']:.4f}",
                        f"{ev['hit']:.4f}",
                        f"{ev['q_zero']:.4f}",
                    ]
                )

    metrics = {
        "started": started,
        "n_train": len(train),
        "n_val": len(val),
        "n_test": len(test),
        "n_skip_gate": n_skip,
        "promoted": promoted[0] if promoted else None,
        "val": promoted[1] if promoted else None,
        "test": test_ev,
        "naive_val": naive_va,
        "naive_test": naive_te,
        "prints_money": prints,
    }
    (LOG_DIR / "stage_c_metrics.json").write_text(json.dumps(metrics, default=str, indent=2))

    def row(name, ev):
        if ev is None:
            return f"| {name} | — | — | — | — | — |"
        pnl = "hard-fail" if ev["hard_fail"] else f"{ev['pnl']:.2f}"
        return (
            f"| {name} | {ev['n_part']:,} | {ev['per_hour']:.2f} | {pnl} | "
            f"{ev['hit']:.3f} | {ev['q_zero']:.3f} |"
        )

    lines = [
        "# Stage C sequential (frozen Stage B + extras)",
        "",
        f"Generated {started}. Wall {time.time()-t0:.1f}s. "
        f"Trigger {x_lo:,}–{x_hi:,}. Stage A {PREFILTER_NEED}-of-{PREFILTER_WINDOW}. "
        f"Frozen Stage B `{B_JOB.name}` ({len(b_keys)} cols).",
        "",
        "Stage B outputs (`p_act_1`, `p_bar[±k, H60]`) are **inputs**. Extra",
        "last-100-block features Stage B does not emit: unique-account",
        "`top_share` / `hhi` / `n_acct` (window and at `X`), fee bit.",
        "Selective decoder: frozen B `p_act_1` plus a **train-only 30-block**",
        "activity/barrier head. Fire only if FOK-bar, 30-block liquidity, and",
        "a 2/3/4-tick range all clear. Pricing: passive / pay1 / pay2 ticks",
        "on the FOK. GTC window 30 or 60. 1/hour is a floor. Train ranks,",
        "val",
        "promotes only if **≥1 activation/hour** and val P&L **> 0**.",
        "Test never selects. Always-skip is $0. Naive is imbalance-side",
        "`+0 / +1` tick after the same cooldown.",
        "",
        f"Split train {len(train):,}  val {len(val):,}  test {len(test):,}",
        f"(embargo {emb}). {PREFILTER_NEED}-of-{PREFILTER_WINDOW} dropped {n_skip:,}.",
        "",
        "## Headline",
        "",
    ]
    if prints:
        lines.append(
            f"**Prints on test: ${test_ev['pnl']:.2f}** at {test_ev['per_hour']:.2f}/h "
            f"({test_ev['n_part']} tickets, hit {test_ev['hit']:.3f}, "
            f"zero {test_ev['q_zero']:.3f})."
        )
    elif promoted is None:
        lines.append(
            "**No candidate.** Nothing cleared val ≥1/hour **and** val P&L > 0. "
            "Do not quote a test dollar number as a win."
        )
    else:
        lines.append(
            f"**Val promoted, test did not print.** Val P&L "
            f"${promoted[1]['pnl']:.2f} at {promoted[1]['per_hour']:.2f}/h. "
            f"Test P&L "
            + (
                "hard-fail"
                if test_ev["hard_fail"]
                else f"${test_ev['pnl']:.2f}"
            )
            + f" at {test_ev['per_hour']:.2f}/h. Overfit or still red."
        )
    lines += [
        "",
        "Promoted params: "
        + (
            ", ".join(f"{k}={v}" for k, v in promoted[0].items())
            if promoted
            else "(none)"
        ),
        "",
        "| split | n | /hour | P&L $ | FOK hit | leftover $0 share |",
        "| --- | --- | --- | --- | --- | --- |",
        "| always-skip | 0 | 0.00 | 0.00 | — | — |",
        row("naive val", naive_va),
        row("naive test", naive_te),
        row("C val", promoted[1] if promoted else None),
        row("C test", test_ev),
        "",
        "## Train grid leaders (not used to pick if val is red)",
        "",
        "| t_fill | t_liq | t_range | k | price | z | /hour | train P&L |",
        "| --- | --- | --- | --- | --- | --- | --- | --- |",
    ]
    for p, ev in ranked[:12]:
        pnl = "hard-fail" if ev["hard_fail"] else f"{ev['pnl']:.2f}"
        lines.append(
            f"| {p['t_fill']:.2f} | {p['t_liq']:.2f} | {p['t_range']:.2f} | {p['k_need']} | "
            f"{p['price']} | {p['exit_end']} | {ev['per_hour']:.2f} | {pnl} |"
        )
    lines += [
        "",
        "## Val scores of those train survivors",
        "",
        "| t_fill | t_liq | t_range | k | price | z | /hour | val P&L |",
        "| --- | --- | --- | --- | --- | --- | --- | --- |",
    ]
    for p, ev in sorted(val_ranked, key=_key, reverse=True):
        pnl = "hard-fail" if ev["hard_fail"] else f"{ev['pnl']:.2f}"
        lines.append(
            f"| {p['t_fill']:.2f} | {p['t_liq']:.2f} | {p['t_range']:.2f} | {p['k_need']} | "
            f"{p['price']} | {p['exit_end']} | {ev['per_hour']:.2f} | {pnl} |"
        )
    lines += [
        "",
        "Search log: `" + str(logp) + "`.",
        "",
        "Stage B stays frozen. This card is Stage C.",
        "",
    ]
    SNAP.write_text("\n".join(lines))
    print(f"wrote {SNAP}  {time.time()-t0:.1f}s", flush=True)
    if prints:
        print(f"PRINTS  test ${test_ev['pnl']:.2f}  {test_ev['per_hour']:.2f}/h", flush=True)
    elif promoted is None:
        print("NO CANDIDATE on val", flush=True)
    else:
        print(
            f"VAL only  ${promoted[1]['pnl']:.2f}  test "
            + ("hard-fail" if test_ev["hard_fail"] else f"${test_ev['pnl']:.2f}"),
            flush=True,
        )
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
