#!/usr/bin/env python3
"""PRIME → quiet whale → other activity (Stage A), then fade B/C.

PRIME: one taker fills at ≥2 distinct maker prices (no maker-account
cap). Trigger is not X+1. At T we need a PRIME in the last A blocks,
the whale has not bet again on this cid, and a calibrated activity
rule has fired after PRIME.
"""

from __future__ import annotations

import json
import os
import sys
import time
from datetime import datetime, timezone
from pathlib import Path

import duckdb
import numpy as np
from dotenv import load_dotenv

_HERE = Path(__file__).resolve().parent
_ROOT = _HERE.parents[1]
sys.path.insert(0, str(_ROOT))
load_dotenv(_ROOT / ".env")

from lib.partition_utils import PARTITION_10K_SIZE, partition_dir, partition_start  # noqa: E402

FILLS = os.environ["FILLS_V1_DIR"]
SCRATCH = os.environ["SCRATCH_DIR"]
LOOKBACK = 100
A_MAX = 300
H_MAX = 120
HORIZONS = (1, 5, 30, 120)
MIN_SPAN = 2000
N = 5.5
COOLDOWN = 180
A_GRID = (15, 30, 60, 120, 300)
WALK_GRID = (0.01, 0.02, 0.05)
RULES = (
    ("other1", "≥1 other-account fill"),
    ("other3", "≥3 other-account fills"),
    ("acct2", "≥2 distinct other accounts"),
    ("otk1", "≥1 other taker"),
    ("same1", "≥1 other taker same-way as whale"),
    ("opp1", "≥1 other taker against whale"),
    ("tick1", "≥1 other fill ≥1¢ from sweep-end"),
)
NONLEAK = ("other1", "other3", "acct2", "otk1", "same1")
SNAP = _HERE / "snapshot.md"
STAGE_A = _HERE / "STAGE_A.md"
STAGE_B = _HERE / "STAGE_B.md"
STAGE_C = _HERE / "STAGE_C.md"


def _files(root: Path, lo: int, hi: int) -> list[str]:
    out = []
    k = partition_start(lo)
    while k <= hi:
        p = root / partition_dir(k) / "data.parquet"
        if p.exists() and p.stat().st_size > 500:
            out.append(p.as_posix())
        k += PARTITION_10K_SIZE
    return out


def _sql(paths: list[str]) -> str:
    return "[" + ",".join("'" + p.replace("'", "''") + "'" for p in paths) + "]"


def _frontier(root: Path) -> int:
    metas = sorted(root.glob("1M=*/10K=*/metadata.json"))
    last = json.loads(metas[-1].read_text())
    return int((last.get("parameters") or {}).get("max_block") or 0)


def _pct(xs: np.ndarray, q: float) -> float:
    if xs.size == 0:
        return float("nan")
    return float(np.quantile(xs, q))


def _md_float(x: float, nd: int = 3) -> str:
    if x is None or (isinstance(x, float) and not np.isfinite(x)):
        return ""
    return f"{x:.{nd}f}"


def arm_scan(
    blk: np.ndarray,
    lfi: np.ndarray,
    acct: np.ndarray,
    ypx: np.ndarray,
    sh: np.ndarray,
    is_tk: np.ndarray,
    start: int,
    stop: int,
    p_blk: int,
    p_lfi: int,
    taker: int,
    d: float,
    end: float,
) -> dict[str, int | None]:
    """First block after PRIME where each activity rule is true. Whale fills skipped."""
    first: dict[str, int | None] = {k: None for k, _ in RULES}
    first["whale"] = None
    n_other = 0
    accts: set[int] = set()
    i = start
    # skip legs at or before the prime match
    while i < stop and (int(blk[i]) < p_blk or (int(blk[i]) == p_blk and int(lfi[i]) <= p_lfi)):
        i += 1
    lim = p_blk + A_MAX
    while i < stop and int(blk[i]) <= lim:
        b = int(blk[i])
        if int(acct[i]) == taker:
            if first["whale"] is None:
                first["whale"] = b
            i += 1
            continue
        n_other += 1
        accts.add(int(acct[i]))
        if first["other1"] is None:
            first["other1"] = b
        if n_other >= 3 and first["other3"] is None:
            first["other3"] = b
        if len(accts) >= 2 and first["acct2"] is None:
            first["acct2"] = b
        tk = bool(is_tk[i])
        if tk and first["otk1"] is None:
            first["otk1"] = b
        if tk:
            od = 1.0 if float(sh[i]) > 0 else -1.0
            if od == d and first["same1"] is None:
                first["same1"] = b
            if od != d and first["opp1"] is None:
                first["opp1"] = b
        if abs(float(ypx[i]) - end) >= 0.01 and first["tick1"] is None:
            first["tick1"] = b
        i += 1
    return first


def snapshot_at(
    blk, lfi, acct, ypx, sh, is_tk, start, stop,
    p_blk, p_lfi, taker, d, t_blk,
) -> dict:
    """Known-at-T features from fills in (PRIME, T]."""
    n_other = 0
    accts: set[int] = set()
    same_n = 0
    opp_n = 0
    last = None
    i = start
    while i < stop and (int(blk[i]) < p_blk or (int(blk[i]) == p_blk and int(lfi[i]) <= p_lfi)):
        i += 1
    while i < stop and int(blk[i]) <= t_blk:
        if int(acct[i]) == taker:
            i += 1
            continue
        n_other += 1
        accts.add(int(acct[i]))
        last = float(ypx[i])
        if bool(is_tk[i]):
            od = 1.0 if float(sh[i]) > 0 else -1.0
            if od == d:
                same_n += 1
            else:
                opp_n += 1
        i += 1
    return {
        "n_other": n_other,
        "n_acct": len(accts),
        "same_n": same_n,
        "opp_n": opp_n,
        "last": last,
    }


def last_in(blk, ypx, start, stop, lo_blk, hi_blk) -> float | None:
    """Last YES close in (lo_blk, hi_blk]."""
    a = int(np.searchsorted(blk[start:stop], lo_blk, side="right")) + start
    b = int(np.searchsorted(blk[start:stop], hi_blk, side="right")) + start
    if b <= a:
        return None
    return float(ypx[b - 1])


def fok_fade(
    blk, lfi, ypx, sh, is_tk, start, stop, t_blk, d: float, p_cap: float, n: float = N,
) -> tuple[bool, float | None]:
    """FOK at T+1: buy YES if d<0 (whale sold YES), sell YES if d>0.

    Depth = maker legs in T+1 of the side we take (interpretation C).
    """
    a = int(np.searchsorted(blk[start:stop], t_blk + 1, side="left")) + start
    b = int(np.searchsorted(blk[start:stop], t_blk + 1, side="right")) + start
    legs = []
    for i in range(a, b):
        if bool(is_tk[i]):
            continue
        px, q = float(ypx[i]), abs(float(sh[i]))
        maker_buy_yes = float(sh[i]) > 0
        if d > 0:
            # we sell YES → take bids = maker-buy YES at px >= cap
            if maker_buy_yes and px + 1e-12 >= p_cap:
                legs.append((px, q))
        else:
            # we buy YES → take asks = maker-sell YES at px <= cap
            if (not maker_buy_yes) and px - 1e-12 <= p_cap:
                legs.append((px, q))
    if d > 0:
        legs.sort(key=lambda z: -z[0])
    else:
        legs.sort(key=lambda z: z[0])
    need, cost, got = n, 0.0, 0.0
    for px, q in legs:
        take = min(need, q)
        cost += take * px
        got += take
        need -= take
        if need <= 1e-12:
            return True, cost / got
    return False, None


def gtc_fade_fill(
    blk, ypx, sh, is_tk, start, stop, t_blk, h: int, d: float, p_exit: float, n: float = N,
) -> float:
    """GTC close of the fade in T+2..T+h. Returns shares filled.

    Short YES (d>0): we buy back, liquidity = taker-sell YES at ypx <= p_exit.
    Long YES (d<0): we sell, liquidity = taker-buy YES at ypx >= p_exit.
    """
    a = int(np.searchsorted(blk[start:stop], t_blk + 2, side="left")) + start
    b = int(np.searchsorted(blk[start:stop], t_blk + h, side="right")) + start
    got = 0.0
    for i in range(a, b):
        if not bool(is_tk[i]):
            continue
        px, q = float(ypx[i]), abs(float(sh[i]))
        taker_buy_yes = float(sh[i]) > 0
        if d > 0:
            if (not taker_buy_yes) and px - 1e-12 <= p_exit:
                got += q
        else:
            if taker_buy_yes and px + 1e-12 >= p_exit:
                got += q
        if got >= n:
            return n
    return got


def sequential(events: list[dict], cooldown: int = COOLDOWN) -> list[dict]:
    last: dict[int, int] = {}
    out = []
    for e in sorted(events, key=lambda z: (z["t"], z["cid_h"])):
        prev = last.get(e["cid_h"])
        if prev is not None and e["t"] < prev + cooldown:
            continue
        last[e["cid_h"]] = e["t"]
        out.append(e)
    return out


def split70(events: list[dict]) -> tuple[list[dict], list[dict]]:
    if not events:
        return [], []
    xs = sorted(events, key=lambda z: z["t"])
    k = max(1, int(0.7 * len(xs)))
    if k >= len(xs):
        return xs, []
    return xs[:k], xs[k:]


def main() -> int:
    fills_root = Path(FILLS)
    frontier = _frontier(fills_root)
    x_hi = frontier - (A_MAX + H_MAX + 100)
    x_lo = x_hi - int(4 * 86400 / 1.5)
    fill_lo, fill_hi = x_lo - LOOKBACK, x_hi + A_MAX + H_MAX
    print(f"PRIME-quiet-activity  P {x_lo:,}–{x_hi:,}  frontier {frontier:,}", flush=True)
    ff = _files(fills_root, fill_lo, fill_hi)
    con = duckdb.connect()
    con.execute(f"SET temp_directory='{SCRATCH}'")
    con.execute("SET memory_limit='10GB'")
    print(f"  fills files {len(ff)}", flush=True)
    ypx = """
      CASE
        WHEN (gross_usdc > 0) = (net_yes_tokens > 0)
          THEN gross_usdc::DOUBLE / net_yes_tokens::DOUBLE
        ELSE 1.0 + gross_usdc::DOUBLE / net_yes_tokens::DOUBLE
      END
    """
    opx = "abs(gross_usdc)::DOUBLE / abs(net_yes_tokens)::DOUBLE"
    t0 = time.time()
    con.execute(
        f"""
        CREATE TABLE fills AS
        SELECT block_number, logical_fill_index, transaction_index,
               (hash(condition_id) % 9223372036854775783)::BIGINT AS cid_h,
               hex(condition_id) AS cid,
               (hash(account) % 9223372036854775783)::BIGINT AS acct, is_taker, net_yes_tokens, gross_usdc,
               fee_usdc, {ypx} AS ypx, {opx} AS opx,
               net_yes_tokens::DOUBLE / 1e6 AS sh
        FROM read_parquet({_sql(ff)})
        WHERE block_number BETWEEN {fill_lo} AND {fill_hi}
          AND net_yes_tokens <> 0 AND gross_usdc <> 0
        """
    )
    n_fills = con.execute("SELECT count(*) FROM fills").fetchone()[0]
    print(f"  fills {n_fills:,}  {time.time()-t0:.1f}s", flush=True)

    con.execute(
        """
        CREATE TABLE tagged AS
        SELECT *,
          sum(CASE WHEN is_taker THEN 1 ELSE 0 END)
            OVER (
              PARTITION BY block_number, transaction_index
              ORDER BY logical_fill_index
            ) AS match_id
        FROM fills
        """
    )
    con.execute(
        f"""
        CREATE TABLE primes AS
        SELECT
          block_number AS p,
          max(logical_fill_index) AS p_lfi,
          cid_h, any_value(cid) AS cid,
          any_value(acct) FILTER (WHERE is_taker) AS taker,
          count(*) FILTER (WHERE NOT is_taker) AS n_makers,
          count(DISTINCT round(opx, 4)) FILTER (WHERE NOT is_taker) AS n_px,
          count(DISTINCT sign(net_yes_tokens)) FILTER (WHERE NOT is_taker) AS n_signs,
          max(opx) FILTER (WHERE NOT is_taker)
            - min(opx) FILTER (WHERE NOT is_taker) AS walk,
          any_value(net_yes_tokens) FILTER (WHERE is_taker) AS taker_net,
          sum(abs(gross_usdc)) FILTER (WHERE is_taker)::DOUBLE / 1e6 AS taker_usdc,
          max(ypx) FILTER (WHERE NOT is_taker) AS maker_yhi,
          min(ypx) FILTER (WHERE NOT is_taker) AS maker_ylo,
          bool_or(fee_usdc > 0) AS fee
        FROM tagged
        WHERE block_number BETWEEN {x_lo} AND {x_hi}
        GROUP BY block_number, transaction_index, match_id, cid_h
        HAVING count(*) FILTER (WHERE is_taker) = 1
           AND count(DISTINCT cid_h) = 1
           AND count(DISTINCT round(opx, 4)) FILTER (WHERE NOT is_taker) >= 2
           AND max(opx) FILTER (WHERE NOT is_taker)
             - min(opx) FILTER (WHERE NOT is_taker) >= 0.01
        """
    )
    n_pr = con.execute("SELECT count(*) FROM primes").fetchone()[0]
    print(f"  primes (1 taker, ≥2 prices, walk≥1¢) {n_pr:,}", flush=True)

    con.execute(
        """
        CREATE TABLE life AS
        SELECT cid_h, min(block_number) mn, max(block_number) mx,
               max(fee_usdc) > 0 has_fee
        FROM fills GROUP BY 1
        """
    )
    primes = con.execute(
        """
        SELECT pr.p, pr.p_lfi, pr.cid_h, pr.taker, pr.n_makers, pr.n_px, pr.n_signs,
               pr.walk, pr.taker_net, pr.taker_usdc, pr.maker_yhi, pr.maker_ylo,
               pr.fee, l.has_fee, (l.mx - l.mn) AS span
        FROM primes pr
        INNER JOIN life l ON l.cid_h = pr.cid_h
        WHERE pr.n_signs = 1
        ORDER BY pr.cid_h, pr.p, pr.p_lfi
        """
    ).fetchdf()
    print(f"  prime rows {len(primes):,}  {time.time()-t0:.1f}s", flush=True)

    tape = con.execute(
        """
        SELECT f.cid_h, f.block_number AS blk, f.logical_fill_index AS lfi,
               f.acct, f.ypx, f.sh, f.is_taker
        FROM fills f
        WHERE f.cid_h IN (SELECT DISTINCT cid_h FROM primes)
        ORDER BY f.cid_h, blk, lfi
        """
    ).fetchdf()
    print(f"  tape rows {len(tape):,}  {time.time()-t0:.1f}s", flush=True)

    cid_h = tape["cid_h"].to_numpy(dtype=np.int64)
    blk = tape["blk"].to_numpy(dtype=np.int64)
    lfi = tape["lfi"].to_numpy(dtype=np.int64)
    acct = tape["acct"].to_numpy(dtype=np.int64)
    ypx_a = tape["ypx"].to_numpy(dtype=np.float64)
    sh_a = tape["sh"].to_numpy(dtype=np.float64)
    tk_a = tape["is_taker"].to_numpy(dtype=np.bool_)
    breaks = np.flatnonzero(cid_h[1:] != cid_h[:-1]) + 1
    bounds: dict[int, tuple[int, int]] = {}
    s0 = 0
    for b in breaks.tolist():
        bounds[int(cid_h[s0])] = (s0, b)
        s0 = b
    bounds[int(cid_h[s0])] = (s0, len(cid_h))
    del tape

    primed = []
    n_pr_rows = len(primes)
    for i_pr, r in enumerate(primes.itertuples(index=False)):
        if i_pr and i_pr % 100000 == 0:
            print(f"  arm {i_pr:,}/{n_pr_rows:,}  {time.time()-t0:.1f}s", flush=True)
        ch = int(r.cid_h)
        sl = bounds.get(ch)
        if sl is None:
            continue
        d = 1.0 if float(r.taker_net) > 0 else -1.0
        end = float(r.maker_yhi) if d > 0 else float(r.maker_ylo)
        near = float(r.maker_ylo) if d > 0 else float(r.maker_yhi)
        i0 = sl[0] + int(np.searchsorted(blk[sl[0]:sl[1]], int(r.p), side="left"))
        first = arm_scan(
            blk, lfi, acct, ypx_a, sh_a, tk_a,
            i0, sl[1], int(r.p), int(r.p_lfi), int(r.taker), d, end,
        )
        primed.append({
            "p": int(r.p),
            "p_lfi": int(r.p_lfi),
            "cid_h": ch,
            "taker": int(r.taker),
            "n_makers": int(r.n_makers),
            "n_px": int(r.n_px),
            "walk": float(r.walk),
            "usd": float(r.taker_usdc),
            "d": d,
            "end": end,
            "near": near,
            "has_fee": bool(r.has_fee),
            "span": int(r.span),
            "fee_free_long": (not bool(r.has_fee)) and int(r.span) >= MIN_SPAN,
            "first": first,
            "sl": sl,
        })
    print(f"  armed {len(primed):,}  {time.time()-t0:.1f}s", flush=True)

    def make_rec(pr: dict, t: int) -> dict | None:
        snap = snapshot_at(
            blk, lfi, acct, ypx_a, sh_a, tk_a,
            pr["sl"][0], pr["sl"][1],
            pr["p"], pr["p_lfi"], pr["taker"], pr["d"], t,
        )
        L = snap["last"]
        if L is None:
            return None
        rec = {
            **{k: pr[k] for k in (
                "p", "cid_h", "walk", "usd", "d", "end", "near",
                "n_makers", "n_px", "fee_free_long",
            )},
            "t": int(t),
            "delay": int(t) - pr["p"],
            "last": L,
            "n_other": snap["n_other"],
            "n_acct": snap["n_acct"],
            "same_n": snap["same_n"],
            "opp_n": snap["opp_n"],
            "sl": pr["sl"],
        }
        rec["remaining"] = (L - pr["near"]) * pr["d"]
        rec["retrace"] = 0.0 if pr["walk"] <= 1e-9 else ((pr["end"] - L) * pr["d"]) / pr["walk"]
        for h in HORIZONS:
            later = last_in(blk, ypx_a, pr["sl"][0], pr["sl"][1], t, t + h)
            rec[f"later{h}"] = later
            rec[f"fade{h}"] = None if later is None else (L - later) * pr["d"]
        rec["traded30"] = rec["fade30"] is not None
        rec["traded120"] = rec["fade120"] is not None
        return rec

    recs_by_rule: dict[str, list[dict]] = {rid: [] for rid, _ in RULES}
    n_built = 0
    for pr in primed:
        whale = pr["first"]["whale"]
        cache: dict[int, dict | None] = {}
        for rid, _ in RULES:
            t = pr["first"].get(rid)
            if t is None:
                continue
            if whale is not None and whale <= t:
                continue
            if t not in cache:
                cache[t] = make_rec(pr, int(t))
            rec = cache[t]
            if rec is None:
                continue
            recs_by_rule[rid].append(rec)
            n_built += 1
    print(f"  recs {n_built:,}  {time.time()-t0:.1f}s", flush=True)

    def events_for(rule: str, a: int, min_walk: float, fee_free_long: bool) -> list[dict]:
        out = []
        for rec in recs_by_rule[rule]:
            if rec["walk"] < min_walk:
                continue
            if fee_free_long and not rec["fee_free_long"]:
                continue
            if rec["delay"] > a:
                continue
            out.append(rec)
        return sequential(out)

    def summarize(ev: list[dict]) -> dict:
        n = len(ev)
        if n == 0:
            return {"n": 0}
        fd30 = np.array([e["fade30"] for e in ev if e["fade30"] is not None], dtype=float)
        fd1 = np.array([e["fade1"] for e in ev if e["fade1"] is not None], dtype=float)
        fd120 = np.array([e["fade120"] for e in ev if e["fade120"] is not None], dtype=float)
        delay = np.array([e["delay"] for e in ev], dtype=float)
        walk = np.array([e["walk"] for e in ev], dtype=float)
        out = {
            "n": n,
            "n_cid": len({e["cid_h"] for e in ev}),
            "delay_p50": float(np.median(delay)),
            "walk_p50": float(np.median(walk)),
            "n30": int(fd30.size),
            "n1": int(fd1.size),
            "n120": int(fd120.size),
        }
        if fd1.size:
            out["m1"] = float(fd1.mean())
            out["p1"] = float((fd1 > 0).mean())
            out["d1"] = float(N * fd1.mean())
        if fd30.size:
            out["m30"] = float(fd30.mean())
            out["p30"] = float((fd30 > 0).mean())
            out["d30"] = float(N * fd30.mean())
            out["p30_2c"] = float((fd30 >= 0.02).mean())
            out["p10"] = _pct(fd30, 0.1)
            out["p50"] = _pct(fd30, 0.5)
            out["p90"] = _pct(fd30, 0.9)
        if fd120.size:
            out["m120"] = float(fd120.mean())
            out["d120"] = float(N * fd120.mean())
        return out

    days = (x_hi - x_lo) * 1.5 / 86400

    def row_md(s: dict) -> str:
        if s["n"] == 0:
            return "| 0 |  |  |  |  |  |  |  |  |"
        per_h = s["n"] / max(days, 1e-9) / 24
        m30 = f"{100*s['m30']:+.2f}" if "m30" in s else ""
        p30 = f"{100*s['p30']:.1f}%" if "p30" in s else ""
        d30 = f"{s['d30']:+.4f}" if "d30" in s else ""
        p2 = f"{100*s['p30_2c']:.1f}%" if "p30_2c" in s else ""
        n30 = s.get("n30", 0)
        return (
            f"| {s['n']:,} | {per_h:.2f} | {s['n_cid']:,} | {s['delay_p50']:.0f} | "
            f"{n30:,} | {m30} | {p30} | {p2} | {d30} |"
        )

    # ---------- Stage A grid ----------
    a_lines = [
        "# Stage A — PRIME, quiet whale, then activity",
        "",
        f"Generated {datetime.now(timezone.utc).strftime('%Y-%m-%dT%H:%M:%SZ')}.",
        f"PRIME window P {x_lo:,}–{x_hi:,} (~4d). "
        "PRIME = 1 taker, ≥2 distinct maker prices, walk ≥ 1¢, same-sign makers. "
        "No cap on maker accounts. Trigger block **T** is the first block after "
        "PRIME where the activity rule is true, the whale has not filled this "
        f"cid again, and T−P ≤ A. Sequential cooldown {COOLDOWN} blocks/cid.",
        "",
        "Fade is measured **from last(T)**, not from the sweep-end: "
        "`(last(T) − later) × whale_dir` in YES-space. Positive = the shock "
        "gave back after we could have acted. Horizons use the last fill in "
        "(T, T+h]; rows with no post-T print are excluded from means. "
        f"5.5 sh, no fees. ~{days:.2f} calendar days.",
        "",
        f"Raw primes (walk≥1¢, 2+ prices): **{len(primed):,}**. "
        f"Fee-free long (no fee_usdc, span≥{MIN_SPAN}): "
        f"**{sum(1 for p in primed if p['fee_free_long']):,}**.",
        "",
        "Non-leaky activity = someone else traded (count / accounts / taker / "
        "same-way). `opp1` and `tick1` already know the tape moved; they are "
        "diagnostics, not freeze candidates.",
        "",
    ]

    cells: dict[tuple, list[dict]] = {}
    best = None  # (holdout d30, name, events)

    def consider(name, ev, leaky: bool):
        nonlocal best
        if leaky or not ev or not name.startswith("fee-free"):
            return
        tr, ho = split70(ev)
        s_ho = summarize(ho)
        if s_ho["n"] < 80 or "d30" not in s_ho:
            return
        key = (s_ho["d30"], s_ho.get("p30", 0), name)
        if best is None or key > (best[0], best[1], best[2]):
            best = (s_ho["d30"], s_ho.get("p30", 0), name, ev, tr, ho)

    for fee_free_long, univ in ((True, "fee-free long"), (False, "all")):
        a_lines.append(f"## Universe: {univ}")
        a_lines.append("")
        for min_walk in WALK_GRID:
            a_lines.append(f"### min walk {100*min_walk:.0f}¢")
            a_lines.append("")
            a_lines.append(
                "| rule | A | n | /hour | cid | delay p50 | n T+30 | mean fade ¢ | "
                "fade>0 | fade≥2¢ | 5.5sh $ |"
            )
            a_lines.append("|---|---:|---:|---:|---:|---:|---:|---:|---:|---:|---:|")
            for rid, rlabel in RULES:
                for a in A_GRID:
                    ev = events_for(rid, a, min_walk, fee_free_long)
                    cells[(univ, min_walk, rid, a)] = ev
                    s = summarize(ev)
                    leaky = rid not in NONLEAK
                    consider(f"{univ} | walk≥{100*min_walk:.0f}¢ | {rid} A={a}", ev, leaky)
                    extra = row_md(s)
                    a_lines.append(f"| {rid} | {a} " + extra)
            a_lines.append("")

    freeze_name = None
    freeze_ev: list[dict] = []
    freeze_tr: list[dict] = []
    freeze_ho: list[dict] = []
    if best:
        freeze_name, freeze_ev, freeze_tr, freeze_ho = best[2], best[3], best[4], best[5]
        a_lines.append("## Freeze")
        a_lines.append("")
        a_lines.append(
            f"Selected on **holdout** 5.5sh $ at T+30 among non-leaky cells "
            f"with ≥80 holdout events: **{freeze_name}**."
        )
        a_lines.append("")
        st, sh_ = summarize(freeze_tr), summarize(freeze_ho)
        a_lines.append("| split | n | n T+30 | mean fade ¢ | fade>0 | fade≥2¢ | 5.5sh $ |")
        a_lines.append("|---|---:|---:|---:|---:|---:|---:|")
        for lab, s in (("train 70%", st), ("holdout 30%", sh_)):
            m = f"{100*s['m30']:+.2f}" if "m30" in s else ""
            p = f"{100*s['p30']:.1f}%" if "p30" in s else ""
            p2 = f"{100*s['p30_2c']:.1f}%" if "p30_2c" in s else ""
            d = f"{s['d30']:+.4f}" if "d30" in s else ""
            a_lines.append(f"| {lab} | {s['n']:,} | {s.get('n30',0):,} | {m} | {p} | {p2} | {d} |")
        a_lines.append("")
        a_lines.append(
            "If holdout dollars are ~0 with fade>0 ≤ 50%, Stage A found live "
            "post-whale tape, not a fadable dislocation. Stage B/C then run on "
            "this freeze anyway so we can see whether a subset or a fill recipe prints."
        )
        a_lines.append("")
    else:
        # fallback: fee-free long, 2¢, other1, A=60
        freeze_name = "fallback fee-free long | walk≥2¢ | other1 A=60 (no cell met n_holdout)"
        freeze_ev = cells.get(("fee-free long", 0.02, "other1", 60), [])
        freeze_tr, freeze_ho = split70(freeze_ev)
        a_lines.append("## Freeze")
        a_lines.append("")
        a_lines.append(
            f"No non-leaky cell had ≥80 holdout events with a T+30 mark. "
            f"Fallback: **{freeze_name}**, n={len(freeze_ev):,}."
        )
        a_lines.append("")

    STAGE_A.write_text("\n".join(a_lines) + "\n")
    print(f"  wrote {STAGE_A}  freeze={freeze_name} n={len(freeze_ev):,}", flush=True)

    # ---------- Stage B ----------
    b_lines = [
        "# Stage B — is it fadable?",
        "",
        f"Generated {datetime.now(timezone.utc).strftime('%Y-%m-%dT%H:%M:%SZ')}.",
        f"On Stage A freeze: **{freeze_name}**. Features at T only. "
        "Label = last fill in (T, T+30] faded ≥ 2¢ from last(T) in the "
        "anti-whale direction. Train = first 70% of events by T, holdout = last 30%.",
        "",
        "No 67-column HGB. Slices plus one small `HistGradientBoostingClassifier` "
        "if n allows. Promote a slice only if holdout 5.5sh $ beats always-fade "
        "on the same holdout and n_holdout ≥ 40.",
        "",
    ]
    b_keep: list[dict] = list(freeze_ev)
    if not freeze_ev:
        b_lines.append("No Stage A events. Stage B skipped.")
        b_lines.append("")
        STAGE_B.write_text("\n".join(b_lines) + "\n")
    else:
        st, sh_ = summarize(freeze_tr), summarize(freeze_ho)
        b_lines.append(
            f"Always-fade envelope: train n={st['n']:,} T+30 ${st.get('d30', float('nan')):+.4f} "
            f"({100*st.get('p30', float('nan')):.1f}% fade>0); "
            f"holdout n={sh_['n']:,} T+30 ${sh_.get('d30', float('nan')):+.4f} "
            f"({100*sh_.get('p30', float('nan')):.1f}% fade>0)."
        )
        b_lines.append("")
        slices = [
            ("walk≥5¢", lambda e: e["walk"] >= 0.05),
            ("walk≥10¢", lambda e: e["walk"] >= 0.10),
            ("walk 2–10¢", lambda e: 0.02 <= e["walk"] < 0.10),
            ("delay≥5", lambda e: e["delay"] >= 5),
            ("delay≥30", lambda e: e["delay"] >= 30),
            ("retrace<0.2 (still extended)", lambda e: e["retrace"] < 0.20),
            ("retrace 0.1–0.5 (started)", lambda e: 0.10 <= e["retrace"] < 0.50),
            ("remaining≥5¢", lambda e: e["remaining"] >= 0.05),
            ("remaining≥10¢", lambda e: e["remaining"] >= 0.10),
            ("taker≥$25", lambda e: e["usd"] >= 25),
            ("taker≥$100", lambda e: e["usd"] >= 100),
            ("n_other≥3", lambda e: e["n_other"] >= 3),
            ("same_n≥1", lambda e: e["same_n"] >= 1),
            ("opp_n=0", lambda e: e["opp_n"] == 0),
            ("extended + remaining≥5¢", lambda e: e["retrace"] < 0.20 and e["remaining"] >= 0.05),
            ("walk≥5¢ + remaining≥5¢ + retrace<0.3",
             lambda e: e["walk"] >= 0.05 and e["remaining"] >= 0.05 and e["retrace"] < 0.30),
        ]
        b_lines.append("| slice | train n | tr $ | tr fade>0 | ho n | ho $ | ho fade>0 | ho fade≥2¢ |")
        b_lines.append("|---|---:|---:|---:|---:|---:|---:|---:|")
        promoted = None
        ho_base = sh_.get("d30", 0.0)
        for name, fn in slices:
            tr = [e for e in freeze_tr if fn(e)]
            ho = [e for e in freeze_ho if fn(e)]
            str_ = summarize(tr)
            sho = summarize(ho)
            b_lines.append(
                f"| {name} | {str_['n']:,} | {str_.get('d30', float('nan')):+.4f} | "
                f"{100*str_.get('p30', float('nan')):.1f}% | {sho['n']:,} | "
                f"{sho.get('d30', float('nan')):+.4f} | {100*sho.get('p30', float('nan')):.1f}% | "
                f"{100*sho.get('p30_2c', float('nan')):.1f}% |"
            )
            if (
                sho.get("n", 0) >= 40 and "d30" in sho
                and np.isfinite(sho["d30"]) and sho["d30"] > ho_base + 0.005
                and str_.get("d30", -1) > 0
            ):
                if promoted is None or sho["d30"] > promoted[0]:
                    promoted = (sho["d30"], name, fn)
        b_lines.append("")

        # tiny HGB
        hgb_note = "HGB skipped (import or n)."
        try:
            from sklearn.ensemble import HistGradientBoostingClassifier
            from sklearn.metrics import roc_auc_score

            def Xy(ev):
                rows, y = [], []
                for e in ev:
                    if e["fade30"] is None:
                        continue
                    rows.append([
                        e["walk"], e["delay"], np.log10(max(e["usd"], 0.01)),
                        e["n_other"], e["n_acct"], e["retrace"], e["last"],
                        e["n_px"], e["same_n"], e["opp_n"], e["remaining"],
                    ])
                    y.append(1 if e["fade30"] >= 0.02 else 0)
                if not rows:
                    return None, None
                return np.array(rows, dtype=float), np.array(y, dtype=int)

            Xtr, ytr = Xy(freeze_tr)
            Xho, yho = Xy(freeze_ho)
            if Xtr is not None and ytr.size >= 80 and ytr.sum() not in (0, ytr.size) and Xho is not None and yho.size >= 40:
                clf = HistGradientBoostingClassifier(
                    max_depth=3, max_iter=80, learning_rate=0.08, random_state=0,
                )
                clf.fit(Xtr, ytr)
                p_tr = clf.predict_proba(Xtr)[:, 1]
                p_ho = clf.predict_proba(Xho)[:, 1]
                auc_tr = float(roc_auc_score(ytr, p_tr))
                auc_ho = float(roc_auc_score(yho, p_ho)) if yho.sum() not in (0, yho.size) else float("nan")
                # threshold on train $ of selected (need original events aligned)
                tr_lab = [e for e in freeze_tr if e["fade30"] is not None]
                ho_lab = [e for e in freeze_ho if e["fade30"] is not None]
                best_t, best_d = 0.5, -999.0
                for thr in (0.40, 0.50, 0.55, 0.60, 0.65, 0.70):
                    sel = [e for e, p in zip(tr_lab, p_tr) if p >= thr]
                    s = summarize(sel)
                    if s.get("n", 0) >= 30 and s.get("d30", -999) > best_d:
                        best_t, best_d = thr, s["d30"]
                ho_sel = [e for e, p in zip(ho_lab, p_ho) if p >= best_t]
                sho = summarize(ho_sel)
                hgb_note = (
                    f"HGB max_depth=3, label fade30≥2¢. train AUC {auc_tr:.3f}  "
                    f"holdout AUC {auc_ho:.3f}. Train-chosen p≥{best_t:.2f}: "
                    f"holdout n={sho.get('n',0):,}  ${sho.get('d30', float('nan')):+.4f}  "
                    f"fade>0 {100*sho.get('p30', float('nan')):.1f}%."
                )
                if (
                    sho.get("n", 0) >= 40 and sho.get("d30", -999) > ho_base + 0.005
                    and auc_ho == auc_ho and auc_ho >= 0.55
                ):
                    promoted = (sho["d30"], f"HGB p≥{best_t:.2f}", None)
                    b_keep = []
                    # keep function via scores — store selected holdout+train above thr for C
                    keep_set = {(e["cid_h"], e["t"]) for e in [x for x, p in zip(tr_lab, p_tr) if p >= best_t]}
                    keep_set |= {(e["cid_h"], e["t"]) for e in ho_sel}
                    b_keep = [e for e in freeze_ev if (e["cid_h"], e["t"]) in keep_set]
            else:
                hgb_note = "HGB skipped: not enough labeled rows or no label mix."
        except Exception as ex:
            hgb_note = f"HGB skipped: {type(ex).__name__}: {ex}"
        b_lines.append(hgb_note)
        b_lines.append("")
        if promoted and promoted[2] is not None:
            b_lines.append(f"**Promoted slice:** {promoted[1]} (holdout ${promoted[0]:+.4f}).")
            b_keep = [e for e in freeze_ev if promoted[2](e)]
        elif promoted and promoted[2] is None:
            b_lines.append(f"**Promoted:** {promoted[1]} (holdout ${promoted[0]:+.4f}).")
        else:
            b_lines.append(
                "**No slice promoted.** Holdout never beat always-fade by ≥0.5¢/clip "
                "with n≥40. Stage C runs on the full Stage A freeze."
            )
            b_keep = list(freeze_ev)
        b_lines.append("")
        sb = summarize(b_keep)
        b_lines.append(
            f"Stage B keep: n={sb['n']:,}  T+30 n={sb.get('n30',0):,}  "
            f"mean fade {100*sb.get('m30', float('nan')):+.2f}¢  "
            f"${sb.get('d30', float('nan')):+.4f}."
        )
        b_lines.append("")
        STAGE_B.write_text("\n".join(b_lines) + "\n")
    print(f"  wrote {STAGE_B}  B-keep {len(b_keep):,}", flush=True)

    # ---------- Stage C ----------
    c_lines = [
        "# Stage C — how to fade, and does it print?",
        "",
        f"Generated {datetime.now(timezone.utc).strftime('%Y-%m-%dT%H:%M:%SZ')}.",
        f"Entry at **T+1** (not T). Fade = opposite the whale in YES-space. "
        "FOK depth = maker legs in T+1 on the side we take (same interpretation C "
        "as directional-fok). Miss → $0. Hit → 5.5 sh at VWAP, then either dump "
        "at the last print in (T, T+h] or a GTC take-profit at last(T) ± 50% of "
        "remaining walk (partials; leftover dumped at horizon).",
        "",
        f"Universe: Stage B keep (n={len(b_keep):,}) on freeze `{freeze_name}`.",
        "",
    ]
    if not b_keep:
        c_lines.append("No events.")
        STAGE_C.write_text("\n".join(c_lines) + "\n")
    else:
        recipes = [
            ("FOK at last(T), dump T+30", 0.0, 30, None),
            ("FOK 1 tick through, dump T+30", 0.01, 30, None),
            ("FOK 2 ticks through, dump T+30", 0.02, 30, None),
            ("FOK at last, dump T+120", 0.0, 120, None),
            ("FOK at last, GTC 50% remaining, dump 30", 0.0, 30, 0.5),
        ]
        c_lines.append("| recipe | FOK hit | n | mean $ (all) | mean $ |hit| | GTC fill p50 sh |")
        c_lines.append("|---|---:|---:|---:|---:|---:|")
        for name, through, h, gtc_frac in recipes:
            pnls_all = []
            pnls_hit = []
            hits = 0
            gtc_got = []
            for e in b_keep:
                sl0, sl1 = e["sl"]
                L = e["last"]
                d = e["d"]
                if d > 0:
                    cap = L - through  # sell, pay down
                else:
                    cap = L + through
                hit, vwap = fok_fade(
                    blk, lfi, ypx_a, sh_a, tk_a, sl0, sl1, e["t"], d, cap, N,
                )
                if not hit:
                    pnls_all.append(0.0)
                    continue
                hits += 1
                entry = float(vwap)
                later = e.get(f"later{h}")
                if gtc_frac is None:
                    if later is None:
                        pnl = 0.0  # leftover marked flat (no print)
                    else:
                        pnl = N * (entry - later) * d  # short YES profits if later < entry
                    pnls_hit.append(pnl)
                    pnls_all.append(pnl)
                    continue
                rem = max(e["remaining"], 0.0)
                if d > 0:
                    p_exit = entry - gtc_frac * rem  # buy back lower
                else:
                    p_exit = entry + gtc_frac * rem
                filled = gtc_fade_fill(
                    blk, ypx_a, sh_a, tk_a, sl0, sl1, e["t"], h, d, p_exit, N,
                )
                gtc_got.append(filled)
                # filled shares: round-trip at (entry vs p_exit); rest dump at later or 0
                pnl_tp = filled * (entry - p_exit) * d
                rest = N - filled
                if rest > 1e-9 and later is not None:
                    pnl_tp += rest * (entry - later) * d
                pnls_hit.append(pnl_tp)
                pnls_all.append(pnl_tp)
            pa = np.array(pnls_all, dtype=float)
            ph = np.array(pnls_hit, dtype=float) if pnls_hit else np.array([0.0])
            gg = np.array(gtc_got, dtype=float) if gtc_got else np.array([np.nan])
            c_lines.append(
                f"| {name} | {100*hits/max(len(b_keep),1):.1f}% | {len(b_keep):,} | "
                f"{pa.mean():+.4f} | {ph.mean():+.4f} | "
                f"{(float(np.median(gg)) if gtc_got else float('nan')):.2f} |"
            )
        c_lines.append("")
        c_lines.append(
            "FOK hit rate is the print question. After a YES-up sweep the bid "
            "usually sits below last(T); a sell FOK at last often misses unless "
            "we pay through. Paying through is selling closer to fair, so the "
            "fade edge has to live in the remaining walk, not in the 60¢ the "
            "whale already ate."
        )
        c_lines.append("")
        STAGE_C.write_text("\n".join(c_lines) + "\n")
    print(f"  wrote {STAGE_C}", flush=True)

    # ---------- snapshot ----------
    s_a = summarize(freeze_ev)
    s_b = summarize(b_keep)
    snap = [
        "# Large taker fade after activity (not X+1)",
        "",
        f"Generated {datetime.now(timezone.utc).strftime('%Y-%m-%dT%H:%M:%SZ')}.",
        "Stage A waits until **after** the sweep: PRIME in the last A blocks, "
        "whale quiet on this cid, then someone else trades. Stage B asks if "
        "that is fadable. Stage C tries to actually fade at T+1.",
        "",
        f"- Primes (1 taker, 2+ prices, walk≥1¢): **{len(primed):,}**",
        f"- Freeze: **{freeze_name}**",
        f"- Stage A n={s_a['n']:,}  T+30 mean fade {100*s_a.get('m30', float('nan')):+.2f}¢  "
        f"${s_a.get('d30', float('nan')):+.4f}  fade>0 {100*s_a.get('p30', float('nan')):.1f}%",
        f"- Stage B keep n={s_b['n']:,}  T+30 ${s_b.get('d30', float('nan')):+.4f}",
        "",
        "Details: [STAGE_A.md](STAGE_A.md), [STAGE_B.md](STAGE_B.md), [STAGE_C.md](STAGE_C.md).",
        "",
    ]
    SNAP.write_text("\n".join(snap) + "\n")
    print(SNAP.read_text())
    print(f"wrote snapshots  {time.time()-t0:.1f}s", flush=True)
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
