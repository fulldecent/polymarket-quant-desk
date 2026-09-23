#!/usr/bin/env python3
"""Fee-free, long-lived: can we rest YES@last and NO@(1-last-d) as makers
and get BOTH hit in X+1 (or a short window), without 10s-to-resolve crypto?

Writes wide-spread-maker.md
"""

from __future__ import annotations

import os
import sys
import time
from collections import defaultdict
from datetime import datetime, timezone
from pathlib import Path

import duckdb
from dotenv import load_dotenv

_HERE = Path(__file__).resolve().parent
_ROOT = _HERE.parents[1]
sys.path.insert(0, str(_ROOT))
load_dotenv(_ROOT / ".env")

from lib.partition_utils import PARTITION_10K_SIZE, partition_dir, partition_start  # noqa: E402

FILLS = os.environ["FILLS_V1_DIR"]
CBB = os.environ.get("CONDITION_BY_BLOCK_V1_DIR", "")
C10K = os.environ.get("CONDITION_BY_10K_V1_DIR", "")
SCRATCH = os.environ["SCRATCH_DIR"]
D = 0.03  # lock 3c → pair 97c
MIN_SPAN = 2000  # ~50 min at 1.5s; drop 5m factories
RESOLVE_BAN = 200  # ~5 min; not "ten seconds to expiry"
WINDOWS = (1, 5, 30, 120)  # blocks after X


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


def main() -> int:
    fills_root = Path(FILLS)
    metas = sorted(fills_root.glob("1M=*/10K=*/metadata.json"))
    import json

    frontier = int((json.loads(metas[-1].read_text()).get("parameters") or {}).get("max_block") or 0)
    # 4 calendar days of triggers, leave 2h of path
    x_hi = frontier - 5000
    x_lo = x_hi - int(4 * 86400 / 1.5)
    fill_lo, fill_hi = x_lo - 100, x_hi + 2000
    print(f"wide-spread maker  X {x_lo:,}–{x_hi:,}  frontier {frontier:,}", flush=True)

    ff = _files(fills_root, fill_lo, fill_hi)
    con = duckdb.connect()
    con.execute(f"SET temp_directory='{SCRATCH}'")
    con.execute("SET memory_limit='10GB'")
    print(f"  fills files {len(ff)}", flush=True)
    con.execute(
        f"""
        CREATE TABLE fills AS
        SELECT block_number, hex(condition_id) cid, is_taker,
               net_yes_tokens, gross_usdc, fee_usdc
        FROM read_parquet({_sql(ff)})
        WHERE block_number BETWEEN {fill_lo} AND {fill_hi}
          AND net_yes_tokens <> 0 AND gross_usdc <> 0
        """
    )
    con.execute(
        """
        CREATE TABLE life AS
        SELECT cid, min(block_number) mn, max(block_number) mx,
               max(fee_usdc) > 0 has_fee
        FROM fills GROUP BY 1
        """
    )
    con.execute(
        f"""
        CREATE TABLE keep AS
        SELECT cid FROM life
        WHERE (mx - mn) >= {MIN_SPAN} AND NOT has_fee
        """
    )
    n_keep = con.execute("SELECT count(*) FROM keep").fetchone()[0]
    print(f"  fee-free long-lived cids {n_keep:,}", flush=True)

    # BBO from maker prints
    con.execute(
        """
        CREATE TABLE bbo AS
        SELECT
            f.block_number,
            f.cid,
            min(CASE WHEN NOT f.is_taker AND f.gross_usdc < 0 AND f.net_yes_tokens < 0
                THEN (-f.gross_usdc)::DOUBLE / abs(f.net_yes_tokens) END) AS ask_yes,
            max(CASE WHEN NOT f.is_taker AND f.gross_usdc > 0 AND f.net_yes_tokens > 0
                THEN (f.gross_usdc)::DOUBLE / abs(f.net_yes_tokens) END) AS bid_yes,
            min(CASE WHEN NOT f.is_taker AND f.gross_usdc < 0 AND f.net_yes_tokens > 0
                THEN (-f.gross_usdc)::DOUBLE / abs(f.net_yes_tokens) END) AS ask_no,
            max(CASE WHEN NOT f.is_taker AND f.gross_usdc > 0 AND f.net_yes_tokens < 0
                THEN (f.gross_usdc)::DOUBLE / abs(f.net_yes_tokens) END) AS bid_no,
            arg_max(
                CASE WHEN (f.gross_usdc > 0) = (f.net_yes_tokens > 0)
                     THEN f.gross_usdc::DOUBLE / f.net_yes_tokens
                     ELSE 1.0 + f.gross_usdc::DOUBLE / f.net_yes_tokens END,
                f.block_number
            ) AS last_yes
        FROM fills f
        INNER JOIN keep k ON k.cid = f.cid
        WHERE f.block_number BETWEEN {x_lo} AND {x_hi}
        GROUP BY 1, 2
        """.replace("{x_lo}", str(x_lo)).replace("{x_hi}", str(x_hi))
    )
    # last_yes via proper arg_max needs a fill index; use avg of implied yes as proxy
    con.execute(
        f"""
        CREATE TABLE lastp AS
        SELECT block_number, cid,
               avg(CASE WHEN (gross_usdc > 0) = (net_yes_tokens > 0)
                        THEN gross_usdc::DOUBLE / net_yes_tokens
                        ELSE 1.0 + gross_usdc::DOUBLE / net_yes_tokens END) AS last_yes
        FROM fills
        WHERE block_number BETWEEN {x_lo} AND {x_hi}
        GROUP BY 1, 2
        """
    )
    # sell prints (hit our bid): maker sells outcome
    con.execute(
        f"""
        CREATE TABLE sells AS
        SELECT block_number, cid,
               min(CASE WHEN NOT is_taker AND gross_usdc < 0 AND net_yes_tokens < 0
                        THEN (-gross_usdc)::DOUBLE / abs(net_yes_tokens) END) AS sell_yes_px,
               min(CASE WHEN NOT is_taker AND gross_usdc < 0 AND net_yes_tokens > 0
                        THEN (-gross_usdc)::DOUBLE / abs(net_yes_tokens) END) AS sell_no_px,
               bool_or(NOT is_taker AND gross_usdc < 0 AND net_yes_tokens < 0) AS any_sell_yes,
               bool_or(NOT is_taker AND gross_usdc < 0 AND net_yes_tokens > 0) AS any_sell_no
        FROM fills
        WHERE block_number BETWEEN {x_lo + 1} AND {x_hi + max(WINDOWS)}
        GROUP BY 1, 2
        """
    )
    resolved = "SELECT NULL::VARCHAR cid, NULL::INTEGER resolved_block WHERE 1=0"
    c10 = Path(C10K) if C10K else None
    if c10 and c10.exists():
        rf = _files(c10, fill_lo, fill_hi)
        if rf:
            con.execute(
                f"""
                CREATE TABLE res AS
                SELECT hex(condition_id) cid, min(resolved_block) resolved_block
                FROM read_parquet({_sql(rf)})
                WHERE resolved_block IS NOT NULL
                GROUP BY 1
                """
            )
            resolved = "SELECT cid, resolved_block FROM res"

    t0 = time.time()
    df = con.execute(
        f"""
        SELECT b.block_number x, b.cid,
               b.ask_yes, b.bid_yes, b.ask_no, b.bid_no,
               l.last_yes,
               r.resolved_block
        FROM bbo b
        INNER JOIN lastp l ON l.block_number = b.block_number AND l.cid = b.cid
        LEFT JOIN ({resolved}) r ON r.cid = b.cid
        WHERE l.last_yes BETWEEN 0.15 AND 0.85
        ORDER BY b.cid, b.block_number
        """
    ).fetchdf()
    print(f"  bbo rows {len(df):,}  {time.time()-t0:.1f}s", flush=True)

    sells = con.execute(
        "SELECT block_number, cid, sell_yes_px, sell_no_px, any_sell_yes, any_sell_no FROM sells"
    ).fetchdf()
    sell_map = {}
    for row in sells.itertuples(index=False):
        sell_map[(int(row.block_number), row.cid)] = row

    def _num(v):
        try:
            if v is None or v != v:
                return None
            return float(v)
        except (TypeError, ValueError):
            return None

    # Carry BBO forward per cid (stale quotes up to 200 blocks).
    n = n_maker = n_wide = 0
    hits = {w: 0 for w in WINDOWS}
    hits_yes = {w: 0 for w in WINDOWS}
    hits_no = {w: 0 for w in WINDOWS}
    wide_hits = {w: 0 for w in WINDOWS}
    imminent = 0
    stale: dict[str, dict] = {}
    TICK = 0.01
    STALE = 200

    for row in df.itertuples(index=False):
        n += 1
        cid = row.cid
        x = int(row.x)
        q = stale.get(cid) or {}
        for k, raw in (
            ("ask_yes", row.ask_yes),
            ("bid_yes", row.bid_yes),
            ("ask_no", row.ask_no),
            ("bid_no", row.bid_no),
        ):
            v = _num(raw)
            if v is not None:
                q[k] = v
                q[k + "_blk"] = x
        q["last"] = float(row.last_yes)
        stale[cid] = q
        last = q["last"]
        ask_y, ask_n = q.get("ask_yes"), q.get("ask_no")
        bid_y, bid_n = q.get("bid_yes"), q.get("bid_no")
        if ask_y is None or ask_n is None:
            continue
        if x - q.get("ask_yes_blk", x) > STALE or x - q.get("ask_no_blk", x) > STALE:
            continue
        try:
            rb = row.resolved_block
            if rb is not None and int(rb) - x <= RESOLVE_BAN:
                imminent += 1
                continue
        except (TypeError, ValueError):
            pass
        # Inside both spreads, sum to 0.97. Need room under the asks and at/above bids.
        if bid_y is None or bid_n is None:
            continue
        if bid_y + bid_n > 0.97 - 1e-12:
            continue
        # sit 1 tick inside each ask, then shave so the pair is 0.97
        py = ask_y - TICK
        pn = ask_n - TICK
        if py + pn > 0.97:
            extra = py + pn - 0.97
            py -= extra / 2
            pn -= extra / 2
        if py <= bid_y or pn <= bid_n or py <= 0.01 or pn <= 0.01:
            continue
        if py >= ask_y or pn >= ask_n:
            continue
        n_maker += 1
        is_wide = False
        if bid_y is not None and bid_n is not None:
            is_wide = (ask_y - bid_y) >= 0.05 and (ask_n - bid_n) >= 0.05
        if is_wide:
            n_wide += 1
        for w in WINDOWS:
            hy = hn = False
            for dt in range(1, w + 1):
                s = sell_map.get((x + dt, cid))
                if s is None:
                    continue
                sy, sn = _num(s.sell_yes_px), _num(s.sell_no_px)
                if s.any_sell_yes and sy is not None and sy <= py + 1e-12:
                    hy = True
                if s.any_sell_no and sn is not None and sn <= pn + 1e-12:
                    hn = True
                if hy and hn:
                    break
            if hy:
                hits_yes[w] += 1
            if hn:
                hits_no[w] += 1
            if hy and hn:
                hits[w] += 1
                if is_wide:
                    wide_hits[w] += 1

    def pct(a, b):
        return f"{a:,} ({100.0 * a / b:.2f}%)" if b else "0"

    lines = [
        "# Wide-spread maker pair (YES@last + NO@(1-last-3¢) ≤ 97¢)",
        "",
        f"Generated {datetime.now(timezone.utc).strftime('%Y-%m-%dT%H:%M:%SZ')}.",
        f"Trigger X {x_lo:,}–{x_hi:,}. Fee-free, cid span ≥ {MIN_SPAN} blocks "
        f"(~{MIN_SPAN * 1.5 / 60:.0f} min). Skip resolve within {RESOLVE_BAN} blocks.",
        "Maker: quotes strictly below both asks (do not take). Fill = maker-**sell** "
        "print at or below our bid on that outcome.",
        "",
        f"BBO rows (last in [0.15, 0.85]): **{n:,}**",
        f"Imminent-resolve skipped: {imminent:,}",
        f"Can rest 1 tick inside both asks with sum ≤ 97¢: **{n_maker:,}**",
        f"Of those, both quoted spreads ≥ 5¢: **{n_wide:,}**",
        "",
        "| window | both hit | YES hit | NO hit | both | wide-book both |",
        "|---|---:|---:|---:|---:|---:|",
    ]
    for w in WINDOWS:
        den = n_maker or 1
        lines.append(
            f"| X+1..X+{w} | {pct(hits[w], n_maker)} | {pct(hits_yes[w], n_maker)} | "
            f"{pct(hits_no[w], n_maker)} | {hits[w]:,} | {wide_hits[w]:,} / {n_wide:,} |"
        )
    lines += [
        "",
        "Both-hit in **one block (X+1)** is the user's ask. "
        "If that rate is tiny, GTC pair is a longer rest, not a next-block lock.",
        "",
    ]
    out = _HERE / "wide-spread-maker.md"
    out.write_text("\n".join(lines))
    print(out.read_text())
    print(f"wrote {out}", flush=True)
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
