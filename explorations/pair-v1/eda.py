#!/usr/bin/env python3
"""First-look EDA for complementary YES+NO market making.

Reads landed parquet only. Writes eda-snapshot.md next to this file.
A dirty git tree is fine.
"""

from __future__ import annotations

import argparse
import json
import os
import sys
import time
from datetime import datetime, timezone
from pathlib import Path

import duckdb
from dotenv import load_dotenv

_PROJECT_ROOT = Path(__file__).resolve().parents[2]
load_dotenv(_PROJECT_ROOT / ".env")

FILLS_DIR = os.environ.get("FILLS_V1_DIR", "")
C10K_DIR = os.environ.get("CONDITION_BY_10K_V1_DIR", "")
CBB_DIR = os.environ.get("CONDITION_BY_BLOCK_V1_DIR", "")
SCRATCH = os.environ.get("SCRATCH_DIR", "")
OUT_PATH = Path(__file__).resolve().parent / "eda-snapshot.md"

# Floor for a signed buy: N ≥ 5 shares and N × P_worst ≥ $1.20.
MIN_SHARES = 5_000_000       # 5 shares, 6 decimals
MIN_WORST_USDC = 1_200_000   # $1.20 of worst-price notional
KELLY_SIZE_RATIO = 4
GRACE_BLOCKS = 15
COMPLEMENT_BLOCKS = 30
BLOCK_SEC = 2.1

# SQL predicate: one outcome has enough consumed depth to sign the floor.
_YES_TAKER_FLOOR = (
    f"coalesce(yes_taker_buy_tokens, 0) >= {MIN_SHARES} "
    f"AND coalesce(yes_taker_buy_usdc, 0) >= {MIN_WORST_USDC}"
)
_NO_TAKER_FLOOR = (
    f"coalesce(no_taker_buy_tokens, 0) >= {MIN_SHARES} "
    f"AND coalesce(no_taker_buy_usdc, 0) >= {MIN_WORST_USDC}"
)
_YES_ASK_FLOOR = (
    f"coalesce(yes_maker_sell_tokens, 0) >= {MIN_SHARES} "
    f"AND coalesce(yes_maker_sell_usdc, 0) >= {MIN_WORST_USDC}"
)
_NO_ASK_FLOOR = (
    f"coalesce(no_maker_sell_tokens, 0) >= {MIN_SHARES} "
    f"AND coalesce(no_maker_sell_usdc, 0) >= {MIN_WORST_USDC}"
)


def _fail(msg: str) -> None:
    sys.exit(msg)


def _connect() -> duckdb.DuckDBPyConnection:
    if not SCRATCH:
        _fail("SCRATCH_DIR is not set. Add it to .env.")
    Path(SCRATCH).mkdir(parents=True, exist_ok=True)
    con = duckdb.connect()
    con.execute(f"SET temp_directory = '{SCRATCH}'")
    con.execute("SET memory_limit = '10GB'")
    con.execute("SET threads = 4")
    return con


def _parquet_files(root: Path) -> list[Path]:
    return sorted(root.glob("1M=*/10K=*/data.parquet"))


def _load_metadata(root: Path) -> list[dict]:
    rows = []
    for p in sorted(root.glob("1M=*/10K=*/metadata.json")):
        meta = json.loads(p.read_text())
        params = meta.get("parameters") or {}
        rows.append(
            {
                "path": str(p.parent / "data.parquet"),
                "row_count": int(meta.get("row_count") or 0),
                "file_size_bytes": int(meta.get("file_size_bytes") or 0),
                "min_block": int(params.get("min_block") or 0),
                "max_block": int(params.get("max_block") or 0),
            }
        )
    return rows


def _fmt_int(n: object) -> str:
    if n is None:
        return "—"
    return f"{int(n):,}"


def _fmt_usd(n: object, digits: int = 2) -> str:
    if n is None:
        return "—"
    return f"${float(n):,.{digits}f}"


def _fmt_pct(n: object, digits: int = 1) -> str:
    if n is None:
        return "—"
    return f"{100.0 * float(n):.{digits}f}%"


def _fmt_num(n: object, digits: int = 3) -> str:
    if n is None:
        return "—"
    return f"{float(n):,.{digits}f}"


def _md_table(headers: list[str], rows: list[list[str]]) -> str:
    out = ["| " + " | ".join(headers) + " |", "| " + " | ".join("---" for _ in headers) + " |"]
    for row in rows:
        out.append("| " + " | ".join(row) + " |")
    return "\n".join(out)


def _sql_list(paths: list[str]) -> str:
    return "[" + ", ".join("'" + p.replace("'", "''") + "'" for p in paths) + "]"


def inventory_section(fills_meta: list[dict], c10k_meta: list[dict], cbb_meta: list[dict]) -> str:
    def summarize(name: str, meta: list[dict]) -> list[str]:
        rows = sum(m["row_count"] for m in meta)
        nbytes = sum(m["file_size_bytes"] for m in meta)
        nonzero = sum(1 for m in meta if m["row_count"] > 0)
        min_b = min((m["min_block"] for m in meta if m["min_block"]), default=0)
        max_b = max((m["max_block"] for m in meta if m["max_block"]), default=0)
        span_days = (max_b - min_b + 1) * BLOCK_SEC / 86400.0 if max_b > min_b else 0
        return [
            name,
            _fmt_int(len(meta)),
            _fmt_int(nonzero),
            _fmt_int(rows),
            f"{nbytes / 1e9:.2f} GB",
            _fmt_int(min_b),
            _fmt_int(max_b),
            f"{span_days:.0f} d",
        ]

    lines = [
        "## Data inventory",
        "",
        "From partition `metadata.json` files. No parquet scan.",
        "",
        _md_table(
            [
                "dataset",
                "partitions",
                "non-empty",
                "rows",
                "parquet",
                "first block",
                "last block",
                "span",
            ],
            [
                summarize("fills_v1", fills_meta),
                summarize("condition_by_block_v1", cbb_meta),
                summarize("condition_by_10k_v1", c10k_meta),
            ],
        ),
        "",
        f"Polygon ~{BLOCK_SEC}s/block. Last fills partition "
        f"{fills_meta[-1]['min_block']:,}–{fills_meta[-1]['max_block']:,} "
        f"({fills_meta[-1]['row_count']:,} legs).",
        "",
    ]
    return "\n".join(lines)


def universe_section(con: duckdb.DuckDBPyConnection) -> str:
    glob = str(Path(C10K_DIR) / "1M=*" / "10K=*" / "data.parquet")
    print("  scanning condition_by_10k_v1 …", flush=True)
    t0 = time.time()
    uni = con.execute(
        f"""
        WITH src AS (
            SELECT
                condition_id,
                fill_count,
                matched_usdc,
                fee_usdc,
                resolved_block,
                payout_numerators,
                "10K" AS k
            FROM read_parquet('{glob}', hive_partitioning=true)
        ),
        per_cond AS (
            SELECT
                condition_id,
                sum(fill_count) AS fill_count,
                sum(matched_usdc) AS matched_usdc,
                sum(fee_usdc) AS fee_usdc,
                min(CASE WHEN fill_count > 0 THEN k END) AS first_trade_k,
                max(CASE WHEN fill_count > 0 THEN k END) AS last_trade_k,
                min(CASE WHEN resolved_block IS NOT NULL THEN k END) AS resolved_k,
                max(payout_numerators) FILTER (WHERE resolved_block IS NOT NULL) AS payout
            FROM src
            GROUP BY 1
        )
        SELECT
            count(*) AS conditions,
            sum(CASE WHEN fill_count > 0 THEN 1 ELSE 0 END) AS traded,
            sum(CASE WHEN resolved_k IS NOT NULL THEN 1 ELSE 0 END) AS resolved,
            sum(CASE WHEN resolved_k IS NOT NULL AND fill_count > 0 THEN 1 ELSE 0 END) AS traded_and_resolved,
            sum(CASE WHEN replace(payout, ' ', '') = '["1","0"]' THEN 1 ELSE 0 END) AS yes_wins,
            sum(CASE WHEN replace(payout, ' ', '') = '["0","1"]' THEN 1 ELSE 0 END) AS no_wins,
            sum(CASE WHEN replace(payout, ' ', '') = '["1","1"]' THEN 1 ELSE 0 END) AS voids,
            sum(matched_usdc) / 1e6 AS matched_usdc,
            sum(fee_usdc) / 1e6 AS fee_usdc,
            sum(CASE WHEN fee_usdc > 0 THEN 1 ELSE 0 END) AS fee_conditions,
            sum(CASE WHEN fee_usdc > 0 THEN matched_usdc ELSE 0 END) / 1e6 AS fee_matched_usdc,
            quantile_cont(matched_usdc / 1e6, 0.50) FILTER (WHERE fill_count > 0) AS p50_usdc,
            quantile_cont(matched_usdc / 1e6, 0.90) FILTER (WHERE fill_count > 0) AS p90_usdc,
            quantile_cont(matched_usdc / 1e6, 0.99) FILTER (WHERE fill_count > 0) AS p99_usdc
        FROM per_cond
        """
    ).fetchone()
    silent = con.execute(
        f"""
        WITH src AS (
            SELECT
                condition_id,
                fill_count,
                resolved_block,
                "10K" AS k
            FROM read_parquet('{glob}', hive_partitioning=true)
        ),
        per_cond AS (
            SELECT
                condition_id,
                max(CASE WHEN fill_count > 0 THEN k END) AS last_trade_k,
                min(CASE WHEN resolved_block IS NOT NULL THEN k END) AS resolved_k
            FROM src
            GROUP BY 1
        )
        SELECT
            count(*) FILTER (WHERE resolved_k IS NOT NULL AND last_trade_k IS NOT NULL) AS n,
            quantile_cont((resolved_k - last_trade_k) / 10000.0, 0.50)
                FILTER (WHERE resolved_k IS NOT NULL AND last_trade_k IS NOT NULL) AS p50_part,
            quantile_cont((resolved_k - last_trade_k) / 10000.0, 0.90)
                FILTER (WHERE resolved_k IS NOT NULL AND last_trade_k IS NOT NULL) AS p90_part,
            sum(CASE WHEN resolved_k IS NOT NULL AND last_trade_k IS NOT NULL
                      AND resolved_k > last_trade_k + 10000 THEN 1 ELSE 0 END) AS silent_gt_1_part,
            sum(CASE WHEN resolved_k IS NOT NULL AND last_trade_k IS NOT NULL
                      AND resolved_k > last_trade_k + 100000 THEN 1 ELSE 0 END) AS silent_gt_10_part
        FROM per_cond
        """
    ).fetchone()
    dt = time.time() - t0
    print(f"  condition_by_10k_v1 done in {dt:.1f}s", flush=True)

    n_cond, traded, resolved, both, yes_w, no_w, voids, m_usdc, f_usdc, n_fee, fee_m, p50, p90, p99 = uni
    lines = [
        "## Universe (`condition_by_10k_v1`, all partitions)",
        "",
        f"Scan time {dt:.1f}s.",
        "",
        _md_table(
            ["metric", "value"],
            [
                ["distinct conditions", _fmt_int(n_cond)],
                ["ever traded", _fmt_int(traded)],
                ["resolved (in-sample)", _fmt_int(resolved)],
                ["traded and resolved", _fmt_int(both)],
                ["YES wins `['1','0']` (slot 0)", _fmt_int(yes_w)],
                ["NO wins `['0','1']` (slot 1)", _fmt_int(no_w)],
                ["void/draw `['1','1']`", _fmt_int(voids)],
                ["lifetime matched USDC", _fmt_usd(m_usdc, 0)],
                ["lifetime fee_usdc (sell-side net)", _fmt_usd(f_usdc, 0)],
                ["conditions with any fee", _fmt_int(n_fee)],
                ["matched USDC on fee conditions", _fmt_usd(fee_m, 0)],
                ["per-condition matched USDC p50 / p90 / p99",
                 f"{_fmt_usd(p50, 0)} / {_fmt_usd(p90, 0)} / {_fmt_usd(p99, 0)}"],
            ],
        ),
        "",
        "Silence between last 10K-with-fills and the resolution 10K "
        "(1 partition ≈ 5.8 hours). This is the hold-to-resolution tail.",
        "",
        _md_table(
            ["metric", "value"],
            [
                ["resolved conditions with at least one fill", _fmt_int(silent[0])],
                ["last-trade → resolve gap p50 (partitions)", _fmt_num(silent[1], 2)],
                ["last-trade → resolve gap p90 (partitions)", _fmt_num(silent[2], 2)],
                ["silent > 1 partition (~6h)", _fmt_int(silent[3])],
                ["silent > 10 partitions (~2.4d)", _fmt_int(silent[4])],
            ],
        ),
        "",
        "Fee share of volume is the fraction of matched USDC on conditions "
        "that ever printed a USDC fee. Buy-side token fees are not in `fee_usdc`.",
        "",
    ]
    if m_usdc:
        lines.insert(
            -2,
            f"Fee-condition volume share: {_fmt_pct((fee_m or 0) / m_usdc)} of matched USDC.",
        )
        lines.insert(-2, "")
    return "\n".join(lines)


def _register_window(con: duckdb.DuckDBPyConnection, paths: list[str], name: str) -> None:
    con.execute(
        f"""
        CREATE OR REPLACE VIEW {name} AS
        SELECT
            block_number,
            logical_fill_index,
            is_taker,
            net_yes_tokens,
            gross_usdc,
            fee_usdc,
            condition_id,
            market_id
        FROM read_parquet({_sql_list(paths)})
        """
    )


def window_section(
    con: duckdb.DuckDBPyConnection,
    paths: list[str],
    title: str,
) -> str:
    print(f"  scanning {len(paths)} fills partitions ({title}) …", flush=True)
    t0 = time.time()
    _register_window(con, paths, "fills_w")

    overview = con.execute(
        """
        SELECT
            count(*) AS legs,
            count(DISTINCT condition_id) AS conditions,
            min(block_number) AS min_block,
            max(block_number) AS max_block,
            sum(abs(gross_usdc)) / 1e6 AS abs_usdc_legs,
            sum(abs(gross_usdc)) FILTER (WHERE is_taker) / 1e6 AS taker_abs_usdc,
            sum(fee_usdc) / 1e6 AS fee_usdc,
            sum(CASE WHEN fee_usdc > 0 THEN 1 ELSE 0 END) AS fee_legs,
            sum(CASE WHEN is_taker THEN 1 ELSE 0 END) AS taker_legs,
            sum(CASE WHEN market_id IS NOT NULL THEN 1 ELSE 0 END) AS negrisk_legs,
            quantile_cont(abs(gross_usdc) / 1e6, 0.50) AS p50,
            quantile_cont(abs(gross_usdc) / 1e6, 0.90) AS p90,
            quantile_cont(abs(gross_usdc) / 1e6, 0.99) AS p99,
            avg(abs(gross_usdc) / 1e6) AS mean_abs
        FROM fills_w
        """
    ).fetchone()

    sides = con.execute(
        """
        SELECT
            CASE
                WHEN gross_usdc > 0 AND net_yes_tokens > 0 THEN 'buy_yes'
                WHEN gross_usdc > 0 AND net_yes_tokens < 0 THEN 'buy_no'
                WHEN gross_usdc < 0 AND net_yes_tokens < 0 THEN 'sell_yes'
                WHEN gross_usdc < 0 AND net_yes_tokens > 0 THEN 'sell_no'
                ELSE 'other'
            END AS side,
            count(*) AS legs,
            sum(abs(gross_usdc)) / 1e6 AS abs_usdc,
            sum(CASE WHEN is_taker THEN 1 ELSE 0 END) AS taker_legs,
            sum(abs(gross_usdc)) FILTER (WHERE is_taker) / 1e6 AS taker_usdc
        FROM fills_w
        GROUP BY 1
        ORDER BY 1
        """
    ).fetchall()

    # Per (block, condition) consumed depth. A floor opportunity is
    # ≥ 5 shares AND ≥ $1.20 USDC on that outcome (proxy for N × P_worst).
    con.execute(
        """
        CREATE OR REPLACE TABLE bars AS
        SELECT
            block_number,
            condition_id,
            max(CASE WHEN fee_usdc > 0 THEN 1 ELSE 0 END) AS any_fee,
            sum(net_yes_tokens) FILTER (
                WHERE is_taker AND gross_usdc > 0 AND net_yes_tokens > 0
            ) AS yes_taker_buy_tokens,
            sum(gross_usdc) FILTER (
                WHERE is_taker AND gross_usdc > 0 AND net_yes_tokens > 0
            ) AS yes_taker_buy_usdc,
            sum(-net_yes_tokens) FILTER (
                WHERE is_taker AND gross_usdc > 0 AND net_yes_tokens < 0
            ) AS no_taker_buy_tokens,
            sum(gross_usdc) FILTER (
                WHERE is_taker AND gross_usdc > 0 AND net_yes_tokens < 0
            ) AS no_taker_buy_usdc,
            sum(-net_yes_tokens) FILTER (
                WHERE NOT is_taker AND gross_usdc < 0 AND net_yes_tokens < 0
            ) AS yes_maker_sell_tokens,
            sum(-gross_usdc) FILTER (
                WHERE NOT is_taker AND gross_usdc < 0 AND net_yes_tokens < 0
            ) AS yes_maker_sell_usdc,
            sum(net_yes_tokens) FILTER (
                WHERE NOT is_taker AND gross_usdc < 0 AND net_yes_tokens > 0
            ) AS no_maker_sell_tokens,
            sum(-gross_usdc) FILTER (
                WHERE NOT is_taker AND gross_usdc < 0 AND net_yes_tokens > 0
            ) AS no_maker_sell_usdc,
            sum(gross_usdc) FILTER (
                WHERE is_taker AND gross_usdc > 0 AND net_yes_tokens > 0
            )::DOUBLE /
            nullif(sum(net_yes_tokens) FILTER (
                WHERE is_taker AND gross_usdc > 0 AND net_yes_tokens > 0
            ), 0)::DOUBLE AS yes_taker_vwap,
            sum(gross_usdc) FILTER (
                WHERE is_taker AND gross_usdc > 0 AND net_yes_tokens < 0
            )::DOUBLE /
            nullif(sum(-net_yes_tokens) FILTER (
                WHERE is_taker AND gross_usdc > 0 AND net_yes_tokens < 0
            ), 0)::DOUBLE AS no_taker_vwap
        FROM fills_w
        GROUP BY 1, 2
        """
    )

    depth = con.execute(
        f"""
        SELECT
            count(*) AS bars,
            sum(CASE WHEN {_YES_TAKER_FLOOR} THEN 1 ELSE 0 END) AS yes_taker_floor,
            sum(CASE WHEN {_NO_TAKER_FLOOR} THEN 1 ELSE 0 END) AS no_taker_floor,
            sum(CASE WHEN {_YES_TAKER_FLOOR} AND {_NO_TAKER_FLOOR} THEN 1 ELSE 0 END)
                AS both_taker_floor,
            sum(CASE WHEN {_YES_ASK_FLOOR} THEN 1 ELSE 0 END) AS yes_ask_floor,
            sum(CASE WHEN {_NO_ASK_FLOOR} THEN 1 ELSE 0 END) AS no_ask_floor,
            sum(CASE WHEN {_YES_ASK_FLOOR} AND {_NO_ASK_FLOOR} THEN 1 ELSE 0 END)
                AS both_ask_floor,
            sum(CASE WHEN {_YES_TAKER_FLOOR} AND {_NO_TAKER_FLOOR}
                      AND yes_taker_vwap + no_taker_vwap < 1.0 THEN 1 ELSE 0 END)
                AS both_taker_pair_lt1,
            sum(CASE WHEN {_YES_TAKER_FLOOR} AND {_NO_TAKER_FLOOR}
                      AND yes_taker_vwap + no_taker_vwap < 0.99 THEN 1 ELSE 0 END)
                AS both_taker_pair_lt099,
            sum(CASE WHEN {_YES_TAKER_FLOOR} AND {_NO_TAKER_FLOOR}
                      AND yes_taker_vwap + no_taker_vwap < 0.98 THEN 1 ELSE 0 END)
                AS both_taker_pair_lt098,
            avg(yes_taker_vwap + no_taker_vwap) FILTER (
                WHERE {_YES_TAKER_FLOOR} AND {_NO_TAKER_FLOOR}
            ) AS mean_pair_cost,
            quantile_cont(yes_taker_vwap + no_taker_vwap, 0.10) FILTER (
                WHERE {_YES_TAKER_FLOOR} AND {_NO_TAKER_FLOOR}
            ) AS p10_pair_cost,
            quantile_cont(yes_taker_vwap + no_taker_vwap, 0.50) FILTER (
                WHERE {_YES_TAKER_FLOOR} AND {_NO_TAKER_FLOOR}
            ) AS p50_pair_cost,
            quantile_cont(yes_taker_vwap + no_taker_vwap, 0.90) FILTER (
                WHERE {_YES_TAKER_FLOOR} AND {_NO_TAKER_FLOOR}
            ) AS p90_pair_cost
        FROM bars
        """
    ).fetchone()

    # 1× floor vs 4× floor (Kelly size cap), still requiring both share and notional.
    clips = con.execute(
        f"""
        SELECT mult, yes_n, no_n, both_n FROM (
            SELECT 1 AS mult,
                sum(CASE WHEN {_YES_TAKER_FLOOR} THEN 1 ELSE 0 END) AS yes_n,
                sum(CASE WHEN {_NO_TAKER_FLOOR} THEN 1 ELSE 0 END) AS no_n,
                sum(CASE WHEN {_YES_TAKER_FLOOR} AND {_NO_TAKER_FLOOR} THEN 1 ELSE 0 END) AS both_n
            FROM bars
            UNION ALL
            SELECT 2,
                sum(CASE WHEN coalesce(yes_taker_buy_tokens, 0) >= {2 * MIN_SHARES}
                          AND coalesce(yes_taker_buy_usdc, 0) >= {2 * MIN_WORST_USDC}
                         THEN 1 ELSE 0 END),
                sum(CASE WHEN coalesce(no_taker_buy_tokens, 0) >= {2 * MIN_SHARES}
                          AND coalesce(no_taker_buy_usdc, 0) >= {2 * MIN_WORST_USDC}
                         THEN 1 ELSE 0 END),
                sum(CASE WHEN coalesce(yes_taker_buy_tokens, 0) >= {2 * MIN_SHARES}
                          AND coalesce(yes_taker_buy_usdc, 0) >= {2 * MIN_WORST_USDC}
                          AND coalesce(no_taker_buy_tokens, 0) >= {2 * MIN_SHARES}
                          AND coalesce(no_taker_buy_usdc, 0) >= {2 * MIN_WORST_USDC}
                         THEN 1 ELSE 0 END)
            FROM bars
            UNION ALL
            SELECT {KELLY_SIZE_RATIO},
                sum(CASE WHEN coalesce(yes_taker_buy_tokens, 0) >= {KELLY_SIZE_RATIO * MIN_SHARES}
                          AND coalesce(yes_taker_buy_usdc, 0) >= {KELLY_SIZE_RATIO * MIN_WORST_USDC}
                         THEN 1 ELSE 0 END),
                sum(CASE WHEN coalesce(no_taker_buy_tokens, 0) >= {KELLY_SIZE_RATIO * MIN_SHARES}
                          AND coalesce(no_taker_buy_usdc, 0) >= {KELLY_SIZE_RATIO * MIN_WORST_USDC}
                         THEN 1 ELSE 0 END),
                sum(CASE WHEN coalesce(yes_taker_buy_tokens, 0) >= {KELLY_SIZE_RATIO * MIN_SHARES}
                          AND coalesce(yes_taker_buy_usdc, 0) >= {KELLY_SIZE_RATIO * MIN_WORST_USDC}
                          AND coalesce(no_taker_buy_tokens, 0) >= {KELLY_SIZE_RATIO * MIN_SHARES}
                          AND coalesce(no_taker_buy_usdc, 0) >= {KELLY_SIZE_RATIO * MIN_WORST_USDC}
                         THEN 1 ELSE 0 END)
            FROM bars
        )
        ORDER BY mult
        """
    ).fetchall()

    print("  sided-exposure join …", flush=True)
    sided = con.execute(
        f"""
        WITH yes_hit AS (
            SELECT block_number, condition_id
            FROM bars
            WHERE {_YES_TAKER_FLOOR}
        ),
        no_hit AS (
            SELECT block_number, condition_id
            FROM bars
            WHERE {_NO_TAKER_FLOOR}
        ),
        joined AS (
            SELECT
                y.block_number,
                y.condition_id,
                min(n.block_number) AS first_no_block
            FROM yes_hit y
            LEFT JOIN no_hit n
              ON y.condition_id = n.condition_id
             AND n.block_number >= y.block_number
             AND n.block_number <= y.block_number + {COMPLEMENT_BLOCKS}
            GROUP BY 1, 2
        )
        SELECT
            count(*) AS yes_clips,
            sum(CASE WHEN first_no_block = block_number THEN 1 ELSE 0 END) AS no_same_block,
            sum(CASE WHEN first_no_block IS NOT NULL
                      AND first_no_block - block_number BETWEEN 1 AND {GRACE_BLOCKS}
                     THEN 1 ELSE 0 END) AS no_in_1_to_15,
            sum(CASE WHEN first_no_block IS NOT NULL
                      AND first_no_block - block_number BETWEEN 1 AND {COMPLEMENT_BLOCKS}
                     THEN 1 ELSE 0 END) AS no_in_1_to_30,
            sum(CASE WHEN first_no_block IS NOT NULL THEN 1 ELSE 0 END) AS no_within_30_incl_same,
            sum(CASE WHEN first_no_block IS NULL THEN 1 ELSE 0 END) AS no_miss_30
        FROM joined
        """
    ).fetchone()

    sided_rev = con.execute(
        f"""
        WITH no_hit AS (
            SELECT block_number, condition_id
            FROM bars
            WHERE {_NO_TAKER_FLOOR}
        ),
        yes_hit AS (
            SELECT block_number, condition_id
            FROM bars
            WHERE {_YES_TAKER_FLOOR}
        ),
        joined AS (
            SELECT
                n.block_number,
                n.condition_id,
                min(y.block_number) AS first_yes_block
            FROM no_hit n
            LEFT JOIN yes_hit y
              ON n.condition_id = y.condition_id
             AND y.block_number >= n.block_number
             AND y.block_number <= n.block_number + {COMPLEMENT_BLOCKS}
            GROUP BY 1, 2
        )
        SELECT
            count(*) AS no_clips,
            sum(CASE WHEN first_yes_block = block_number THEN 1 ELSE 0 END) AS yes_same_block,
            sum(CASE WHEN first_yes_block IS NOT NULL
                      AND first_yes_block - block_number BETWEEN 1 AND {COMPLEMENT_BLOCKS}
                     THEN 1 ELSE 0 END) AS yes_in_1_to_30,
            sum(CASE WHEN first_yes_block IS NULL THEN 1 ELSE 0 END) AS yes_miss_30
        FROM joined
        """
    ).fetchone()

    # Floor-clip VWAP walk: cheapest shares until N ≥ 5 and N × p_marginal ≥ $1.20.
    print("  floor-clip VWAP walk (dual taker-buy bars) …", flush=True)
    walk = con.execute(
        f"""
        WITH dual AS (
            SELECT block_number, condition_id
            FROM bars
            WHERE {_YES_TAKER_FLOOR} AND {_NO_TAKER_FLOOR}
        ),
        buys AS (
            SELECT
                f.block_number,
                f.condition_id,
                CASE WHEN f.net_yes_tokens > 0 THEN 'yes' ELSE 'no' END AS outcome,
                f.gross_usdc AS usdc,
                abs(f.net_yes_tokens) AS tokens,
                abs(f.gross_usdc)::DOUBLE / abs(f.net_yes_tokens)::DOUBLE AS px,
                f.logical_fill_index
            FROM fills_w f
            INNER JOIN dual d
              ON f.block_number = d.block_number
             AND f.condition_id = d.condition_id
            WHERE f.is_taker AND f.gross_usdc > 0 AND f.net_yes_tokens <> 0
        ),
        ordered AS (
            SELECT
                *,
                sum(tokens) OVER (
                    PARTITION BY block_number, condition_id, outcome
                    ORDER BY px, logical_fill_index
                    ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW
                ) AS cum_tokens,
                sum(usdc) OVER (
                    PARTITION BY block_number, condition_id, outcome
                    ORDER BY px, logical_fill_index
                    ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW
                ) AS cum_usdc
            FROM buys
        ),
        flagged AS (
            SELECT
                *,
                (cum_tokens >= {MIN_SHARES} AND cum_tokens * px >= {MIN_WORST_USDC}) AS viable
            FROM ordered
        ),
        first_ok AS (
            SELECT
                block_number,
                condition_id,
                outcome,
                cum_tokens,
                cum_usdc,
                px,
                row_number() OVER (
                    PARTITION BY block_number, condition_id, outcome
                    ORDER BY px, logical_fill_index
                ) AS rn
            FROM flagged
            WHERE viable
        ),
        vw AS (
            SELECT
                block_number,
                condition_id,
                outcome,
                cum_usdc::DOUBLE / nullif(cum_tokens, 0)::DOUBLE AS vwap
            FROM first_ok
            WHERE rn = 1
        ),
        pair AS (
            SELECT
                y.block_number,
                y.condition_id,
                y.vwap AS yes_vwap,
                n.vwap AS no_vwap,
                y.vwap + n.vwap AS pair_cost
            FROM vw y
            INNER JOIN vw n
              ON y.block_number = n.block_number
             AND y.condition_id = n.condition_id
            WHERE y.outcome = 'yes' AND n.outcome = 'no'
        )
        SELECT
            count(*) AS n,
            avg(pair_cost) AS mean_cost,
            quantile_cont(pair_cost, 0.10) AS p10,
            quantile_cont(pair_cost, 0.50) AS p50,
            quantile_cont(pair_cost, 0.90) AS p90,
            sum(CASE WHEN pair_cost < 1.0 THEN 1 ELSE 0 END) AS lt1,
            sum(CASE WHEN pair_cost < 0.99 THEN 1 ELSE 0 END) AS lt099,
            sum(CASE WHEN pair_cost < 0.98 THEN 1 ELSE 0 END) AS lt098,
            sum(CASE WHEN pair_cost < 0.97 THEN 1 ELSE 0 END) AS lt097,
            avg(1.0 - pair_cost) FILTER (WHERE pair_cost < 1.0) AS mean_edge_if_lt1
        FROM pair
        """
    ).fetchone()

    dt = time.time() - t0
    print(f"  {title} done in {dt:.1f}s", flush=True)

    min_b, max_b = overview[2], overview[3]
    span_h = (max_b - min_b + 1) * BLOCK_SEC / 3600.0 if max_b and min_b else 0
    n_yes = sided[0] or 0
    n_no = sided_rev[0] or 0

    side_rows = []
    for side, legs, abs_usdc, taker_legs, taker_usdc in sides:
        side_rows.append(
            [side, _fmt_int(legs), _fmt_usd(abs_usdc, 0), _fmt_int(taker_legs), _fmt_usd(taker_usdc, 0)]
        )

    clip_rows = []
    for mult, yes_n, no_n, both_n in clips:
        both_pct = (both_n / yes_n) if yes_n else None
        clip_rows.append(
            [
                f"{int(mult)}× floor",
                _fmt_int(yes_n),
                _fmt_int(no_n),
                _fmt_int(both_n),
                _fmt_pct(both_pct) if both_pct is not None else "—",
            ]
        )

    lines = [
        f"## Microstructure — {title}",
        "",
        f"{len(paths)} fills partitions, scan {dt:.1f}s, "
        f"blocks {_fmt_int(min_b)}–{_fmt_int(max_b)} (~{span_h:.1f} h).",
        "",
        _md_table(
            ["metric", "value"],
            [
                ["fill legs", _fmt_int(overview[0])],
                ["distinct conditions", _fmt_int(overview[1])],
                ["sum |gross_usdc| (both legs, double-counts matches)", _fmt_usd(overview[4], 0)],
                ["taker |gross_usdc|", _fmt_usd(overview[5], 0)],
                ["fee_usdc", _fmt_usd(overview[6], 2)],
                ["legs with fee_usdc > 0", _fmt_int(overview[7])],
                ["taker legs", _fmt_int(overview[8])],
                ["NegRisk legs", _fmt_int(overview[9])],
                ["|gross_usdc| p50 / p90 / p99 / mean",
                 f"{_fmt_usd(overview[10])} / {_fmt_usd(overview[11])} / {_fmt_usd(overview[12])} / {_fmt_usd(overview[13])}"],
            ],
        ),
        "",
        "Leg sides (`gross_usdc` sign × `net_yes_tokens` sign):",
        "",
        _md_table(
            ["side", "legs", "|USDC|", "taker legs", "taker |USDC|"],
            side_rows,
        ),
        "",
        "Opportunity screen per `(block, condition)`: a side is a floor "
        "cross if consumed depth is **≥ 5 shares and ≥ $1.20 USDC** (proxy "
        "for `N × P_worst`). Taker-buy is interpretation A in fill-model.md; "
        "maker-sell is interpretation C.",
        "",
        _md_table(
            ["metric", "count"],
            [
                ["(block, condition) bars with any fill", _fmt_int(depth[0])],
                ["YES taker-buy floor", _fmt_int(depth[1])],
                ["NO taker-buy floor", _fmt_int(depth[2])],
                ["both taker-buy floors same block", _fmt_int(depth[3])],
                ["YES maker-sell (ask) floor", _fmt_int(depth[4])],
                ["NO maker-sell (ask) floor", _fmt_int(depth[5])],
                ["both ask floors same block", _fmt_int(depth[6])],
                ["both taker floors and VWAP pair < $1.00", _fmt_int(depth[7])],
                ["both taker floors and VWAP pair < $0.99", _fmt_int(depth[8])],
                ["both taker floors and VWAP pair < $0.98", _fmt_int(depth[9])],
                ["mean pair cost (all-print VWAP, both floors)", _fmt_num(depth[10], 4)],
                ["pair cost p10 / p50 / p90",
                 f"{_fmt_num(depth[11], 4)} / {_fmt_num(depth[12], 4)} / {_fmt_num(depth[13], 4)}"],
            ],
        ),
        "",
        "Depth multiples of the floor (5 shares and $1.20, then 2× and 4×). "
        "4× is the Kelly size cap: the largest positive stake is at most "
        "four times the smallest. `both / YES` is same-block completion at "
        "that multiple.",
        "",
        _md_table(
            ["size", "YES bars", "NO bars", "both same block", "both / YES"],
            clip_rows,
        ),
        "",
        "Sided exposure. Each YES floor bar is a synthetic aggressive fill. "
        "Complements are NO floor bars in `[0, 30]` blocks.",
        "",
        _md_table(
            ["metric", "count", "share of YES floor bars"],
            [
                ["YES floor bars", _fmt_int(n_yes), "100%"],
                ["NO floor same block", _fmt_int(sided[1]), _fmt_pct((sided[1] or 0) / n_yes if n_yes else None)],
                ["NO floor in +1..+15 (grace)", _fmt_int(sided[2]), _fmt_pct((sided[2] or 0) / n_yes if n_yes else None)],
                ["NO floor in +1..+30 (model window)", _fmt_int(sided[3]), _fmt_pct((sided[3] or 0) / n_yes if n_yes else None)],
                ["NO floor anywhere in [0, 30]", _fmt_int(sided[4]), _fmt_pct((sided[4] or 0) / n_yes if n_yes else None)],
                ["NO miss through +30", _fmt_int(sided[5]), _fmt_pct((sided[5] or 0) / n_yes if n_yes else None)],
            ],
        ),
        "",
        "Reverse (aggressive = NO):",
        "",
        _md_table(
            ["metric", "count", "share of NO floor bars"],
            [
                ["NO floor bars", _fmt_int(n_no), "100%"],
                ["YES floor same block", _fmt_int(sided_rev[1]), _fmt_pct((sided_rev[1] or 0) / n_no if n_no else None)],
                ["YES floor in +1..+30", _fmt_int(sided_rev[2]), _fmt_pct((sided_rev[2] or 0) / n_no if n_no else None)],
                ["YES miss through +30", _fmt_int(sided_rev[3]), _fmt_pct((sided_rev[3] or 0) / n_no if n_no else None)],
            ],
        ),
        "",
        "Floor-clip VWAP walk on bars that already clear the floor on **both** "
        "outcomes in the same block. Walk cheapest shares until `N ≥ 5` and "
        "`N × p_marginal ≥ $1.20`. Pair cost = YES VWAP + NO VWAP of that "
        "prefix. Interpretation A, no signed cap, no competition κ. This is "
        "the merge edge at the **minimum** order, not at a model-chosen size.",
        "",
        _md_table(
            ["metric", "value"],
            [
                ["dual bars with a floor walk on both books", _fmt_int(walk[0])],
                ["mean pair cost", _fmt_num(walk[1], 4)],
                ["p10 / p50 / p90 pair cost",
                 f"{_fmt_num(walk[2], 4)} / {_fmt_num(walk[3], 4)} / {_fmt_num(walk[4], 4)}"],
                ["pair < $1.00", f"{_fmt_int(walk[5])} ({_fmt_pct((walk[5] or 0) / walk[0] if walk[0] else None)})"],
                ["pair < $0.99", f"{_fmt_int(walk[6])} ({_fmt_pct((walk[6] or 0) / walk[0] if walk[0] else None)})"],
                ["pair < $0.98", f"{_fmt_int(walk[7])} ({_fmt_pct((walk[7] or 0) / walk[0] if walk[0] else None)})"],
                ["pair < $0.97", f"{_fmt_int(walk[8])} ({_fmt_pct((walk[8] or 0) / walk[0] if walk[0] else None)})"],
                ["mean edge when pair < $1 (per complete share)", _fmt_usd(walk[9], 4) if walk[9] is not None else "—"],
            ],
        ),
        "",
    ]
    return "\n".join(lines)


def sample_rows_section(con: duckdb.DuckDBPyConnection, path: str) -> str:
    print("  sample rows …", flush=True)
    rows = con.execute(
        f"""
        SELECT
            block_number,
            logical_fill_index,
            is_taker,
            net_yes_tokens / 1e6 AS yes_tokens,
            gross_usdc / 1e6 AS usdc,
            fee_usdc / 1e6 AS fee,
            CASE
                WHEN net_yes_tokens = 0 THEN NULL
                WHEN gross_usdc * net_yes_tokens > 0
                    THEN gross_usdc::DOUBLE / net_yes_tokens::DOUBLE
                ELSE 1.0 + gross_usdc::DOUBLE / net_yes_tokens::DOUBLE
            END AS yes_price,
            market_id IS NOT NULL AS neg_risk
        FROM read_parquet('{path}')
        LIMIT 12
        """
    ).fetchall()
    table = _md_table(
        ["block", "idx", "taker", "yes tokens", "USDC", "fee", "yes px", "neg-risk"],
        [
            [
                _fmt_int(r[0]),
                str(r[1]),
                "Y" if r[2] else "N",
                _fmt_num(r[3], 4),
                _fmt_num(r[4], 4),
                _fmt_num(r[5], 4),
                _fmt_num(r[6], 4),
                "Y" if r[7] else "N",
            ]
            for r in rows
        ],
    )
    return (
        "## Typical fill rows\n\n"
        f"First 12 legs of `{path}`.\n\n"
        "YES price formula matches `condition_by_block_v1`: same-sign "
        "`gross_usdc / net_yes_tokens`, opposite-sign `1 + ratio`.\n\n"
        + table
        + "\n"
    )


def main() -> int:
    parser = argparse.ArgumentParser(description="Complementary MM first-look EDA")
    parser.add_argument("--last-partitions", type=int, default=48,
                        help="how many recent non-empty fills partitions to scan (default 48, ~11.6d)")
    parser.add_argument("--skip-universe", action="store_true")
    args = parser.parse_args()

    if not FILLS_DIR or not Path(FILLS_DIR).exists():
        _fail("FILLS_V1_DIR is not set or does not exist.")
    if not C10K_DIR or not Path(C10K_DIR).exists():
        _fail("CONDITION_BY_10K_V1_DIR is not set or does not exist.")
    if not CBB_DIR or not Path(CBB_DIR).exists():
        _fail("CONDITION_BY_BLOCK_V1_DIR is not set or does not exist.")

    fills_meta = _load_metadata(Path(FILLS_DIR))
    c10k_meta = _load_metadata(Path(C10K_DIR))
    cbb_meta = _load_metadata(Path(CBB_DIR))
    if not fills_meta:
        _fail("No fills_v1 partitions found.")

    nonempty = [m for m in fills_meta if m["row_count"] > 0]
    recent = nonempty[-args.last_partitions:]
    # Older slice: ~6 months back if present (≈ 7.4M blocks at 2.1s).
    target = recent[0]["min_block"] - 7_400_000 if recent else 0
    older = [m for m in nonempty if abs(m["min_block"] - target) < 200_000]
    if not older:
        older = nonempty[len(nonempty) // 2 : len(nonempty) // 2 + min(12, args.last_partitions)]
    else:
        older = older[: min(12, args.last_partitions)]

    started = datetime.now(timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ")
    sections = [
        "# Complementary MM — EDA snapshot",
        "",
        f"Generated {started} by `explorations/pair-v1/eda.py`.",
        "",
        "Numbers are consumed on-chain volume, not L2 books. An opportunity "
        "is a floor cross: **≥ 5 shares and ≥ $1.20 worst-price notional** "
        "on each side being lifted. Counts are an **upper bound** on how "
        "often that floor order could have filled; they ignore competition, "
        "the strict-better-than-cap rule, and mint/merge tagging. Read with "
        "[`fill-model.md`](fill-model.md).",
        "",
        inventory_section(fills_meta, c10k_meta, cbb_meta),
    ]

    con = _connect()
    try:
        if not args.skip_universe:
            sections.append(universe_section(con))
        sections.append(sample_rows_section(con, recent[-1]["path"]))
        sections.append(
            window_section(
                con,
                [m["path"] for m in recent],
                f"last {len(recent)} non-empty partitions",
            )
        )
        if older:
            sections.append(
                window_section(
                    con,
                    [m["path"] for m in older],
                    f"older slice ({len(older)} partitions near block {older[0]['min_block']:,})",
                )
            )
    finally:
        con.close()

    sections.append(
        "## What this does **not** measure\n\n"
        "- Trigger patterns (the model). These counts are unconditional "
        "floor depth, not 'after a signal at block X'.\n"
        "- Signed size above the floor (Kelly 1×–4×). The walk is the "
        "minimum viable order only.\n"
        "- Strict-better-than-cap fills (needs a signed `P` from block X).\n"
        "- Competition multiplier κ.\n"
        "- Buy-side token fees.\n"
        "- Inventory mark to resolution on one-sided clips (needs the "
        "simulator + payout join).\n"
        "- Live grace-period cancel denials (needs CLOB `oas` / timing logs).\n"
    )
    text = "\n".join(sections).rstrip() + "\n"
    OUT_PATH.write_text(text)
    print(f"wrote {OUT_PATH}", flush=True)
    print(text)
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
