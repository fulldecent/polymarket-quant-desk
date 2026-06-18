#!/usr/bin/env python3
"""Temporary benchmark harness for fills_v1 running-total cost.

This script benchmarks two SQL variants on the same recent partitions and writes
results to temporary parquet outputs under TEMP_DIR:

- baseline: current production SQL with net_yes_position_after running window
- no_running_total: same pipeline without net_yes_position_after

It is intentionally non-production and should not publish into FILLS_V1_DIR.
"""

from __future__ import annotations

import argparse
import logging
import signal
import sys
import threading
import time
from pathlib import Path

import duckdb
from dotenv import load_dotenv
from rich.console import Console

_project_root = Path(__file__).resolve().parent.parent.parent
load_dotenv(_project_root / ".env")

sys.path.insert(0, str(_project_root))

from lib.env import require_env  # noqa: E402
from lib.partition_utils import enumerate_partitions, partition_dir, partition_end  # noqa: E402
from lib.derived_frontier import scan_frontier_1M_10K_folders  # noqa: E402
from lib.run_logging import setup_logging  # noqa: E402
from raw_data.polygon_contract_events_v3 import SCRAPE_START_BLOCK, get_sunk_frontier  # noqa: E402

from derived_data.fills_v1.main import (  # noqa: E402
    EXCHANGES,
    _build_partition_sql,
    _load_token_map,
    _load_condition_resolution,
    _update_balances,
    _ensure_balances_sidecar,
    _empty_chunk_rows_sql,
    _existing,
)

console = Console()
_stop_event = threading.Event()
_global_con: duckdb.DuckDBPyConnection | None = None

RAW = require_env("POLYGON_CONTRACT_EVENTS_V3_DIR")
TOKEN_MAP = require_env("TOKEN_ID_MAP_V1_DIR")
FILLS_OUT = require_env("FILLS_V1_DIR")
TEMP_DIR = require_env("TEMP_DIR")


def _build_partition_sql_no_running(k_val: int, leg_paths: list[tuple[dict, str]]) -> str:
    """Partition SQL variant that excludes the running-total column."""
    from derived_data.fills_v1.main import _legs_select, _refund_select, ZERO32_SQL

    legs_union = " UNION ALL ".join(_legs_select(exch, path) for exch, path in leg_paths)
    refund_sql = _refund_select(k_val)
    refund_cte = refund_sql if refund_sql else "SELECT NULL::BLOB transaction_hash, NULL::BLOB order_hash, NULL::HUGEINT fee_charged WHERE FALSE"

    return f"""
    WITH raw_legs AS (
        {legs_union}
    ),
    taker_legs AS (
        SELECT transaction_hash, neg_log_index, order_hash, buy
        FROM raw_legs WHERE is_taker
    ),
    refunds AS (
        {refund_cte}
    ),
    assoc AS (
        SELECT
            m.*,
            CASE WHEN m.is_taker THEN m.order_hash ELSE t.order_hash END AS match_key,
            CASE WHEN m.is_taker THEN m.buy        ELSE t.buy        END AS taker_buys
        FROM raw_legs m
        ASOF LEFT JOIN taker_legs t
          ON m.transaction_hash = t.transaction_hash
         AND m.neg_log_index >= t.neg_log_index
        WHERE m.is_taker OR t.order_hash IS NOT NULL
    ),
    enriched AS (
        SELECT
            a.block_number,
            a.transaction_index,
            a.log_index,
            a.account,
            a.token_id,
            tok.condition_id,
            tok.market_id,
            tok.index_set,
            a.is_taker,
            a.match_key,
            a.taker_buys,
            ( a.q
              * (CASE WHEN a.buy THEN 1 ELSE -1 END)
              * (CASE WHEN tok.index_set = 1 THEN 1 ELSE -1 END) ) AS net_yes_tokens,
            ( a.c * (CASE WHEN a.buy THEN 1 ELSE -1 END) ) AS gross_usdc,
            ( CASE
                WHEN NOT a.fee_is_usdc THEN 0
                WHEN a.is_v1 THEN COALESCE(r.fee_charged, 0)
                ELSE a.gross_fee
              END ) AS fee_usdc,
            ( (a.c::DOUBLE / a.q::DOUBLE) * (CASE WHEN a.taker_buys THEN 1 ELSE -1 END) ) AS price_rank
        FROM assoc a
        LEFT JOIN tok ON a.token_id = tok.token_id
        LEFT JOIN refunds r
          ON a.is_v1 AND a.transaction_hash = r.transaction_hash AND a.order_hash = r.order_hash
    ),
    keyed AS (
        SELECT
            *,
            MIN(log_index) OVER (PARTITION BY block_number, transaction_index, match_key) AS match_order_key
        FROM enriched
    ),
    indexed AS (
        SELECT
            *,
            CAST(ROW_NUMBER() OVER (
                PARTITION BY block_number
                ORDER BY
                    transaction_index,
                    match_order_key,
                    is_taker DESC,
                    price_rank,
                    log_index
            ) - 1 AS UINTEGER) AS logical_fill_index
        FROM keyed
    )
    SELECT
        CAST(block_number AS UINTEGER)        AS block_number,
        logical_fill_index,
        CAST(transaction_index AS UINTEGER)   AS transaction_index,
        CAST(log_index AS UINTEGER)           AS log_index,
        account,
        token_id,
        condition_id,
        market_id,
        is_taker,
        CAST(net_yes_tokens AS BIGINT)        AS net_yes_tokens,
        CAST(gross_usdc AS BIGINT)            AS gross_usdc,
        CAST(fee_usdc AS BIGINT)              AS fee_usdc,
        index_set,
        condition_id IS NULL                  AS _missing_token
    FROM indexed
    ORDER BY block_number, logical_fill_index
    """


def _select_recent_todo_partitions(limit_count: int, log: logging.Logger) -> list[int]:
    """Return the first N incomplete 10K partitions at the latest frontier."""
    cold_frontier = get_sunk_frontier(RAW)
    token_map_latest = scan_frontier_1M_10K_folders(
        base_path=TOKEN_MAP,
        starting_partition=SCRAPE_START_BLOCK,
        tmp_suffix=".tmp",
        cb_progress=lambda _partition: None,
    )
    token_map_frontier = partition_end(token_map_latest) if token_map_latest is not None else SCRAPE_START_BLOCK - 1
    frontier = min(cold_frontier, token_map_frontier)

    todo = []
    for _m, k in enumerate_partitions(SCRAPE_START_BLOCK, frontier):
        part_dir = Path(FILLS_OUT) / partition_dir(k)
        if not part_dir.exists():
            todo.append(k)
            if len(todo) >= limit_count:
                break

    log.info("benchmark frontier=%s selected_partitions=%s", frontier, ",".join(str(x) for x in todo))
    return todo


def _prepare_chunk_rows(
    con: duckdb.DuckDBPyConnection,
    *,
    k_val: int,
    variant: str,
    log: logging.Logger,
) -> int:
    """Build temporary chunk_rows for one partition and return surviving row count."""
    leg_paths = [(exch, _existing(f"{exch['name']}/order_filled", k_val)) for exch in EXCHANGES]
    leg_paths = [(exch, path) for exch, path in leg_paths if path]

    if not leg_paths:
        con.execute("DROP TABLE IF EXISTS chunk_rows")
        con.execute(_empty_chunk_rows_sql())
        return 0

    if variant == "baseline":
        sql = _build_partition_sql(k_val, leg_paths)
    elif variant == "no_running_total":
        sql = _build_partition_sql_no_running(k_val, leg_paths)
    else:
        raise ValueError(f"unknown variant: {variant}")

    con.execute(f"CREATE OR REPLACE TEMP TABLE chunk_rows AS {sql}")

    dropped = con.execute("""
        SELECT COUNT(*)
        FROM chunk_rows
        WHERE _missing_token
    """).fetchone()[0]
    if dropped:
        con.execute("DELETE FROM chunk_rows WHERE _missing_token")
        log.warning("10K=%s %s: dropped %s fill legs with token_ids absent from token_id_map_v1", k_val, variant, f"{dropped:,}")

    bad = con.execute("""
        SELECT COUNT(*) FILTER (WHERE index_set NOT IN (1, 2)) AS nonbinary
        FROM chunk_rows
    """).fetchone()[0]
    if bad:
        raise RuntimeError(
            f"10K={k_val} {variant}: {bad} fill legs have index_set outside {{1, 2}}"
        )

    return con.execute("SELECT COUNT(*) FROM chunk_rows").fetchone()[0]


def _write_temp_output(
    con: duckdb.DuckDBPyConnection,
    *,
    variant: str,
    k_val: int,
    base_dir: Path,
) -> None:
    """Write chunk_rows as temporary benchmark parquet for this variant/partition."""
    out_dir = base_dir / variant / partition_dir(k_val)
    out_dir.mkdir(parents=True, exist_ok=True)
    out_file = out_dir / "data.parquet"

    if variant == "baseline":
        cols = (
            "block_number, logical_fill_index, transaction_index, log_index, "
            "account, token_id, condition_id, market_id, is_taker, net_yes_tokens, "
            "gross_usdc, fee_usdc, net_yes_position_after"
        )
    else:
        cols = (
            "block_number, logical_fill_index, transaction_index, log_index, "
            "account, token_id, condition_id, market_id, is_taker, net_yes_tokens, "
            "gross_usdc, fee_usdc"
        )

    con.execute(f"""
        COPY (
            SELECT {cols}
            FROM chunk_rows
            ORDER BY block_number, logical_fill_index
        ) TO '{out_file.as_posix()}' (FORMAT PARQUET, COMPRESSION ZSTD)
    """)


def _run_variant(
    con: duckdb.DuckDBPyConnection,
    *,
    variant: str,
    partitions: list[int],
    out_base: Path,
    log: logging.Logger,
) -> dict[int, float]:
    """Run one variant over partitions and return per-partition total seconds."""
    timings: dict[int, float] = {}

    for idx, k_val in enumerate(partitions, start=1):
        if _stop_event.is_set():
            break

        t0 = time.perf_counter()
        row_count = _prepare_chunk_rows(con, k_val=k_val, variant=variant, log=log)
        t_build = time.perf_counter() - t0

        tw = time.perf_counter()
        _write_temp_output(con, variant=variant, k_val=k_val, base_dir=out_base)
        t_write = time.perf_counter() - tw

        if variant == "baseline":
            _update_balances(con)

        total = time.perf_counter() - t0
        timings[k_val] = total

        log.info(
            "benchmark variant=%s partition=%s/%s 10K=%s rows=%s build=%0.3fs write=%0.3fs total=%0.3fs",
            variant,
            idx,
            len(partitions),
            k_val,
            f"{row_count:,}",
            t_build,
            t_write,
            total,
        )

    return timings


def main() -> None:
    global _global_con

    parser = argparse.ArgumentParser(description="Temporary fills_v1 running-total benchmark")
    parser.add_argument(
        "--partitions",
        type=str,
        default="",
        help="comma-separated 10K partition starts (default: first missing recent partitions)",
    )
    parser.add_argument(
        "--count",
        type=int,
        default=3,
        help="when --partitions is omitted, benchmark this many recent missing partitions",
    )
    args = parser.parse_args()

    log = setup_logging("fills_v1_benchmark_tmp", __file__, console)

    def _handle_sigint(sig, frame):
        _stop_event.set()
        try:
            if _global_con is not None:
                _global_con.interrupt()
        except Exception:
            pass

    signal.signal(signal.SIGINT, _handle_sigint)

    if args.partitions.strip():
        partitions = [int(x.strip()) for x in args.partitions.split(",") if x.strip()]
    else:
        partitions = _select_recent_todo_partitions(args.count, log)

    if not partitions:
        log.info("No partitions selected; exiting")
        return

    out_base = Path(TEMP_DIR) / "fills_v1_benchmark_no_running_total_tmp"
    out_base.mkdir(parents=True, exist_ok=True)
    log.info("benchmark output root: %s", out_base)

    # Baseline run: includes balances carry state and production SQL shape.
    con_base = duckdb.connect()
    _global_con = con_base
    con_base.execute(f"SET temp_directory = '{TEMP_DIR}'")
    con_base.execute("SET preserve_insertion_order = false")
    _load_token_map(con_base, log)
    frontier = get_sunk_frontier(RAW)
    _load_condition_resolution(con_base, log, frontier)
    _ensure_balances_sidecar(con_base, log)

    baseline = _run_variant(
        con_base,
        variant="baseline",
        partitions=partitions,
        out_base=out_base,
        log=log,
    )

    # No-running-total run: no balances sidecar needed.
    con_nr = duckdb.connect()
    _global_con = con_nr
    con_nr.execute(f"SET temp_directory = '{TEMP_DIR}'")
    con_nr.execute("SET preserve_insertion_order = false")
    _load_token_map(con_nr, log)
    _load_condition_resolution(con_nr, log, frontier)

    no_running = _run_variant(
        con_nr,
        variant="no_running_total",
        partitions=partitions,
        out_base=out_base,
        log=log,
    )

    for k_val in partitions:
        b = baseline.get(k_val)
        n = no_running.get(k_val)
        if b is None or n is None:
            continue
        speedup = (b / n) if n > 0 else float("inf")
        pct = ((b - n) / b * 100.0) if b > 0 else 0.0
        log.info(
            "benchmark compare 10K=%s baseline=%0.3fs no_running_total=%0.3fs speedup=%0.2fx improvement=%0.1f%%",
            k_val,
            b,
            n,
            speedup,
            pct,
        )


if __name__ == "__main__":
    main()
