#!/usr/bin/env python3
"""
Materializes the condition_by_10k_v1 derived table.

Per-10K-partition OHLC YES price, matched volume, fees, and resolution outcome
for each condition that traded or resolved in that partition.

OUTPUT
------
Partitioned parquet at:

    {CONDITION_BY_10K_V1_DIR}/1M={N}/10K={K}/data.parquet
    {CONDITION_BY_10K_V1_DIR}/1M={N}/10K={K}/metadata.json

See DATA_DICTIONARY.md for the full schema and invariant documentation.

USAGE
-----
    python derived_data/condition_by_10k_v1/main.py [options]

    --dry-run       print the work plan without writing any data
    --sample N      process only the first N incomplete partitions
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

_project_root = Path(__file__).resolve().parent.parent.parent
load_dotenv(_project_root / ".env")

sys.path.insert(0, str(_project_root))
from lib.env import require_directory_env, require_env  # noqa: E402
from lib.git_utils import assert_git_clean  # noqa: E402
from lib.metadata_utils import create_parquet_metadata_json, parquet_content_hash  # noqa: E402
from lib.partition_utils import (  # noqa: E402
    enumerate_partitions,
    partition_dir,
    partition_end,
)
from lib.run_logging import (  # noqa: E402
    PhaseWork,
    RunOutput,
    configure_duckdb_progress,
    print_partition_sunk,
    print_paths,
    print_run_summary,
    print_work_plan,
    run_sql,
)
from lib.atomic_publish import (  # noqa: E402
    cleanup_temp,
    create_temp_location,
    publish_atomically,
)
from lib.derived_frontier import scan_frontier_1M_10K_folders  # noqa: E402
from raw_data.polygon_contract_events_v3 import get_sunk_frontier, SCRAPE_START_BLOCK  # noqa: E402

DATASET = "condition_by_10k_v1"
SOURCE_SCRIPT = "derived_data/condition_by_10k_v1/main.py"

RAW = require_env("POLYGON_CONTRACT_EVENTS_V3_DIR")
TOKEN_MAP = require_env("TOKEN_ID_MAP_V1_DIR")
FILLS = require_env("FILLS_V1_DIR")
OUT_DIR = require_env("CONDITION_BY_10K_V1_DIR")
SCRATCH_DIR = require_directory_env("SCRATCH_DIR")

_stop_event = threading.Event()
_global_con: duckdb.DuckDBPyConnection | None = None


def _sql_quote(value: str) -> str:
    return value.replace("'", "''")


def yes_price_expr(alias: str = "f") -> str:
    """Implied YES price from a fills_v1 leg. Zero amounts are rejected separately."""
    return f"""
        CASE
          WHEN ({alias}.gross_usdc > 0) = ({alias}.net_yes_tokens > 0)
            THEN {alias}.gross_usdc::DOUBLE / {alias}.net_yes_tokens::DOUBLE
          ELSE 1.0 + ({alias}.gross_usdc::DOUBLE / {alias}.net_yes_tokens::DOUBLE)
        END
    """


def _fills_path(k_val: int) -> Path:
    return Path(FILLS) / partition_dir(k_val) / "data.parquet"


def _resolution_path(k_val: int) -> Path:
    return (
        Path(RAW)
        / "ConditionalTokens"
        / "condition_resolution"
        / partition_dir(k_val)
        / "data.parquet"
    )


def _assert_no_zero_amounts(con: duckdb.DuckDBPyConnection, fills_path: Path, k_val: int) -> None:
    n = con.execute(
        f"""
        SELECT COUNT(*)
        FROM read_parquet('{_sql_quote(fills_path.as_posix())}')
        WHERE net_yes_tokens = 0 OR gross_usdc = 0
        """
    ).fetchone()[0]
    if n:
        raise RuntimeError(
            f"10K={k_val}: {n} fills_v1 legs have net_yes_tokens = 0 or gross_usdc = 0"
        )


def _empty_fill_agg_sql() -> str:
    return """
        SELECT
            CAST(NULL AS BLOB) AS condition_id,
            CAST(NULL AS DOUBLE) AS open_yes_price,
            CAST(NULL AS DOUBLE) AS high_yes_price,
            CAST(NULL AS DOUBLE) AS low_yes_price,
            CAST(NULL AS DOUBLE) AS close_yes_price,
            CAST(0 AS UBIGINT) AS matched_yes_tokens,
            CAST(0 AS UBIGINT) AS matched_usdc,
            CAST(0 AS UBIGINT) AS fee_usdc,
            CAST(0 AS UINTEGER) AS fill_count
        WHERE FALSE
    """


def _empty_resolution_sql() -> str:
    return """
        SELECT
            CAST(NULL AS BLOB) AS condition_id,
            CAST(NULL AS UINTEGER) AS resolved_block,
            CAST(NULL AS UINTEGER) AS resolved_log_index,
            CAST(NULL AS UINTEGER) AS outcome_slot_count,
            CAST(NULL AS VARCHAR) AS payout_numerators
        WHERE FALSE
    """


def aggregate_sql(fills_path: Path | None, resolution_path: Path | None) -> str:
    if fills_path is not None:
        path = _sql_quote(fills_path.as_posix())
        fill_agg = f"""
            SELECT
                condition_id,
                CAST(arg_min(yes_price, ord) AS DOUBLE) AS open_yes_price,
                CAST(max(yes_price) AS DOUBLE) AS high_yes_price,
                CAST(min(yes_price) AS DOUBLE) AS low_yes_price,
                CAST(arg_max(yes_price, ord) AS DOUBLE) AS close_yes_price,
                CAST(sum(abs_yes) AS UBIGINT) // 2 AS matched_yes_tokens,
                CAST(sum(abs_usdc) AS UBIGINT) // 2 AS matched_usdc,
                CAST(sum(fee_usdc) AS UBIGINT) AS fee_usdc,
                CAST(count(*) AS UINTEGER) AS fill_count
            FROM (
                SELECT
                    f.condition_id,
                    (CAST(f.block_number AS BIGINT) * 1000000000
                        + CAST(f.logical_fill_index AS BIGINT)) AS ord,
                    {yes_price_expr("f")} AS yes_price,
                    abs(f.net_yes_tokens) AS abs_yes,
                    abs(f.gross_usdc) AS abs_usdc,
                    f.fee_usdc
                FROM read_parquet('{path}') f
            )
            GROUP BY condition_id
        """
    else:
        fill_agg = _empty_fill_agg_sql()

    if resolution_path is not None:
        rpath = _sql_quote(resolution_path.as_posix())
        resolutions = f"""
            SELECT
                r.condition_id,
                CAST(r.block_number AS UINTEGER) AS resolved_block,
                CAST(r.log_index AS UINTEGER) AS resolved_log_index,
                CAST(r.outcome_slot_count AS UINTEGER) AS outcome_slot_count,
                CAST(r.payout_numerators AS VARCHAR) AS payout_numerators
            FROM read_parquet('{rpath}') r
            INNER JOIN polymarket_conditions p ON p.condition_id = r.condition_id
        """
    else:
        resolutions = _empty_resolution_sql()

    return f"""
        WITH fill_agg AS (
            {fill_agg}
        ),
        resolutions AS (
            {resolutions}
        )
        SELECT
            COALESCE(f.condition_id, r.condition_id) AS condition_id,
            f.open_yes_price,
            f.high_yes_price,
            f.low_yes_price,
            f.close_yes_price,
            COALESCE(f.matched_yes_tokens, CAST(0 AS UBIGINT)) AS matched_yes_tokens,
            COALESCE(f.matched_usdc, CAST(0 AS UBIGINT)) AS matched_usdc,
            COALESCE(f.fee_usdc, CAST(0 AS UBIGINT)) AS fee_usdc,
            COALESCE(f.fill_count, CAST(0 AS UINTEGER)) AS fill_count,
            r.resolved_block,
            r.resolved_log_index,
            r.outcome_slot_count,
            r.payout_numerators
        FROM fill_agg f
        FULL OUTER JOIN resolutions r ON f.condition_id = r.condition_id
        ORDER BY COALESCE(f.condition_id, r.condition_id)
    """


def _partition_input_hashes(k_val: int) -> dict[str, str]:
    hashes: dict[str, str] = {}
    fills_path = _fills_path(k_val)
    if fills_path.exists():
        hashes[str(fills_path.relative_to(FILLS))] = parquet_content_hash(fills_path)
    res_path = _resolution_path(k_val)
    if res_path.exists():
        hashes[str(res_path.relative_to(RAW))] = parquet_content_hash(res_path)
    return hashes


def _write_metadata(
    con: duckdb.DuckDBPyConnection,
    chunk_dir: Path,
    m_val: int,
    k_val: int,
    input_hashes: dict[str, str],
) -> None:
    part = chunk_dir / "data.parquet"
    if not part.exists():
        raise RuntimeError(f"data.parquet not found at {part} after write; cannot create metadata")
    create_parquet_metadata_json(
        part,
        dataset=DATASET,
        source_script=SOURCE_SCRIPT,
        input_hashes=input_hashes,
        parameters={
            "1M": m_val,
            "10K": k_val,
            "min_block": k_val,
            "max_block": partition_end(k_val),
        },
        row_count_connection=con,
        created_at=time.strftime("%Y-%m-%dT%H:%M:%SZ", time.gmtime()),
    )


def _assert_resolution_invariants(
    con: duckdb.DuckDBPyConnection,
    k_val: int,
    resolution_path: Path | None,
) -> None:
    if resolution_path is None:
        return
    path = _sql_quote(resolution_path.as_posix())
    dup = con.execute(
        f"""
        SELECT COUNT(*) FROM (
            SELECT condition_id
            FROM read_parquet('{path}')
            INNER JOIN polymarket_conditions p USING (condition_id)
            GROUP BY condition_id
            HAVING COUNT(*) > 1
        )
        """
    ).fetchone()[0]
    if dup:
        raise RuntimeError(
            f"10K={k_val}: {dup} condition_id values have more than one condition_resolution row"
        )


def process_chunk(
    con: duckdb.DuckDBPyConnection,
    m_val: int,
    k_val: int,
    log: logging.Logger,
    *,
    work: PhaseWork | None = None,
) -> int:
    chunk_dir = Path(OUT_DIR) / partition_dir(k_val)
    if chunk_dir.exists():
        sys.exit(f"FATAL: refusing to overwrite immutable partition: {chunk_dir}")
    chunk_dir.parent.mkdir(parents=True, exist_ok=True)

    fills_path = _fills_path(k_val)
    if not fills_path.exists():
        raise RuntimeError(f"fills_v1 partition missing for 10K={k_val}: {fills_path}")
    res_path = _resolution_path(k_val)
    resolution_path = res_path if res_path.exists() else None

    if work is not None:
        work.phase("building")
    n_src = con.execute(
        f"SELECT COUNT(*) FROM read_parquet('{_sql_quote(fills_path.as_posix())}')"
    ).fetchone()[0]
    fills_for_sql: Path | None = fills_path if n_src else None
    if n_src:
        _assert_no_zero_amounts(con, fills_path, k_val)
    _assert_resolution_invariants(con, k_val, resolution_path)

    sql = aggregate_sql(fills_for_sql, resolution_path)

    temp_loc = create_temp_location(
        parent_dir=chunk_dir.parent,
        final_name=chunk_dir.name,
        temp_suffix=".tmp",
    )
    if work is not None:
        work.phase("hashing")
    input_hashes = _partition_input_hashes(k_val)
    try:
        out_parquet = temp_loc.path / "data.parquet"
        if work is not None:
            work.phase("writing")
        run_sql(
            con,
            f"CREATE OR REPLACE TEMP TABLE chunk_rows AS {sql}",
            work=work,
        )
        bad_slots = con.execute(
            """
            SELECT COUNT(*)
            FROM chunk_rows
            WHERE resolved_block IS NOT NULL AND outcome_slot_count <> 2
            """
        ).fetchone()[0]
        if bad_slots:
            raise RuntimeError(
                f"10K={k_val}: {bad_slots} resolution rows have outcome_slot_count <> 2"
            )
        run_sql(
            con,
            f"""
            COPY (
                SELECT
                    condition_id,
                    open_yes_price,
                    high_yes_price,
                    low_yes_price,
                    close_yes_price,
                    matched_yes_tokens,
                    matched_usdc,
                    fee_usdc,
                    fill_count,
                    resolved_block,
                    resolved_log_index,
                    outcome_slot_count,
                    payout_numerators
                FROM chunk_rows
                ORDER BY condition_id
            ) TO '{_sql_quote(out_parquet.as_posix())}' (FORMAT PARQUET, COMPRESSION ZSTD)
            """,
            work=work,
        )
        _write_metadata(con, temp_loc.path, m_val, k_val, input_hashes)
        publish_atomically(temp_loc)
        row_count = int(
            con.execute(
                "SELECT COUNT(*) FROM read_parquet(?)",
                [(chunk_dir / "data.parquet").as_posix()],
            ).fetchone()[0]
        )
        log.info(f"10K={k_val}: wrote {row_count:,} condition bars")
        return row_count
    except Exception:
        cleanup_temp(temp_loc)
        raise


def _load_polymarket_conditions(
    con: duckdb.DuckDBPyConnection,
    work: PhaseWork | None = None,
) -> int:
    glob = _sql_quote(f"{TOKEN_MAP}/**/data.parquet")
    run_sql(
        con,
        f"""
        CREATE OR REPLACE TEMP TABLE polymarket_conditions AS
        SELECT DISTINCT condition_id
        FROM read_parquet('{glob}')
        """,
        work=work,
    )
    return int(con.execute("SELECT COUNT(*) FROM polymarket_conditions").fetchone()[0])


def _frontier() -> int:
    none_below = SCRAPE_START_BLOCK - 1
    fills_latest = scan_frontier_1M_10K_folders(
        base_path=FILLS,
        starting_partition=SCRAPE_START_BLOCK,
        tmp_suffix=".tmp",
        cb_progress=lambda _partition: None,
    )
    fills_frontier = partition_end(fills_latest) if fills_latest is not None else none_below
    token_latest = scan_frontier_1M_10K_folders(
        base_path=TOKEN_MAP,
        starting_partition=SCRAPE_START_BLOCK,
        tmp_suffix=".tmp",
        cb_progress=lambda _partition: None,
    )
    token_frontier = partition_end(token_latest) if token_latest is not None else none_below
    raw_frontier = get_sunk_frontier(RAW)
    return min(fills_frontier, token_frontier, raw_frontier)


def main() -> None:
    global _global_con
    parser = argparse.ArgumentParser(description=f"Materialize {DATASET}")
    parser.add_argument("--dry-run", action="store_true", help="print work plan without writing")
    parser.add_argument("--sample", type=int, default=0, help="process only first N partitions")
    args = parser.parse_args()

    assert_git_clean(_project_root)

    out = RunOutput(DATASET, __file__)
    run_start = time.monotonic()
    none_below = SCRAPE_START_BLOCK - 1
    print_paths(
        out,
        output=OUT_DIR,
        extra=[
            ("raw", RAW),
            ("token_id_map", TOKEN_MAP),
            ("fills", FILLS),
            ("scratch", SCRATCH_DIR),
        ],
    )

    def _handle_sigint(sig, frame):
        _stop_event.set()
        try:
            if _global_con is not None:
                _global_con.interrupt()
        except Exception:
            pass

    signal.signal(signal.SIGINT, _handle_sigint)

    frontier = _frontier()
    all_partitions = enumerate_partitions(SCRAPE_START_BLOCK, frontier)
    latest_landed = scan_frontier_1M_10K_folders(
        base_path=OUT_DIR,
        starting_partition=SCRAPE_START_BLOCK,
        tmp_suffix=".tmp",
        cb_progress=lambda _partition: None,
    )
    self_frontier = partition_end(latest_landed) if latest_landed is not None else none_below
    todo_start = max(SCRAPE_START_BLOCK, self_frontier + 1)
    todo = enumerate_partitions(todo_start, frontier)
    already_landed = len(all_partitions) - len(todo)
    if args.sample:
        todo = todo[: args.sample]

    print_work_plan(
        out,
        frontier=frontier,
        total=len(all_partitions),
        already_landed=already_landed,
        todo=len(todo),
        sample=args.sample,
    )
    out.log_only(f"# main — start at {time.strftime('%Y-%m-%dT%H:%M:%SZ', time.gmtime())}")
    out.log_only(f"upstream frontier:    {frontier:,}" if frontier > none_below else "upstream frontier:    none")
    out.log_only(
        f"self frontier:        {self_frontier:,}"
        if self_frontier > none_below
        else "self frontier:        none"
    )
    out.log_only(f"partitions total:     {len(all_partitions):,}")
    out.log_only(f"already landed:       {already_landed:,}")
    out.log_only(f"to process:           {len(todo):,}")
    if args.sample:
        out.log_only(f"sample:               {args.sample:,}")

    if args.dry_run:
        if todo:
            first_m, first_k = todo[0]
            last_m, last_k = todo[-1]
            out.print(f"dry-run: would process {len(todo):,} partitions")
            out.print(f"  first: 1M={first_m} 10K={first_k}")
            out.print(f"  last:  1M={last_m} 10K={last_k}")
        else:
            out.print("dry-run: nothing to do")
        return

    if not todo:
        print_run_summary(
            out,
            status="nothing to do",
            elapsed=time.monotonic() - run_start,
            partitions_done=0,
            rows_done=0,
            self_frontier=self_frontier,
            upstream_frontier=frontier,
            none_below=none_below,
        )
        return

    con = duckdb.connect()
    _global_con = con
    con.execute(f"SET temp_directory = '{_sql_quote(SCRATCH_DIR)}'")
    con.execute("SET preserve_insertion_order = false")
    configure_duckdb_progress(con)

    processed = 0
    rows_done = 0
    status = "OK"
    with out.status:
        out.status.total(0, len(todo))
        load = PhaseWork(out.status, "Loading Polymarket conditions")
        load.phase()
        t_load = time.monotonic()
        n_cond = _load_polymarket_conditions(con, work=load)
        out.print(f"loaded {n_cond:,} Polymarket condition_ids in {time.monotonic() - t_load:.1f}s")

        for m_val, k_val in todo:
            if _stop_event.is_set():
                status = "interrupted"
                out.log_only("interrupted by user")
                break
            work = PhaseWork(out.status, f"10K={k_val:,}")
            work.phase("building")
            t0 = time.monotonic()
            row_count = process_chunk(con, m_val, k_val, out.log, work=work)
            print_partition_sunk(out, k_val, row_count, time.monotonic() - t0)
            processed += 1
            rows_done += row_count
            out.status.total(processed, len(todo))
        out.status.clear_partition()
    if _stop_event.is_set():
        status = "interrupted"

    if processed:
        self_frontier = partition_end(todo[processed - 1][1])

    print_run_summary(
        out,
        status=status,
        elapsed=time.monotonic() - run_start,
        partitions_done=processed,
        rows_done=rows_done,
        self_frontier=self_frontier,
        upstream_frontier=frontier,
        none_below=none_below,
    )


if __name__ == "__main__":
    main()
