#!/usr/bin/env python3
"""
Materializes the account_condition_by_10k_v1 derived table.

Per-partition summary of how one account traded one condition, aggregated
from fills_v1.

OUTPUT
------
Partitioned parquet at:

    {ACCOUNT_CONDITION_BY_10K_V1_DIR}/1M={N}/10K={K}/data.parquet
    {ACCOUNT_CONDITION_BY_10K_V1_DIR}/1M={N}/10K={K}/metadata.json

See DATA_DICTIONARY.md for the full schema and invariant documentation.

USAGE
-----
    python derived_data/account_condition_by_10k_v1/main.py [options]

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
from raw_data.polygon_contract_events_v3 import SCRAPE_START_BLOCK  # noqa: E402

DATASET = "account_condition_by_10k_v1"
SOURCE_SCRIPT = "derived_data/account_condition_by_10k_v1/main.py"

FILLS = require_env("FILLS_V1_DIR")
OUT_DIR = require_env("ACCOUNT_CONDITION_BY_10K_V1_DIR")
SCRATCH_DIR = require_directory_env("SCRATCH_DIR")

_stop_event = threading.Event()
_global_con: duckdb.DuckDBPyConnection | None = None


def _sql_quote(value: str) -> str:
    return value.replace("'", "''")


def _fills_path(k_val: int) -> Path:
    return Path(FILLS) / partition_dir(k_val) / "data.parquet"


def aggregate_sql(fills_path: Path) -> str:
    path = _sql_quote(fills_path.as_posix())
    return f"""
        SELECT
            account,
            condition_id,
            CAST(count(*) AS UINTEGER) AS fill_count,
            CAST(sum(abs(gross_usdc)) AS UBIGINT) AS volume_usdc,
            CAST(sum(abs(net_yes_tokens)) AS UBIGINT) AS volume_yes_tokens,
            CAST(sum(net_yes_tokens) AS BIGINT) AS net_yes_tokens,
            CAST(sum(fee_usdc) AS UBIGINT) AS fee_usdc,
            CAST(min(block_number) AS UINTEGER) AS first_fill_block,
            CAST(max(block_number) AS UINTEGER) AS last_fill_block
        FROM read_parquet('{path}')
        GROUP BY account, condition_id
        ORDER BY account, condition_id
    """


def empty_sql() -> str:
    return """
        SELECT
            CAST(NULL AS BLOB) AS account,
            CAST(NULL AS BLOB) AS condition_id,
            CAST(NULL AS UINTEGER) AS fill_count,
            CAST(NULL AS UBIGINT) AS volume_usdc,
            CAST(NULL AS UBIGINT) AS volume_yes_tokens,
            CAST(NULL AS BIGINT) AS net_yes_tokens,
            CAST(NULL AS UBIGINT) AS fee_usdc,
            CAST(NULL AS UINTEGER) AS first_fill_block,
            CAST(NULL AS UINTEGER) AS last_fill_block
        WHERE FALSE
    """


def _partition_input_hashes(k_val: int) -> dict[str, str]:
    hashes: dict[str, str] = {}
    path = _fills_path(k_val)
    if path.exists():
        hashes[str(path.relative_to(FILLS))] = parquet_content_hash(path)
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

    if work is not None:
        work.phase("building")
    n_src = con.execute(
        f"SELECT COUNT(*) FROM read_parquet('{_sql_quote(fills_path.as_posix())}')"
    ).fetchone()[0]
    sql = aggregate_sql(fills_path) if n_src else empty_sql()

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
            f"COPY ({sql}) TO '{_sql_quote(out_parquet.as_posix())}' "
            f"(FORMAT PARQUET, COMPRESSION ZSTD)",
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
        log.info(f"10K={k_val}: wrote {row_count:,} account-condition rows")
        return row_count
    except Exception:
        cleanup_temp(temp_loc)
        raise


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

    fills_latest = scan_frontier_1M_10K_folders(
        base_path=FILLS,
        starting_partition=SCRAPE_START_BLOCK,
        tmp_suffix=".tmp",
        cb_progress=lambda _partition: None,
    )
    frontier = partition_end(fills_latest) if fills_latest is not None else none_below
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
    out.log_only(f"fills frontier:       {frontier:,}" if frontier > none_below else "fills frontier:       none")
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
