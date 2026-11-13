#!/usr/bin/env python3
"""
Materializes the account_condition_by_day_v2 derived table.

Per-UTC-day summary of how one account traded one condition, aggregated
from fills_v2.

OUTPUT
------
Partitioned parquet at:

    {ACCOUNT_CONDITION_BY_DAY_V2_DIR}/year=YYYY/month=MM/day=DD/data.parquet
    {ACCOUNT_CONDITION_BY_DAY_V2_DIR}/year=YYYY/month=MM/day=DD/metadata.json

See DATA_DICTIONARY.md for the full schema and invariant documentation.

USAGE
-----
    python derived_data/account_condition_by_day_v2/main.py [options]

    --dry-run       print the work plan without writing any data
    --sample N      process only the first N incomplete days
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
from lib.run_logging import (  # noqa: E402
    PhaseWork,
    RunOutput,
    configure_duckdb_progress,
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
from lib.derived_frontier import scan_frontier_ymd_folders  # noqa: E402
from lib.ymd_partition import (  # noqa: E402
    assert_contiguous_day_keys,
    day_block_span,
    day_dir,
    day_key_parts,
    day_label,
    iter_manifest_day_keys,
)
from raw_data.polygon_contract_events_v4 import SCRAPE_START_BLOCK  # noqa: E402

DATASET = "account_condition_by_day_v2"
SOURCE_SCRIPT = "derived_data/account_condition_by_day_v2/main.py"

RAW = require_env("POLYGON_CONTRACT_EVENTS_V4_DIR")
FILLS = require_env("FILLS_V2_DIR")
OUT_DIR = require_env("ACCOUNT_CONDITION_BY_DAY_V2_DIR")
SCRATCH_DIR = require_directory_env("SCRATCH_DIR")

_stop_event = threading.Event()
_global_con: duckdb.DuckDBPyConnection | None = None


def _sql_quote(value: str) -> str:
    return value.replace("'", "''")


def _fills_path(day_key: int) -> Path:
    return Path(FILLS) / day_dir(day_key) / "data.parquet"


def _manifest_days() -> list[int]:
    keys = iter_manifest_day_keys(RAW)
    assert_contiguous_day_keys(keys)
    return keys


def _covered_days(roots: list[str]) -> list[int]:
    """Manifested days covered by a contiguous landed prefix of every root."""
    raw_days = _manifest_days()
    if not raw_days:
        return []
    covered = raw_days
    start = raw_days[0]
    for root in roots:
        latest = scan_frontier_ymd_folders(
            base_path=root,
            starting_day_key=start,
            tmp_suffix=".tmp",
            cb_progress=lambda _day_key: None,
        )
        if latest is None:
            return []
        covered = [key for key in covered if key <= latest]
        if not covered:
            return []
    return covered


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


def _day_input_hashes(day_key: int) -> dict[str, str]:
    hashes: dict[str, str] = {}
    path = _fills_path(day_key)
    if path.exists():
        hashes[str(path.relative_to(FILLS))] = parquet_content_hash(path)
    return hashes


def _write_metadata(
    con: duckdb.DuckDBPyConnection,
    chunk_dir: Path,
    day_key: int,
    min_block: int,
    max_block: int,
    input_hashes: dict[str, str],
) -> None:
    part = chunk_dir / "data.parquet"
    if not part.exists():
        raise RuntimeError(f"data.parquet not found at {part} after write; cannot create metadata")
    year, month, day = day_key_parts(day_key)
    create_parquet_metadata_json(
        part,
        dataset=DATASET,
        source_script=SOURCE_SCRIPT,
        input_hashes=input_hashes,
        parameters={
            "year": year,
            "month": month,
            "day": day,
            "min_block": min_block,
            "max_block": max_block,
        },
        row_count_connection=con,
        created_at=time.strftime("%Y-%m-%dT%H:%M:%SZ", time.gmtime()),
    )


def process_chunk(
    con: duckdb.DuckDBPyConnection,
    day_key: int,
    min_block: int,
    max_block: int,
    log: logging.Logger,
    *,
    work: PhaseWork | None = None,
) -> int:
    chunk_dir = Path(OUT_DIR) / day_dir(day_key)
    if chunk_dir.exists():
        sys.exit(f"FATAL: refusing to overwrite immutable partition: {chunk_dir}")
    chunk_dir.parent.mkdir(parents=True, exist_ok=True)

    fills_path = _fills_path(day_key)
    label = day_label(day_key)
    if not fills_path.exists():
        raise RuntimeError(f"fills_v2 day missing for {label}: {fills_path}")

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
    input_hashes = _day_input_hashes(day_key)
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
        _write_metadata(con, temp_loc.path, day_key, min_block, max_block, input_hashes)
        publish_atomically(temp_loc)
        row_count = int(
            con.execute(
                "SELECT COUNT(*) FROM read_parquet(?)",
                [(chunk_dir / "data.parquet").as_posix()],
            ).fetchone()[0]
        )
        log.info(f"{label}: wrote {row_count:,} account-condition rows")
        return row_count
    except Exception:
        cleanup_temp(temp_loc)
        raise


def _print_day_sunk(out: RunOutput, day_key: int, rows: int, elapsed: float) -> None:
    out.print(f"  -> day {day_label(day_key)}  rows {rows:,}  {elapsed:.1f}s")


def main() -> None:
    global _global_con
    parser = argparse.ArgumentParser(description=f"Materialize {DATASET}")
    parser.add_argument("--dry-run", action="store_true", help="print work plan without writing")
    parser.add_argument("--sample", type=int, default=0, help="process only first N days")
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

    available = _covered_days([FILLS])
    frontier = day_block_span(RAW, available[-1])[1] if available else none_below
    todo = [key for key in available if not (Path(OUT_DIR) / day_dir(key)).exists()]
    already_landed = len(available) - len(todo)
    self_latest = (
        scan_frontier_ymd_folders(
            base_path=OUT_DIR,
            starting_day_key=available[0],
            tmp_suffix=".tmp",
            cb_progress=lambda _day_key: None,
        )
        if available
        else None
    )
    self_frontier = day_block_span(RAW, self_latest)[1] if self_latest is not None else none_below
    if args.sample:
        todo = todo[: args.sample]

    print_work_plan(
        out,
        frontier=frontier,
        total=len(available),
        already_landed=already_landed,
        todo=len(todo),
        sample=args.sample,
    )
    out.log_only(f"# main — start at {time.strftime('%Y-%m-%dT%H:%M:%SZ', time.gmtime())}")
    out.log_only(
        f"fills frontier:       {frontier:,}"
        if frontier > none_below
        else "fills frontier:       none"
    )
    out.log_only(
        f"self frontier:        {self_frontier:,}"
        if self_frontier > none_below
        else "self frontier:        none"
    )
    out.log_only(f"days total:           {len(available):,}")
    out.log_only(f"already landed:       {already_landed:,}")
    out.log_only(f"to process:           {len(todo):,}")
    if args.sample:
        out.log_only(f"sample:               {args.sample:,}")

    if args.dry_run:
        if todo:
            out.print(f"dry-run: would process {len(todo):,} days")
            out.print(f"  first: {day_label(todo[0])}")
            out.print(f"  last:  {day_label(todo[-1])}")
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
        for day_key in todo:
            if _stop_event.is_set():
                status = "interrupted"
                out.log_only("interrupted by user")
                break
            work = PhaseWork(out.status, day_label(day_key))
            work.phase("building")
            t0 = time.monotonic()
            min_block, max_block = day_block_span(RAW, day_key)
            row_count = process_chunk(con, day_key, min_block, max_block, out.log, work=work)
            _print_day_sunk(out, day_key, row_count, time.monotonic() - t0)
            processed += 1
            rows_done += row_count
            out.status.total(processed, len(todo))
        out.status.clear_partition()
    if _stop_event.is_set():
        status = "interrupted"

    if processed:
        self_frontier = day_block_span(RAW, todo[processed - 1])[1]

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
