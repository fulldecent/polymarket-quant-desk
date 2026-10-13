#!/usr/bin/env python3
"""
Materializes the token_id_map_v2 derived table.

Maps (collateral_token, parent_collection_id, condition_id, index_set) → token_id
for every outcome token that trades on a Polymarket exchange.

METHOD
------
A ConditionalTokens split/merge is treated as Polymarket-related only when it
shares a transaction with an exchange ``orders_matched`` event (v1 or v2,
standard or NegRisk). Such a split/merge carries the collateral, parent
collection, condition, and partition (index sets) that fully determine each
token_id, so token IDs are derived directly with no collateral-resolution
heuristic and no dependency on ``token_registered``. Partitions are processed in
ascending block order; a 4-tuple is materialized in the first UTC day in
which it is discovered and suppressed thereafter.

OUTPUT
------
Partitioned parquet at:

    {TOKEN_ID_MAP_V2_DIR}/year=YYYY/month=MM/day=DD/data.parquet
    {TOKEN_ID_MAP_V2_DIR}/year=YYYY/month=MM/day=DD/metadata.json

See DATA_DICTIONARY.md for full schema and invariant documentation.

REQUIRED ENV VARS
-----------------
    POLYGON_CONTRACT_EVENTS_V4_DIR   root of raw {contract}/{event}/... parquet
    TOKEN_ID_MAP_V2_DIR              output directory
    SCRATCH_DIR                      DuckDB spill directory; safe to wipe between runs

USAGE
-----
    python derived_data/token_id_map_v2/main.py [options]

    --dry-run       print the work plan without writing any data
    --sample N      process only the first N incomplete days
"""

import argparse
import logging
import os
import signal
import sys
import threading
import time
from pathlib import Path

import duckdb
import pyarrow as pa
import pyarrow.parquet as pq
from dotenv import load_dotenv

_project_root = Path(__file__).resolve().parent.parent.parent
load_dotenv(_project_root / ".env")

sys.path.insert(0, str(_project_root))
from lib.ct_helpers import get_collection_id, get_position_id  # noqa: E402
from lib.env import require_directory_env, require_env  # noqa: E402
from lib.git_utils import assert_git_clean  # noqa: E402
from lib.metadata_utils import create_parquet_metadata_json, parquet_content_hash  # noqa: E402
from lib.derived_frontier import scan_frontier_ymd_folders  # noqa: E402
from lib.ymd_partition import (  # noqa: E402
    assert_contiguous_day_keys,
    day_block_span,
    day_dir,
    day_key_parts,
    day_label,
    iter_manifest_day_keys,
)

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
    create_temp_location,
    publish_atomically,
    cleanup_temp,
)
from raw_data.polygon_contract_events_v4 import SCRAPE_START_BLOCK  # noqa: E402

# ============================================================================
# constants
# ============================================================================

# Exchange orders_matched event tables (v1 + v2, standard + NegRisk). A
# ConditionalTokens split/merge is treated as Polymarket-related only when it
# shares a transaction with one of these matches.
ORDERS_MATCHED_TABLES = (
    "CTFExchange/orders_matched",
    "CTFExchangeV2/orders_matched",
    "NegRiskCtfExchange/orders_matched",
    "NegRiskCtfExchangeV2/orders_matched",
)

# ConditionalTokens split/merge event tables. Each row carries the collateral,
# parent collection, condition, and partition (index sets) needed to derive a
# token_id directly.
#
# Both split AND merge are scanned. On-chain, a merge can only burn tokens that a
# prior split (or NegRisk conversion) minted, so every merged token was split at
# some point. That does NOT make merges redundant here: discovery is restricted
# to CT operations that share a transaction with an exchange match ("trade-linked"),
# and a token's split can be non-trade-linked (e.g. a market maker calling
# splitPosition directly) while its first trade-linked CT operation is a merge.
# Empirically, scanning splits only would miss 10 such 4-tuples (5 conditions x
# index sets 1/2) across the dataset, so merges are required for complete coverage.
CT_SPLIT_MERGE_TABLES = (
    "ConditionalTokens/position_split",
    "ConditionalTokens/positions_merge",
)

# NegRiskAdapter address (the oracle of every NegRisk condition). When a condition
# is prepared via NegRiskAdapter.prepareQuestion it calls
# ctf.prepareCondition(address(this), questionId, 2), so the ConditionalTokens
# condition_preparation row carries oracle == this address and question_id == the
# NegRisk questionId. The market_id is then questionId with its final byte cleared
# (NegRiskIdLib.getMarketId: questionId & ~0xFF). Non-NegRisk (e.g. UMA) conditions
# have a different oracle and therefore no market_id.
NEG_RISK_ADAPTER = bytes.fromhex("d91e80cf2e7be2e162c6513ced06f1dd0da35296")

# Output Parquet schema. Types MUST match the raw polygon_contract_events_v4
# logical Parquet types so the columns join cleanly:
#   BLOB  -> Parquet BYTE_ARRAY (no logical type, variable length) == pa.binary()
#            (NOT FIXED_LEN_BYTE_ARRAY; do not use pa.binary(20)/pa.binary(32))
#   UINTEGER -> Parquet INT(bitWidth=32, isSigned=false) == pa.uint32()
# market_id is nullable: it is populated only for NegRisk conditions and is NULL
# for standard (binary CTF / UMA) conditions.
_OUTPUT_SCHEMA = pa.schema([
    pa.field("token_id",              pa.binary()),
    pa.field("collateral_token",      pa.binary()),
    pa.field("parent_collection_id",  pa.binary()),
    pa.field("condition_id",          pa.binary()),
    pa.field("index_set",             pa.uint32()),
    pa.field("market_id",             pa.binary(), nullable=True),
])

# Global stop event used by the SIGINT handler and long-running loops
_stop_event = threading.Event()
# Global connection reference so the SIGINT handler can interrupt running DuckDB queries
_global_con: duckdb.DuckDBPyConnection | None = None


# ============================================================================
# configuration
# ============================================================================

RAW      = require_env("POLYGON_CONTRACT_EVENTS_V4_DIR")
OUT_DIR  = require_env("TOKEN_ID_MAP_V2_DIR")
SCRATCH_DIR = require_directory_env("SCRATCH_DIR")


# ============================================================================
# provenance helpers
# ============================================================================

def _write_metadata(
    con: duckdb.DuckDBPyConnection,
    chunk_dir: Path,
    day_key: int,
    min_block: int,
    max_block: int,
    input_hashes: dict[str, str],
    log: logging.Logger,
) -> None:
    """Write metadata.json per docs/Metadata files.md schema."""
    part = chunk_dir / "data.parquet"
    if not part.exists():
        raise RuntimeError(f"data.parquet not found at {part} after write; cannot create metadata")
    year, month, day = day_key_parts(day_key)
    create_parquet_metadata_json(
        part,
        dataset="token_id_map_v2",
        source_script="derived_data/token_id_map_v2/main.py",
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


# ============================================================================
# token ID computation and market_id resolution
# ============================================================================

def _load_negrisk_market_lookup(
    con: duckdb.DuckDBPyConnection,
    work: PhaseWork | None = None,
) -> dict[bytes, bytes]:
    """Return condition_id -> market_id for every NegRisk condition.

    A condition is NegRisk iff its ConditionalTokens condition_preparation row was
    emitted with oracle == NegRiskAdapter (set at NegRiskAdapter.sol prepareQuestion
    via ctf.prepareCondition(address(this), questionId, 2)). For those rows the
    market_id is the registered questionId with its final byte cleared
    (NegRiskIdLib.getMarketId: questionId & ~0xFF). condition_preparation has exactly
    one row per condition_id (verified across the dataset), so this is a function.
    Non-NegRisk conditions are absent from the result and map to NULL market_id.
    """
    keys = iter_manifest_day_keys(RAW)
    assert_contiguous_day_keys(keys)
    paths = _existing_event_paths(
        ("ConditionalTokens/condition_preparation",),
        keys,
    )
    if not paths:
        return {}
    rows = run_sql(con, f"""
        SELECT condition_id, question_id
        FROM read_parquet({_parquet_list_literal(paths)})
        WHERE oracle = ?
    """, [NEG_RISK_ADAPTER], work=work).fetchall()
    lookup: dict[bytes, bytes] = {}
    for condition_id, question_id in rows:
        q = bytes(question_id)
        # Clear the final byte: market_id = questionId & ~0xFF.
        lookup[bytes(condition_id)] = q[:31] + b"\x00"
    return lookup


def _existing_event_paths(tables: tuple[str, ...], day_keys: list[int] | int) -> list[str]:
    """Return data.parquet paths that exist for these tables on these UTC days.

    Only days passed in are considered. Callers pass manifested day keys so a
    raw day without ``_SUCCESS`` is never read.
    """
    keys = [day_keys] if isinstance(day_keys, int) else day_keys
    paths: list[str] = []
    for day_key in keys:
        rel = day_dir(day_key)
        for table in tables:
            p = Path(RAW) / table / rel / "data.parquet"
            if p.exists():
                paths.append(str(p))
    return paths


def _parquet_list_literal(paths: list[str]) -> str:
    """Render a DuckDB list literal of single-quoted, escaped file paths."""
    quoted = ["'" + p.replace("'", "''") + "'" for p in paths]
    return "[" + ", ".join(quoted) + "]"


def _day_input_hashes(day_key: int) -> dict[str, str]:
    """SHA-256 of every raw input file read to build this UTC day.

    Covers the ``orders_matched`` filter tables and the ConditionalTokens
    split/merge row-source tables for this day — the files that fully
    determine which token tuples are discovered here. Keys are paths relative to
    the raw root so the provenance is portable across storage locations.
    """
    hashes: dict[str, str] = {}
    tables = ORDERS_MATCHED_TABLES + CT_SPLIT_MERGE_TABLES
    for path in _existing_event_paths(tables, day_key):
        rel = os.path.relpath(path, RAW)
        hashes[rel] = parquet_content_hash(Path(path))
    return hashes


def _discover_day_tuples(
    con: duckdb.DuckDBPyConnection,
    day_key: int,
    work: PhaseWork | None = None,
) -> list[tuple[bytes, bytes, bytes, int]]:
    """Discover the token grain tuples introduced by trades on this UTC day.

    A ConditionalTokens split/merge is Polymarket-related when it shares a
    transaction with an exchange ``orders_matched`` event (v1 or v2, standard or
    NegRisk). Both legs live in the same transaction and therefore the same block
    and UTC day, so the join is day-local. Each qualifying split/merge
    carries the collateral, parent collection, condition, and partition (index
    sets) that fully determine a token_id. The returned tuples are distinct and
    not yet filtered against previously seen tuples.
    """
    match_paths = _existing_event_paths(ORDERS_MATCHED_TABLES, day_key)
    ct_paths = _existing_event_paths(CT_SPLIT_MERGE_TABLES, day_key)
    if not match_paths or not ct_paths:
        return []

    rows = run_sql(con, f"""
        WITH match_txs AS (
            SELECT DISTINCT block_number, transaction_index
            FROM read_parquet({_parquet_list_literal(match_paths)})
        ),
        ct_ops AS (
            SELECT
                collateral_token,
                parent_collection_id,
                condition_id,
                CAST(unnest(CAST(json(partition) AS BIGINT[])) AS UINTEGER) AS index_set
            FROM read_parquet({_parquet_list_literal(ct_paths)})
            WHERE (block_number, transaction_index) IN (SELECT block_number, transaction_index FROM match_txs)
        )
        SELECT DISTINCT collateral_token, parent_collection_id, condition_id, index_set
        FROM ct_ops
        WHERE index_set > 0
    """, work=work).fetchall()
    return [(bytes(r[0]), bytes(r[1]), bytes(r[2]), int(r[3])) for r in rows]


def _build_output_table(
    rows: list[tuple[bytes, bytes, bytes, bytes, int, bytes | None]]
) -> pa.Table:
    """Build the output Arrow table from grain-sorted rows (valid for an empty list)."""
    return pa.table({
        "token_id": pa.array([r[0] for r in rows], type=pa.binary()),
        "collateral_token": pa.array([r[1] for r in rows], type=pa.binary()),
        "parent_collection_id": pa.array([r[2] for r in rows], type=pa.binary()),
        "condition_id": pa.array([r[3] for r in rows], type=pa.binary()),
        "index_set": pa.array([r[4] for r in rows], type=pa.uint32()),
        "market_id": pa.array([r[5] for r in rows], type=pa.binary()),
    }, schema=_OUTPUT_SCHEMA)


def process_chunk(
    con: duckdb.DuckDBPyConnection,
    day_key: int,
    min_block: int,
    max_block: int,
    *,
    condition_to_market: dict[bytes, bytes],
    log: logging.Logger,
    work: PhaseWork | None = None,
) -> tuple[int, dict[str, str]]:
    """Process one UTC day: discover new tuples, compute token_ids, write atomically.

    Every day ALWAYS produces an output (data.parquet + metadata.json), even
    when there are zero new tokens. The first-seen rule suppresses any 4-tuple that
    was already materialized on an earlier day, so a day may legitimately
    contain zero rows. Newly written tuples are inserted into seen_tuples so that
    subsequent days in the same run honour the first-seen invariant.
    """
    chunk_dir = Path(OUT_DIR) / day_dir(day_key)
    if chunk_dir.exists():
        sys.exit(
            f"FATAL: refusing to overwrite immutable partition: {chunk_dir}"
        )
    chunk_dir.parent.mkdir(parents=True, exist_ok=True)

    # Discover candidate grain tuples from this day's trade-linked CT ops,
    # then keep only those not already materialized on an earlier day.
    if work is not None:
        work.phase("discovering")
    candidates = _discover_day_tuples(con, day_key, work=work)
    new_grain: list[tuple[bytes, bytes, bytes, int]] = []
    if candidates:
        con.register("candidates", pa.table({
            "collateral_token": pa.array([c[0] for c in candidates], type=pa.binary()),
            "parent_collection_id": pa.array([c[1] for c in candidates], type=pa.binary()),
            "condition_id": pa.array([c[2] for c in candidates], type=pa.binary()),
            "index_set": pa.array([c[3] for c in candidates], type=pa.uint32()),
        }))
        new_rows = run_sql(con, """
            SELECT
                c.collateral_token,
                c.parent_collection_id,
                c.condition_id,
                c.index_set
            FROM candidates c
            LEFT JOIN seen_tuples s
              ON c.collateral_token = s.collateral_token
             AND c.parent_collection_id = s.parent_collection_id
             AND c.condition_id = s.condition_id
             AND c.index_set = s.index_set
            WHERE s.condition_id IS NULL
        """, work=work).fetchall()
        con.unregister("candidates")
        new_grain = [(bytes(r[0]), bytes(r[1]), bytes(r[2]), int(r[3])) for r in new_rows]

    # Derive the token_id for each new tuple (cached EC math in ct_helpers) and
    # attach market_id (NULL for non-NegRisk conditions).
    if work is not None:
        work.phase("computing")
    out_rows = [
        (
            get_position_id(
                collateral, get_collection_id(parent, condition, index_set)
            ).to_bytes(32, "big"),
            collateral,
            parent,
            condition,
            index_set,
            condition_to_market.get(condition),
        )
        for (collateral, parent, condition, index_set) in new_grain
    ]
    # Output contract: rows are sorted ascending by token_id.
    out_rows.sort(key=lambda row: row[0])
    new_table = _build_output_table(out_rows)
    row_count = new_table.num_rows

    # Atomic write — ALWAYS, even for zero rows.
    temp_loc = create_temp_location(
        parent_dir=chunk_dir.parent,
        final_name=chunk_dir.name,
        temp_suffix=".tmp",
    )
    if work is not None:
        work.phase("hashing")
    input_hashes = _day_input_hashes(day_key)
    try:
        if work is not None:
            work.phase("writing")
        pq.write_table(new_table, temp_loc.path / "data.parquet", compression="zstd")
        _write_metadata(con, temp_loc.path, day_key, min_block, max_block, input_hashes, log)
        publish_atomically(temp_loc)
        # Record newly materialized tuples so later partitions suppress duplicates.
        if row_count:
            con.register("new_rows", new_table)
            run_sql(con, """
                INSERT INTO seen_tuples
                SELECT collateral_token, parent_collection_id, condition_id, index_set
                FROM new_rows
            """, work=work)
            con.unregister("new_rows")
        log.info(f"{day_label(day_key)}: wrote {row_count:,} token mappings")
        return row_count, input_hashes
    except Exception:
        cleanup_temp(temp_loc)
        raise


# ============================================================================
# main
# ============================================================================

def _load_seen_tuples(con: duckdb.DuckDBPyConnection, work: PhaseWork | None = None) -> int:
    """Load all previously materialized token_id_map_v2 partitions into a temp table.

    Called once at startup. Subsequent partitions suppress duplicates against this
    table (first-seen rule); process_chunk inserts newly written tuples as it goes.

    Returns the number of rows in the first-seen table (0 on a first run).
    """
    glob = f"{OUT_DIR}/year=*/month=*/day=*/data.parquet"
    run_sql(con, """
        CREATE OR REPLACE TEMP TABLE seen_tuples (
            collateral_token BLOB,
            parent_collection_id BLOB,
            condition_id BLOB,
            index_set UINTEGER
        )
    """, work=work)
    try:
        run_sql(con, f"""
            INSERT INTO seen_tuples
            SELECT DISTINCT collateral_token, parent_collection_id, condition_id, index_set
            FROM read_parquet('{glob}')
        """, work=work)
    except duckdb.IOException:
        # No output files exist yet (first run) — leave the table empty.
        pass
    return int(con.execute("SELECT COUNT(*) FROM seen_tuples").fetchone()[0])


def _manifest_days() -> list[int]:
    keys = iter_manifest_day_keys(RAW)
    assert_contiguous_day_keys(keys)
    return keys


def _print_day_sunk(out: RunOutput, day_key: int, rows: int, elapsed: float) -> None:
    out.print(f"  -> day {day_label(day_key)}  rows {rows:,}  {elapsed:.1f}s")


def main() -> None:
    global _global_con
    parser = argparse.ArgumentParser(description="Materialize token_id_map_v2")
    parser.add_argument("--dry-run", action="store_true", help="print work plan without writing")
    parser.add_argument("--sample", type=int, default=0, help="process only first N days")
    args = parser.parse_args()

    assert_git_clean(_project_root)

    out = RunOutput("token_id_map_v2", __file__)
    run_start = time.monotonic()
    none_below = SCRAPE_START_BLOCK - 1
    print_paths(
        out,
        output=OUT_DIR,
        extra=[
            ("raw", RAW),
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

    # Every contiguous manifested UTC day is one output folder, including a day
    # with no source events (zero-row data.parquet + metadata.json).
    available = _manifest_days()
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
        f"upstream frontier:    {frontier:,}"
        if frontier > none_below
        else "upstream frontier:    none"
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
    con.execute(f"SET temp_directory = '{SCRATCH_DIR}'")
    configure_duckdb_progress(con)

    processed = 0
    rows_done = 0
    status = "OK"
    with out.status:
        out.status.total(0, len(todo))
        seen_work = PhaseWork(out.status, "Loading seen tuples")
        seen_work.phase()
        t_load = time.monotonic()
        n_seen = _load_seen_tuples(con, work=seen_work)
        out.print(f"loaded {n_seen:,} seen tuples in {time.monotonic() - t_load:.1f}s")

        nr_work = PhaseWork(out.status, "Loading NegRisk market lookup")
        nr_work.phase()
        t_load = time.monotonic()
        condition_to_market = _load_negrisk_market_lookup(con, work=nr_work)
        out.print(
            f"loaded {len(condition_to_market):,} NegRisk condition->market mappings "
            f"in {time.monotonic() - t_load:.1f}s"
        )

        for day_key in todo:
            if _stop_event.is_set():
                status = "interrupted"
                out.log_only("interrupted by user")
                break
            work = PhaseWork(out.status, day_label(day_key))
            work.phase("discovering")
            t0 = time.monotonic()
            min_block, max_block = day_block_span(RAW, day_key)
            row_count, _ = process_chunk(
                con,
                day_key,
                min_block,
                max_block,
                condition_to_market=condition_to_market,
                log=out.log,
                work=work,
            )
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
