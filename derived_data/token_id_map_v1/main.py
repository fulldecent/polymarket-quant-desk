#!/usr/bin/env python3
"""
Materializes the token_id_map_v1 derived table.

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
ascending block order; a 4-tuple is materialized in the first 10K partition in
which it is discovered and suppressed thereafter.

OUTPUT
------
Partitioned parquet at:

    {TOKEN_ID_MAP_V1_DIR}/1M={N}/10K={K}/data.parquet
    {TOKEN_ID_MAP_V1_DIR}/1M={N}/10K={K}/metadata.json

See DATA_DICTIONARY.md for full schema and invariant documentation.

REQUIRED ENV VARS
-----------------
    POLYGON_CONTRACT_EVENTS_V3_DIR   root of raw {contract}/{event}/... parquet
    TOKEN_ID_MAP_V1_DIR              output directory
    TEMP_DIR                         DuckDB spill directory

USAGE
-----
    python derived_data/token_id_map_v1/main.py [options]

    --dry-run       print the work plan without writing any data
    --sample N      process only the first N incomplete chunks
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
from rich.console import Console

_project_root = Path(__file__).resolve().parent.parent.parent
load_dotenv(_project_root / ".env")

sys.path.insert(0, str(_project_root))
from lib.ct_helpers import get_collection_id, get_position_id  # noqa: E402
from lib.env import require_env  # noqa: E402
from lib.git_utils import assert_git_clean  # noqa: E402
from lib.metadata_utils import create_parquet_metadata_json, parquet_content_hash  # noqa: E402
from lib.partition_utils import (  # noqa: E402
    PARTITION_10K_LABEL,
    PARTITION_1M_LABEL,
    enumerate_partitions,
    partition_dir,
    partition_end,
    partition_start,
)
from lib.derived_frontier import scan_frontier_1M_10K_folders  # noqa: E402

from lib.run_logging import make_progress, setup_logging  # noqa: E402
from lib.atomic_publish import (  # noqa: E402
    create_temp_location,
    publish_atomically,
    cleanup_temp,
)
from raw_data.polygon_contract_events_v3 import get_sunk_frontier, SCRAPE_START_BLOCK  # noqa: E402

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

# The first 10K partition Polymarket data can occupy. Every consecutive 10K
# partition from here up to the frontier is materialized, with no gaps. Even a
# partition with zero new tokens gets a (possibly zero-row) data.parquet and a
# metadata.json. This matches SCRAPE_START_BLOCK of polygon_contract_events_v3.
START_PARTITION_10K = partition_start(SCRAPE_START_BLOCK)

# Output Parquet schema. Types MUST match the raw polygon_contract_events_v3
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

console = Console()

# Global stop event used by the SIGINT handler and long-running loops
_stop_event = threading.Event()
# Global connection reference so the SIGINT handler can interrupt running DuckDB queries
_global_con: duckdb.DuckDBPyConnection | None = None


# ============================================================================
# configuration
# ============================================================================

RAW      = require_env("POLYGON_CONTRACT_EVENTS_V3_DIR")
OUT_DIR  = require_env("TOKEN_ID_MAP_V1_DIR")
TEMP_DIR = require_env("TEMP_DIR")


# ============================================================================
# provenance helpers
# ============================================================================

def _write_metadata(
    con: duckdb.DuckDBPyConnection,
    chunk_dir: Path,
    m_val: int,
    k_val: int,
    input_hashes: dict[str, str],
    log: logging.Logger,
) -> None:
    """Write metadata.json per docs/Metadata files.md schema."""
    part = chunk_dir / "data.parquet"
    if not part.exists():
        raise RuntimeError(f"data.parquet not found at {part} after write; cannot create metadata")

    create_parquet_metadata_json(
        part,
        dataset="token_id_map_v1",
        source_script="derived_data/token_id_map_v1/main.py",
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


# ============================================================================
# token ID computation and market_id resolution
# ============================================================================

def _load_negrisk_market_lookup(con: duckdb.DuckDBPyConnection) -> dict[bytes, bytes]:
    """Return condition_id -> market_id for every NegRisk condition.

    A condition is NegRisk iff its ConditionalTokens condition_preparation row was
    emitted with oracle == NegRiskAdapter (set at NegRiskAdapter.sol prepareQuestion
    via ctf.prepareCondition(address(this), questionId, 2)). For those rows the
    market_id is the registered questionId with its final byte cleared
    (NegRiskIdLib.getMarketId: questionId & ~0xFF). condition_preparation has exactly
    one row per condition_id (verified across the dataset), so this is a function.
    Non-NegRisk conditions are absent from the result and map to NULL market_id.
    """
    glob = f"{RAW}/ConditionalTokens/condition_preparation/**/data.parquet"
    rows = con.execute(f"""
        SELECT condition_id, question_id
        FROM read_parquet('{glob}')
        WHERE oracle = ?
    """, [NEG_RISK_ADAPTER]).fetchall()
    lookup: dict[bytes, bytes] = {}
    for condition_id, question_id in rows:
        q = bytes(question_id)
        # Clear the final byte: market_id = questionId & ~0xFF.
        lookup[bytes(condition_id)] = q[:31] + b"\x00"
    return lookup


def _existing_partition_paths(tables: tuple[str, ...], k_val: int) -> list[str]:
    """Return the data.parquet paths that exist for these tables in this 10K partition."""
    paths: list[str] = []
    for table in tables:
        p = Path(RAW) / table / partition_dir(k_val) / "data.parquet"
        if p.exists():
            paths.append(str(p))
    return paths


def _parquet_list_literal(paths: list[str]) -> str:
    """Render a DuckDB list literal of single-quoted, escaped file paths."""
    quoted = ["'" + p.replace("'", "''") + "'" for p in paths]
    return "[" + ", ".join(quoted) + "]"


def _partition_input_hashes(k_val: int) -> dict[str, str]:
    """SHA-256 of every raw input partition file read to build this partition.

    Covers the ``orders_matched`` filter tables and the ConditionalTokens
    split/merge row-source tables for this 10K partition — the files that fully
    determine which token tuples are discovered here. Keys are paths relative to
    the raw root so the provenance is portable across storage locations.
    """
    hashes: dict[str, str] = {}
    for path in _existing_partition_paths(ORDERS_MATCHED_TABLES + CT_SPLIT_MERGE_TABLES, k_val):
        rel = os.path.relpath(path, RAW)
        hashes[rel] = parquet_content_hash(Path(path))
    return hashes


def _discover_partition_tuples(
    con: duckdb.DuckDBPyConnection, k_val: int
) -> list[tuple[bytes, bytes, bytes, int]]:
    """Discover the token grain tuples introduced by trades in this 10K partition.

    A ConditionalTokens split/merge is Polymarket-related when it shares a
    transaction with an exchange ``orders_matched`` event (v1 or v2, standard or
    NegRisk). Both legs live in the same transaction and therefore the same block
    and partition, so the join is partition-local. Each qualifying split/merge
    carries the collateral, parent collection, condition, and partition (index
    sets) that fully determine a token_id. The returned tuples are distinct and
    not yet filtered against previously seen tuples.
    """
    match_paths = _existing_partition_paths(ORDERS_MATCHED_TABLES, k_val)
    ct_paths = _existing_partition_paths(CT_SPLIT_MERGE_TABLES, k_val)
    if not match_paths or not ct_paths:
        return []

    rows = con.execute(f"""
        WITH match_txs AS (
            SELECT DISTINCT transaction_hash
            FROM read_parquet({_parquet_list_literal(match_paths)})
        ),
        ct_ops AS (
            SELECT
                collateral_token,
                parent_collection_id,
                condition_id,
                CAST(unnest(CAST(json(partition) AS BIGINT[])) AS UINTEGER) AS index_set
            FROM read_parquet({_parquet_list_literal(ct_paths)})
            WHERE transaction_hash IN (SELECT transaction_hash FROM match_txs)
        )
        SELECT DISTINCT collateral_token, parent_collection_id, condition_id, index_set
        FROM ct_ops
        WHERE index_set > 0
    """).fetchall()
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
    m_val: int,
    k_val: int,
    *,
    condition_to_market: dict[bytes, bytes],
    log: logging.Logger,
) -> tuple[int, dict[str, str]]:
    """Process one 10K partition: discover new tuples, compute token_ids, write atomically.

    Every partition ALWAYS produces an output (data.parquet + metadata.json), even
    when there are zero new tokens. The first-seen rule suppresses any 4-tuple that
    was already materialized in an earlier partition, so a partition may legitimately
    contain zero rows. Newly written tuples are inserted into seen_tuples so that
    subsequent partitions in the same run honour the first-seen invariant.
    """
    chunk_dir = Path(OUT_DIR) / partition_dir(k_val)
    if chunk_dir.exists():
        sys.exit(
            f"FATAL: refusing to overwrite immutable partition: {chunk_dir}"
        )
    chunk_dir.parent.mkdir(parents=True, exist_ok=True)

    # Discover candidate grain tuples from this partition's trade-linked CT ops,
    # then keep only those not already materialized in an earlier partition.
    candidates = _discover_partition_tuples(con, k_val)
    new_grain: list[tuple[bytes, bytes, bytes, int]] = []
    if candidates:
        con.register("candidates", pa.table({
            "collateral_token": pa.array([c[0] for c in candidates], type=pa.binary()),
            "parent_collection_id": pa.array([c[1] for c in candidates], type=pa.binary()),
            "condition_id": pa.array([c[2] for c in candidates], type=pa.binary()),
            "index_set": pa.array([c[3] for c in candidates], type=pa.uint32()),
        }))
        new_rows = con.execute("""
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
        """).fetchall()
        con.unregister("candidates")
        new_grain = [(bytes(r[0]), bytes(r[1]), bytes(r[2]), int(r[3])) for r in new_rows]

    # Derive the token_id for each new tuple (cached EC math in ct_helpers) and
    # attach market_id (NULL for non-NegRisk conditions).
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
    chunk_name = chunk_dir.name  # e.g., "10K=33600000"
    temp_loc = create_temp_location(
        parent_dir=chunk_dir.parent,
        final_name=chunk_name,
        temp_suffix=".tmp",
    )
    input_hashes = _partition_input_hashes(k_val)
    try:
        pq.write_table(new_table, temp_loc.path / "data.parquet", compression="zstd")
        _write_metadata(con, temp_loc.path, m_val, k_val, input_hashes, log)
        publish_atomically(temp_loc)
        # Record newly materialized tuples so later partitions suppress duplicates.
        if row_count:
            con.register("new_rows", new_table)
            con.execute("""
                INSERT INTO seen_tuples
                SELECT collateral_token, parent_collection_id, condition_id, index_set
                FROM new_rows
            """)
            con.unregister("new_rows")
        log.info(f"10K={k_val}: wrote {row_count} token mappings")
        return row_count, input_hashes
    except Exception:
        cleanup_temp(temp_loc)
        raise


# ============================================================================
# main
# ============================================================================

def _load_seen_tuples(con: duckdb.DuckDBPyConnection) -> None:
    """Load all previously materialized token_id_map_v1 partitions into a temp table.

    Called once at startup. Subsequent partitions suppress duplicates against this
    table (first-seen rule); process_chunk inserts newly written tuples as it goes.
    """
    glob = f"{OUT_DIR}/**/*.parquet"
    con.execute("""
        CREATE OR REPLACE TEMP TABLE seen_tuples (
            collateral_token BLOB,
            parent_collection_id BLOB,
            condition_id BLOB,
            index_set UINTEGER
        )
    """)
    try:
        con.execute(f"""
            INSERT INTO seen_tuples
            SELECT DISTINCT collateral_token, parent_collection_id, condition_id, index_set
            FROM read_parquet('{glob}')
        """)
    except duckdb.IOException:
        # No output files exist yet (first run) — leave the table empty.
        pass


def main() -> None:
    global _global_con
    parser = argparse.ArgumentParser(description="Materialize token_id_map_v1")
    parser.add_argument("--dry-run", action="store_true", help="print work plan without writing")
    parser.add_argument("--sample", type=int, default=0, help="process only first N partitions")
    args = parser.parse_args()

    assert_git_clean(_project_root)

    log = setup_logging("token_id_map_v1", __file__, console)
    log.info("token_id_map_v1 materializer starting")

    # Install a SIGINT handler for clean interruption.
    def _handle_sigint(sig, frame):
        _stop_event.set()
        try:
            if _global_con is not None:
                _global_con.interrupt()
        except Exception:
            pass

    signal.signal(signal.SIGINT, _handle_sigint)

    # Every consecutive 10K partition from the Polymarket start partition up to the
    # frontier must exist as an output partition (no gaps). Partition discovery is by
    # block range, NOT by source-folder existence: a 10K range with no source events
    # still produces a zero-row data.parquet + metadata.json.
    frontier = get_sunk_frontier(RAW)
    all_partitions = enumerate_partitions(SCRAPE_START_BLOCK, frontier)

    # Discover self frontier via strict contiguous 1M/10K scanner (tmp-aware),
    # then plan only partitions above that frontier.
    latest_landed_partition = scan_frontier_1M_10K_folders(
        base_path=OUT_DIR,
        starting_partition=SCRAPE_START_BLOCK,
        tmp_suffix=".tmp",
        cb_progress=lambda _partition: None,
    )
    self_frontier = (
        partition_end(latest_landed_partition)
        if latest_landed_partition is not None
        else SCRAPE_START_BLOCK - 1
    )
    todo_start = max(SCRAPE_START_BLOCK, self_frontier + 1)
    todo = enumerate_partitions(todo_start, frontier)

    log.info(
        f"frontier={frontier}, self_frontier={self_frontier}, start_partition_10K={START_PARTITION_10K}, "
        f"total={len(all_partitions)}, todo={len(todo)}"
    )

    if args.dry_run:
        for m, k in todo[:10]:
            log.info(f"DRY-RUN would process 1M={m} 10K={k}")
        if len(todo) > 10:
            log.info(f"... and {len(todo) - 10} more")
        return

    if args.sample:
        todo = todo[:args.sample]

    con = duckdb.connect()
    _global_con = con
    con.execute(f"SET temp_directory = '{TEMP_DIR}'")

    # Load the first-seen set once; process_chunk maintains it incrementally.
    _load_seen_tuples(con)

    # Load the condition_id -> market_id lookup once. Attached to NegRisk rows;
    # standard conditions get NULL market_id.
    condition_to_market = _load_negrisk_market_lookup(con)
    log.info(f"loaded {len(condition_to_market)} NegRisk condition->market mappings")

    console.print(
        f"frontier={frontier}  |  total={len(all_partitions):,}  |  "
        f"[green]{len(all_partitions) - len(todo):,} already landed[/green]  |  "
        f"[yellow]{len(todo):,} to process[/yellow]"
    )

    if not todo:
        console.print("[green]Nothing to do.[/green]")
        return

    with make_progress(console) as progress:
        task = progress.add_task("Materializing token_id_map_v1", total=len(todo))

        processed = 0
        for m_val, k_val in todo:
            if _stop_event.is_set():
                log.info("interrupted by user")
                break

            row_count, _ = process_chunk(con, m_val, k_val, condition_to_market=condition_to_market, log=log)
            processed += 1
            progress.update(task, advance=1)

    log.info(f"token_id_map_v1 materializer finished. Processed {processed} partitions.")
    console.print(f"[green]Complete! Processed {processed} partitions.[/green]")


if __name__ == "__main__":
    main()
