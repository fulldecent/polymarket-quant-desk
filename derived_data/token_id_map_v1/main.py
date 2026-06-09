#!/usr/bin/env python3
"""
Materializes the token_id_map_v1 derived table.

Maps (collateral_token, parent_collection_id, condition_id, index_set) → token_id
for all Polymarket-registered conditions.

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
from datetime import datetime, timezone
from pathlib import Path

import duckdb
import pyarrow as pa
import pyarrow.parquet as pq
from dotenv import load_dotenv
from rich.console import Console
from rich.logging import RichHandler
from rich.progress import (
    BarColumn,
    MofNCompleteColumn,
    Progress,
    SpinnerColumn,
    TaskID,
    TaskProgressColumn,
    TextColumn,
    TimeElapsedColumn,
    TimeRemainingColumn,
)

_project_root = Path(__file__).resolve().parent.parent.parent
load_dotenv(_project_root / ".env")

sys.path.insert(0, str(_project_root))
from lib.ct_helpers import get_collection_id, get_position_id  # noqa: E402
from lib.git_utils import assert_git_clean  # noqa: E402
from lib.metadata_utils import create_parquet_metadata_json, parquet_content_hash  # noqa: E402
from lib.partition_utils import (
    PARTITION_10K_LABEL,
    PARTITION_1M_LABEL,
    partition_dir,
    partition_end,
    partition_start,
)
from lib.atomic_publish import (  # noqa: E402
    create_temp_location,
    publish_atomically,
    cleanup_old_temp_artifacts,
    cleanup_on_failure,
    cleanup_temp,
)
from raw_data.polygon_contract_events_v3 import get_sunk_frontier, SCRAPE_START_BLOCK  # noqa: E402

# ============================================================================
# constants
# ============================================================================

ZERO32 = b"\x00" * 32
ZERO32_HEX = "00" * 32
USDC_E = bytes.fromhex("2791bca1f2de4661ed88a30c99a7a9449aa84174")
USDC_E_HEX = "2791bca1f2de4661ed88a30c99a7a9449aa84174"

# The first 10K partition Polymarket data can occupy. Every consecutive 10K
# partition from here up to the frontier is materialized, with no gaps. Even a
# partition with zero new tokens gets a (possibly zero-row) data.parquet and a
# metadata.json. This matches SCRAPE_START_BLOCK of polygon_contract_events_v3.
START_PARTITION_10K = partition_start(SCRAPE_START_BLOCK)
_PARTITION_10K_SIZE = 10_000
_PARTITION_1M_SIZE = 1_000_000

# Output Parquet schema. Types MUST match the raw polygon_contract_events_v3
# logical Parquet types so the columns join cleanly:
#   BLOB  -> Parquet BYTE_ARRAY (no logical type, variable length) == pa.binary()
#            (NOT FIXED_LEN_BYTE_ARRAY; do not use pa.binary(20)/pa.binary(32))
#   UINTEGER -> Parquet INT(bitWidth=32, isSigned=false) == pa.uint32()
_OUTPUT_SCHEMA = pa.schema([
    pa.field("collateral_token",      pa.binary()),
    pa.field("parent_collection_id",  pa.binary()),
    pa.field("condition_id",          pa.binary()),
    pa.field("index_set",             pa.uint32()),
    pa.field("token_id",              pa.binary()),
])

console = Console()

# Global stop event used by the SIGINT handler and long-running loops
_stop_event = threading.Event()
# Global connection reference so the SIGINT handler can interrupt running DuckDB queries
_global_con: duckdb.DuckDBPyConnection | None = None


# ============================================================================
# configuration
# ============================================================================

def _require_env(name: str) -> str:
    val = os.environ.get(name, "")
    if not val:
        sys.exit(f"{name} is not set. Add it to .env.")
    return val


RAW      = _require_env("POLYGON_CONTRACT_EVENTS_V3_DIR")
OUT_DIR  = _require_env("TOKEN_ID_MAP_V1_DIR")
TEMP_DIR = _require_env("TEMP_DIR")


# ============================================================================
# logging
# ============================================================================

def _setup_logging() -> logging.Logger:
    # One timestamped log file per run, under a logs/ folder next to this script.
    # Filename embeds the run start time in ISO 8601 zulu (basic format, no colons
    # so it is filesystem-safe).
    log_dir = Path(__file__).resolve().parent / "logs"
    log_dir.mkdir(parents=True, exist_ok=True)
    ts = datetime.now(timezone.utc).strftime("%Y-%m-%dT%H%M%SZ")
    log_path = log_dir / f"main-{ts}.log"

    fmt = logging.Formatter(
        "%(asctime)s  %(levelname)-7s  %(message)s",
        datefmt="%Y-%m-%dT%H:%M:%S",
    )
    fh = logging.FileHandler(log_path)
    fh.setLevel(logging.DEBUG)
    fh.setFormatter(fmt)

    # Route console logs through the shared rich Console so they cooperate with
    # the live Progress display (log lines scroll above a pinned progress bar).
    ch = RichHandler(
        console=console,
        show_path=False,
        rich_tracebacks=True,
        omit_repeated_times=False,
    )
    ch.setLevel(logging.INFO)
    ch.setFormatter(logging.Formatter("%(message)s", datefmt="%Y-%m-%dT%H:%M:%S"))

    log = logging.getLogger("token_id_map_v1")
    log.setLevel(logging.DEBUG)
    log.addHandler(fh)
    log.addHandler(ch)
    return log


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
        log.warning(f"data.parquet not found at {part}")
        return

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
# token ID computation (only for CT rows with varying collateral)
# ============================================================================

def _src_path(contract_event: str, m_val: int, k_val: int) -> str | None:
    p = Path(RAW) / contract_event / partition_dir(k_val) / "data.parquet"
    if p.exists():
        return str(p)
    return None


def _load_polymarket_conditions(con: duckdb.DuckDBPyConnection) -> None:
    """Load the set of Polymarket condition_ids from both token_registered tables."""
    ctf = _src_path("CTFExchange/token_registered", 0, 0)  # glob across all
    nr = _src_path("NegRiskCtfExchange/token_registered", 0, 0)
    # Use full glob for the global condition set
    ctf_glob = f"{RAW}/CTFExchange/token_registered/**/data.parquet"
    nr_glob = f"{RAW}/NegRiskCtfExchange/token_registered/**/data.parquet"
    con.execute(f"""
        CREATE OR REPLACE TEMP TABLE polymarket_conditions AS
        SELECT DISTINCT condition_id FROM read_parquet('{ctf_glob}')
        UNION
        SELECT DISTINCT condition_id FROM read_parquet('{nr_glob}')
    """)


def _sql_blob_list(paths: list[str]) -> str:
    quoted = [f"'{path.replace("'", "''")}'" for path in paths]
    return f"[{', '.join(quoted)}]"


def _existing_partition_files(contract_event: str, partitions: list[tuple[int, int]]) -> list[str]:
    paths: list[str] = []
    for m_val, k_val in partitions:
        path = _src_path(contract_event, m_val, k_val)
        if path:
            paths.append(path)
    return paths


def _load_nr_collateral_candidates(
    con: duckdb.DuckDBPyConnection,
    partitions: list[tuple[int, int]],
    *,
    log: logging.Logger,
    progress: Progress | None = None,
    task_id: TaskID | None = None,
) -> None:
    """Load candidate CT collateral tokens for each Polymarket condition.

    NR token_registered rows do not carry collateral. We infer the CTF collateral
    by matching token0/token1 against CT-derived token ids for candidate
    collateral tokens observed in root CT split/merge/redeem events.
    """
    if "polymarket_conditions" not in [r[0] for r in con.execute("SHOW TABLES").fetchall()]:
        _load_polymarket_conditions(con)

    split_files = _existing_partition_files("ConditionalTokens/position_split", partitions)
    merge_files = _existing_partition_files("ConditionalTokens/positions_merge", partitions)
    redeem_files = _existing_partition_files("ConditionalTokens/payout_redemption", partitions)

    con.execute("""
        CREATE OR REPLACE TEMP TABLE nr_collateral_candidates (
            condition_id BLOB,
            collateral_token BLOB
        )
    """)

    source_files = [
        ("position_split", split_files),
        ("positions_merge", merge_files),
        ("payout_redemption", redeem_files),
    ]
    total_files = sum(len(files) for _, files in source_files)
    if total_files == 0:
        log.info("No CT files found for NR collateral candidate resolution")
        return

    loaded = 0
    for label, files in source_files:
        if not files:
            continue
        log.info(f"Loading NR collateral candidates from {label}: {len(files)} partitions")
        for path in files:
            con.execute(f"""
                INSERT INTO nr_collateral_candidates
                SELECT DISTINCT condition_id, collateral_token
                FROM read_parquet('{path.replace("'", "''")}')
                WHERE parent_collection_id = unhex('{ZERO32_HEX}')
                  AND condition_id IN (SELECT condition_id FROM polymarket_conditions)
            """)
            loaded += 1
            if progress is not None and task_id is not None:
                progress.update(
                    task_id,
                    advance=1,
                    description=f"Resolving NR collateral candidates ({loaded}/{total_files})",
                )

    con.execute("""
        CREATE OR REPLACE TEMP TABLE nr_collateral_candidates AS
        SELECT DISTINCT condition_id, collateral_token
        FROM nr_collateral_candidates
    """)
    row_count = con.execute("SELECT COUNT(*) FROM nr_collateral_candidates").fetchone()[0]
    log.info(f"Loaded {row_count} NR collateral candidate rows")


def _resolve_nr_collateral(
    con: duckdb.DuckDBPyConnection,
    condition_id: bytes,
    token0: bytes,
    token1: bytes,
    *,
    k_val: int,
) -> bytes:
    candidates = [
        bytes(row[0])
        for row in con.execute(
            "SELECT collateral_token FROM nr_collateral_candidates WHERE condition_id = ?",
            [condition_id],
        ).fetchall()
    ]

    matches: list[bytes] = []
    for collateral_token in candidates:
        expected_token0 = get_position_id(
            collateral_token,
            get_collection_id(ZERO32, condition_id, 1),
        ).to_bytes(32, "big")
        expected_token1 = get_position_id(
            collateral_token,
            get_collection_id(ZERO32, condition_id, 2),
        ).to_bytes(32, "big")
        if {expected_token0, expected_token1} == {token0, token1}:
            matches.append(collateral_token)

    if len(matches) == 1:
        return matches[0]

    if not matches:
        raise ValueError(
            "Could not resolve NR collateral from raw CT events: "
            f"10K={k_val} condition_id={condition_id.hex()} "
            f"token0={token0.hex()} token1={token1.hex()} "
            f"candidate_collaterals={[c.hex() for c in candidates]}"
        )

    raise ValueError(
        "Ambiguous NR collateral resolution from raw CT events: "
        f"10K={k_val} condition_id={condition_id.hex()} "
        f"token0={token0.hex()} token1={token1.hex()} "
        f"matching_collaterals={[c.hex() for c in matches]}"
    )


def _process_ct_partition(con: duckdb.DuckDBPyConnection, m_val: int, k_val: int) -> list[dict]:
    """Extract unique (collateral, parent, condition, index_set) from CT events in this 10K partition."""
    # Only process if the partition has CT events
    tables = [
        ("ConditionalTokens/position_split", "partition"),
        ("ConditionalTokens/positions_merge", "partition"),
        ("ConditionalTokens/payout_redemption", "index_sets"),
    ]
    tuples: set[tuple[bytes, bytes, bytes, int]] = set()
    for table, col in tables:
        path = _src_path(table, m_val, k_val)
        if not path:
            continue
        # Filter to Polymarket conditions and parent = ZERO32 (per assertion)
        rows = con.execute(f"""
            SELECT DISTINCT
                collateral_token,
                parent_collection_id,
                condition_id,
                CAST(unnest(CAST(json({col}) AS BIGINT[])) AS UINTEGER) AS index_set
            FROM read_parquet('{path}')
            WHERE condition_id IN (SELECT condition_id FROM polymarket_conditions)
              AND parent_collection_id = unhex('{ZERO32_HEX}')
              AND {col} IS NOT NULL
        """).fetchall()
        for r in rows:
            if r[3] > 0:  # index_set must be > 0
                tuples.add((bytes(r[0]), bytes(r[1]), bytes(r[2]), int(r[3])))
    return [
        {
            "collateral_token": t[0],
            "parent_collection_id": t[1],
            "condition_id": t[2],
            "index_set": t[3],
            "token_id": get_position_id(t[0], get_collection_id(t[1], t[2], t[3])).to_bytes(32, "big"),
            "source": "ct",
        }
        for t in tuples
    ]


def _process_nr_partition(con: duckdb.DuckDBPyConnection, m_val: int, k_val: int) -> list[dict]:
    """Emit NR rows by resolving collateral from raw CT activity for each condition."""
    path = _src_path("NegRiskCtfExchange/token_registered", m_val, k_val)
    if not path:
        return []
    rows = con.execute(f"""
        WITH ranked AS (
            SELECT
                token0,
                token1,
                condition_id,
                ROW_NUMBER() OVER (
                    PARTITION BY condition_id
                    ORDER BY block_number, log_index, transaction_hash
                ) AS rn
            FROM read_parquet('{path}')
        )
        SELECT token0, token1, condition_id
        FROM ranked
        WHERE rn = 1
    """).fetchall()
    # Deduplicate at the 4-tuple level: use a dict keyed by (collateral, parent, condition, index_set).
    tuples: dict[tuple[bytes, bytes, bytes, int], dict] = {}
    for token0, token1, cond in rows:
        cond_bytes = bytes(cond)
        collateral_token = _resolve_nr_collateral(con, cond_bytes, bytes(token0), bytes(token1), k_val=k_val)
        tuples[(collateral_token, ZERO32, cond_bytes, 1)] = {
            "collateral_token": collateral_token,
            "parent_collection_id": ZERO32,
            "condition_id": cond_bytes,
            "index_set": 1,
            "token_id": bytes(token0),
            "source": "nr",
        }
        tuples[(collateral_token, ZERO32, cond_bytes, 2)] = {
            "collateral_token": collateral_token,
            "parent_collection_id": ZERO32,
            "condition_id": cond_bytes,
            "index_set": 2,
            "token_id": bytes(token1),
            "source": "nr",
        }
    return list(tuples.values())


def _rows_to_table(rows: list[dict]) -> pa.Table:
    """Build the output Arrow table (works for an empty list too)."""
    return pa.table({
        "collateral_token": pa.array([r["collateral_token"] for r in rows], type=pa.binary()),
        "parent_collection_id": pa.array([r["parent_collection_id"] for r in rows], type=pa.binary()),
        "condition_id": pa.array([r["condition_id"] for r in rows], type=pa.binary()),
        "index_set": pa.array([r["index_set"] for r in rows], type=pa.uint32()),
        "token_id": pa.array([r["token_id"] for r in rows], type=pa.binary()),
    }, schema=_OUTPUT_SCHEMA)


def process_chunk(
    con: duckdb.DuckDBPyConnection,
    m_val: int,
    k_val: int,
    *,
    log: logging.Logger,
) -> tuple[int, dict[str, str]]:
    """Process one 10K partition: collect unique new tuples, compute token_ids, write atomically.

    Every partition ALWAYS produces an output (data.parquet + metadata.json), even
    when there are zero new tokens. The first-seen rule suppresses any 4-tuple that
    was already materialized in an earlier partition, so a partition may legitimately
    contain zero rows. Newly written tuples are inserted into seen_tuples so that
    subsequent partitions in the same run honour the first-seen invariant.
    """
    chunk_dir = Path(OUT_DIR) / partition_dir(k_val)
    chunk_dir.parent.mkdir(parents=True, exist_ok=True)

    # Ensure Polymarket conditions are loaded
    if "polymarket_conditions" not in [r[0] for r in con.execute("SHOW TABLES").fetchall()]:
        _load_polymarket_conditions(con)

    ct_rows = _process_ct_partition(con, m_val, k_val)
    nr_rows = _process_nr_partition(con, m_val, k_val)
    all_rows = ct_rows + nr_rows

    # First-seen filter via a single SQL anti-join against seen_tuples, plus
    # deduplication within this partition.
    if all_rows:
        con.register("candidates", pa.table({
            "collateral_token": pa.array([r["collateral_token"] for r in all_rows], type=pa.binary()),
            "parent_collection_id": pa.array([r["parent_collection_id"] for r in all_rows], type=pa.binary()),
            "condition_id": pa.array([r["condition_id"] for r in all_rows], type=pa.binary()),
            "index_set": pa.array([r["index_set"] for r in all_rows], type=pa.uint32()),
            "token_id": pa.array([r["token_id"] for r in all_rows], type=pa.binary()),
            "source": pa.array([r["source"] for r in all_rows], type=pa.string()),
        }))
        new_table = con.execute("""
            WITH filtered AS (
                SELECT
                    c.collateral_token,
                    c.parent_collection_id,
                    c.condition_id,
                    c.index_set,
                    c.token_id,
                    c.source
                FROM candidates c
                LEFT JOIN seen_tuples s
                  ON c.collateral_token = s.collateral_token
                 AND c.parent_collection_id = s.parent_collection_id
                 AND c.condition_id = s.condition_id
                 AND c.index_set = s.index_set
                WHERE s.condition_id IS NULL
            ),
            ranked AS (
                SELECT
                    collateral_token,
                    parent_collection_id,
                    condition_id,
                    index_set,
                    token_id,
                    ROW_NUMBER() OVER (
                        PARTITION BY collateral_token, parent_collection_id, condition_id, index_set
                        ORDER BY
                            CASE source
                                WHEN 'nr' THEN 0
                                ELSE 1
                            END,
                            token_id
                    ) AS rn
                FROM filtered
            )
            SELECT
                collateral_token,
                parent_collection_id,
                condition_id,
                index_set,
                token_id
            FROM ranked
            WHERE rn = 1
            ORDER BY
                collateral_token,
                parent_collection_id,
                condition_id,
                index_set
        """).fetch_arrow_table().cast(_OUTPUT_SCHEMA)
        con.unregister("candidates")
    else:
        new_table = _rows_to_table([])

    row_count = new_table.num_rows

    # Atomic write — ALWAYS, even for zero rows.
    chunk_name = chunk_dir.name  # e.g., "10K=33600000"
    temp_loc = create_temp_location(
        parent_dir=chunk_dir.parent,
        final_name=chunk_name,
        temp_suffix=".tmp",
    )
    try:
        pq.write_table(new_table, temp_loc.path / "data.parquet", compression="zstd")
        _write_metadata(con, temp_loc.path, m_val, k_val, {}, log)
        publish_atomically(temp_loc, allow_overwrite=False)
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
        return row_count, {}
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


def _enumerate_consecutive_partitions(frontier: int) -> list[tuple[int, int]]:
    """Every consecutive 10K partition from START_PARTITION_10K up to the frontier.

    A partition is included only if its inclusive end block is within the frontier
    (i.e., fully sunk upstream). The result has no gaps.
    """
    parts: list[tuple[int, int]] = []
    k = START_PARTITION_10K
    while partition_end(k) <= frontier:
        m = (k // _PARTITION_1M_SIZE) * _PARTITION_1M_SIZE
        parts.append((m, k))
        k += _PARTITION_10K_SIZE
    return parts


def main() -> None:
    global _global_con
    parser = argparse.ArgumentParser(description="Materialize token_id_map_v1")
    parser.add_argument("--dry-run", action="store_true", help="print work plan without writing")
    parser.add_argument("--sample", type=int, default=0, help="process only first N partitions")
    args = parser.parse_args()

    assert_git_clean(_project_root)

    log = _setup_logging()
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
    all_partitions = _enumerate_consecutive_partitions(frontier)

    # Already-landed partitions (output folder exists) are immutable and skipped.
    # They never appear in the progress bar.
    todo = [
        (m, k)
        for (m, k) in all_partitions
        if not (Path(OUT_DIR) / f"{PARTITION_1M_LABEL}={m}" / f"{PARTITION_10K_LABEL}={k}").exists()
    ]

    log.info(
        f"frontier={frontier}, start_partition_10K={START_PARTITION_10K}, "
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

    console.print(
        f"frontier={frontier}  |  total={len(all_partitions):,}  |  "
        f"[green]{len(all_partitions) - len(todo):,} already landed[/green]  |  "
        f"[yellow]{len(todo):,} to process[/yellow]"
    )

    if not todo:
        console.print("[green]Nothing to do.[/green]")
        return

    preload_total = sum(
        len(_existing_partition_files(contract_event, all_partitions))
        for contract_event in (
            "ConditionalTokens/position_split",
            "ConditionalTokens/positions_merge",
            "ConditionalTokens/payout_redemption",
        )
    )

    with Progress(
        SpinnerColumn(),
        TextColumn("[progress.description]{task.description}"),
        BarColumn(),
        TaskProgressColumn(),
        MofNCompleteColumn(),
        TimeElapsedColumn(),
        TimeRemainingColumn(),
        console=console,
    ) as preload_progress:
        preload_task = preload_progress.add_task(
            "Resolving NR collateral candidates",
            total=preload_total,
        )
        _load_nr_collateral_candidates(
            con,
            all_partitions,
            log=log,
            progress=preload_progress,
            task_id=preload_task,
        )

    with Progress(
        SpinnerColumn(),
        TextColumn("[progress.description]{task.description}"),
        BarColumn(),
        TaskProgressColumn(),
        MofNCompleteColumn(),
        TimeElapsedColumn(),
        TimeRemainingColumn(),
        console=console,
    ) as progress:
        task = progress.add_task("Materializing token_id_map_v1", total=len(todo))

        processed = 0
        for m_val, k_val in todo:
            if _stop_event.is_set():
                log.info("interrupted by user")
                break

            row_count, _ = process_chunk(con, m_val, k_val, log=log)
            processed += 1
            progress.update(task, advance=1)

    log.info(f"token_id_map_v1 materializer finished. Processed {processed} partitions.")
    console.print(f"[green]Complete! Processed {processed} partitions.[/green]")


if __name__ == "__main__":
    main()
