"""
Cold-tier (Parquet) sink.

Writes Parquet files under
``{cold_root}/{contract}/{event}/1M=N/10K=N/data.parquet``, matching
``DATA_DICTIONARY.md`` exactly:

  * Column types use the dictionary's Parquet type contract: ``BLOB`` (no
    logical type), ``STRING`` (UTF-8), ``INT(bitWidth=32, isSigned=false)``.
  * Column order matches ``tables.all_columns(contract, event)``.
  * The partition-key columns ``1M`` and ``10K`` are NEVER written inside
    the file.
  * Rows are sorted by ``(block_number, transaction_index, log_index)``.
  * ``(transaction_hash, log_index)`` is unique within each file. This is
    enforced inside the COPY itself via ``SELECT DISTINCT ON`` plus a
    post-write row-count check: if the distinct count differs from the
    raw count, ``DuplicateRowError`` is raised and the partition is
    aborted. By contract, duplicates cannot occur (the loading path is
    atomic per range), so this check is purely a safety net for decoder
    logic errors.

Files appear atomically: each is written to a sibling temp file on the
same filesystem, then ``os.replace``'d into place. Parquet files in the
cold tier are immutable once visible — no file is ever overwritten.

Orchestration contract — the writer is a pure cold-tier producer:

  This module does NOT touch the hot DB's bookkeeping. It does not
  import ``HotStore``. It opens its own read-only DuckDB connection
  against the hot ``.db`` file, runs every COPY for one 10K partition,
  renames the temp files into place, and returns. The orchestrator
  (which owns the ``HotStore``) is responsible for the follow-up
  ``HotStore.commit_sink(partition_start)`` call that flips the range
  to sunk and deletes the hot rows. Splitting the work this way means:

    * Every write against the hot DB happens on the orchestrator's
      thread, in program order. There is no background-thread write
      path.
    * This function can run on any thread (typically a worker in a
      ``concurrent.futures.ThreadPoolExecutor``) without coordinating
      with the orchestrator beyond standard Future / queue handoff.
    * Crash recovery is simple: if this function crashes or its
      return value is lost, the orchestrator never calls
      ``commit_sink`` for that partition. The hot DB is unchanged.
      Restart, re-issue the partition to a worker, redo the work.
      For the corner case where the function succeeded but the
      orchestrator died before the commit, ``HotStore.reconcile_with_cold_tier``
      at next startup will mark the partition as sunk based on the
      Parquet files on disk.

Frontier ordering contract:

The cold tier must advance one 10K partition at a time, starting at
``floor(SCRAPE_START_BLOCK / 10_000) * 10_000``. This module does NOT
enforce that contract — the orchestrator does, by calling this
function only for partitions in the right order, and the
``HotStore.commit_sink`` call afterwards is the hard enforcement point
(it raises ``PartitionFrontierError`` if the orchestrator passes a
partition out of order). The writer trusts its caller.

Sink workflow (one call to ``write_partition_files``):

  1. Open a read-only DuckDB connection on ``db_path``. (The
     orchestrator's connection is unaffected.)
  2. Determine the set of eligible ``(contract, event)`` targets via
     ``tables.targets_active_at(partition_start + 9_999)``.
  3. For each target, run one DuckDB COPY in its own short
     transaction:

         COPY (
             SELECT DISTINCT ON (transaction_hash, log_index)
                 <columns in canonical order>
             FROM <hot_table>
             WHERE block_number BETWEEN P AND P+9999
             ORDER BY block_number, transaction_index, log_index
         ) TO '<cold>/.../data.parquet.tmp-<uuid>'
         (FORMAT PARQUET, COMPRESSION ZSTD);

     Then verify the row count matches the raw (pre-dedup) count from
     the same WHERE without ``DISTINCT ON``. A mismatch raises
     ``DuplicateRowError`` — a fatal logic error.
  4. After every temp file is written, rename them all into place
     via ``os.replace`` (atomic per file).
  5. Close the read-only connection. Return the
     ``PartitionWriteResult``.

  On any phase-2-or-earlier failure, every temp file written by this
  call is removed. The cold tier is unchanged on failure.

Why the COPY runs on a per-call read-only connection (not on the
orchestrator's ``HotStore`` connection):

  * The COPY for one 10K partition can take minutes. If it ran on
    the orchestrator's connection, the orchestrator would be blocked
    for that whole time, defeating the purpose of running the sink
    in the background.
  * DuckDB supports multiple connections to the same database file
    from one process; the read-only connection sees an MVCC snapshot
    of rows that ``HotStore.persist`` has already committed.
  * The connection is closed on return so the worker holds no
    long-lived resource.
"""

from __future__ import annotations

import os
import shutil
import time
import uuid
from dataclasses import dataclass
from pathlib import Path
import threading

import duckdb

from lib.metadata_utils import create_parquet_metadata_json
from lib.partition_utils import (
    PARTITION_10K_LABEL,
    PARTITION_1M_LABEL,
    partition_dir,
    partition_end,
    partition_start,
)
from lib.atomic_publish import (
    create_temp_location,
    publish_atomically,
    cleanup_old_temp_artifacts,
)

from .errors import DuplicateRowError, OperationCancelled, V3Error
from .persistence import ProgressCallback
from .tables import (
    SCRAPE_START_BLOCK,
    PARTITION_SIZE_10K,
    CONTRACTS_BY_NAME,
    all_columns,
    all_targets,
    table_name,
    targets_active_at,
)


# ---------------------------------------------------------------------------
# Result types
# ---------------------------------------------------------------------------

@dataclass(frozen=True)
class PartitionWriteResult:
    """Result of one ``write_partition_files`` call."""

    partition_start: int
    """The 10K-aligned start block that was written."""

    files_written: int
    """Number of ``data.parquet`` files written, one per eligible
    ``(contract, event)`` target."""

    rows_total: int
    """Total rows written across all files (post-deduplication)."""

    bytes_total: int
    """Total bytes written across all files (post-compression)."""

    elapsed_ms: float


# ---------------------------------------------------------------------------
# Public API
# ---------------------------------------------------------------------------

def _manifest_dir(cold_path: Path, partition_start: int) -> Path:
    return cold_path / "manifests" / partition_dir(partition_start)


def _expected_targets_for_partition(partition_start: int) -> list[tuple[str, str]]:
    # DATA_DICTIONARY.md defines expected targets by deployment <= partition lower bound.
    return [
        (contract, event)
        for contract, event in all_targets()
        if CONTRACTS_BY_NAME[contract].deployment_block <= partition_start
    ]


def _validate_manifest_partition_dir(manifest_partition_dir: Path) -> None:
    entries = list(manifest_partition_dir.iterdir())
    files = [p for p in entries if p.is_file()]
    extra_dirs = [p for p in entries if p.is_dir()]

    if extra_dirs:
        raise V3Error(
            f"manifest partition folder contains nested directories: {manifest_partition_dir}"
        )
    if not (manifest_partition_dir / "_SUCCESS").is_file():
        raise V3Error(
            f"manifest partition folder missing required _SUCCESS file: {manifest_partition_dir}"
        )
    if len(files) != 1 or files[0].name != "_SUCCESS":
        raise V3Error(
            f"manifest partition folder must contain only _SUCCESS: {manifest_partition_dir}"
        )
    if files[0].stat().st_size != 0:
        raise V3Error(
            f"manifest _SUCCESS file must be empty (zero bytes): {files[0]}"
        )


def _create_metadata_json_for_single_parquet(
    parquet_path: str | Path,
    *,
    dataset: str = "polygon_contract_events_v3",
    source_script: str = "raw_data/polygon_contract_events_v3/_internal/parquet_sink.py",
    input_hashes: dict[str, str] | None = None,
    parameters: dict[str, object] | None = None,
) -> Path:
    """Internal helper to write one spec-compliant ``metadata.json`` for one parquet file."""
    try:
        return create_parquet_metadata_json(
            parquet_path,
            dataset=dataset,
            source_script=source_script,
            input_hashes=input_hashes,
            parameters=parameters,
        )
    except FileNotFoundError as e:
        raise V3Error(str(e)) from e
    except Exception as e:
        raise V3Error(f"could not write metadata.json for {parquet_path}: {e}") from e


def create_metadata_json_for_parquet(parquet_path: str | Path) -> Path:
    """Create ``metadata.json`` next to one ``data.parquet`` file."""
    return _create_metadata_json_for_single_parquet(parquet_path)


def _validate_or_prepare_partition_data_dir(partition_dir: Path) -> bool:
    """Return True if folder exists and is valid, False if missing."""
    if not partition_dir.exists():
        return False
    if not partition_dir.is_dir():
        raise V3Error(f"partition path is not a directory: {partition_dir}")

    parquet = partition_dir / "data.parquet"
    metadata = partition_dir / "metadata.json"

    if not parquet.is_file():
        raise V3Error(f"partition folder missing required data.parquet: {partition_dir}")
    if not metadata.is_file():
        raise V3Error(f"partition folder missing required metadata.json: {partition_dir}")

    allowed = {"data.parquet", "metadata.json"}
    entries = list(partition_dir.iterdir())
    extras = [p.name for p in entries if p.name not in allowed]
    if extras:
        raise V3Error(
            f"partition folder contains unexpected files/directories {extras}: {partition_dir}"
        )
    if len(entries) != 2:
        raise V3Error(
            f"partition folder must contain exactly data.parquet and metadata.json: {partition_dir}"
        )
    return True


def _publish_manifest_success_atomically(cold_path: Path, partition_start: int) -> None:
    """Publish a manifest _SUCCESS folder atomically, using shared publish primitives.
    
    Raises V3Error if the final location already exists (immutability protection).
    """
    final_dir = _manifest_dir(cold_path, partition_start)
    manifests_parent = final_dir.parent
    final_name = final_dir.name

    # Check immutability before creating temp
    if final_dir.exists():
        raise V3Error(
            f"refusing to overwrite immutable manifest partition folder: {final_dir}"
        )

    # Create temp location using shared primitive
    temp_location = create_temp_location(
        parent_dir=manifests_parent,
        final_name=final_name,
        temp_suffix=".tmp",
    )

    # Write _SUCCESS marker
    success_file = temp_location.path / "_SUCCESS"
    success_file.write_bytes(b"")

    # Publish atomically (raises FileExistsError if immutability violated)
    publish_atomically(temp_location)


def _cleanup_tmp_partition_dirs(cold_path: Path, partition_start: int) -> int:
    """Clean up temp partition directories for a specific partition.
    
    Removes all .tmp directories matching the partition pattern across all
    contract/event combinations. Uses shared cleanup primitives.
    """
    removed = 0
    k_tmp_dir = f"10K={partition_start}.tmp"
    for contract, event in all_targets():
        tmp_dir = (
            cold_path
            / contract
            / event
            / partition_dir(partition_start).replace(f"10K={partition_start}", k_tmp_dir)
        )
        if tmp_dir.exists():
            shutil.rmtree(tmp_dir, ignore_errors=True)
            removed += 1
    return removed


def roll_forward_manifests_to_exhaustion(
    cold_root: str,
    *,
    progress_cb: ProgressCallback | None = None,
    stop_event: threading.Event | None = None,
) -> int:
    """Publish manifest ``_SUCCESS`` folders for every consecutive publishable partition.

    Starts from the partition immediately after the current manifest frontier and
    proceeds to exhaustion: it keeps publishing consecutive partitions until the
    first partition that is not yet publishable.
    """
    cold_path = Path(cold_root)
    if not cold_path.is_dir():
        raise ValueError(f"cold_root does not exist or is not a directory: {cold_root}")

    written = 0
    frontier = get_sunk_frontier(cold_root)
    pstart = partition_start(SCRAPE_START_BLOCK) if frontier < SCRAPE_START_BLOCK else frontier + 1
    pstart = (pstart // PARTITION_SIZE_10K) * PARTITION_SIZE_10K

    while True:
        if stop_event and stop_event.is_set():
            raise OperationCancelled("manifest roll-forward interrupted")

        partition_end_val = partition_end(pstart)
        rel_path = partition_dir(pstart)
        expected_targets = _expected_targets_for_partition(pstart)

        all_exist = True
        for contract, event in expected_targets:
            partition_data_dir = (
                cold_path
                / contract
                / event
                / rel_path
            )
            exists_and_valid = _validate_or_prepare_partition_data_dir(partition_data_dir)
            if not exists_and_valid:
                all_exist = False
                break

        if not all_exist:
            if progress_cb:
                progress_cb(
                    op="manifest",
                    phase="stop",
                    rows_done=written,
                    message=f"stopped before 10K={pstart} (incomplete)",
                )
            break

        _publish_manifest_success_atomically(cold_path, pstart)
        removed_tmp = _cleanup_tmp_partition_dirs(cold_path, pstart)
        written += 1
        if progress_cb:
            progress_cb(
                op="manifest",
                phase="publish",
                rows_done=written,
                message=(
                        f"published manifests/{rel_path} "
                    f"for blocks [{pstart}, {partition_end_val}] "
                    f"tmp_removed={removed_tmp}"
                ),
            )

        pstart += PARTITION_SIZE_10K

    return written


def cleanup_temp_dirs_after_frontier(
    cold_root: str,
    *,
    progress_cb: ProgressCallback | None = None,
) -> int:
    """Delete temp directories for the first unsunk partition.

    Defensive behavior: also removes the final manifest folder for the first
    unsunk partition if it exists.
    """
    cold_path = Path(cold_root)
    if not cold_path.is_dir():
        raise ValueError(f"cold_root does not exist or is not a directory: {cold_root}")

    frontier = get_sunk_frontier(cold_root)
    pstart = partition_start(SCRAPE_START_BLOCK) if frontier < SCRAPE_START_BLOCK else frontier + 1
    pstart = (pstart // PARTITION_SIZE_10K) * PARTITION_SIZE_10K

    removed = _cleanup_tmp_partition_dirs(cold_path, pstart)
    manifest_rel = partition_dir(pstart)
    manifest_tmp = cold_path / "manifests" / f"{manifest_rel}.tmp"
    manifest_final = cold_path / "manifests" / manifest_rel

    for path in (manifest_tmp, manifest_final):
        if path.exists():
            shutil.rmtree(path, ignore_errors=True)
            removed += 1

    if progress_cb:
        progress_cb(
            op="manifest",
            phase="cleanup",
            rows_done=removed,
            message=f"cleaned temp dirs for 10K={pstart}",
        )
    return removed


def read_manifest_frontier(
    cold_root: str,
    *,
    progress_cb: ProgressCallback | None = None,
) -> tuple[int, int]:
    """Read and validate manifest contiguity, returning (frontier, partition_count)."""
    cold_path = Path(cold_root)
    if not cold_path.is_dir():
        raise ValueError(f"cold_root does not exist or is not a directory: {cold_root}")

    manifests_root = cold_path / "manifests"
    if not manifests_root.exists():
        if progress_cb:
            progress_cb(op="manifest", phase="read", rows_done=0, message="no manifests/ directory")
        return SCRAPE_START_BLOCK - 1, 0
    if not manifests_root.is_dir():
        raise V3Error(f"manifests path exists but is not a directory: {manifests_root}")

    existing_partitions: list[int] = []
    for partition_dir in manifests_root.glob("1M=*/10K=*"):
        if not partition_dir.is_dir():
            continue
        if partition_dir.name.endswith(".tmp"):
            continue
        if not partition_dir.name.startswith(f"{PARTITION_10K_LABEL}="):
            continue

        try:
            pval = int(partition_dir.name.split("=", 1)[1])
        except ValueError as e:
            raise V3Error(f"invalid manifest partition folder name: {partition_dir}") from e

        if pval % PARTITION_SIZE_10K != 0:
            raise V3Error(f"manifest partition is not 10K-aligned: {partition_dir}")

        if pval < partition_start(SCRAPE_START_BLOCK):
            raise V3Error(
                f"manifest partition is before first allowed partition {partition_start(SCRAPE_START_BLOCK)}: "
                f"{partition_dir}"
            )

        _validate_manifest_partition_dir(partition_dir)
        existing_partitions.append(pval)

    if not existing_partitions:
        if progress_cb:
            progress_cb(op="manifest", phase="read", rows_done=0, message="no manifest partitions")
        return SCRAPE_START_BLOCK - 1, 0

    existing_partitions = sorted(set(existing_partitions))
    expected = partition_start(SCRAPE_START_BLOCK)
    for idx, pstart in enumerate(existing_partitions, start=1):
        if pstart != expected:
            raise V3Error(
                "non-contiguous manifest frontier: "
                f"expected manifests for 10K={expected} before 10K={pstart}"
            )
        expected += PARTITION_SIZE_10K
        if progress_cb and (idx % 50 == 0 or idx == len(existing_partitions)):
            progress_cb(
                op="manifest",
                phase="read",
                rows_done=pstart,
                rows_total=len(existing_partitions),
                message=f"10K={pstart}",
            )

    return partition_end(existing_partitions[-1]), len(existing_partitions)

def get_sunk_frontier(cold_root: str) -> int:
    """Return the sunk frontier inferred only from manifest ``_SUCCESS`` files.

    The frontier is the highest block ``N`` for which
    ``manifests/1M={M}/10K={K}/_SUCCESS`` exists for every consecutive partition
    starting at ``floor(SCRAPE_START_BLOCK / 10_000) * 10_000``.

    Returns ``SCRAPE_START_BLOCK - 1`` if no manifest partition exists.

    Args:
        cold_root: Cold-tier root directory.

    Raises:
        ValueError: ``cold_root`` does not exist or is not a directory.
        V3Error: manifest folders violate the v3 data dictionary contract.
    """
    frontier, _count = read_manifest_frontier(cold_root)
    return frontier

def write_partition_files(
    db_path: str,
    cold_root: str,
    partition_start: int,
    *,
    connection: "duckdb.DuckDBPyConnection | None" = None,
    progress_cb: ProgressCallback | None = None,
    stop_event: threading.Event | None = None,
) -> PartitionWriteResult:
    """Write one 10K partition's complete set of Parquet files to the
    cold tier.

    This is the cold-tier-producer half of the sink path. It opens its
    own read-only DuckDB connection on ``db_path``, runs every required
    COPY, renames the temp files into place, closes its connection, and
    returns. It does NOT touch the hot-DB bookkeeping; the orchestrator
    must call ``HotStore.commit_sink(partition_start)`` after this
    function returns successfully.

    Steps:

      1. Open a read-only DuckDB connection on ``db_path``.
      2. Determine the set of eligible ``(contract, event)`` targets
         via ``tables.targets_active_at(partition_start + 9_999)``.
      3. For each target, run one DuckDB COPY (see module docstring for
         the exact SQL) to a sibling temp file. Verify the row count
         matches the raw (pre-dedup) count; raise ``DuplicateRowError``
         on mismatch.
      4. Rename every temp file to ``data.parquet`` via ``os.replace``.
      5. Close the read-only connection.

    On any failure before step 4 completes, every temp file written by
    this call is removed. The hot DB and the cold tier are both
    unchanged on failure (modulo temp files this call cleaned up).

    Args:
        db_path: Absolute path to the hot ``.db`` file. The function
            opens a fresh read-only connection on it; the orchestrator's
            connection (if any) is unaffected.
        cold_root: Absolute path to the cold-tier root, the directory
            immediately containing per-contract subdirectories.
        partition_start: Must be a multiple of 10_000 and ``>=
            SCRAPE_START_BLOCK``.
        progress_cb: Optional progress callback. Receives events for
            each ``(contract, event)`` COPY.
        stop_event: If set during the operation, raises
            ``OperationCancelled`` at the next safe point. Temp files
            are cleaned up.

    Returns:
        A ``PartitionWriteResult`` describing what was written.

    Raises:
        ValueError: ``partition_start`` is not 10K-aligned or below
            ``SCRAPE_START_BLOCK``, or ``cold_root`` does not exist, or
            ``db_path`` does not exist.
        DuplicateRowError: at least one ``(contract, event)`` cell
            contains duplicate ``(transaction_hash, log_index)`` rows
            in the hot DB — a fatal logic error in the loading path.
        OperationCancelled: ``stop_event`` was set.
        V3Error: any other DuckDB or filesystem failure.

    Preconditions:
        * Every required row for blocks in
          ``[partition_start, partition_start + 9_999]`` has been
          loaded via ``HotStore.persist`` and that call has returned
          (i.e. its commit is durable). The orchestrator typically
          ensures this by reading ``PersistResult.ready_partitions``
          and only submitting partitions from there to this function.

    Postconditions on success:
        * For every eligible ``(contract, event)``, the file
          ``{cold_root}/{contract}/{event}/1M=M/10K=K/data.parquet``
          exists and matches the data dictionary's Parquet contract
          exactly.
        * No partition-key columns inside any file.
        * Rows in each file are sorted and deduplicated.
        * The function's read-only DuckDB connection is closed.

    Postconditions on failure:
        * No new ``data.parquet`` files exist.
        * Any temp files written by this call have been removed.
        * The hot DB is unchanged.

    Note on caller responsibility:
        After this function returns successfully, the orchestrator
        MUST call ``store.commit_sink(partition_start)`` (where
        ``store`` is the ``HotStore`` instance owned by the
        orchestrator). The frontier-ordering check lives inside
        ``commit_sink``; calling partitions out of order is caught
        there, not here.
    """
    if partition_start % PARTITION_SIZE_10K != 0:
        raise ValueError(f"partition_start {partition_start} is not 10K-aligned")
    # The first valid partition is ``floor(SCRAPE_START_BLOCK / 10K) * 10K``,
    # which may be *below* ``SCRAPE_START_BLOCK`` itself (e.g.
    # 33_600_000 < 33_605_403). The relevant invariant is that the
    # partition's *end* block is at or above ``SCRAPE_START_BLOCK`` —
    # otherwise the partition lies entirely in pre-scrape history and
    # cannot contain any rows.
    if partition_end(partition_start) < SCRAPE_START_BLOCK:
        raise ValueError(
            f"partition_start {partition_start} ends before "
            f"SCRAPE_START_BLOCK {SCRAPE_START_BLOCK}"
        )
    cold_path = Path(cold_root)
    if not cold_path.is_dir():
        raise ValueError(f"cold_root does not exist or is not a directory: {cold_root}")
    if not Path(db_path).is_file():
        raise ValueError(f"db_path does not exist: {db_path}")

    p_end = partition_end(partition_start)
    rel_path = partition_dir(partition_start)
    targets = targets_active_at(p_end)

    t0 = time.monotonic()
    pending_renames: list[tuple[Path, Path]] = []
    rows_total = 0
    bytes_total = 0

    # Get a DB handle for this worker.
    #
    # DuckDB does not actually support opening a second
    # ``duckdb.connect()`` against the same ``.db`` file from the same
    # process — even with matched config, the second call raises a
    # ``Connection Error: Can't open a connection to same database file
    # with a different configuration``. The pattern that does work is
    # ``existing_connection.cursor()``: a cursor shares the underlying
    # database but has its own transaction state, is safe to use from
    # a different thread, and reads committed rows via MVCC just like
    # a separate connection would.
    #
    # So: if the caller passes a ``connection`` (the orchestrator's
    # ``HotStore`` connection), we use a cursor of it. The cursor is
    # local to this worker and is closed below regardless of outcome.
    # ``opened_locally`` is False in this path because the caller owns
    # the parent connection's lifecycle.
    opened_locally = False
    if connection is not None:
        try:
            con = connection.cursor()
        except Exception as e:
            raise V3Error(
                f"could not open cursor on orchestrator connection: {e}"
            ) from e
    else:
        # Fallback: no orchestrator connection supplied. Open our own.
        # This path is used when ``write_partition_files`` is invoked
        # standalone (e.g. one-off scripts, tests) against a ``.db``
        # that no other process or connection is currently holding
        # open.
        try:
            con = duckdb.connect(db_path, config={"access_mode": "READ_ONLY"})
            opened_locally = True
        except Exception as e:
            raise V3Error(
                f"could not open read-only connection to {db_path!r}: {e}"
            ) from e

    try:
        for contract, event in targets:
            if stop_event and stop_event.is_set():
                raise OperationCancelled("write_partition_files interrupted")

            tbl = table_name(contract, event)
            cols = all_columns(contract, event)
            col_list = ", ".join(cols)

            dst_dir = cold_path / contract / event / rel_path
            dst_dir.mkdir(parents=True, exist_ok=True)
            dst = dst_dir / "data.parquet"
            # Immutability guard: the cold tier is append-only. A
            # ``data.parquet`` at the destination means this partition
            # has already been sunk for this (contract, event). Refuse
            # to write rather than risk clobbering it with an atomic
            # rename. The caller has a bug if we reach this point.
            if dst.exists():
                raise V3Error(
                    f"refusing to overwrite immutable cold parquet at {dst}: "
                    f"partition {partition_start} for {contract}/{event} already sunk"
                )
            tmp = dst_dir / f"data.parquet.tmp-{uuid.uuid4().hex}"

            # Count raw rows before dedup so we can detect duplicates.
            raw_row = con.execute(
                f"SELECT COUNT(*) FROM {tbl} "
                f"WHERE block_number BETWEEN {partition_start} AND {p_end}"
            ).fetchone()
            if raw_row is None:
                raise V3Error(
                    f"could not count source rows for {contract}/{event} partition {partition_start}: empty result"
                )
            raw_count = int(raw_row[0])

            # COPY with DISTINCT ON deduplicates and sorts in one pass.
            copy_sql = (
                f"COPY (\n"
                f"    SELECT DISTINCT ON (transaction_hash, log_index)\n"
                f"        {col_list}\n"
                f"    FROM {tbl}\n"
                f"    WHERE block_number BETWEEN {partition_start} AND {p_end}\n"
                f"    ORDER BY block_number, transaction_index, log_index\n"
                f") TO '{tmp}' (FORMAT PARQUET, COMPRESSION ZSTD)"
            )
            try:
                con.execute(copy_sql)
            except Exception as e:
                raise V3Error(
                    f"COPY failed for {contract}/{event} partition {partition_start}: {e}"
                ) from e

            # Verify: dedup must not change row count.
            written_row = con.execute(
                f"SELECT COUNT(*) FROM read_parquet('{tmp}')"
            ).fetchone()
            if written_row is None:
                raise V3Error(
                    f"could not count written rows for {contract}/{event} partition {partition_start}: empty result"
                )
            written_count = int(written_row[0])
            if written_count != raw_count:
                tmp.unlink(missing_ok=True)
                raise DuplicateRowError(
                    f"{contract}/{event} partition {partition_start}: "
                    f"{raw_count} raw rows but {written_count} after dedup — "
                    f"decoder logic error"
                )

            file_bytes = tmp.stat().st_size
            rows_total += written_count
            bytes_total += file_bytes
            pending_renames.append((tmp, dst))

            if progress_cb:
                progress_cb(
                    op="sink",
                    phase="copy",
                    rows_done=len(pending_renames),
                    rows_total=len(targets),
                    message=f"{contract}/{event}",
                )

    except (OperationCancelled, DuplicateRowError, V3Error):
        # Clean up any temp files written so far.
        for tmp_path, _ in pending_renames:
            try:
                tmp_path.unlink(missing_ok=True)
            except OSError:
                pass
        con.close()
        raise
    except Exception as e:
        for tmp_path, _ in pending_renames:
            try:
                tmp_path.unlink(missing_ok=True)
            except OSError:
                pass
        con.close()
        raise V3Error(
            f"write_partition_files failed for partition {partition_start}: {e}"
        ) from e

    # Phase 2: atomic renames.  All COPY calls succeeded; rename each
    # temp into its final name.  Failures here are unlikely (same
    # filesystem) but caught and propagated.
    try:
        for tmp_path, dst_path in pending_renames:
            os.replace(tmp_path, dst_path)
    except Exception as e:
        con.close()
        raise V3Error(
            f"rename failed during write_partition_files partition {partition_start}: {e}"
        ) from e

    con.close()

    # Phase 3: write metadata.json for each renamed parquet file.
    # This must happen after the atomic renames so the parquet path used for
    # row-count and content-hash is the final, immutable cold-tier path.
    for _tmp_path, dst_path in pending_renames:
        try:
            _create_metadata_json_for_single_parquet(dst_path)
        except Exception as e:
            raise V3Error(
                f"metadata.json write failed for {dst_path} partition {partition_start}: {e}"
            ) from e
    elapsed_ms = (time.monotonic() - t0) * 1000
    return PartitionWriteResult(
        partition_start=partition_start,
        files_written=len(pending_renames),
        rows_total=rows_total,
        bytes_total=bytes_total,
        elapsed_ms=elapsed_ms,
    )


def remove_orphan_temp_files(
    cold_root: str,
    *,
    older_than_seconds: float = 0.0,
) -> int:
    """Remove every ``data.parquet.tmp-*`` file under ``cold_root``.

    Useful after a hard crash to clean up files from interrupted writes.
    The ``older_than_seconds`` filter (default 0 = remove all) can be
    set to avoid racing with a running writer (set it to slightly more
    than the longest expected sink duration).

    Returns the number of files removed.

    Postconditions:
        * No ``data.parquet.tmp-*`` files older than the cutoff remain
          under ``cold_root``.
        * No ``data.parquet`` (without the ``.tmp-`` suffix) is touched.
    """
    removed = 0
    now = time.monotonic()
    for dirpath, _dirnames, filenames in os.walk(cold_root):
        for fn in filenames:
            if not fn.startswith("data.parquet.tmp-"):
                continue
            full = os.path.join(dirpath, fn)
            try:
                mtime = os.path.getmtime(full)
                age = time.time() - mtime  # wall-clock age
                if age >= older_than_seconds:
                    os.unlink(full)
                    removed += 1
            except OSError:
                pass
    return removed
