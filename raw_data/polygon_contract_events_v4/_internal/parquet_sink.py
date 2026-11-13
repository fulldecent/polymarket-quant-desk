"""
Cold-tier (Parquet) sink.

Writes Parquet files under
``{cold_root}/{contract}/{event}/year=YYYY/month=MM/day=DD/data.parquet``, matching
``DATA_DICTIONARY.md`` exactly:

  * Column types use the dictionary's Parquet type contract: ``BLOB`` (no
    logical type), ``STRING`` (UTF-8), ``INT(bitWidth=32, isSigned=false)``.
  * Column order matches ``tables.all_columns(contract, event)``.
  * The partition-key columns ``year``, ``month``, and ``day`` are NEVER written inside
    the file.
  * Rows are sorted by ``(block_number, transaction_index, log_index)``.
  * ``(block_number, transaction_index, log_index)`` is unique within each file. This is
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
  against the hot ``.db`` file, runs every COPY for one UTC day,
  renames the temp files into place, and returns. The orchestrator
  (which owns the ``HotStore``) is responsible for the follow-up
  ``HotStore.commit_sink(day_key)`` call that flips the range
  to sunk and deletes the hot rows. Splitting the work this way means:

    * Every write against the hot DB happens on the orchestrator's
      thread, in program order. There is no background-thread write
      path.
    * This function can run on any thread (typically a worker in a
      ``concurrent.futures.ThreadPoolExecutor``) without coordinating
      with the orchestrator beyond standard Future / queue handoff.
    * Crash recovery: if this function returns unsuccessfully, or the
      process dies before it returns, the orchestrator never calls
      ``commit_sink`` for that partition. The hot DB still holds the
      rows. Restart runs ``cleanup_temp_dirs_after_frontier`` (incomplete
      unpublished day folders are unpublished junk) then re-issues the
      day. A caught phase-2 or phase-3 failure also unlinks every
      ``data.parquet`` and ``metadata.json`` this call created.
      If the function succeeded and the orchestrator died after
      ``publish_manifest`` but before ``commit_sink``,
      ``HotStore.reconcile_with_cold_tier`` at next startup marks the
      partition sunk from the manifest. If it died after a complete
      write but before ``publish_manifest``, roll-forward publishes
      ``_SUCCESS`` because every expected target already has both files.

Frontier ordering contract:

The cold tier must advance one UTC day at a time, starting at the
UTC day of ``SCRAPE_START_BLOCK``. This module does NOT enforce that
contract — the orchestrator does, by calling this function only for
days in calendar order, and the ``HotStore.commit_sink`` call
afterwards is the hard enforcement point (it raises
``PartitionFrontierError`` if the orchestrator passes a day out of
order). The writer trusts its caller.

Sink workflow (one call to ``write_partition_files``):

  1. Open a read-only DuckDB connection on ``db_path``. (The
     orchestrator's connection is unaffected.)
  2. Determine the set of eligible ``(contract, event)`` targets via
     ``tables.targets_active_at(last_block)``.
  3. For each target, run one DuckDB COPY in its own short
     transaction:

         COPY (
             SELECT DISTINCT ON (block_number, transaction_index, log_index)
                 <columns in canonical order>
             FROM <hot_table>
             WHERE block_number BETWEEN first_block AND last_block
             ORDER BY block_number, transaction_index, log_index
         ) TO '<cold>/.../data.parquet.tmp-<uuid>'
         (FORMAT PARQUET, COMPRESSION ZSTD);

     Then verify the row count matches the raw (pre-dedup) count from
     the same WHERE without ``DISTINCT ON``. A mismatch raises
     ``DuplicateRowError`` — a fatal logic error.
  4. After every temp file is written, rename them all into place
     via ``os.replace`` (atomic per file).
  5. Write ``metadata.json`` next to each renamed parquet.
  6. Close the read-only connection. Return the
     ``PartitionWriteResult``.

  On any failure, every temp file written by this call is removed, and
  every ``data.parquet`` / ``metadata.json`` this call created is unlinked.
  The hot DB is unchanged. Incomplete folders above the frontier that
  survive a hard kill are removed at next startup.

Why the COPY runs on a per-call read-only connection (not on the
orchestrator's ``HotStore`` connection):

  * The COPY for one UTC day can take minutes. If it ran on
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
from lib.ymd_partition import (
    assert_contiguous_day_keys,
    day_block_span,
    day_dir,
    day_key_from_hive_dir,
    day_key_parts,
    day_label,
    iter_manifest_day_keys,
    next_day_key,
    utc_midnight,
)
from lib.atomic_publish import (
    create_temp_location,
    publish_atomically,
    cleanup_old_temp_artifacts,
)

from .errors import DuplicateRowError, OperationCancelled, V4Error
from .fee_charged import sql_poison_predicate
from .persistence import ProgressCallback
from .tables import (
    SCRAPE_START_BLOCK,
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

    day_key: int
    """UTC day key ``YYYYMMDD`` that was written."""

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

def _manifest_dir(cold_path: Path, day_key: int) -> Path:
    return cold_path / "manifests" / day_dir(day_key)


def _expected_targets_for_day(last_block: int) -> list[tuple[str, str]]:
    """Contracts deployed by the day's last block still get that day's files."""
    return targets_active_at(last_block)


def _day_last_block(cold_path: Path, day_key: int) -> int | None:
    """``max_block`` from metadata already on disk, or None if no file exists."""
    try:
        _min_block, max_block = day_block_span(cold_path, day_key)
    except ValueError:
        return None
    return max_block


def _validate_manifest_partition_dir(manifest_partition_dir: Path) -> None:
    entries = list(manifest_partition_dir.iterdir())
    files = [p for p in entries if p.is_file()]
    extra_dirs = [p for p in entries if p.is_dir()]

    if extra_dirs:
        raise V4Error(
            f"manifest partition folder contains nested directories: {manifest_partition_dir}"
        )
    if not (manifest_partition_dir / "_SUCCESS").is_file():
        raise V4Error(
            f"manifest partition folder missing required _SUCCESS file: {manifest_partition_dir}"
        )
    if len(files) != 1 or files[0].name != "_SUCCESS":
        raise V4Error(
            f"manifest partition folder must contain only _SUCCESS: {manifest_partition_dir}"
        )
    if files[0].stat().st_size != 0:
        raise V4Error(
            f"manifest _SUCCESS file must be empty (zero bytes): {files[0]}"
        )


def _create_metadata_json_for_single_parquet(
    parquet_path: str | Path,
    *,
    dataset: str = "polygon_contract_events_v4",
    source_script: str = "raw_data/polygon_contract_events_v4/_internal/parquet_sink.py",
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
        raise V4Error(str(e)) from e
    except Exception as e:
        raise V4Error(f"could not write metadata.json for {parquet_path}: {e}") from e


def create_metadata_json_for_parquet(parquet_path: str | Path) -> Path:
    """Create ``metadata.json`` next to one ``data.parquet`` file."""
    return _create_metadata_json_for_single_parquet(parquet_path)


def _validate_or_prepare_partition_data_dir(partition_dir: Path) -> bool:
    """Return True if folder exists and is valid, False if missing."""
    if not partition_dir.exists():
        return False
    if not partition_dir.is_dir():
        raise V4Error(f"partition path is not a directory: {partition_dir}")

    # A ``data.parquet.tmp-*`` file is an interrupted write, never published content. Dropping it
    # here keeps crash recovery O(1) per partition instead of a full scan of the cold tier.
    for entry in list(partition_dir.iterdir()):
        if entry.is_file() and entry.name.startswith("data.parquet.tmp-"):
            entry.unlink(missing_ok=True)

    entries = list(partition_dir.iterdir())
    if not entries:
        # Recover from interrupted runs that created the partition directory
        # but never wrote any files.
        try:
            partition_dir.rmdir()
        except OSError as e:
            raise V4Error(
                f"empty partition folder could not be removed: {partition_dir}: {e}"
            ) from e
        return False

    parquet = partition_dir / "data.parquet"
    metadata = partition_dir / "metadata.json"

    if not parquet.is_file():
        raise V4Error(f"partition folder missing required data.parquet: {partition_dir}")
    if not metadata.is_file():
        raise V4Error(f"partition folder missing required metadata.json: {partition_dir}")

    allowed = {"data.parquet", "metadata.json"}
    extras = [p.name for p in entries if p.name not in allowed]
    if extras:
        raise V4Error(
            f"partition folder contains unexpected files/directories {extras}: {partition_dir}"
        )
    if len(entries) != 2:
        raise V4Error(
            f"partition folder must contain exactly data.parquet and metadata.json: {partition_dir}"
        )
    return True


def _publish_manifest_success_atomically(cold_path: Path, day_key: int) -> None:
    """Publish a manifest _SUCCESS folder atomically, using shared publish primitives.
    
    Raises V4Error if the final location already exists (immutability protection).
    """
    final_dir = _manifest_dir(cold_path, day_key)
    manifests_parent = final_dir.parent
    final_name = final_dir.name

    # Check immutability before creating temp
    if final_dir.exists():
        raise V4Error(
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


def _day_tmp_rel(day_key: int) -> str:
    """``year=YYYY/month=MM/day=DD.tmp`` beside the final day folder."""
    rel = day_dir(day_key)
    leaf = Path(rel).name
    return str(Path(rel).with_name(leaf + ".tmp"))


def _cleanup_tmp_partition_dirs(cold_path: Path, day_key: int) -> int:
    """Remove ``day=DD.tmp`` directories for one day across every target."""
    removed = 0
    tmp_rel = _day_tmp_rel(day_key)
    for contract, event in all_targets():
        tmp_dir = cold_path / contract / event / tmp_rel
        if tmp_dir.exists():
            shutil.rmtree(tmp_dir, ignore_errors=True)
            removed += 1
    return removed


def publish_manifest(cold_root: str, day_key: int) -> bool:
    """Declare one UTC day sunk by publishing its manifest ``_SUCCESS``.

    A day is sunk when, and only when, this marker exists. The day must be
    the next calendar day after the last manifest (or the first manifest).
    Returns False when its Parquet files are not all present yet.
    """
    cold_path = Path(cold_root)
    if not cold_path.is_dir():
        raise ValueError(f"cold_root does not exist or is not a directory: {cold_root}")

    day_key = int(day_key)
    manifest_dir = _manifest_dir(cold_path, day_key)
    if manifest_dir.exists():
        _validate_manifest_partition_dir(manifest_dir)
        return True

    keys = iter_manifest_day_keys(cold_path)
    if keys and day_key != next_day_key(keys[-1]):
        raise V4Error(
            f"refusing to declare {day_label(day_key)} sunk before "
            f"{day_label(next_day_key(keys[-1]))} has a manifest"
        )

    last_block = _day_last_block(cold_path, day_key)
    if last_block is None:
        return False
    rel_path = day_dir(day_key)
    for contract, event in _expected_targets_for_day(last_block):
        if not _validate_or_prepare_partition_data_dir(
            cold_path / contract / event / rel_path
        ):
            return False

    _publish_manifest_success_atomically(cold_path, day_key)
    _cleanup_tmp_partition_dirs(cold_path, day_key)
    return True


def roll_forward_manifests_to_exhaustion(
    cold_root: str,
    *,
    first_day_key: int | None = None,
    progress_cb: ProgressCallback | None = None,
    stop_event: threading.Event | None = None,
) -> int:
    """Publish ``_SUCCESS`` for every consecutive day whose files are complete.

    With manifests already on disk, the next day is the calendar successor.
    With none, ``first_day_key`` is the day of ``SCRAPE_START_BLOCK``. Without
    either, there is nothing to publish.
    """
    cold_path = Path(cold_root)
    if not cold_path.is_dir():
        raise ValueError(f"cold_root does not exist or is not a directory: {cold_root}")

    keys = iter_manifest_day_keys(cold_path)
    if keys:
        day_key = next_day_key(keys[-1])
    elif first_day_key is not None:
        day_key = int(first_day_key)
    else:
        return 0

    written = 0
    while True:
        if stop_event and stop_event.is_set():
            raise OperationCancelled("manifest roll-forward interrupted")

        if not publish_manifest(cold_root, day_key):
            if progress_cb:
                progress_cb(
                    op="manifest",
                    phase="stop",
                    rows_done=written,
                    partition=day_key,
                    message=f"stopped before {day_label(day_key)} (incomplete)",
                )
            break

        written += 1
        if progress_cb:
            progress_cb(
                op="manifest",
                phase="publish",
                rows_done=written,
                partition=day_key,
                message=f"published manifests/{day_dir(day_key)}",
            )
        day_key = next_day_key(day_key)

    return written


def _discover_day_keys(cold_path: Path) -> set[int]:
    """Every event-tree ``year=/month=/day=`` folder, including ``day=DD.tmp``."""
    found: set[int] = set()
    for folder in cold_path.glob("*/*/year=*/month=*/day=*"):
        if not folder.is_dir():
            continue
        try:
            found.add(day_key_from_hive_dir(folder))
        except ValueError:
            continue
    return found


def _folder_is_complete_two_file(folder: Path) -> bool:
    """True iff ``folder`` exists and holds exactly data.parquet + metadata.json."""
    if not folder.is_dir():
        return False
    names: list[str] = []
    for entry in folder.iterdir():
        if entry.is_file() and entry.name.startswith("data.parquet.tmp-"):
            return False
        names.append(entry.name)
    return (
        set(names) == {"data.parquet", "metadata.json"}
        and (folder / "data.parquet").is_file()
        and (folder / "metadata.json").is_file()
    )


def _unpublished_day_is_complete(cold_path: Path, day_key: int) -> bool:
    """True iff every target deployed by the day's last block has both files."""
    last_block = _day_last_block(cold_path, day_key)
    if last_block is None:
        return False
    expected = _expected_targets_for_day(last_block)
    rel = day_dir(day_key)
    expected_set = set(expected)
    for contract, event in expected:
        if not _folder_is_complete_two_file(cold_path / contract / event / rel):
            return False
    for contract, event in all_targets():
        if (contract, event) in expected_set:
            continue
        extra = cold_path / contract / event / rel
        if extra.exists():
            return False
    return True


def _remove_unpublished_path(path: Path) -> None:
    if not path.exists():
        return
    try:
        if path.is_dir():
            shutil.rmtree(path)
        else:
            path.unlink()
    except OSError as e:
        raise V4Error(f"could not remove unpublished path {path}: {e}") from e


def _cleanup_unsunk_manifest_folder(cold_path: Path, day_key: int) -> int:
    """Remove an unpublished manifest folder. A ``_SUCCESS`` is refused."""
    removed = 0
    manifest_rel = day_dir(day_key)
    manifest_final = cold_path / "manifests" / manifest_rel
    manifest_tmp = cold_path / "manifests" / _day_tmp_rel(day_key)
    if (manifest_final / "_SUCCESS").is_file():
        raise V4Error(
            f"refusing to delete manifested day {day_label(day_key)}: {manifest_final}"
        )
    for path in (manifest_tmp, manifest_final):
        if path.exists():
            _remove_unpublished_path(path)
            removed += 1
    return removed


def _wipe_unpublished_day_event_folders(cold_path: Path, day_key: int) -> int:
    """Delete every event folder for an unpublished day."""
    success = _manifest_dir(cold_path, day_key) / "_SUCCESS"
    if success.is_file():
        raise V4Error(
            f"refusing to delete event folders for manifested day "
            f"{day_label(day_key)}: {success}"
        )
    removed = 0
    rel = day_dir(day_key)
    tmp_rel = _day_tmp_rel(day_key)
    for contract, event in all_targets():
        for folder in (
            cold_path / contract / event / rel,
            cold_path / contract / event / tmp_rel,
        ):
            if folder.exists():
                _remove_unpublished_path(folder)
                removed += 1
    return removed


def cleanup_temp_dirs_after_frontier(
    cold_root: str,
    *,
    progress_cb: ProgressCallback | None = None,
) -> int:
    """Remove unpublished leftovers above the manifest frontier.

    Incomplete day folders above the last manifested day are wiped so the
    next write can redo them. A complete two-file day is left for
    roll-forward. Days at or below the frontier are not inspected.
    """
    cold_path = Path(cold_root)
    if not cold_path.is_dir():
        raise ValueError(f"cold_root does not exist or is not a directory: {cold_root}")

    manifested = iter_manifest_day_keys(cold_path)
    last = manifested[-1] if manifested else None
    removed = 0
    for day_key in sorted(_discover_day_keys(cold_path)):
        if last is not None and day_key <= last:
            continue
        if (_manifest_dir(cold_path, day_key) / "_SUCCESS").is_file():
            raise V4Error(
                "unpublished cleanup found _SUCCESS above the manifest frontier "
                f"at {day_label(day_key)}"
            )
        removed += _cleanup_tmp_partition_dirs(cold_path, day_key)
        removed += _cleanup_unsunk_manifest_folder(cold_path, day_key)
        if not _unpublished_day_is_complete(cold_path, day_key):
            removed += _wipe_unpublished_day_event_folders(cold_path, day_key)

    if progress_cb:
        progress_cb(
            op="manifest",
            phase="cleanup",
            rows_done=removed,
            partition=last,
            message="cleaned unpublished day folders above the frontier",
        )
    return removed


def read_manifest_frontier(
    cold_root: str,
    *,
    first_day_key: int | None = None,
    progress_cb: ProgressCallback | None = None,
) -> tuple[int, int]:
    """Return ``(max_block, day_count)`` of the contiguous manifest run.

    ``max_block`` comes from one ``metadata.json`` of the last day. No
    manifests means ``SCRAPE_START_BLOCK - 1``. When ``first_day_key`` is
    set, the lowest manifest must be that day.
    """
    cold_path = Path(cold_root)
    if not cold_path.is_dir():
        raise ValueError(f"cold_root does not exist or is not a directory: {cold_root}")

    try:
        keys = iter_manifest_day_keys(cold_path)
    except ValueError as exc:
        raise V4Error(str(exc)) from exc
    if not keys:
        if progress_cb:
            progress_cb(op="manifest", phase="read", rows_done=0, message="no manifest days")
        return SCRAPE_START_BLOCK - 1, 0
    if first_day_key is not None and keys[0] != int(first_day_key):
        raise V4Error(
            f"lowest manifest is {day_label(keys[0])}, "
            f"expected {day_label(int(first_day_key))}"
        )
    try:
        assert_contiguous_day_keys(keys)
    except ValueError as exc:
        raise V4Error(str(exc)) from exc

    for idx, day_key in enumerate(keys, start=1):
        _validate_manifest_partition_dir(_manifest_dir(cold_path, day_key))
        if progress_cb:
            progress_cb(
                op="manifest",
                phase="read",
                rows_done=idx,
                rows_total=len(keys),
                partition=day_key,
                message=day_label(day_key),
            )
    try:
        _min_block, max_block = day_block_span(cold_path, keys[-1])
    except ValueError as exc:
        raise V4Error(str(exc)) from exc
    return max_block, len(keys)


def get_sunk_frontier(cold_root: str, *, first_day_key: int | None = None) -> int:
    """Highest sunk block, from contiguous ``manifests/year=/month=/day=/_SUCCESS``.

    The block number is ``max_block`` in that day's metadata. No manifests
    returns ``SCRAPE_START_BLOCK - 1``.
    """
    frontier, _count = read_manifest_frontier(cold_root, first_day_key=first_day_key)
    return frontier


def _rollback_this_call_partition_files(
    pending_renames: list[tuple[Path, Path]],
) -> None:
    """Unlink parquet, metadata, and leftover temps this call created.

    These files are unpublished: ``write_partition_files`` has not returned,
    and the orchestrator has not published ``_SUCCESS``.
    """
    for tmp_path, dst_path in pending_renames:
        try:
            tmp_path.unlink(missing_ok=True)
        except OSError:
            pass
        try:
            dst_path.unlink(missing_ok=True)
        except OSError:
            pass
        meta = dst_path.parent / "metadata.json"
        try:
            meta.unlink(missing_ok=True)
        except OSError:
            pass


def write_partition_files(
    db_path: str,
    cold_root: str,
    day_key: int,
    *,
    duckdb_connect_config: dict[str, str] | None = None,
    progress_cb: ProgressCallback | None = None,
    stop_event: threading.Event | None = None,
) -> PartitionWriteResult:
    """Write one UTC day's complete set of Parquet files to the cold tier.

    This is the cold-tier-producer half of the sink path. It opens its
    own read-only DuckDB connection on ``db_path``, runs every required
    COPY, renames the temp files into place, closes its connection, and
    returns. It does NOT touch the hot-DB bookkeeping; the orchestrator
    must call ``HotStore.commit_sink(day_key)`` after this function
    returns successfully.

    The block span comes from ``metadata.json`` already on disk, or from
    ``utc_day_span`` in the hot DB when that day has not been written.
    Eligible targets are ``tables.targets_active_at(last_block)``.

    Steps:

      1. Open a read-only DuckDB connection on ``db_path`` unless every
         expected file is already complete.
      2. Resolve ``first_block`` and ``last_block``.
      3. For each target, run one DuckDB COPY (see module docstring for
         the exact SQL) to a sibling temp file. Verify the row count
         matches the raw (pre-dedup) count; raise ``DuplicateRowError``
         on mismatch. Every ``block_timestamp`` must fall on ``day_key``.
      4. Rename every temp file to ``data.parquet`` via ``os.replace``.
      5. Write ``metadata.json`` next to each renamed parquet, with
         ``year``, ``month``, ``day``, ``min_block``, and ``max_block``.
      6. Close the read-only connection.

    On any failure, every temp file written by this call is removed, and
    every ``data.parquet`` / ``metadata.json`` this call created is unlinked.
    The hot DB is unchanged.

    Args:
        db_path: Absolute path to the hot ``.db`` file. The function
            opens a fresh connection on it; the orchestrator's
            connection (if any) is unaffected.
        cold_root: Absolute path to the cold-tier root, the directory
            immediately containing per-contract subdirectories.
        day_key: UTC day ``YYYYMMDD``. Its ``first_block`` must be
            ``>= SCRAPE_START_BLOCK``.
        progress_cb: Optional progress callback. Receives events for
            each ``(contract, event)`` COPY.
        stop_event: If set during the operation, raises
            ``OperationCancelled`` at the next safe point. Temp files
            are cleaned up.

    Returns:
        A ``PartitionWriteResult`` describing what was written.

    Raises:
        ValueError: ``day_key`` is not a calendar day, the span starts
            before ``SCRAPE_START_BLOCK``, the day is not a closed span,
            ``cold_root`` does not exist, or ``db_path`` does not exist
            when a write is required.
        DuplicateRowError: at least one ``(contract, event)`` cell
            contains duplicate ``(block_number, transaction_index,
            log_index)`` rows in the hot DB — a fatal logic error in
            the loading path.
        OperationCancelled: ``stop_event`` was set.
        V4Error: any other DuckDB or filesystem failure, including a
            row whose timestamp is not on ``day_key``.

    Preconditions:
        * ``utc_day_span`` has a closed row for ``day_key``, unless
          metadata for that day is already on disk.
        * Every required row for blocks in ``[first_block, last_block]``
          has been loaded via ``HotStore.persist`` and that call has
          returned. The orchestrator submits days from
          ``PersistResult.ready_partitions``.

    Postconditions on success:
        * For every eligible ``(contract, event)``, the file
          ``{cold_root}/{contract}/{event}/year=YYYY/month=MM/day=DD/data.parquet``
          exists and matches the data dictionary's Parquet contract.
        * No partition-key columns inside any file.
        * Rows in each file are sorted and deduplicated, and each
          ``block_timestamp`` falls on that UTC day.
        * The function's DuckDB connection is closed.

    Postconditions on failure:
        * No ``data.parquet`` or ``metadata.json`` files created by this
          call remain (they are unlinked on a caught phase-2 or phase-3
          error). A hard kill can leave unpublished folders above the
          frontier; startup cleanup removes those.
        * Any temp files written by this call have been removed.
        * The hot DB is unchanged.

    Note on caller responsibility:
        After this function returns successfully, the orchestrator
        MUST call ``store.commit_sink(day_key)``. The frontier-ordering
        check lives inside ``commit_sink``; calling days out of order
        is caught there, not here.
    """
    day_key = int(day_key)
    day_key_parts(day_key)
    cold_path = Path(cold_root)
    if not cold_path.is_dir():
        raise ValueError(f"cold_root does not exist or is not a directory: {cold_root}")

    rel_path = day_dir(day_key)
    known_span: tuple[int, int] | None
    try:
        known_span = day_block_span(cold_path, day_key)
    except ValueError as exc:
        # Missing metadata means the day has not been written. A span
        # disagreement or a metadata file without min/max is corrupt.
        if str(exc).startswith("no metadata.json"):
            known_span = None
        else:
            raise V4Error(str(exc)) from exc

    def _classify(targets: list[tuple[str, str]]) -> tuple[list[Path], int]:
        complete: list[Path] = []
        absent = 0
        for contract, event in targets:
            folder = cold_path / contract / event / rel_path
            pq = folder / "data.parquet"
            meta = folder / "metadata.json"
            has_pq = pq.is_file()
            has_meta = meta.is_file()
            if has_pq and has_meta:
                complete.append(pq)
            elif has_pq or has_meta:
                raise V4Error(
                    f"partial cold files for {contract}/{event} "
                    f"{day_label(day_key)}: expected data.parquet and metadata.json"
                )
            else:
                absent += 1
        return complete, absent

    if known_span is not None:
        first_block, last_block = known_span
        if first_block < SCRAPE_START_BLOCK:
            raise ValueError(
                f"{day_label(day_key)} starts at {first_block}, "
                f"before SCRAPE_START_BLOCK {SCRAPE_START_BLOCK}"
            )
        targets = _expected_targets_for_day(last_block)
        complete_files, missing = _classify(targets)
        if complete_files and missing:
            raise V4Error(
                f"{day_label(day_key)} is partially on disk "
                f"({len(complete_files)} complete, {missing} missing targets)"
            )
        if complete_files and not missing:
            return PartitionWriteResult(
                day_key=day_key,
                files_written=len(complete_files),
                rows_total=0,
                bytes_total=sum(p.stat().st_size for p in complete_files),
                elapsed_ms=0.0,
            )
    else:
        first_block = 0
        last_block = 0
        targets = []

    span_from_db = known_span is None
    if not Path(db_path).is_file():
        raise ValueError(f"db_path does not exist: {db_path}")

    t0 = time.monotonic()
    pending_renames: list[tuple[Path, Path]] = []
    rows_total = 0
    bytes_total = 0

    # Open a dedicated worker connection using the same connect config as
    # the writer connection to avoid DuckDB config mismatch errors.
    opened_locally = False
    connect_config = dict(duckdb_connect_config or {})
    try:
        con = duckdb.connect(db_path, config=connect_config)
        opened_locally = True
    except Exception as e:
        raise V4Error(
            f"could not open worker connection to {db_path!r}: {e}"
        ) from e

    try:
        if span_from_db:
            row = con.execute(
                "SELECT first_block, last_block FROM utc_day_span WHERE day_key = ?",
                [day_key],
            ).fetchone()
            if row is None or row[1] is None:
                raise ValueError(
                    f"{day_label(day_key)} is not a closed span in the hot DB"
                )
            first_block = int(row[0])
            last_block = int(row[1])
            if first_block < SCRAPE_START_BLOCK:
                raise ValueError(
                    f"{day_label(day_key)} starts at {first_block}, "
                    f"before SCRAPE_START_BLOCK {SCRAPE_START_BLOCK}"
                )
            if last_block < first_block:
                raise ValueError(
                    f"{day_label(day_key)} span [{first_block}, {last_block}] is empty"
                )
            targets = _expected_targets_for_day(last_block)
            complete_files, missing = _classify(targets)
            if complete_files and missing:
                raise V4Error(
                    f"{day_label(day_key)} is partially on disk "
                    f"({len(complete_files)} complete, {missing} missing targets)"
                )

        year, month, dom = day_key_parts(day_key)
        meta_parameters: dict[str, object] = {
            "year": year,
            "month": month,
            "day": dom,
            "min_block": first_block,
            "max_block": last_block,
        }
        ts_lo = utc_midnight(day_key)
        ts_hi = utc_midnight(next_day_key(day_key))
        label = day_label(day_key)

        for contract, event in targets:
            if stop_event and stop_event.is_set():
                raise OperationCancelled("write_partition_files interrupted")

            # Report before the COPY so the caller can show which file is being written.
            if progress_cb:
                progress_cb(
                    op="sink",
                    phase="copy",
                    rows_done=len(pending_renames),
                    rows_total=len(targets),
                    partition=day_key,
                    message=f"{contract}/{event}",
                )

            tbl = table_name(contract, event)
            cols = all_columns(contract, event)
            col_list = ", ".join(cols)

            dst_dir = cold_path / contract / event / rel_path
            dst_dir.mkdir(parents=True, exist_ok=True)
            dst = dst_dir / "data.parquet"
            # Immutability guard: the cold tier is append-only. A
            # ``data.parquet`` at the destination means this day has
            # already been written for this (contract, event). Refuse
            # to write rather than risk clobbering it with an atomic
            # rename. The caller has a bug if we reach this point.
            if dst.exists():
                raise V4Error(
                    f"refusing to overwrite immutable cold parquet at {dst}: "
                    f"{label} for {contract}/{event} already written"
                )
            tmp = dst_dir / f"data.parquet.tmp-{uuid.uuid4().hex}"

            # Count raw rows before dedup so we can detect duplicates.
            raw_row = con.execute(
                f"SELECT COUNT(*) FROM {tbl} "
                f"WHERE block_number BETWEEN {first_block} AND {last_block}"
            ).fetchone()
            if raw_row is None:
                raise V4Error(
                    f"could not count source rows for {contract}/{event} "
                    f"{label}: empty result"
                )
            raw_count = int(raw_row[0])

            if event == "fee_refunded" and raw_count > 0:
                poison = con.execute(
                    f"SELECT COUNT(*) FROM {tbl} "
                    f"WHERE block_number BETWEEN {first_block} AND {last_block} "
                    f"AND ({sql_poison_predicate()})"
                ).fetchone()
                n_poison = int(poison[0]) if poison else 0
                if n_poison:
                    raise V4Error(
                        f"{contract}/{event} {label}: "
                        f"{n_poison} fee_charged value(s) are not uint256 decimals. "
                        "The decoder stored the indexed feeCharged topic as 32 raw bytes. "
                        "Delete the hot DB and rescrape unsunk blocks. "
                        "Do not publish this day."
                    )

            # COPY with DISTINCT ON deduplicates and sorts in one pass.
            copy_sql = (
                f"COPY (\n"
                f"    SELECT DISTINCT ON (block_number, transaction_index, log_index)\n"
                f"        {col_list}\n"
                f"    FROM {tbl}\n"
                f"    WHERE block_number BETWEEN {first_block} AND {last_block}\n"
                f"    ORDER BY block_number, transaction_index, log_index\n"
                f") TO '{tmp}' (FORMAT PARQUET, COMPRESSION ZSTD)"
            )
            try:
                con.execute(copy_sql)
            except Exception as e:
                raise V4Error(
                    f"COPY failed for {contract}/{event} {label}: {e}"
                ) from e

            # Verify: dedup must not change row count.
            written_row = con.execute(
                f"SELECT COUNT(*) FROM read_parquet('{tmp}')"
            ).fetchone()
            if written_row is None:
                raise V4Error(
                    f"could not count written rows for {contract}/{event} "
                    f"{label}: empty result"
                )
            written_count = int(written_row[0])
            if written_count != raw_count:
                tmp.unlink(missing_ok=True)
                raise DuplicateRowError(
                    f"{contract}/{event} {label}: "
                    f"{raw_count} raw rows but {written_count} after dedup — "
                    f"decoder logic error"
                )

            off_day = con.execute(
                f"SELECT COUNT(*) FROM read_parquet('{tmp}') "
                f"WHERE block_timestamp < {ts_lo} OR block_timestamp >= {ts_hi}"
            ).fetchone()
            n_off = int(off_day[0]) if off_day else 0
            if n_off:
                tmp.unlink(missing_ok=True)
                raise V4Error(
                    f"{contract}/{event} {label}: {n_off} row(s) have "
                    f"block_timestamp outside that UTC day"
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
                    partition=day_key,
                    message=f"{contract}/{event}",
                )

    except (OperationCancelled, DuplicateRowError, V4Error, ValueError):
        # Clean up any temp files written so far.
        for tmp_path, _ in pending_renames:
            try:
                tmp_path.unlink(missing_ok=True)
            except OSError:
                pass
        if opened_locally:
            con.close()
        raise
    except Exception as e:
        for tmp_path, _ in pending_renames:
            try:
                tmp_path.unlink(missing_ok=True)
            except OSError:
                pass
        if opened_locally:
            con.close()
        raise V4Error(
            f"write_partition_files failed for {day_label(day_key)}: {e}"
        ) from e

    # Phase 2: atomic renames.  All COPY calls succeeded; rename each
    # temp into its final name.  Failures here are unlikely (same
    # filesystem) but caught and propagated.
    try:
        for tmp_path, dst_path in pending_renames:
            os.replace(tmp_path, dst_path)
    except Exception as e:
        _rollback_this_call_partition_files(pending_renames)
        if opened_locally:
            con.close()
        raise V4Error(
            f"rename failed during write_partition_files {day_label(day_key)}: {e}"
        ) from e

    if opened_locally:
        con.close()

    # Phase 3: write metadata.json for each renamed parquet file.
    # This must happen after the atomic renames so the parquet path used for
    # row-count and content-hash is the final, immutable cold-tier path.
    try:
        for _tmp_path, dst_path in pending_renames:
            _create_metadata_json_for_single_parquet(
                dst_path, parameters=meta_parameters
            )
    except Exception as e:
        _rollback_this_call_partition_files(pending_renames)
        raise V4Error(
            f"metadata.json write failed for {day_label(day_key)}: {e}"
        ) from e
    elapsed_ms = (time.monotonic() - t0) * 1000
    return PartitionWriteResult(
        day_key=day_key,
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
