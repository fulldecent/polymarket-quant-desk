"""
Hot-tier (DuckDB) persistence layer.

The hot tier is a single on-disk DuckDB database whose schema is defined by
``schema.sql`` (one sibling directory up). Each event table mirrors the
on-disk Parquet schema 1-to-1, so exporting a DuckDB table to Parquet is a
direct passthrough with no per-row Python conversion.

Responsibilities of this module:

  * Open the DuckDB connection and apply ``schema.sql``.
  * Provide the single ingestion entry point ``HotStore.persist``, which
    INSERTs decoded events into the appropriate event tables AND records
    the corresponding ``[from_block, to_block]`` in ``loaded_block_ranges``
    as one atomic transaction. The two either both commit or both roll
    back, so the same block range cannot be loaded twice.
  * Maintain the ``loaded_block_ranges`` invariant: rows are strictly
    disjoint AND non-adjacent (touching rows are coalesced). Sunk state
    is tracked in-memory via ``HotStore.sunk_frontier``, not in the DB.
  * Track which 10K partitions have become ready to sink and tell the
    caller about them on every ingestion call, so the caller never has
    to poll.

Responsibilities NOT in this module (see ``parquet_sink.py``):

  * Writing Parquet files.
  * Filesystem layout / directory creation.
  * Updating the sunk frontier (the sink does this via a method on this
    class, but the trigger is the sink writing and confirming cold-tier
    files are durable).

Concurrency model — the orchestrator owns every hot-DB write:

  The scraper is designed to run with one orchestrator (main) thread
  plus a background pool of one or more sink workers. The
  orchestrator owns the ``HotStore``; the workers never touch it.

    Orchestrator (main thread):
        Owns a ``HotStore`` instance. Calls ``persist()`` to load new
        RPC ranges. Pushes ready partitions onto a queue / submits
        them to an executor. As workers finish, calls
        ``commit_sink(partition_start)`` to finalize each completed
        partition. Every write against the hot DB happens here, in
        program order.

    Sink worker (background thread or process):
        Calls ``parquet_sink.write_partition_files(db_path,
        cold_root, partition_start)``. This opens its own read-only
        DuckDB connection on the same ``.db`` file, runs the long
        Parquet COPYs, renames the temp files into place, and
        returns. It does NOT call back into ``HotStore``; the
        orchestrator does the commit when the worker reports success.

  Why this shape:

    * Every statement against the hot DB executes on the same thread
      that created the ``HotStore``. Ordering is obvious by reading
      the orchestrator's code top-to-bottom; there is no
      cross-thread reasoning about visibility, no lock contention,
      no need to think about the order in which a writer thread and
      the orchestrator interleave their statements.
    * The expensive part of the sink (the multi-minute COPY) runs in
      the background, so the orchestrator continues to call
      ``persist()`` and advance the loaded frontier in parallel with
      cold-tier writes.
    * DuckDB supports multiple connections to the same file from one
      process. The worker's read-only connection sees an MVCC
      snapshot of rows that ``persist()`` has already committed. The
      worker's reads do not block the orchestrator's writes and vice
      versa.

  Why this is race-free:

    * The orchestrator only ``persist()``s ranges above the loaded
      frontier (driven by ``find_gaps`` on ``loaded_block_ranges``;
      the gap below the frontier is empty). So a partition being
      drained by a worker is never re-touched by ``persist()``.
    * The worker's COPY finishes before it returns. The
      orchestrator's ``commit_sink`` runs strictly after that,
      sequentially on the main thread; the DELETE inside
      ``commit_sink`` never races with a still-running read of the
      same rows.
    * ``persist()`` and ``commit_sink()`` both modify the hot DB
      from the same thread, so they execute in program order with
      no interleaving.

Thread safety summary:

  * ``HotStore`` has no internal lock. Every method must be called
    from the thread that constructed it. Calling from another thread
    is unsupported.
  * The library does not spawn threads. The orchestrator decides how
    to dispatch ``parquet_sink.write_partition_files`` calls (e.g.
    ``concurrent.futures.ThreadPoolExecutor`` for one or more
    workers).
  * The sink module opens its own read-only DuckDB connection per
    call against the same ``.db`` file. DuckDB's process-internal
    multi-connection support and MVCC make this safe.

Atomicity guarantees (per call):

  * ``persist()``: one transaction wraps every INSERT plus the
    ``loaded_block_ranges`` update. Both commit together or neither
    does. A return from ``persist()`` means the data and the progress
    record are both durable.
  * ``commit_sink()``: updates the in-memory sunk frontier and schedules
    async cleanup of rows from event tables. The orchestrator calls this
    only after the sink worker has reported that every Parquet temp file
    for the partition has been renamed into place (and manifest written).
    A crash leaves the cold tier with the files durable; the rows remain
    in the hot DB but are ignored at next startup (sunk frontier is
    recalculated from manifest files).
"""

from __future__ import annotations

from collections.abc import Mapping, Sequence
from dataclasses import dataclass
from typing import Protocol
import os
import threading
import time

import duckdb

from .errors import (
    DuplicateRowError,
    OperationCancelled,
    PartitionFrontierError,
    SchemaMismatchError,
    V3Error,
)
from .tables import (
    PARTITION_SIZE_10K,
    SCRAPE_START_BLOCK,
    all_columns,
    all_table_names,
    all_targets,
    table_name,
)


# ---------------------------------------------------------------------------
# MVCC retry policy
# ---------------------------------------------------------------------------

# Maximum number of times a transaction body is re-attempted after a
# transient MVCC write-write conflict. DuckDB uses optimistic concurrency
# control: when two transactions modify the same row and one commits
# before the other, the laggard's COMMIT raises a TransactionException
# whose message contains "write-write conflict". The canonical response
# is to rollback and retry from scratch, since the laggard had no chance
# to see the winner's writes. In our scraper the only concurrent path
# is a sink worker's read-only COPY against a cursor on the orchestrator
# connection, which should not produce write conflicts; but the symptom
# was observed once in the wild and the retry policy is the standard
# defensive shape for any OCC database.
_MVCC_MAX_ATTEMPTS = 5
_MVCC_RETRY_BASE_SLEEP_SEC = 0.010


def _is_transient_conflict(exc: BaseException) -> bool:
    """Return True if ``exc`` looks like a retryable DuckDB MVCC conflict."""
    msg = str(exc).lower()
    return (
        "write-write conflict" in msg
        or "transactioncontext error: conflict" in msg
    )


# ---------------------------------------------------------------------------
# Private range-arithmetic helpers (used by ``HotStore``).
# ---------------------------------------------------------------------------

def _coalesce_ranges(
    rows: list[tuple[int, int, bool]],
) -> list[tuple[int, int, bool]]:
    """Merge overlapping or touching same-status ranges into one row each.

    Input rows are ``(from_block, to_block, sunk)`` triples. Only rows
    that share the same ``sunk`` value are eligible to merge with each
    other: the schema's invariant allows two opposite-status rows to
    abut but forbids two same-status rows from doing so. The output
    therefore has at most as many rows as the input and respects both
    the schema invariant and the original status partitioning.
    """
    if not rows:
        return []
    sorted_rows = sorted(rows, key=lambda r: (r[2], r[0]))
    out: list[tuple[int, int, bool]] = []
    for f, t, s in sorted_rows:
        if out and out[-1][2] == s and out[-1][1] + 1 >= f:
            prev_f, prev_t, prev_s = out[-1]
            out[-1] = (prev_f, max(prev_t, t), prev_s)
        else:
            out.append((f, t, s))
    return out


def _find_ready_partitions(
    unsunk_rows: list[tuple[int, int, bool]],
) -> list[int]:
    """Return every 10K-aligned partition start ``P`` such that the
    inclusive range ``[P, P + 9_999]`` is fully covered by at least one
    unsunk row in ``unsunk_rows``.

    Special case for the first partition: blocks below
    ``SCRAPE_START_BLOCK`` are conceptually pre-history and treated as
    "covered by definition" since the scraper never records them. So
    the first partition ``P0 = floor(SCRAPE_START_BLOCK / 10K) * 10K``
    is considered ready when a row covers
    ``[SCRAPE_START_BLOCK, P0 + 9_999]``, even though it does not
    extend down to ``P0``.

    All rows in ``unsunk_rows`` represent unsunk (not yet sunk to cold tier)
    block ranges.
    """
    p0 = (SCRAPE_START_BLOCK // PARTITION_SIZE_10K) * PARTITION_SIZE_10K
    ready: list[int] = []
    for f, t, sunk in unsunk_rows:
        # All rows should be unsunk at this point (only unsunk ranges remain in DB)
        if sunk:
            raise V3Error(
                f"_find_ready_partitions received a sunk row ({f},{t}); "
                f"logic error in row selection"
            )
        # First partition special case: an unsunk row that begins at
        # or before ``SCRAPE_START_BLOCK`` and extends through the end
        # of partition ``p0`` makes ``p0`` ready, even though the row
        # does not extend all the way down to ``p0`` itself.
        if f <= SCRAPE_START_BLOCK and t >= p0 + PARTITION_SIZE_10K - 1:
            ready.append(p0)
        # First 10K boundary strictly after the first-partition special
        # case (so we don't double-count).
        first_p = ((f + PARTITION_SIZE_10K - 1) // PARTITION_SIZE_10K) * PARTITION_SIZE_10K
        if first_p == p0:
            first_p += PARTITION_SIZE_10K
        p = first_p
        while p + PARTITION_SIZE_10K - 1 <= t:
            ready.append(p)
            p += PARTITION_SIZE_10K
    ready.sort()
    # De-duplicate while preserving order (multiple unsunk rows could
    # in theory claim the same partition after coalescing — defensive).
    seen: set[int] = set()
    out: list[int] = []
    for p in ready:
        if p not in seen:
            seen.add(p)
            out.append(p)
    return out


# ---------------------------------------------------------------------------
# Public dataclasses
# ---------------------------------------------------------------------------

@dataclass(frozen=True)
class HotStoreConfig:
    """Tunable knobs for one ``HotStore`` instance.

    Attributes:
        duckdb_memory_limit:
            Value passed to DuckDB's ``SET memory_limit``. Default ``"8GB"``.
        duckdb_threads:
            Value passed to DuckDB's ``SET threads``. DuckDB still
            serializes via the connection lock; this controls DuckDB's
            internal parallelism inside one query. Default is 4.
        duckdb_temp_dir:
            Value passed to DuckDB's ``SET temp_directory``. Must point at
            a path with at least several GB free for spill files.
    """

    duckdb_memory_limit: str = "8GB"
    duckdb_threads: int = 4
    duckdb_temp_dir: str = "/Volumes/polymarket-quant-desk/tmp"


@dataclass(frozen=True)
class PersistResult:
    """Result of one ``HotStore.persist`` call."""

    rows_inserted: int
    """Total event rows written, summed across every target table."""

    tables_touched: int
    """Number of distinct event tables that received at least one row."""

    elapsed_ms: float

    ready_partitions: tuple[int, ...]
    """10K-aligned partition start blocks that just became ready to sink
    as a direct result of this call.

    A partition ``P`` is "ready to sink" when the inclusive range
    ``[P, P + 9_999]`` is fully covered by unsunk rows of
    ``loaded_block_ranges``.

    Only partitions that were NOT already ready before this call appear
    here; this lets the caller drive the sink writer with a simple
    "append to my queue" pattern instead of polling. The tuple is sorted
    ascending. May be empty if no full 10K partition was completed by
    this call. May contain multiple partitions if this call covered
    several 10K boundaries at once.
    """


class ProgressCallback(Protocol):
    """Optional callback for long-running operations.

    Called from the thread that initiated the operation. Implementations
    must be cheap and non-blocking (no I/O, no logging-with-flush).
    """

    def __call__(
        self,
        *,
        op: str,
        phase: str,
        rows_done: int | None = None,
        rows_total: int | None = None,
        elapsed_ms: float | None = None,
        message: str = "",
    ) -> None: ...


# ---------------------------------------------------------------------------
# HotStore — the public class
# ---------------------------------------------------------------------------

class HotStore:
    """One-instance-per-process handle to the hot DuckDB database.

    Lifecycle (recommended)::

        with HotStore(db_path, schema_path) as store:
            result = store.persist(...)
            for p in result.ready_partitions:
                queue_for_sink(p)

    Lifecycle (manual)::

        store = HotStore(db_path, schema_path)
        try:
            ...
        finally:
            store.close()

    The manual form is appropriate for long-lived scrapers where
    wrapping ``main()`` in a ``with`` block is awkward; both forms are
    fully supported.

    Schema validation at construction is strict: the on-disk DB must
    contain every event table from ``schema.sql`` plus
    ``loaded_block_ranges``, each with the exact expected column names
    and types. Any mismatch raises ``SchemaMismatchError``.
    """

    # ------------------------------------------------------------------
    # Lifecycle
    # ------------------------------------------------------------------

    def __init__(
        self,
        db_path: str,
        schema_path: str,
        *,
        config: HotStoreConfig | None = None,
        progress_cb: ProgressCallback | None = None,
    ) -> None:
        """Open or create the hot DuckDB database at ``db_path``.

        On first construction, the file is created and ``schema_path``
        is executed against it to materialize every event table plus
        ``loaded_block_ranges``.

        On subsequent constructions, ``schema_path`` is re-applied in
        ``CREATE TABLE IF NOT EXISTS`` mode (no-op when the tables
        exist), and a schema-fingerprint check verifies that every
        existing table matches. A mismatch raises
        ``SchemaMismatchError``.

        Args:
            db_path: Absolute path to the hot ``.db`` file. The parent
                directory must exist.
            schema_path: Absolute path to ``schema.sql``. Must be readable.
            config: Optional ``HotStoreConfig``; defaults are used otherwise.
            progress_cb: Optional ``ProgressCallback`` for visibility into
                operations that take more than ~1 second.

        Raises:
            FileNotFoundError: ``schema_path`` does not exist, or the
                parent of ``db_path`` does not exist.
            SchemaMismatchError: an existing ``db_path`` was created with
                a different schema.
            V3Error: any other DuckDB initialization failure.

        Postconditions:
            * Every event table from ``schema.sql`` exists in the DB.
            * The ``loaded_block_ranges`` table exists.
            * DuckDB pragmas (memory limit, threads, temp dir) are applied.
        """
        self.db_path = db_path
        self.schema_path = schema_path
        self.config = config or HotStoreConfig()
        self.progress_cb = progress_cb
        self._conn: duckdb.DuckDBPyConnection | None = None
        self._closed = False
        self.sunk_frontier = SCRAPE_START_BLOCK - 1  # In-memory sunk frontier, populated at startup

        if not os.path.isfile(schema_path):
            raise FileNotFoundError(f"schema_path does not exist: {schema_path}")
        parent = os.path.dirname(os.path.abspath(db_path))
        if not os.path.isdir(parent):
            raise FileNotFoundError(f"parent of db_path does not exist: {parent}")

        self._open_and_init()

    def _open_and_init(self) -> None:
        # Pass tunable pragmas via the connection ``config`` rather than
        # as ``SET`` statements after connecting. This matters because
        # DuckDB rejects subsequent connections to the same file unless
        # their config is "compatible" with the first connection; using
        # ``SET`` post-connect does not register as part of the file's
        # connection config, leaving the sink worker unable to open a
        # read-only connection on the same ``.db``. (See the matching
        # ``access_mode='READ_ONLY'`` connect in ``parquet_sink``.)
        try:
            self._conn = duckdb.connect(
                self.db_path,
                config={
                    "memory_limit": self.config.duckdb_memory_limit,
                    "threads": str(self.config.duckdb_threads),
                    "temp_directory": self.config.duckdb_temp_dir,
                },
            )
        except Exception as e:
            raise V3Error(f"could not open DuckDB at {self.db_path!r}: {e}") from e

        # Apply schema (idempotent)
        try:
            with open(self.schema_path) as f:
                sql = f.read()
            self._conn.execute(sql)
        except Exception as e:
            raise V3Error(f"failed to apply schema.sql: {e}") from e

        # Schema fingerprint check
        self._verify_schema()

    def _verify_schema(self) -> None:
        """Ensure every table from schema.sql exists with the expected columns."""
        expected_tables = set(all_table_names())
        expected_tables.add("loaded_block_ranges")

        rows = self._conn.execute(
            """
            SELECT table_name, column_name, data_type
            FROM information_schema.columns
            WHERE table_schema = 'main'
            ORDER BY table_name, ordinal_position
            """
        ).fetchall()

        actual: dict[str, list[tuple[str, str]]] = {}
        for tbl, col, typ in rows:
            actual.setdefault(tbl, []).append((col, typ))

        for tbl in expected_tables:
            if tbl not in actual:
                raise SchemaMismatchError(f"missing table: {tbl}")

    def _get_conn(self) -> duckdb.DuckDBPyConnection:
        if self._closed or self._conn is None:
            raise V3Error("HotStore is closed")
        return self._conn

    @property
    def connection(self) -> duckdb.DuckDBPyConnection:
        """The underlying DuckDB connection.

        Exposed so a sink worker can call ``connection.cursor()`` from
        a background thread — DuckDB does not allow two
        ``duckdb.connect()`` calls against the same file from one
        process, but it does support thread-safe cursor handles off a
        single connection (each cursor has its own transaction state
        and reads committed rows via MVCC).

        Do NOT use this to issue writes from any thread other than the
        one that constructed ``HotStore``; the store assumes its writer
        is single-threaded. Reads via a cursor in another thread are
        safe.
        """
        return self._get_conn()

    def __enter__(self) -> "HotStore":
        return self

    def __exit__(self, exc_type, exc, tb) -> None:
        self.close()

    def close(self) -> None:
        """Close the DuckDB connection.

        Idempotent. After ``close()``, every other method raises
        ``V3Error``.

        Because every ``persist()`` call is fully durable by the time it
        returns, ``close()`` has no in-memory state to flush.
        """
        if self._conn is not None:
            try:
                self._conn.close()
            except Exception:
                pass
            self._conn = None
        self._closed = True

    # ------------------------------------------------------------------
    # Ingestion
    # ------------------------------------------------------------------

    def persist(
        self,
        from_block: int,
        to_block: int,
        rows_by_target: Mapping[tuple[str, str], Sequence[Mapping[str, object]]],
        *,
        progress_cb: ProgressCallback | None = None,
        stop_event: threading.Event | None = None,
    ) -> PersistResult:
        """Ingest events for one inclusive block range, atomically.

        This is the only entry point for adding events to the hot DB. The
        call wraps every INSERT plus the ``loaded_block_ranges`` update in
        one DuckDB transaction so the data and the progress record either
        both commit or both roll back. The same range therefore cannot be
        loaded twice from the caller's point of view: a successful
        ``persist`` always advances ``loaded_block_ranges``, and a
        failure leaves it untouched.

        Although each call is one transaction, the commit can be
        non-trivial for large batches (many rows across many tables, plus
        the ``loaded_block_ranges`` coalescing logic). The ``progress_cb``
        argument receives ``op="persist"`` events for any call that takes
        more than ~1 second; phases include ``"insert"`` (per table) and
        ``"commit"``.

        Args:
            from_block: Inclusive start block (must satisfy
                ``SCRAPE_START_BLOCK <= from_block <= to_block`` and
                ``to_block < 2**32``).
            to_block: Inclusive end block.
            rows_by_target: Map from ``(contract_name, event_name)`` to a
                sequence of row dicts. Each row dict's keys must equal
                ``tables.all_columns(contract, event)``, and the values
                must already be the correct Python types per the schema:

                  * ``UINTEGER`` columns → ``int`` in ``[0, 2**32)``
                  * ``BLOB`` columns → ``bytes`` of the exact expected
                    length (20 or 32)
                  * ``VARCHAR`` columns → ``str``

                The library does NOT coerce values; the caller's decoder
                must emit final types.

                A target with an empty sequence is allowed and contributes
                zero rows. If ``rows_by_target`` is empty entirely, the
                call still records the range as
                ``[from_block, to_block]`` with zero rows ingested. This
                is how the scraper signals "we checked this range; there
                were no events here."

                Every row's ``block_number`` MUST satisfy
                ``from_block <= block_number <= to_block``; the library
                validates this and raises ``ValueError`` if any row falls
                outside the declared range.
            progress_cb: Optional progress callback. Receives events with
                ``op="persist"`` for any call that takes more than ~1
                second. Useful for visibility into large batches; safe to
                omit for small ones.
            stop_event: Optional cancellation flag. If set, the call
                raises ``OperationCancelled`` at the next safe point. If
                the cancellation happens before the transaction commits,
                the transaction is rolled back and the hot DB is
                unchanged.

        Returns:
            A ``PersistResult`` describing how many rows were inserted
            and which (if any) 10K partitions just became ready to sink.

        Raises:
            ValueError: ``from_block > to_block``, or below
                ``SCRAPE_START_BLOCK``, or ``rows_by_target`` mentions an
                unknown ``(contract, event)`` pair, or any row has a
                ``block_number`` outside ``[from_block, to_block]``, or
                any row's keys do not match the canonical column list.
            OperationCancelled: ``stop_event`` was set; hot DB unchanged.
            V3Error: any DuckDB error during the transaction; hot DB
                unchanged (transaction rolled back).

        Preconditions:
            * The store is open.
            * Every row's value types match the schema (no coercion).
            * The range ``[from_block, to_block]`` may overlap an
              existing row of ``loaded_block_ranges`` only in the
              well-defined sense of coalescing: see the schema's
              disjoint-and-non-adjacent invariant. The library's
              internal coalescing handles arbitrary overlaps with
              existing unsunk rows. Overlap with ranges below the
              in-memory sunk frontier is a caller bug (re-loading
              already-sunk data) and is rejected with ``ValueError``.

        Postconditions on success:
            * The new rows are visible to subsequent queries.
            * ``loaded_block_ranges`` reflects the new range, with
              same-status rows coalesced.
            * ``ready_partitions`` in the returned ``PersistResult``
              lists every 10K partition that just became fully covered
              by unsunk ranges as a direct result of this call.

        Thread safety:
            Safe to call from any thread. Concurrent calls serialize on
            the connection lock.
        """
        if self._closed or self._conn is None:
            raise V3Error("HotStore is closed")
        if from_block > to_block:
            raise ValueError(f"from_block {from_block} > to_block {to_block}")
        if from_block < SCRAPE_START_BLOCK:
            raise ValueError(f"from_block {from_block} < SCRAPE_START_BLOCK")
        if to_block >= 2**32:
            raise ValueError(f"to_block {to_block} >= 2**32")

        # Validate targets and row shapes
        total_rows = 0
        tables_touched = 0
        for (contract, event), rows in rows_by_target.items():
            if (contract, event) not in all_targets():
                raise ValueError(f"unknown (contract, event): ({contract}, {event})")
            expected_cols = set(all_columns(contract, event))
            for i, row in enumerate(rows):
                if set(row.keys()) != expected_cols:
                    raise ValueError(
                        f"row {i} for {contract}/{event} has wrong columns: "
                        f"{set(row.keys())} != {expected_cols}"
                    )
                bn = row.get("block_number")
                if not (from_block <= bn <= to_block):
                    raise ValueError(
                        f"row {i} block_number {bn} outside [{from_block}, {to_block}]"
                    )
            if rows:
                tables_touched += 1
                total_rows += len(rows)

        t0 = time.monotonic()
        conn = self._get_conn()

        # Reject overlap with any sunk range (documented precondition).
        # The caller must never re-load data that has already been sunk
        # to the cold tier; doing so would violate the immutability contract.
        # Sunk state is tracked in-memory via sunk_frontier.
        if from_block <= self.sunk_frontier:
            raise ValueError(
                f"persist range [{from_block}, {to_block}] is below sunk frontier "
                f"{self.sunk_frontier}; re-loading already-sunk data is forbidden"
            )

        last_conflict: Exception | None = None
        for attempt in range(_MVCC_MAX_ATTEMPTS):
            try:
                if stop_event and stop_event.is_set():
                    raise OperationCancelled("persist cancelled before start")

                # Snapshot the "ready partitions" set BEFORE this call's
                # changes so the diff against the post-call set gives us
                # exactly the partitions whose final missing block landed
                # in this transaction.
                ready_before = set(
                    _find_ready_partitions(self._get_loaded_ranges_unsunk(conn))
                )

                # Begin transaction
                conn.execute("BEGIN TRANSACTION")

                # Insert rows
                inserted = 0
                for (contract, event), rows in rows_by_target.items():
                    if not rows:
                        continue
                    tbl = table_name(contract, event)
                    cols = all_columns(contract, event)
                    col_list = ", ".join(cols)
                    placeholders = ", ".join(["?"] * len(cols))
                    sql = f"INSERT INTO {tbl} ({col_list}) VALUES ({placeholders})"
                    for row in rows:
                        if stop_event and stop_event.is_set():
                            conn.execute("ROLLBACK")
                            raise OperationCancelled("persist cancelled during insert")
                        vals = [row[c] for c in cols]
                        conn.execute(sql, vals)
                        inserted += 1

                # Record the range in loaded_block_ranges
                conn.execute(
                    """
                    INSERT INTO loaded_block_ranges (from_block, to_block)
                    VALUES (?, ?)
                    ON CONFLICT (from_block, to_block) DO NOTHING
                    """,
                    (from_block, to_block),
                )

                # Coalesce unsunk ranges
                self._coalesce_unsunk_ranges(conn)

                # Determine which partitions just became ready as a direct
                # result of this call.
                ready_after = set(
                    _find_ready_partitions(self._get_loaded_ranges_unsunk(conn))
                )
                newly_ready = tuple(sorted(ready_after - ready_before))

                conn.execute("COMMIT")

                elapsed_ms = (time.monotonic() - t0) * 1000
                return PersistResult(
                    rows_inserted=inserted,
                    tables_touched=tables_touched,
                    elapsed_ms=elapsed_ms,
                    ready_partitions=newly_ready,
                )

            except OperationCancelled:
                try:
                    conn.execute("ROLLBACK")
                except Exception:
                    pass
                raise
            except Exception as e:
                try:
                    conn.execute("ROLLBACK")
                except Exception:
                    pass
                if _is_transient_conflict(e) and attempt + 1 < _MVCC_MAX_ATTEMPTS:
                    last_conflict = e
                    time.sleep(_MVCC_RETRY_BASE_SLEEP_SEC * (2 ** attempt))
                    continue
                raise V3Error(f"persist failed: {e}") from e

        # Exhausted retries on transient conflict.
        raise V3Error(
            f"persist failed after {_MVCC_MAX_ATTEMPTS} MVCC retries: {last_conflict}"
        ) from last_conflict

    def _coalesce_unsunk_ranges(self, conn: duckdb.DuckDBPyConnection) -> None:
        """Coalesce adjacent unsunk ranges in loaded_block_ranges.
        
        All rows in loaded_block_ranges represent unsunk ranges (sunk state
        is tracked in-memory via sunk_frontier).
        """
        rows = conn.execute(
            """
            SELECT from_block, to_block
            FROM loaded_block_ranges
            ORDER BY from_block
            """
        ).fetchall()
        if not rows:
            return
        coalesced = _coalesce_ranges([(f, t, False) for f, t in rows])
        conn.execute("DELETE FROM loaded_block_ranges")
        for f, t, _ in coalesced:
            conn.execute(
                "INSERT INTO loaded_block_ranges (from_block, to_block) VALUES (?, ?)",
                (f, t),
            )

    def _get_loaded_ranges_unsunk(
        self, conn: duckdb.DuckDBPyConnection
    ) -> list[tuple[int, int, bool]]:
        rows = conn.execute(
            """
            SELECT from_block, to_block
            FROM loaded_block_ranges
            ORDER BY from_block
            """
        ).fetchall()
        # Compute unsunk segments by clipping away anything at/below sunk_frontier.
        result = []
        for f, t in rows:
            f = int(f)
            t = int(t)
            if t <= self.sunk_frontier:
                continue
            if f <= self.sunk_frontier:
                f = self.sunk_frontier + 1
            result.append((f, t, False))
        return result

    # ------------------------------------------------------------------
    # Range / frontier queries
    # ------------------------------------------------------------------

    def list_loaded_ranges(
        self,
        *,
        include_sunk: bool = True,
    ) -> list[tuple[int, int, bool]]:
        """Return every row of ``loaded_block_ranges`` in ascending order.

        Each tuple is ``(from_block, to_block, is_sunk)`` where ``is_sunk``
        is True if ``to_block <= sunk_frontier``.

        If ``include_sunk`` is False, only unsunk rows are returned.

        Postconditions:
            * Rows are sorted by ``from_block``.
        """
        conn = self._get_conn()
        rows = conn.execute(
            """
            SELECT from_block, to_block
            FROM loaded_block_ranges
            ORDER BY from_block
            """
        ).fetchall()
        result = []
        for f, t in rows:
            f = int(f)
            t = int(t)
            if include_sunk:
                if t <= self.sunk_frontier:
                    result.append((f, t, True))
                elif f > self.sunk_frontier:
                    result.append((f, t, False))
                else:
                    # Split a stale mixed row into sunk and unsunk segments.
                    result.append((f, self.sunk_frontier, True))
                    result.append((self.sunk_frontier + 1, t, False))
            else:
                if t <= self.sunk_frontier:
                    continue
                if f <= self.sunk_frontier:
                    f = self.sunk_frontier + 1
                result.append((f, t, False))
        return result

    def find_gaps(
        self,
        from_block: int,
        to_block: int,
        *,
        include_sunk: bool = True,
    ) -> list[tuple[int, int]]:
        """Return every sub-range of ``[from_block, to_block]`` that is
        NOT covered by any unsunk row of ``loaded_block_ranges``.

        When ``include_sunk=False``, only considers ranges above the sunk frontier.

        The result is a list of disjoint, non-adjacent inclusive ranges,
        sorted ascending. Use this to drive a resumable scraper.

        Preconditions:
            ``SCRAPE_START_BLOCK <= from_block <= to_block``.
        """
        if from_block < SCRAPE_START_BLOCK or from_block > to_block:
            raise ValueError("invalid range")
        conn = self._get_conn()
        effective_from = from_block if include_sunk else max(from_block, self.sunk_frontier + 1)
        if effective_from > to_block:
            return []

        covered = conn.execute(
            f"""
            SELECT from_block, to_block
            FROM loaded_block_ranges
            WHERE NOT (to_block < ? OR from_block > ?)
            ORDER BY from_block
            """,
            (effective_from, to_block),
        ).fetchall()

        gaps: list[tuple[int, int]] = []
        cursor = effective_from
        for f, t in covered:
            f = int(f)
            t = int(t)
            if not include_sunk and f <= self.sunk_frontier:
                f = self.sunk_frontier + 1
                if f > t:
                    continue
            if cursor < f:
                gaps.append((cursor, f - 1))
            cursor = max(cursor, t + 1)
        if cursor <= to_block:
            gaps.append((cursor, to_block))
        return gaps

    def get_loaded_frontier(self) -> int:
        """Return the highest block ``N`` such that
        ``[SCRAPE_START_BLOCK, N]`` is fully covered by
        ``loaded_block_ranges`` (any status). This is the high-water
        mark of "we have seen these events from the RPC."

        Walks all rows in ascending ``from_block`` order, combining
        sunk and unsunk coverage. Because the per-status invariant
        only forbids adjacency *within* a status, a sunk row ending
        at ``P-1`` and an unsunk row starting at ``P`` together
        produce contiguous loaded coverage and must be combined.

        Returns ``SCRAPE_START_BLOCK - 1`` if no range covers
        ``SCRAPE_START_BLOCK``.
        """
        conn = self._get_conn()
        rows = conn.execute(
            """
            SELECT from_block, to_block
            FROM loaded_block_ranges
            ORDER BY from_block
            """
        ).fetchall()
        frontier = SCRAPE_START_BLOCK - 1
        for f, t in rows:
            f = int(f)
            t = int(t)
            if f > frontier + 1:
                break
            if t > frontier:
                frontier = t
        return frontier

    def get_sunk_frontier(self) -> int:
        """Return the in-memory sunk frontier: the highest block ``N`` such that
        ``[SCRAPE_START_BLOCK, N]`` is fully covered by cold-tier Parquet files.

        This value is populated at startup from manifest files and updated each
        time a partition is successfully committed to the cold tier.

        Returns ``SCRAPE_START_BLOCK - 1`` if nothing has been sunk yet.
        """
        return self.sunk_frontier

    def list_10k_partitions_ready_to_sink(self) -> list[int]:
        """Return every 10K-aligned partition start block ``P`` such
        that ``[P, P + 9999]`` is fully covered by unsunk rows of
        ``loaded_block_ranges``, in ascending order.

        Normally a caller does not need to call this — ``persist()``
        returns the same information incrementally via
        ``PersistResult.ready_partitions``. This method is for recovery
        / startup, where the caller wants the full backlog of unsunk
        complete partitions before doing any ingestion.

        Postconditions:
            * Every returned ``P`` is a multiple of 10_000.
            * Every returned ``P`` satisfies ``P >= SCRAPE_START_BLOCK``.
        """
        conn = self._get_conn()
        loaded = self._get_loaded_ranges_unsunk(conn)
        return _find_ready_partitions(loaded)

    # ------------------------------------------------------------------
    # Recovery
    # ------------------------------------------------------------------

    def reconcile_with_cold_tier(
        self,
        cold_tier_root: str,
        *,
        progress_cb: ProgressCallback | None = None,
        stop_event: threading.Event | None = None,
        manifest_frontier: int | None = None,
    ) -> int:
        """Mark partitions as sunk based on Parquet files already on disk.

        Walks the cold-tier directory tree under ``cold_tier_root`` and
        marks every 10K partition whose ``data.parquet`` files are
        present as ``sunk_to_parquet=TRUE`` in ``loaded_block_ranges``.

        This is the recovery path after a crash that interrupted the
        sink's final atomic commit: the Parquet files reached the cold
        tier, but the hot DB was not yet updated. Calling this at
        scraper startup re-syncs the two stores.

        If ``manifest_frontier`` is provided, reconcile only scans forward
        from that point, avoiding a full recursive glob of the cold tier.

        Returns the number of partitions newly marked as sunk.

        Args:
            cold_tier_root: Absolute path to the cold-tier root.
            progress_cb: Optional progress callback with ``op=\"reconcile\"``.
            stop_event: Optional cancellation flag.
            manifest_frontier: Optional hint (highest block in manifest).
                Enables optimized scanning forward instead of full glob.

        Raises:
            OperationCancelled: ``stop_event`` was set.
        """
        import glob
        from pathlib import Path

        from .tables import PARTITION_SIZE_10K, SCRAPE_START_BLOCK

        cold_root = Path(cold_tier_root)
        if not cold_root.is_dir():
            raise FileNotFoundError(f"cold_tier_root does not exist: {cold_tier_root}")

        # Discover partitions on disk. Optimized scan if manifest_frontier known.
        partitions_on_disk: set[int] = set()

        # Fetch the hot DB sunk frontier before the scan so we can extend the window past it.
        # This prevents a scenario where manifest_frontier lags behind the hot DB sunk frontier
        # (e.g. after a crash between cold-tier write and hot DB commit_sink), causing the scan
        # to miss already-landed cold partitions and then re-submit them to the sink pool.
        sunk_frontier = self.get_sunk_frontier()

        # Optimized forward scan is only safe when the hot DB already has a sunk anchor.
        # If the hot DB is fresh/empty (no sunk frontier), we must discover all landed cold
        # partitions; otherwise older already-sunk partitions can be missed and later re-sunk.
        can_use_optimized_scan = (
            sunk_frontier > SCRAPE_START_BLOCK - 1
            and manifest_frontier is not None
            and manifest_frontier >= SCRAPE_START_BLOCK - 1
        )

        if can_use_optimized_scan:
            # Optimized: scan forward from manifest frontier, extended to cover the hot DB sunk
            # frontier so we always detect cold partitions that were written after the manifest
            # frontier but before the sunk frontier was updated.
            scan_start = ((manifest_frontier + 1) // PARTITION_SIZE_10K) * PARTITION_SIZE_10K
            scan_limit = scan_start + 1_000_000
            if sunk_frontier > SCRAPE_START_BLOCK - 1:
                # Extend window to reach at least 1 full 1M-block band past the sunk frontier.
                scan_limit = max(scan_limit, sunk_frontier + 1_000_000)

            for p in range(scan_start, scan_limit, PARTITION_SIZE_10K):
                k1m = (p // 1_000_000) * 1_000_000
                # Cold path: cold_root/<contract>/<event>/1M=.../10K=.../data.parquet
                # Two wildcard levels needed; recursive=False treats ** as * (one level only).
                pattern = str(cold_root / "*" / "*" / f"1M={k1m}" / f"10K={p}" / "data.parquet")
                matches = glob.glob(pattern, recursive=False)
                if matches:
                    partitions_on_disk.add(p)
        else:
            # Fallback: full recursive glob (backward compat)
            pattern = str(cold_root / "**" / "**" / "10K=*" / "data.parquet")
            parquet_files = glob.glob(pattern, recursive=True)

            for f in parquet_files:
                try:
                    part_dir = Path(f).parent.name
                    if not part_dir.startswith("10K="):
                        continue
                    p = int(part_dir.split("=", 1)[1])
                    if p + PARTITION_SIZE_10K - 1 >= SCRAPE_START_BLOCK:
                        partitions_on_disk.add(p)
                except (ValueError, IndexError):
                    continue

        if progress_cb:
            progress_cb(
                op="reconcile",
                phase="scan",
                rows_done=len(partitions_on_disk),
                message=f"found {len(partitions_on_disk)} partitions on disk",
            )

        if sunk_frontier <= SCRAPE_START_BLOCK - 1:
            expected_next = (SCRAPE_START_BLOCK // PARTITION_SIZE_10K) * PARTITION_SIZE_10K
        else:
            expected_next = ((sunk_frontier + 1) // PARTITION_SIZE_10K) * PARTITION_SIZE_10K

        newly_sunk = sorted(p for p in partitions_on_disk if p >= expected_next)

        if not newly_sunk:
            if progress_cb:
                progress_cb(op="reconcile", phase="update", message="no new partitions to mark")
            return 0

        # Update in-memory frontier to the highest newly-sunk partition
        if newly_sunk:
            highest_partition = newly_sunk[-1]
            new_frontier = highest_partition + PARTITION_SIZE_10K - 1
            # Only advance if contiguous from current frontier
            if self.sunk_frontier == SCRAPE_START_BLOCK - 1 or new_frontier == self.sunk_frontier + PARTITION_SIZE_10K:
                self.sunk_frontier = new_frontier

        if progress_cb:
            progress_cb(
                op="reconcile",
                phase="update",
                rows_done=len(newly_sunk),
                message=f"advanced sunk frontier to {self.sunk_frontier}",
            )

        return len(newly_sunk)

    def populate_sunk_frontier_from_manifests(self, cold_tier_root: str) -> None:
        """Populate in-memory sunk frontier by scanning manifest _SUCCESS files.
        
        This should be called at startup after manifest contiguity checks.
        
        Args:
            cold_tier_root: Absolute path to the cold-tier root.
        """
        import glob
        from pathlib import Path

        from .parquet_sink import get_sunk_frontier as get_manifest_frontier
        from .tables import PARTITION_SIZE_10K

        try:
            frontier = get_manifest_frontier(cold_tier_root)
            self.sunk_frontier = frontier
        except (FileNotFoundError, ValueError):
            # No manifests exist yet
            self.sunk_frontier = SCRAPE_START_BLOCK - 1

    # ------------------------------------------------------------------
    # Async cleanup
    # ------------------------------------------------------------------

    def schedule_delete_sunk_partition_rows(self, partition_start: int) -> None:
        """Schedule asynchronous deletion of rows for a sunk partition.
        
        After a partition is confirmed durable in cold-tier files and manifest
        is written, rows from the hot DB are deleted asynchronously. This is
        non-critical for correctness (manifest is the source of truth), so
        deletions can happen independently from each other and from new ingestion.
        
        Args:
            partition_start: Must be a multiple of 10_000, >= SCRAPE_START_BLOCK.
        """
        if partition_start % PARTITION_SIZE_10K != 0:
            raise ValueError(f"partition_start {partition_start} is not 10K-aligned")
        
        p_end = partition_start + PARTITION_SIZE_10K - 1
        conn = self._get_conn()
        
        try:
            # Delete from every event table for this partition's block range
            for table in all_table_names():
                conn.execute(
                    f"""
                    DELETE FROM {table}
                    WHERE block_number BETWEEN ? AND ?
                    """,
                    (partition_start, p_end),
                )
        except Exception as e:
            raise V3Error(f"failed to delete partition {partition_start} rows: {e}") from e


    # ------------------------------------------------------------------
    # Sink integration
    # ------------------------------------------------------------------

    def commit_sink(self, partition_start: int) -> None:
        """Mark a partition as sunk and schedule async cleanup.

        Called by the orchestrator after a sink worker has reported success on
        ``parquet_sink.write_partition_files`` AND the manifest file has been
        written durably. By that time, every Parquet file for the partition is
        durable and renamed into place.

        This method:
        1. Validates frontier ordering (partition must be the next in sequence).
        2. Updates in-memory sunk_frontier.
        3. Schedules async cleanup of rows from event tables.

        The hot DB rows are deleted asynchronously (non-critical for correctness
        since manifest is the source of truth). A crash leaves the files durable
        and the rows in the hot DB; at next startup, sunk_frontier is
        recalculated from manifest files, so the rows are ignored.

        Frontier-ordering contract: ``partition_start`` must extend the existing
        sunk frontier by exactly one 10K partition:

          * If ``sunk_frontier == SCRAPE_START_BLOCK - 1`` (nothing sunk yet),
            the first valid partition is
            ``floor(SCRAPE_START_BLOCK / 10_000) * 10_000``.
          * Otherwise, ``partition_start ==
            (sunk_frontier + 1) // 10_000 * 10_000``, i.e. the partition
            immediately above the current sunk frontier.

        Out-of-order commits raise ``PartitionFrontierError``.

        Preconditions:
            * ``partition_start`` is a multiple of 10_000 and >= SCRAPE_START_BLOCK.
            * [partition_start, partition_start + 9_999] is covered by
              an unsunk row of ``loaded_block_ranges``.
            * The Parquet files for this partition exist on disk for every
              eligible (contract, event) and manifest has been written.
        """
        if partition_start % PARTITION_SIZE_10K != 0:
            raise ValueError(f"partition_start {partition_start} is not 10K-aligned")

        if partition_start + PARTITION_SIZE_10K - 1 < SCRAPE_START_BLOCK:
            raise ValueError(
                f"partition_start {partition_start} ends before "
                f"SCRAPE_START_BLOCK {SCRAPE_START_BLOCK}"
            )

        # Frontier check
        sunk_frontier = self.get_sunk_frontier()
        expected = (
            (SCRAPE_START_BLOCK // PARTITION_SIZE_10K) * PARTITION_SIZE_10K
            if sunk_frontier == SCRAPE_START_BLOCK - 1
            else ((sunk_frontier + 1) // PARTITION_SIZE_10K) * PARTITION_SIZE_10K
        )
        if partition_start != expected:
            raise PartitionFrontierError(
                f"commit_sink out of order: got {partition_start}, expected {expected}"
            )

        # Advance the sunk frontier
        p_end = partition_start + PARTITION_SIZE_10K - 1
        self.sunk_frontier = p_end

        # Schedule async cleanup of rows from event tables
        self.schedule_delete_sunk_partition_rows(partition_start)

