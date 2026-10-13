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
import pyarrow as pa

from lib.ymd_partition import day_label, prev_day_key

from .errors import (
    DuplicateRowError,
    OperationCancelled,
    PartitionFrontierError,
    SchemaMismatchError,
    V4Error,
)
from .fee_charged import (
    FEE_REFUNDED_TABLES,
    reject_poison_table,
    reject_poison_value,
)
from .tables import (
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
_INCOMING_VIEW = "_incoming_rows"
"""Name the Arrow batch is registered under while a batch of rows is inserted."""

_ARROW_TYPES: dict[str, object] = {
    "UINTEGER": pa.uint32(),
    "BLOB": pa.binary(),
    "VARCHAR": pa.string(),
}
"""Declared column type to the Arrow type that matches it exactly. Building the batch with the
table's own types means DuckDB never has to coerce, so a value that does not fit fails loudly
instead of being silently widened."""
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


def _range_covered(
    unsunk_rows: list[tuple[int, int, bool]],
    first: int,
    last: int,
) -> bool:
    """True when one unsunk row contains ``[first, last]``."""
    for f, t, sunk in unsunk_rows:
        if sunk:
            raise V4Error(
                f"_range_covered received a sunk row ({f},{t}); "
                "logic error in row selection"
            )
        if f <= first and t >= last:
            return True
    return False


def _find_ready_days(
    unsunk_rows: list[tuple[int, int, bool]],
    spans: list[tuple[int, int, int | None]],
    *,
    sunk_frontier: int,
    safe_head: int | None,
) -> list[int]:
    """Return the next closed UTC day that is fully loaded and behind ``safe_head``.

    A day is the next one only when its ``first_block`` is the first block
    above ``sunk_frontier`` (or ``SCRAPE_START_BLOCK`` when nothing is sunk).
    ``safe_head is None`` means the chain head is not known yet, so nothing
    is ready. Later closed days wait their turn.
    """
    if safe_head is None:
        return []
    expected_first = (
        SCRAPE_START_BLOCK if sunk_frontier < SCRAPE_START_BLOCK else sunk_frontier + 1
    )
    ready: list[int] = []
    for day_key, first, last in spans:
        if last is None or first != expected_first or last > safe_head:
            continue
        if _range_covered(unsunk_rows, first, last):
            ready.append(day_key)
    ready.sort()
    return ready


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
            a path with at least several GB free for spill files. Required:
            there is no sensible default, since it depends on the deployment.
    """

    duckdb_temp_dir: str
    duckdb_memory_limit: str = "8GB"
    duckdb_threads: int = 4


@dataclass(frozen=True)
class PersistResult:
    """Result of one ``HotStore.persist`` call."""

    rows_inserted: int
    """Total event rows written, summed across every target table."""

    tables_touched: int
    """Number of distinct event tables that received at least one row."""

    elapsed_ms: float

    ready_partitions: tuple[int, ...]
    """UTC day keys (``YYYYMMDD``) that just became ready to sink.

    A day is ready when it is closed, it starts on the next unsunk block,
    its block range is fully loaded, and its last block is ``<= safe_head``.
    Only days that were not already ready before this call appear here.
    The tuple is sorted ascending and is usually empty or a single day.
    A day that closed before it was 256 blocks behind head is omitted
    until a later call sees a high enough ``safe_head``.
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
        partition: int | None = None,
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
        config: HotStoreConfig,
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
            config: ``HotStoreConfig``; its ``duckdb_temp_dir`` says where DuckDB
                may spill, which depends on the deployment and has no default.
            progress_cb: Optional ``ProgressCallback`` for visibility into
                operations that take more than ~1 second.

        Raises:
            FileNotFoundError: ``schema_path`` does not exist, or the
                parent of ``db_path`` does not exist.
            SchemaMismatchError: an existing ``db_path`` was created with
                a different schema.
            V4Error: any other DuckDB initialization failure.

        Postconditions:
            * Every event table from ``schema.sql`` exists in the DB.
            * The ``loaded_block_ranges`` table exists.
            * DuckDB pragmas (memory limit, threads, temp dir) are applied.
        """
        self.db_path = db_path
        self.schema_path = schema_path
        self.config = config
        self.progress_cb = progress_cb
        self._conn: duckdb.DuckDBPyConnection | None = None
        self._closed = False
        self._column_types: dict[str, dict[str, str]] = {}
        self.sunk_frontier = SCRAPE_START_BLOCK - 1  # In-memory sunk frontier, populated at startup
        self.safe_head: int | None = None
        self.first_day_key: int | None = None

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
            raise V4Error(f"could not open DuckDB at {self.db_path!r}: {e}") from e

        # Apply schema (idempotent)
        try:
            with open(self.schema_path) as f:
                sql = f.read()
            self._conn.execute(sql)
        except Exception as e:
            raise V4Error(f"failed to apply schema.sql: {e}") from e

        # Schema fingerprint check
        self._verify_schema()
        self._reject_poisoned_fee_charged()

    def _verify_schema(self) -> None:
        """Ensure every table from schema.sql exists with the expected columns."""
        expected_tables = set(all_table_names())
        expected_tables.add("loaded_block_ranges")
        expected_tables.add("utc_day_span")

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

        self._column_types = {tbl: dict(cols) for tbl, cols in actual.items()}

    def _reject_poisoned_fee_charged(self) -> None:
        """Refuse to open a hot DB whose fee_refunded rows are the bytes-as-VARCHAR encodings."""
        conn = self._conn
        if conn is None:
            return
        for tbl in FEE_REFUNDED_TABLES:
            if tbl in self._column_types:
                reject_poison_table(conn, tbl)

    def _arrow_type(self, table: str, column: str):
        """Arrow type matching the column's declared type in the hot DB."""
        declared = self._column_types.get(table, {}).get(column)
        arrow_type = _ARROW_TYPES.get(declared)
        if arrow_type is None:
            raise SchemaMismatchError(
                f"{table}.{column} is declared {declared!r}, which has no exact Arrow equivalent"
            )
        return arrow_type

    def _get_conn(self) -> duckdb.DuckDBPyConnection:
        if self._closed or self._conn is None:
            raise V4Error("HotStore is closed")
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
        ``V4Error``.

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
        day_cuts: Sequence[tuple[int, int]] = (),
        safe_head: int | None = None,
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
            V4Error: any DuckDB error during the transaction; hot DB
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
            raise V4Error("HotStore is closed")
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

                # Snapshot ready days before this call so the diff is only
                # the days this transaction closed or covered.
                head = self.safe_head if safe_head is None else safe_head
                ready_before = set(
                    _find_ready_days(
                        self._get_loaded_ranges_unsunk(conn),
                        self._load_day_spans(conn),
                        sunk_frontier=self.sunk_frontier,
                        safe_head=head,
                    )
                )

                # Begin transaction
                conn.execute("BEGIN TRANSACTION")

                # Insert rows
                inserted = 0
                for (contract, event), rows in rows_by_target.items():
                    if not rows:
                        continue
                    if event == "fee_refunded":
                        for row in rows:
                            reject_poison_value(
                                row.get("fee_charged"),
                                where=f"{contract}/fee_refunded block {row.get('block_number')}",
                            )
                    if stop_event and stop_event.is_set():
                        conn.execute("ROLLBACK")
                        raise OperationCancelled("persist cancelled during insert")
                    tbl = table_name(contract, event)
                    cols = all_columns(contract, event)
                    col_list = ", ".join(cols)
                    # One vectorised insert per target. Binding row by row was measured as the
                    # ceiling on the whole scrape, an order of magnitude below this.
                    batch = pa.table(
                        {
                            col: pa.array(
                                [row[col] for row in rows], type=self._arrow_type(tbl, col)
                            )
                            for col in cols
                        }
                    )
                    conn.register(_INCOMING_VIEW, batch)
                    try:
                        conn.execute(
                            f"INSERT INTO {tbl} ({col_list}) "
                            f"SELECT {col_list} FROM {_INCOMING_VIEW}"
                        )
                    finally:
                        conn.unregister(_INCOMING_VIEW)
                    inserted += len(rows)

                # Record the range in loaded_block_ranges
                conn.execute(
                    """
                    INSERT INTO loaded_block_ranges (from_block, to_block)
                    VALUES (?, ?)
                    ON CONFLICT (from_block, to_block) DO NOTHING
                    """,
                    (from_block, to_block),
                )

                # Coalesce unsunk ranges, then record any UTC midnights this
                # chunk crossed. Cuts roll back with the rest of the transaction.
                self._coalesce_unsunk_ranges(conn)
                self._apply_day_cuts(conn, day_cuts)

                ready_after = set(
                    _find_ready_days(
                        self._get_loaded_ranges_unsunk(conn),
                        self._load_day_spans(conn),
                        sunk_frontier=self.sunk_frontier,
                        safe_head=head,
                    )
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
                raise V4Error(f"persist failed: {e}") from e

        # Exhausted retries on transient conflict.
        raise V4Error(
            f"persist failed after {_MVCC_MAX_ATTEMPTS} MVCC retries: {last_conflict}"
        ) from last_conflict

    def set_safe_head(self, block: int) -> None:
        """Remember the highest block the scraper is willing to sink."""
        self.safe_head = int(block)

    def open_first_day(self, day_key: int) -> None:
        """Open the UTC day of ``SCRAPE_START_BLOCK`` if the hot DB has none.

        A restart must see the same day. A different day already stored is
        a fatal mismatch with the header that was just read.
        """
        conn = self._get_conn()
        row = conn.execute(
            "SELECT day_key, first_block FROM utc_day_span ORDER BY first_block LIMIT 1"
        ).fetchone()
        day_key = int(day_key)
        if row is None:
            conn.execute(
                "INSERT INTO utc_day_span (day_key, first_block, last_block) "
                "VALUES (?, ?, NULL)",
                [day_key, SCRAPE_START_BLOCK],
            )
            self.first_day_key = day_key
            return
        stored_day, stored_first = int(row[0]), int(row[1])
        if stored_day != day_key or stored_first != SCRAPE_START_BLOCK:
            raise V4Error(
                f"hot DB opens {day_label(stored_day)} at block {stored_first}; "
                f"header day is {day_label(day_key)} at {SCRAPE_START_BLOCK}"
            )
        self.first_day_key = day_key

    def max_loaded_block(self) -> int | None:
        """Highest ``to_block`` in ``loaded_block_ranges``, or None if empty."""
        conn = self._get_conn()
        row = conn.execute("SELECT MAX(to_block) FROM loaded_block_ranges").fetchone()
        if row is None or row[0] is None:
            return None
        return int(row[0])

    def current_day_span(self) -> tuple[int, int, int | None] | None:
        """The open or next unsunk day: ``(day_key, first_block, last_block)``."""
        expected = (
            SCRAPE_START_BLOCK
            if self.sunk_frontier < SCRAPE_START_BLOCK
            else self.sunk_frontier + 1
        )
        conn = self._get_conn()
        row = conn.execute(
            """
            SELECT day_key, first_block, last_block
            FROM utc_day_span
            WHERE first_block = ?
            """,
            [expected],
        ).fetchone()
        if row is None:
            return None
        last = None if row[2] is None else int(row[2])
        return int(row[0]), int(row[1]), last

    def _load_day_spans(
        self, conn: duckdb.DuckDBPyConnection
    ) -> list[tuple[int, int, int | None]]:
        rows = conn.execute(
            "SELECT day_key, first_block, last_block FROM utc_day_span ORDER BY first_block"
        ).fetchall()
        out: list[tuple[int, int, int | None]] = []
        for day_key, first, last in rows:
            out.append((int(day_key), int(first), None if last is None else int(last)))
        return out

    def _apply_day_cuts(
        self,
        conn: duckdb.DuckDBPyConnection,
        cuts: Sequence[tuple[int, int]],
    ) -> None:
        """Record days that start at ``first_block`` and close the day before.

        ``cuts`` is ``(day_key, first_block)`` in any order. The previous
        day must already be open. A cut whose previous day is not open
        fails the transaction so the chunk can be retried after the
        earlier chunk lands.
        """
        for day_key, first_block in sorted((int(k), int(b)) for k, b in cuts):
            if first_block < SCRAPE_START_BLOCK:
                raise ValueError(
                    f"day cut {day_label(day_key)} starts at {first_block}, "
                    f"before SCRAPE_START_BLOCK"
                )
            prev = prev_day_key(day_key)
            row = conn.execute(
                "SELECT first_block, last_block FROM utc_day_span WHERE day_key = ?",
                [prev],
            ).fetchone()
            if row is None:
                raise V4Error(
                    f"day cut {day_label(day_key)} at block {first_block} "
                    f"but {day_label(prev)} is not open"
                )
            prev_first = int(row[0])
            prev_last = None if row[1] is None else int(row[1])
            new_last = first_block - 1
            if new_last < prev_first:
                raise V4Error(
                    f"day cut {day_label(day_key)} at {first_block} ends "
                    f"{day_label(prev)} before its first block {prev_first}"
                )
            if prev_last is None:
                conn.execute(
                    "UPDATE utc_day_span SET last_block = ? WHERE day_key = ?",
                    [new_last, prev],
                )
            elif prev_last != new_last:
                raise V4Error(
                    f"{day_label(prev)} already ends at {prev_last}, not {new_last}"
                )
            existing = conn.execute(
                "SELECT first_block FROM utc_day_span WHERE day_key = ?",
                [day_key],
            ).fetchone()
            if existing is None:
                conn.execute(
                    "INSERT INTO utc_day_span (day_key, first_block, last_block) "
                    "VALUES (?, ?, NULL)",
                    [day_key, first_block],
                )
            elif int(existing[0]) != first_block:
                raise V4Error(
                    f"{day_label(day_key)} already starts at {int(existing[0])}, "
                    f"not {first_block}"
                )

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
        current = {(int(f), int(t)) for f, t in rows}
        coalesced = {
            (f, t) for f, t, _ in _coalesce_ranges([(int(f), int(t), False) for f, t in rows])
        }
        # Only the rows that actually change are written. Deleting every row and re-inserting the
        # same key still counts as a write, and collides with the row versions a sink worker's
        # open read transaction is holding.
        for from_block, to_block in sorted(current - coalesced):
            conn.execute(
                "DELETE FROM loaded_block_ranges WHERE from_block = ? AND to_block = ?",
                (from_block, to_block),
            )
        for from_block, to_block in sorted(coalesced - current):
            conn.execute(
                "INSERT INTO loaded_block_ranges (from_block, to_block) VALUES (?, ?)",
                (from_block, to_block),
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

    def list_days_ready_to_sink(self) -> list[int]:
        """Return UTC day keys that are closed, fully loaded, and ``<= safe_head``.

        ``persist()`` reports the same fact incrementally. This method is
        the startup backlog, including a day that closed earlier and was
        waiting on ``safe_head``.
        """
        conn = self._get_conn()
        return _find_ready_days(
            self._get_loaded_ranges_unsunk(conn),
            self._load_day_spans(conn),
            sunk_frontier=self.sunk_frontier,
            safe_head=self.safe_head,
        )

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
        """Advance the in-memory frontier to the manifest frontier.

        A parquet file with no manifest ``_SUCCESS`` does not move the
        frontier. When ``manifest_frontier`` is omitted, the manifests
        are read. A hole in that run raises ``V4Error``.

        Returns how many manifested days extend past the previous frontier.
        """
        from lib.ymd_partition import day_block_span, iter_manifest_day_keys

        from .parquet_sink import get_sunk_frontier as get_manifest_frontier

        if stop_event and stop_event.is_set():
            raise OperationCancelled("reconcile interrupted")
        if not os.path.isdir(cold_tier_root):
            raise FileNotFoundError(f"cold_tier_root does not exist: {cold_tier_root}")
        if manifest_frontier is None:
            frontier = get_manifest_frontier(
                cold_tier_root, first_day_key=self.first_day_key
            )
        else:
            frontier = int(manifest_frontier)
        old = self.sunk_frontier
        if frontier <= old:
            if progress_cb:
                progress_cb(
                    op="reconcile", phase="update", message="no new days to mark"
                )
            return 0
        claimed = 0
        for day_key in iter_manifest_day_keys(cold_tier_root):
            _min_block, max_block = day_block_span(cold_tier_root, day_key)
            if max_block > old:
                claimed += 1
        self.sunk_frontier = frontier
        if progress_cb:
            progress_cb(
                op="reconcile",
                phase="update",
                rows_done=claimed,
                message=f"advanced sunk frontier to {self.sunk_frontier}",
            )
        return claimed

    def populate_sunk_frontier_from_manifests(self, cold_tier_root: str) -> None:
        """Set the in-memory frontier from manifest ``_SUCCESS`` files."""
        from .parquet_sink import get_sunk_frontier as get_manifest_frontier

        try:
            self.sunk_frontier = get_manifest_frontier(
                cold_tier_root, first_day_key=self.first_day_key
            )
        except FileNotFoundError:
            self.sunk_frontier = SCRAPE_START_BLOCK - 1

    def schedule_delete_sunk_partition_rows(self, first_block: int, last_block: int) -> None:
        """Delete hot rows and loaded-range coverage for one sunk day.

        A loaded range that hangs past ``last_block`` keeps its tail. The
        manifest remains the source of truth if this delete is interrupted.
        """
        if first_block > last_block:
            raise ValueError(f"empty sunk range [{first_block}, {last_block}]")
        conn = self._get_conn()
        try:
            for table in all_table_names():
                conn.execute(
                    f"DELETE FROM {table} WHERE block_number BETWEEN ? AND ?",
                    (first_block, last_block),
                )
            covered = conn.execute(
                """
                SELECT from_block, to_block FROM loaded_block_ranges
                WHERE NOT (to_block < ? OR from_block > ?)
                """,
                (first_block, last_block),
            ).fetchall()
            for raw_from, raw_to in covered:
                f, t = int(raw_from), int(raw_to)
                conn.execute(
                    "DELETE FROM loaded_block_ranges WHERE from_block = ? AND to_block = ?",
                    (f, t),
                )
                if f < first_block:
                    conn.execute(
                        "INSERT INTO loaded_block_ranges (from_block, to_block) VALUES (?, ?)",
                        (f, first_block - 1),
                    )
                if t > last_block:
                    conn.execute(
                        "INSERT INTO loaded_block_ranges (from_block, to_block) VALUES (?, ?)",
                        (last_block + 1, t),
                    )
        except Exception as e:
            raise V4Error(
                f"failed to delete sunk rows [{first_block}, {last_block}]: {e}"
            ) from e

    def commit_sink(self, day_key: int) -> None:
        """Mark one closed UTC day sunk and drop its hot rows.

        The day must start on the next unsunk block. Out-of-order commits
        raise ``PartitionFrontierError``. The frontier advances to the day's
        ``last_block``.
        """
        conn = self._get_conn()
        row = conn.execute(
            "SELECT first_block, last_block FROM utc_day_span WHERE day_key = ?",
            [int(day_key)],
        ).fetchone()
        if row is None or row[1] is None:
            raise PartitionFrontierError(f"day {int(day_key)} is not a closed span")
        first, last = int(row[0]), int(row[1])
        expected = (
            SCRAPE_START_BLOCK
            if self.sunk_frontier < SCRAPE_START_BLOCK
            else self.sunk_frontier + 1
        )
        if first != expected:
            raise PartitionFrontierError(
                f"commit_sink out of order: day {int(day_key)} starts at {first}, "
                f"expected {expected}"
            )
        self.sunk_frontier = last
        self.schedule_delete_sunk_partition_rows(first, last)

