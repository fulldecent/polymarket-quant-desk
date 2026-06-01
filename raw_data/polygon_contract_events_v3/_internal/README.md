# polygon_contract_events_v3 — library

Pure-library modules used by the scraper. No CLI, no `.env` loading, no logging side effects; every module takes its inputs as function arguments and returns plain Python values.

## Modules

| Module | Purpose |
|---|---|
| `errors.py` | Exception types (`V3Error`, `DuplicateRowError`, `SchemaMismatchError`, `PartitionFrontierError`, `OperationCancelled`) |
| `tables.py` | Schema as Python data — contracts, events, deployment blocks, column orderings, table-name conventions |
| `persistence.py` | `HotStore` — owns the hot DuckDB connection; atomic event ingestion; `loaded_block_ranges` maintenance; sink commit |
| `parquet_sink.py` | `write_partition_files` — cold-tier-producer: opens its own read-only DuckDB connection, writes one partition's Parquet files, returns. The orchestrator follows up with `HotStore.commit_sink`. |

## Integrity model

The hot DB has no `UNIQUE` or `NOT NULL` constraints on event tables. Integrity is enforced entirely at the application layer, in two steps.

### Step 1: atomic range loading

Every `HotStore.persist` call wraps its INSERTs and the corresponding `loaded_block_ranges` update in one DuckDB transaction. The two either both commit or both roll back. This means the same `[from_block, to_block]` cannot be loaded twice: a successful `persist` always advances `loaded_block_ranges`, and a failed call leaves the table untouched. The caller drives loading off the current `loaded_block_ranges` gaps (`HotStore.find_gaps`), so it never asks the library to load a range that overlaps an existing row.

### Step 2: deduplication at sink time

The remaining failure mode is a logic error inside one decoder that emits the same `(transaction_hash, log_index)` twice within a single `persist` call. To catch that, `parquet_sink.write_partition_files` issues each COPY as:

```sql
COPY (
    SELECT DISTINCT ON (transaction_hash, log_index)
        <columns in canonical order>
    FROM <hot_table>
    WHERE block_number BETWEEN P AND P + 9999
    ORDER BY block_number, transaction_index, log_index
) TO '<tmp>' (FORMAT PARQUET, COMPRESSION ZSTD);
```

and checks the written row count against the raw `COUNT(*)` from the same WHERE clause without `DISTINCT ON`. A mismatch raises `DuplicateRowError`, which the caller must treat as fatal — the program is designed so that duplicates cannot occur, so observing one means a decoder bug.

Skipping the DuckDB-level UNIQUE constraint avoids index-maintenance cost on the INSERT path, which is on the critical scrape loop.

## Cold-tier immutability

This is specified in `../DATA_DICTIONARY.md`.

## Concurrency model

The library is designed for one orchestrator (main thread) plus one or more sink workers (background threads in an executor). The orchestrator owns the `HotStore`; the workers never touch it.

**The orchestrator is the only writer of the hot DB.** Every statement against it — `persist`, `commit_sink`, `reconcile_with_cold_tier` — runs on the orchestrator's thread, in program order. There is no cross-thread lock and no cross-thread reasoning about visibility: `HotStore` has no internal lock, and calling its methods from any thread other than the constructing thread is unsupported.

The sink workers do all the slow work. Each worker call:

1. Receives `(db_path, cold_root, partition_start)` from the orchestrator.
2. Opens its own read-only DuckDB connection on the hot `.db` file.
3. Runs every `(contract, event)` Parquet COPY for that partition, renames the temp files into place.
4. Closes its connection. Returns a `PartitionWriteResult`.

The worker never calls back into `HotStore`. When the orchestrator receives the worker's result, it calls `store.commit_sink(partition_start)` to delete the partition's rows from the hot DB and flip the `loaded_block_ranges` row to sunk — all on the orchestrator's thread.

This works without races because:

* DuckDB supports multiple connections to the same file from one process. The worker's read-only connection sees an MVCC snapshot of rows that `persist()` has already committed. The worker's reads do not block the orchestrator's writes and vice versa.
* The orchestrator only `persist()`s ranges above the loaded frontier (driven by `find_gaps` on `loaded_block_ranges`), so a partition being drained by a worker is never re-touched.
* The worker's COPY finishes before its `Future` resolves; the orchestrator's `commit_sink` runs strictly after that.

Orchestrator sketch (illustrative — not in the library):

```python
store = HotStore(db_path, schema_path)
pending: dict[Future, int] = {}    # Future -> partition_start
executor = ThreadPoolExecutor(max_workers=N)

while not done:
    result = store.persist(from_block, to_block, rows)
    for p in result.ready_partitions:
        future = executor.submit(
            write_partition_files, db_path, cold_root, p
        )
        pending[future] = p

    # Reap any completed worker(s) and finalize their partitions on
    # the main thread, in commit-frontier order.
    completed = [f for f in pending if f.done()]
    for f in sorted(completed, key=lambda f: pending[f]):
        write_result = f.result()        # raises on worker failure
        store.commit_sink(pending.pop(f))
```

(The real orchestrator handles failure, ordering, shutdown, etc.; the important pattern is that `commit_sink` runs on the same thread as `persist`.)

## `loaded_block_ranges` invariant

The hot DB has exactly one bookkeeping table, `loaded_block_ranges`. Its rows must satisfy:

> Rows are strictly disjoint AND non-adjacent **within each `sunk_to_parquet` value**. Two rows with the same status MUST NOT touch (`a.to_block + 1 == b.from_block` is forbidden — they must be coalesced into one row). Two rows with OPPOSITE status MAY touch (a sunk range ends exactly where an unsunk range begins).

`persist()` enforces this on every insert by splitting any opposite-status row that overlaps the new range, then unioning every same-status row that touches or overlaps it. `commit_sink()` does the same when flipping a range from unsunk to sunk.

## Frontier ordering

Two frontiers matter:

* **Loaded frontier**: highest block `N` such that `[SCRAPE_START_BLOCK, N]` is fully covered by `loaded_block_ranges` (any status). The scrape loop's high-water mark.
* **Sunk frontier**: highest block `N` such that `[SCRAPE_START_BLOCK, N]` is fully covered by rows with `sunk_to_parquet=TRUE`. The cold tier's high-water mark.

The cold-tier Parquet contract requires the sunk frontier to advance one 10K partition at a time, with every eligible `(contract, event)` file landing as a batch. `HotStore.commit_sink` enforces this: every call must be passed the partition that extends the sunk frontier by exactly one. Out-of-order commits raise `PartitionFrontierError`. The check lives on `commit_sink` (not on `write_partition_files`) because `commit_sink` is the only path that actually advances the sunk frontier; the worker is a pure producer that trusts its caller.

## Telling the caller about ready partitions

A naïve client that wants to keep the sink writer busy would call `HotStore.list_10k_partitions_ready_to_sink` after every `persist()` — wasteful when most calls do not complete any partition.

Instead, `persist()` returns a `PersistResult` whose `ready_partitions: tuple[int, ...]` field lists every 10K partition that just became ready as a direct result of that call. The client appends those to its sink queue and forgets about polling. The list may be empty (most calls), or contain multiple values (one `persist` can complete several 10K partitions if it covers a range that straddles them).

`list_10k_partitions_ready_to_sink` remains available for startup / recovery, when the caller wants the full backlog before it has run any `persist`.

## Per-table sink granularity (open optimization)

Currently `commit_sink` is the only point that touches all 42 event tables in one transaction. For very large 10K partitions this DELETE- of-everything-plus-progress-update could become slow.

A future variant could track sink progress on a per-(contract, event) basis instead of per-block-range — splitting the work into 42 small atomic transactions. The frontier-ordering contract still requires the Parquet files to land as one batch, but the hot-DB DELETEs do not have to. The trade-off is per-transaction overhead vs. lock duration; benchmark before changing.

## Caller responsibilities

The library deliberately does NOT:

* Load `.env` or read environment variables. The caller passes paths directly.
* Run an event loop, scrape RPC, or spawn threads. The caller owns concurrency.
* Decode RPC responses. The caller passes already-decoded Python dicts.
* Configure logging. The caller may pass a `ProgressCallback` for visibility.
