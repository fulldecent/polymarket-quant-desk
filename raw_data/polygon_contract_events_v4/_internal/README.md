# polygon_contract_events_v4 — library

Pure-library modules used by the scraper. No CLI, no `.env` loading, no logging side effects; every module takes its inputs as function arguments and returns plain Python values.

## Modules

| Module | Purpose |
|---|---|
| `errors.py` | Exception types (`V4Error`, `DuplicateRowError`, `SchemaMismatchError`, `PartitionFrontierError`, `OperationCancelled`) |
| `tables.py` | Schema as Python data — contracts, events, deployment blocks, column orderings, table-name conventions |
| `rpc_client.py` | `RpcClient` — thread-safe JSON-RPC client with keep-alive connections and typed errors so the caller can tell a refused range from throttling from a permanent failure |
| `event_decoders.py` | Decode an `eth_getLogs` response into `(contract, event, row)` triples |
| `chunk_planner.py` | `ChunkPlanner` — sizes each request from measured events per block, and discovers the provider's result and block-span limits from refusals |
| `epoch_optimizer.py` | `EpochOptimizer` — ramps concurrency and result budget to the provider's envelope, then hill-climbs blocks per second in fixed-parameter epochs |
| `persistence.py` | `HotStore` — owns the hot DuckDB connection; atomic event ingestion; `loaded_block_ranges` maintenance; sink commit |
| `parquet_sink.py` | `write_partition_files` — cold-tier-producer: opens its own read-only DuckDB connection, writes one UTC day's Parquet files, returns. `publish_manifest` then declares the day sunk, and the orchestrator follows up with `HotStore.commit_sink`. |

## Integrity model

The hot DB has no `UNIQUE` or `NOT NULL` constraints on event tables. Integrity is enforced entirely at the application layer, in two steps.

### Step 1: atomic range loading

Every `HotStore.persist` call wraps its INSERTs and the corresponding `loaded_block_ranges` update in one DuckDB transaction. The two either both commit or both roll back. This means the same `[from_block, to_block]` cannot be loaded twice: a successful `persist` always advances `loaded_block_ranges`, and a failed call leaves the table untouched. The caller drives loading off the current `loaded_block_ranges` gaps (`HotStore.find_gaps`), so it never asks the library to load a range that overlaps an existing row.

### Step 2: deduplication at sink time

The remaining failure mode is a logic error inside one decoder that emits the same `(block_number, transaction_index, log_index)` twice within a single `persist` call. To catch that, `parquet_sink.write_partition_files` issues each COPY as:

```sql
COPY (
    SELECT DISTINCT ON (block_number, transaction_index, log_index)
        <columns in canonical order>
    FROM <hot_table>
    WHERE block_number BETWEEN first_block AND last_block
    ORDER BY block_number, transaction_index, log_index
) TO '<tmp>' (FORMAT PARQUET, COMPRESSION ZSTD);
```

and checks the written row count against the raw `COUNT(*)` from the same WHERE clause without `DISTINCT ON`. A mismatch raises `DuplicateRowError`, which the caller must treat as fatal — the program is designed so that duplicates cannot occur, so observing one means a decoder bug.

Skipping the DuckDB-level UNIQUE constraint avoids index-maintenance cost on the INSERT path, which is on the critical scrape loop.

## Cold-tier immutability

This is specified in `../DATA_DICTIONARY.md`.

`cleanup_temp_dirs_after_frontier` runs at scraper startup **before** manifest roll-forward. It
deletes incomplete event folders above the frontier (parquet without metadata, mixed targets,
leftover temps) so `write_partition_files` can redo that UTC day from hot. A complete unpublished
day is left in place for roll-forward to publish `_SUCCESS`. Folders at or below the frontier are
never deleted. A day folder is `year=YYYY/month=MM/day=DD/`.

## Concurrency model

The library is designed for one orchestrator (main thread) plus one or more sink workers (background threads in an executor). The orchestrator owns the `HotStore`; the workers never touch it.

**The orchestrator is the only writer of the hot DB.** Every statement against it — `persist`, `commit_sink`, `reconcile_with_cold_tier` — runs on the orchestrator's thread, in program order. There is no cross-thread lock and no cross-thread reasoning about visibility: `HotStore` has no internal lock, and calling its methods from any thread other than the constructing thread is unsupported.

The sink workers do all the slow work. Each worker call:

1. Receives `(db_path, cold_root, day_key)` from the orchestrator.
2. Opens its own read-only DuckDB connection on the hot `.db` file.
3. Runs every `(contract, event)` Parquet COPY for that UTC day, renames the temp files into place.
4. Closes its connection. Returns a `PartitionWriteResult`.

The worker never calls back into `HotStore`. When the orchestrator receives the worker's result, it calls `store.commit_sink(day_key)` to advance the in-memory frontier and delete that day's rows from the hot DB — all on the orchestrator's thread.

This works without races because:

* DuckDB supports multiple connections to the same file from one process. The worker's read-only connection sees an MVCC snapshot of rows that `persist()` has already committed. The worker's reads do not block the orchestrator's writes and vice versa.
* The orchestrator only `persist()`s ranges above the loaded frontier (driven by `find_gaps` on `loaded_block_ranges`), so a day being drained by a worker is never re-touched.
* The worker's COPY finishes before its `Future` resolves; the orchestrator's `commit_sink` runs strictly after that.

Orchestrator sketch (illustrative — not in the library):

```python
store = HotStore(db_path, schema_path)
pending: dict[Future, int] = {}    # Future -> day_key
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

The hot DB has two bookkeeping tables. `loaded_block_ranges` records ingested block ranges. `utc_day_span` records each opened UTC day (`day_key`, `first_block`, `last_block`). The sunk frontier is in memory, loaded from manifests. Rows of `loaded_block_ranges` must satisfy:

> No two rows overlap or touch. For any pair, either `a.to_block + 1 < b.from_block` or `a.from_block > b.to_block + 1`.

`persist()` coalesces a new range into any unsunk row it touches. `commit_sink()` deletes the covered slice after the frontier advances. `utc_day_span` rows stay after the sink.

## Frontier ordering

Two frontiers matter:

* **Loaded frontier**: highest block `N` such that `[SCRAPE_START_BLOCK, N]` is fully covered by `loaded_block_ranges` (any status). The scrape loop's high-water mark.
* **Sunk frontier**: highest block `N` such that cold manifests cover `[SCRAPE_START_BLOCK, N]`. The cold tier's high-water mark. It lives in memory on `HotStore`, loaded from manifests at startup.

The cold-tier Parquet contract requires the sunk frontier to advance one UTC day at a time, with every eligible `(contract, event)` file landing as a batch. `HotStore.commit_sink` enforces this: every call must be passed the day that starts on the next unsunk block. Out-of-order commits raise `PartitionFrontierError`. The check lives on `commit_sink` (not on `write_partition_files`) because `commit_sink` is the only path that actually advances the sunk frontier; the worker is a pure producer that trusts its caller.

A day is ready only when its `last_block` is set, hot coverage of `[first_block, last_block]` is one unsunk range, and `last_block` is at or below the safe head (chain head minus 256). `safe_head is None` means nothing is ready.

## Telling the caller about ready days

A naïve client that wants to keep the sink writer busy would call `HotStore.list_days_ready_to_sink` after every `persist()` — wasteful when most calls do not complete any day.

Instead, `persist()` returns a `PersistResult` whose `ready_partitions: tuple[int, ...]` field lists every UTC day key (`YYYYMMDD`) that just became ready as a direct result of that call. The client appends those to its sink queue and forgets about polling. The list may be empty (most calls). Only the next unsunk day can be ready, so one call closes at most that day.

`list_days_ready_to_sink` remains available for startup / recovery, when the caller wants the full backlog before it has run any `persist`.

## Per-table sink granularity (open optimization)

Currently `commit_sink` is the only point that touches all event tables in one transaction. For a busy UTC day this DELETE of everything could become slow.

A future variant could track sink progress on a per-(contract, event) basis instead of per-block-range — splitting the work into 42 small atomic transactions. The frontier-ordering contract still requires the Parquet files to land as one batch, but the hot-DB DELETEs do not have to. The trade-off is per-transaction overhead vs. lock duration; benchmark before changing.

## Caller responsibilities

The library deliberately does NOT:

* Load `.env` or read environment variables. The caller passes paths directly.
* Run an event loop, scrape RPC, or spawn threads. The caller owns concurrency.
* Decode RPC responses. The caller passes already-decoded Python dicts.
* Configure logging. The caller may pass a `ProgressCallback` for visibility.
