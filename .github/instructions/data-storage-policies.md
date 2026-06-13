# Data storage policies

Authoritative contract for every raw and derived dataset in this repository. A program that violates any rule here is broken, even if it runs. These rules are not negotiable and are not "nice to haves" — they are the reason the data is trustworthy. Read this file in full before writing or changing any producer.

The base path for all datasets is `/Volumes/polymarket-quant-desk/`.

## Absolute directives

These are the rules that are most often gotten wrong. They are absolute.

1. **Finalized data is immutable.** Once a partition folder (`data.parquet` + `metadata.json`) is published, it is never modified, overwritten, or recomputed. There is no `--force`. A producer that wants to "redo" a landed partition is wrong. To change the logic, bump the dataset version (`_v2`, `_v3`) and materialize a new dataset from scratch.
2. **Never consume data past the frontier.** Data beyond the upstream frontier is UNUSABLE AND UNSTABLE. A derived dataset reads only partitions whose inclusive end block is `<= frontier` (see "Frontier" below). Reading unsunk/unstable data is a correctness bug, not a performance tradeoff.
3. **Errors are never acceptable. Fail fast.** There is no `--skip-errors`. Any error during materialization (corrupt input, failed assertion, unexpected shape) stops the program loudly so a human investigates. Never log-and-continue past a bad partition.
4. **Never run with a dirty git tree.** Provenance depends on `git_commit` in every `metadata.json`. There is no `--run-dirty`. The producer refuses to start if `git status --porcelain` is non-empty. Commit or stash first.
5. **Cover every partition consecutively, with no gaps.** A derived dataset materializes every 10K partition from its start partition up to the frontier, in strict ascending order, with no gaps. A 10K range that happens to have no source rows STILL produces a partition — a zero-row `data.parquet` plus its `metadata.json`. See "Complete consecutive coverage" below.
6. **Match upstream logical Parquet types so joins are clean.** Data types are chosen to make joining easy. Mirror the raw dataset's logical types exactly. In particular `BLOB` is Parquet `BYTE_ARRAY` (variable length, no logical type) — never `FIXED_LEN_BYTE_ARRAY`. See "Logical types" below.
7. **Define and enforce a deterministic sort order.** Every dataset's `DATA_DICTIONARY.md` declares a physical sort order, and the producer enforces it, so each partition file is byte-for-byte reproducible from identical source. See "Sort order" below.

Forbidden command-line flags, anywhere in this repo: `--force`, `--skip-errors`, `--run-dirty`. If you find yourself wanting one, re-read the matching directive.

## Frontier

The frontier is the highest block whose data is finalized and stable upstream. For raw `polygon_contract_events_v3`, get it from `get_sunk_frontier(RAW)`; the stable partitions are exactly those backed by a `manifests/.../_SUCCESS` marker.

- A derived job includes a partition only if `partition_end(k) <= frontier`.
- Discover the frontier once at startup; never read past it.
- Do not infer stability from the mere existence of a raw `contract/event` parquet file — those can change until the manifest entry is published. Use the frontier / manifest, never raw-folder existence, to decide what is safe.

## Complete consecutive coverage

Partition discovery for a derived dataset is by BLOCK RANGE, not by source-folder existence.

- The start partition is the 10K partition containing the dataset's first relevant block. For Polymarket-wide data this is the partition containing `SCRAPE_START_BLOCK` (`raw_data/polygon_contract_events_v3`).
- Enumerate every 10K partition from the start partition through the highest partition with `partition_end(k) <= frontier`. No gaps are permitted.
- Every enumerated partition produces an output folder. If the partition has no rows to emit, write a zero-row `data.parquet` with the correct schema plus its `metadata.json`. A missing partition folder is a bug; an empty partition folder is correct and expected.
- Already-landed partitions (output folder exists) are immutable and skipped. They must not be re-read, re-computed, or shown in progress output.

## Atomic publish

Every partition becomes visible atomically.

- Write `data.parquet` and `metadata.json` into a temp location (`create_temp_location`), then publish with `publish_atomically(temp)`. On any failure, `cleanup_temp(temp)` and re-raise.
- Folder visibility therefore implies completeness: readers and planners may rely on "folder exists" meaning "partition is complete and immutable".
- `publish_atomically` never overwrites an existing final path — there is no opt-out. A `FileExistsError` there means something tried to violate immutability.

## Logical types

Mirror the raw `polygon_contract_events_v3` logical Parquet types so derived columns join cleanly against raw columns and against each other.

| Dictionary type | Parquet encoding | PyArrow type | Notes |
|---|---|---|---|
| `"BLOB"` | `BYTE_ARRAY`, no logical type | `pa.binary()` | Variable length. NEVER `pa.binary(20)` / `pa.binary(32)` — those emit `FIXED_LEN_BYTE_ARRAY` and break joins. |
| `STRING` | `BYTE_ARRAY`, UTF-8 | `pa.string()` | |
| `INT(bitWidth=32, isSigned=false)` | `INT32`, unsigned | `pa.uint32()` | Used for `index_set`, block numbers, log indexes, etc. |

Addresses are 20-byte BLOBs; hashes and IDs are 32-byte BLOBs. The byte length is a value invariant, not a Parquet type constraint — keep the column variable-length `BYTE_ARRAY` and assert the length in tests if needed.

## Sort order

Reproducibility requires a total, declared order.

- `DATA_DICTIONARY.md` must contain a "Physical sort order" section naming the exact columns and direction.
- The producer enforces it with an explicit `ORDER BY` (or equivalent) before writing each partition. `SELECT DISTINCT` alone does NOT guarantee order.
- Prefer sorting by the grain key (the columns that make each row unique). Since the grain key is unique within a partition, sorting by it is a total order and yields byte-for-byte identical files from identical input.

## Metadata

Every partition has a `metadata.json` written via `lib.metadata_utils.create_parquet_metadata_json`. It records `git_commit`, `source_script`, `input_hashes`, partition parameters (`1M`, `10K`, `min_block`, `max_block`), `row_count`, and `created_at`. See `docs/Metadata files.md`.

## Partitioning and naming

- Nested `1M={N}/10K={K}/` directories; `data.parquet` + `metadata.json` inside.
- Partition-key columns (`1M`, `10K`) are NEVER stored inside the file.
- Use `lib.partition_utils` (`partition_start`, `partition_end`, `partition_dir`) for all partition math — do not hand-roll modulo arithmetic.

## Operator experience

Producers are long-running and must be observable (see also `.github/copilot-instructions.md`).

- Show a single sticky progress bar with ETA (rich `Progress`). Route log lines through a `RichHandler` bound to the SAME `Console` as the `Progress` so the bar stays pinned at the bottom while logs scroll above it.
- Progress reflects only real work. Do not list already-landed partitions.
- Write one log file per run to a `logs/` folder next to the producer script. Name it `main-{timestamp}.log` where the timestamp is the run start time in ISO 8601 zulu, basic format (no colons): `datetime.now(timezone.utc).strftime("%Y-%m-%dT%H%M%SZ")` → `logs/main-2026-06-09T175739Z.log`. Never share or append across runs; each run gets its own file. The `logs/` folder is gitignored.
- Update visible output at least once per second; a stall over ~2s reads as broken.
- Handle `SIGINT` cleanly: stop after the current partition, leave only immutable landed partitions behind (atomic publish guarantees no partial partition is ever visible).

## Adding a derived dataset (checklist)

1. Add a row to [`docs/Data catalog.md`](../../docs/Data%20catalog.md) (Producer, Mutability, Inputs, Dictionary, Sort order).
2. Write `DATA_DICTIONARY.md` next to the producer: scope, grain, schema with logical types, physical sort order, partitioning, guarantees, sources.
3. Implement the producer to satisfy every Absolute directive above.
4. Add data-validation tests that assert: start partition present; consecutive coverage with no gaps; each partition has `data.parquet` + `metadata.json`; physical Parquet types match this policy; rows sorted per the declared order.
5. Run on a clean git tree. Confirm tests pass.
