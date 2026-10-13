# lib/

Reusable Python modules shared across producers and scripts in this project. Each module is a flat `.py` file imported as `from lib.<module> import <name>`.

Raw and derived producers depend on these for the contract-critical primitives — partition math, atomic publish, provenance metadata, and the git-clean guard — so the rules in [data-storage-policies](../.github/instructions/data-storage-policies.md) are implemented in exactly one place.

## Modules

### partition_utils.py

Single source of truth for the 1M/10K nested partition scheme used by v1 and v3. v4 raw and v2 derived use [`ymd_partition.py`](#ymd_partitionpy).

| name | description |
|---|---|
| `PARTITION_1M_SIZE`, `PARTITION_10K_SIZE` | partition sizes (`1_000_000`, `10_000`) |
| `PARTITION_1M_LABEL`, `PARTITION_10K_LABEL` | directory labels (`"1M"`, `"10K"`) |
| `partition_start(block)` / `partition_end(block)` | aligned start / inclusive end of the 10K partition containing a block |
| `partition_dir(block)` | canonical relative dir, e.g. `"1M=33000000/10K=33600000"` |
| `mask_partition(partition_start)` | screen form with the last four digits masked, e.g. `"8,45X,XXX"` |
| `enumerate_partitions(start_block, frontier)` | every consecutive `(m, k)` partition from `start_block` up to the frontier, by block range (no gaps) — the partition-planning primitive for v1 derived producers |

### ymd_partition.py

UTC day partitions for v4 raw and v2 derived. A day key is the integer `YYYYMMDD`. Bor `ErrInvalidTimestamp` (commit `9c445ef`, `consensus/bor/bor.go` lines 131–133) keeps header time strictly increasing, so a day folder is a function of block number.

| name | description |
|---|---|
| `day_key_from_timestamp(ts)` | `YYYYMMDD` for a Unix timestamp |
| `day_dir(day_key)` / `day_label(day_key)` | `year=YYYY/month=MM/day=DD` and `YYYY-MM-DD` |
| `next_day_key` / `prev_day_key` / `utc_midnight` | calendar neighbors and the Unix midnight of a day |
| `midnights_crossed(ts_lo, ts_hi)` | UTC midnights strictly after `ts_lo` and at or before `ts_hi` |
| `first_block_on_or_after(ts_at, lo, hi, midnight)` | smallest block in `(lo, hi]` whose header time is at or after `midnight` |
| `iter_manifest_day_keys(cold_root)` | sorted day keys that have a manifest `_SUCCESS` |
| `day_block_span(cold_root, day_key)` | `(min_block, max_block)` from that day's `metadata.json` |

### atomic_publish.py

The create-temp → write → atomically-rename → cleanup-on-failure pattern that gives readers all-or-nothing partition visibility and preserves the immutability contract.

| name | description |
|---|---|
| `create_temp_location(parent_dir, final_name, ...)` | make a predictable temp dir alongside the final path |
| `publish_atomically(temp_location)` | `os.replace` the temp dir into place; always refuses to overwrite an existing final path (immutability) |
| `cleanup_on_failure(temp_location)` | context manager that removes the temp dir on exception |
| `cleanup_temp(temp_location)` | remove a temp dir manually |
| `cleanup_old_temp_artifacts(root_dir, ...)` | sweep orphaned temp dirs from interrupted runs |

### metadata_utils.py

Spec-compliant `metadata.json` provenance next to each `data.parquet` (see [Metadata files](../docs/Metadata%20files.md)). Embeds `git_commit`, `row_count`, `content_hash`, and input hashes.

| name | description |
|---|---|
| `create_parquet_metadata_json(parquet_path, *, dataset, source_script, ...)` | write `metadata.json` beside a parquet file |
| `parquet_content_hash(parquet_path)` | `sha256:`-prefixed content hash, also used for input hashes |

### git_utils.py

| name | description |
|---|---|
| `assert_git_clean(project_root)` | fail fast if the working tree is dirty, so `git_commit` provenance stays honest |

### env.py

| name | description |
|---|---|
| `require_env(name)` | return a required environment variable or exit fail-fast (empty = unset) |

### run_logging.py

Operator-facing console and log contract for partition-producing derived jobs. Screen is the cockpit: bold `output:` path, `log:` path, blank line, a two-row sticky footer, one `-> partition` line per sunk 10K, and a compact `run complete` block. Input paths, scratch, hot, and the work plan are log-only. (The raw scraper uses a separate status-line renderer and does not use this module.)

| name | description |
|---|---|
| `RunOutput(logger_name, script_file)` | themed console + per-run log + `status` footer; `print` writes screen and file, `log_only` writes the file |
| `RunStatus` | two-row Live footer: current partition/phase (percent + elapsed) above Total progress |
| `PhaseWork(status, prefix)` | names a footer phase (`10K=N  building`) and runs SQL with DuckDB `query_progress` |
| `configure_duckdb_progress(con)` | enable DuckDB progress tracking without stderr bars |
| `print_paths(out, output=..., extra=...)` | bold `output:` + `log:` on screen; extra paths to the run log only |
| `print_partition_sunk(out, partition, rows, elapsed)` | `-> partition 8,45X,XXX  rows ##  0.4s` after each published 10K |
| `print_work_plan` | work-plan line, run-log only |
| `print_run_summary` | compact `run complete` / `run interrupted` block |
| `setup_logging(logger_name, script_file, console=None)` | file-only UTC logger; `console` is ignored. Prefer `RunOutput` in producers |

### ct_helpers.py

Pure-Python port of the `CTHelpers` library from the Gnosis ConditionalTokens Solidity contract. Computes ERC-1155 token IDs from `(conditionId, indexSet)` pairs, matching the on-chain implementation at `0x4D97DCd97eC945f40cF65F87097ACe5EA0476045` on Polygon.

| function | description |
|---|---|
| `get_collection_id(parent, condition_id, index_set)` | bytes → bytes; mirrors `CTHelpers.getCollectionId` |
| `get_position_id(collateral_token, collection_id)` | bytes → int; mirrors `CTHelpers.getPositionId` |

All inputs and outputs are raw bytes / ints, matching the variable-length `BYTE_ARRAY` columns in the data model (no `0x`-prefixed hex strings).

**dependency:** `pycryptodome` — install with `pip install pycryptodome`.

## Running the self-tests

Modules that contain a self-test run it when executed directly and exit non-zero on failure, so they are usable in CI:

```sh
# from the project root, activate whatever venv applies to the calling script, then:
python lib/ct_helpers.py        # checks computed token IDs against known on-chain values
python lib/partition_utils.py   # checks partition math against hand-computed values
python lib/ymd_partition.py      # checks UTC day keys, midnights, and the block cut
```

Expected output ends with all checks marked `PASS`; exit code is `0` on success, `1` on any failure.

## Conventions for new modules

- Place each module directly in `lib/` as a flat `.py` file.
- Add it to the table above.
- Only promote code here when it is genuinely shared by multiple programs.
- Where practical, include a `if __name__ == "__main__":` self-test block that verifies at least one known-good value and exits non-zero on failure; document where the expected values came from.
