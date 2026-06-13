# lib/

Reusable Python modules shared across producers and scripts in this project. Each module is a flat `.py` file imported as `from lib.<module> import <name>`.

Raw and derived producers depend on these for the contract-critical primitives — partition math, atomic publish, provenance metadata, and the git-clean guard — so the rules in [data-storage-policies](../.github/instructions/data-storage-policies.md) are implemented in exactly one place.

## Modules

### partition_utils.py

Single source of truth for the 1M/10K nested partition scheme used by every dataset.

| name | description |
|---|---|
| `PARTITION_1M_SIZE`, `PARTITION_10K_SIZE` | partition sizes (`1_000_000`, `10_000`) |
| `PARTITION_1M_LABEL`, `PARTITION_10K_LABEL` | directory labels (`"1M"`, `"10K"`) |
| `partition_start(block)` / `partition_end(block)` | aligned start / inclusive end of the 10K partition containing a block |
| `partition_dir(block)` | canonical relative dir, e.g. `"1M=33000000/10K=33600000"` |
| `enumerate_partitions(start_block, frontier)` | every consecutive `(m, k)` partition from `start_block` up to the frontier, by block range (no gaps) — the canonical partition-planning primitive for derived producers |

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

Operator-facing console UX for partition-producing derived jobs: one sticky progress bar with ETA, log lines scrolling above it, and one timestamped `logs/main-{ts}.log` per run. (The raw scraper uses a separate bespoke status-line renderer and does not use this module.)

| name | description |
|---|---|
| `setup_logging(logger_name, script_file, console)` | per-run `logs/main-{ts}.log` (DEBUG) + a `RichHandler` (INFO) bound to `console` |
| `make_progress(console)` | the standard `Progress` (spinner, bar, M/N, elapsed, ETA), bound to the same `console` |

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
```

Expected output ends with all checks marked `PASS`; exit code is `0` on success, `1` on any failure.

## Conventions for new modules

- Place each module directly in `lib/` as a flat `.py` file.
- Add it to the table above.
- Only promote code here when it is genuinely shared by multiple programs.
- Where practical, include a `if __name__ == "__main__":` self-test block that verifies at least one known-good value and exits non-zero on failure; document where the expected values came from.
