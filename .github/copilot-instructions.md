# Copilot instructions

This project scrapes Polymarket event logs from Polygon blockchain and exports them to Parquet files.

## Absolute directives for data products

These are non-negotiable. Read `.github/instructions/data-storage-policies.md` in full before writing or changing any dataset producer; it is the authoritative contract. The short form:

- **Immutable:** never modify, overwrite, or recompute a landed partition. No `--force`. To change logic, bump the dataset version and rebuild from scratch.
- **Frontier:** never read source data past the upstream frontier — it is unstable and unusable. Derived jobs include a partition only when `partition_end(k) <= frontier`.
- **Fail fast:** errors are never acceptable. No `--skip-errors`. Stop loudly so a human investigates.
- **Clean tree:** never run with a dirty git tree. No `--run-dirty`. Provenance depends on `git_commit` in metadata.
- **Complete coverage:** materialize every 10K partition from the start partition to the frontier with no gaps. Discover partitions by block range, not source-folder existence. A range with no rows still writes a zero-row `data.parquet` + `metadata.json`.
- **Join-compatible types:** mirror upstream logical Parquet types. `BLOB` is `BYTE_ARRAY` (variable length) via `pa.binary()` — never `FIXED_LEN_BYTE_ARRAY` / `pa.binary(N)`. Unsigned 32-bit ints via `pa.uint32()`.
- **Deterministic sort:** every `DATA_DICTIONARY.md` declares a physical sort order and the producer enforces it with an explicit `ORDER BY` so partitions are byte-for-byte reproducible.

The flags `--force`, `--skip-errors`, and `--run-dirty` must never exist in this repo.

## Code conventions

- Python 3.10+
- Use `python-dotenv` for environment configuration
- All environment variables are required and validated at startup
- Do not use "baskwards compatibility aliases" or other BC features unless specifically requested
- You are welcome to propose and implement test assertions about the raw data, and then rely on these assumptions (with fail fast) in the code to improve readability, maintainability and performance.

## Program functionality

- Programs never delete or modify finalized data. This is a production environment. See data guarantees for each product.
- Programs are considered broken if they fail to update their output for more than 2 seconds. Ensure output is updated every second and shows accurate progress updates.
- Long-running producers show a single sticky progress bar with ETA and write one timestamped log file per run to a `logs/` folder next to the script, named `main-{ISO8601-zulu}.log` (e.g. `main-2026-06-09T175739Z.log` from `strftime("%Y-%m-%dT%H%M%SZ")`). Route logs through a rich `RichHandler` bound to the same `Console` as the progress bar so the bar stays pinned while logs scroll above it. Progress reflects only real work — never list already-landed partitions.

## Queries

- The data set is approx 1 TB of raw data. Whenever testing queries, always use a timeout and adapt if you hit problems. If you need a lot of spill space, use the external drive.
