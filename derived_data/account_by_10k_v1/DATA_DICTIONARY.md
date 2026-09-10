# account_by_10k_v1 data dictionary

Per-partition behavioral profile for each account that filled in that 10K: fill count, condition breadth, volume, fees, maker/taker mix, timing, and hour-of-day entropy.

**This document is the data contract.** It defines the on-disk Parquet schema, the derivation of every column, the canonical row ordering, and all guarantees made by the producer. Consumers depend on these definitions.

Files are stored in `$ACCOUNT_BY_10K_V1_DIR`. References below to filenames are relative to this path.

This table is fills-only and **partitioned immutable**. It is not a global mutable snapshot. A private-desk cousin wrote `{ACCOUNT_SUMMARY_DIR}/snapshot.parquet` and counted splits/merges off a dead `token_and_usdc_flows` ledger. Those properties are not part of this dataset. See [Out of scope](#out-of-scope) and [Entropy](#entropy).

## Upstream dependencies

| Dataset | Role |
|---|---|
| `fills_v1` | Sole source. `is_taker` distinguishes maker vs taker; `block_number` feeds hour-of-day bins. |

`fills_v1` must be fully materialized through the target partition. The producer reads only partitions whose inclusive end block is `<=` the `fills_v1` frontier.

`fills_v1` already dropped exchange-contract marketplace legs. This dataset does not re-filter the four exchange addresses.

## Parquet type contract

This dataset reuses the logical Parquet types from [`polygon_contract_events_v3`](../../raw_data/polygon_contract_events_v3/DATA_DICTIONARY.md#parquet-type-contract) — `"BLOB"` is Parquet `BYTE_ARRAY` (variable length, never `FIXED_LEN_BYTE_ARRAY`) and `INT(bitWidth=32, isSigned=false)` is an unsigned 32-bit integer — plus:

| Parquet logical type | Physical type | DuckDB type | Used for |
|---|---|---|---|
| `INT(bitWidth=64, isSigned=false)` | `INT64` | `UBIGINT` | Volume and fees (6 decimal places). These values cannot be negative. |
| `DOUBLE` | `DOUBLE` (`FLOAT64`) | `DOUBLE` | `maker_fill_ratio` and hour-of-day entropy. Never `FLOAT` / `FLOAT32`. |

`NULL` is permitted only for `taker_hour_entropy` and `maker_hour_entropy` (see [Entropy](#entropy)). Every other column is non-null. Every row has at least one contributing fill.

## Grain

One row per `account` that has at least one `fills_v1` leg in this 10K partition.

Metrics are computed from **this partition's fills only**. They are not lifetime-to-date and do not carry a running sidecar. A 10K range with no fills still writes a zero-row `data.parquet` plus `metadata.json`.

## Schema

| Column | Parquet logical type | Nullable | Description |
|---|---|---|---|
| `account` * | `"BLOB"` (20 bytes) | no | Trader address, copied from `fills_v1` |
| `fill_count` | `INT(bitWidth=32, isSigned=false)` | no | Number of `fills_v1` legs for this account in this partition. Always ≥ 1. Equals `maker_fills + taker_fills`. |
| `condition_count` | `INT(bitWidth=32, isSigned=false)` | no | `count(distinct condition_id)` for this account in this partition. Always ≥ 1. |
| `volume_usdc` | `INT(bitWidth=64, isSigned=false)` | no | `sum(abs(gross_usdc))` for this account in this partition. Account-specific; **never** divided by two. Micro USDC, 6 decimals. |
| `fee_usdc` | `INT(bitWidth=64, isSigned=false)` | no | `sum(fee_usdc)` for this account in this partition. Micro USDC, 6 decimals. |
| `maker_fills` | `INT(bitWidth=32, isSigned=false)` | no | Legs with `is_taker = FALSE` |
| `taker_fills` | `INT(bitWidth=32, isSigned=false)` | no | Legs with `is_taker = TRUE` |
| `maker_fill_ratio` | `DOUBLE` | no | `maker_fills / (maker_fills + taker_fills)`. Always in `[0.0, 1.0]`. Every row has `fill_count ≥ 1`, so the denominator is never 0. |
| `taker_hour_entropy` | `DOUBLE` | **yes** | Shannon entropy of this partition's **taker** fills across 24 hour-of-day bins. `NULL` when the entropy preconditions fail (see [Entropy](#entropy)). |
| `maker_hour_entropy` | `DOUBLE` | **yes** | Same, for **maker** fills. |
| `first_fill_block` | `INT(bitWidth=32, isSigned=false)` | no | `min(block_number)` among this account's fills in this partition |
| `last_fill_block` | `INT(bitWidth=32, isSigned=false)` | no | `max(block_number)` among this account's fills in this partition |

Column order is fixed and is part of the contract. Grain columns are marked with `*`.

## Entropy

Hour-of-day is estimated from block numbers at ~2.1 seconds per block:

- `BLOCKS_PER_HOUR = 1714`
- `BLOCKS_PER_DAY = 1714 * 24` (`41136`)
- `hour_slot = floor( (block_number % BLOCKS_PER_DAY) / BLOCKS_PER_HOUR )` → integer `0..23`

The bin identity is globally aligned: hour 14 in one partition is the same clock-hour residue as hour 14 in any other partition. The histogram itself is **not** stored; only `H` is.

For one account and one role (maker or taker), using only that role's fills in **this partition**:

\[
H = -\sum_{h:\,n_h>0} p_h \ln(p_h), \quad p_h = n_h / N
\]

`ln` is natural log. Empty bins do not contribute (`0 ln 0` is treated as 0).

- All fills in one hour: `H = 0` (most concentrated).
- Uniform across `k` occupied hours: `H = ln(k)`.
- Uniform across all 24 hours: `H = ln(24) ≈ 3.178`. A single 10K partition spans `10000 / 1714 ≈ 5.83` hours of chain time, so per-partition `H` cannot reach `ln(24)`. Do not apply lifetime “bot vs human” cutoffs (e.g. `H > 2.9`) to these values.

`H` is computed for a role only when **both** hold:

1. The partition start block is `≥ 70_000_000` (`MIN_BLOCK_FOR_ENTROPY`). That bound is an exact 10K boundary, so a partition is entirely before or entirely after; there is no split partition.
2. That role has `≥ 100` fills in this partition (`MIN_FILLS_FOR_ENTROPY`). Maker and taker are tested separately.

Otherwise the role's entropy column is `NULL`. Partitions before block 70M always have both entropy columns `NULL`.

Lifetime / 24-hour entropy is a query against `fills_v1` (or a future roll-up of per-partition hour histograms, which this v1 does not emit).

## Physical sort order

Within each partition file, rows are sorted ascending by `account` (byte-wise). The producer enforces this with an explicit `ORDER BY` before writing.

## Partitioning

**1M/10K nested partitioning** (same scheme as `fills_v1`).

Do not emit `snapshot.parquet`. The start partition is the 10K partition containing `SCRAPE_START_BLOCK`. Every consecutive 10K partition from there through the upstream frontier is materialized, including empty partitions. Once written, a partition file is immutable.

## Guarantees

When the producer exists, data-validation tests must assert:

- Physical types match this contract (`account` is `BYTE_ARRAY` with no logical type; never `FIXED_LEN_BYTE_ARRAY`; ratios and entropy are `DOUBLE` / `FLOAT64`).
- Column order matches the schema table.
- `NULL` appears only in `taker_hour_entropy` and `maker_hour_entropy`.
- `octet_length(account) = 20`.
- Rows within each file are sorted by `account`.
- `account` is unique within each partition.
- `fill_count = maker_fills + taker_fills`, `fill_count ≥ 1`, `condition_count ≥ 1`, `first_fill_block ≤ last_fill_block`.
- `maker_fill_ratio` is in `[0.0, 1.0]` and equals `maker_fills / fill_count`.
- Entropy is `NULL` whenever the partition start is `< 70_000_000` or the role's fill count in this partition is `< 100`; otherwise it is non-null, finite, and in `[0, ln(24)]` (with floating slack).
- Against `fills_v1` for the same partition, grouped by `account`: counts, volume, fees, and first/last block match.
- Consecutive 10K coverage from the start partition through the landed frontier; every partition folder has `data.parquet` and `metadata.json`.

## Out of scope

Not carried over from the private snapshot, and not added later without a version bump:

- `total_flow_events`, `merge_split_count`, `merge_split_ratio` — those counted CTF splits/merges from `token_and_usdc_flows`
- A single global `snapshot.parquet`
- Lifetime entropy, or a 24-bin histogram column
- Per-condition breakdown (see `account_condition_by_10k_v1`)
- Signed P&L / net token holdings

## Versioning

This is `v1`. The producer shall not make a material breaking change to the schema, ordering, or guarantees without incrementing the version.
