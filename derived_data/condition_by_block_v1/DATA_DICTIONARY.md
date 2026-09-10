# condition_by_block_v1 data dictionary

Per-block OHLC YES price, market-wide matched volume, and fees for each condition that traded in that block.

**This document is the data contract.** It defines the on-disk Parquet schema, the derivation of every column, the canonical row ordering, and all guarantees made by the producer. Consumers depend on these definitions.

Files are stored in `$CONDITION_BY_BLOCK_V1_DIR`. References below to filenames are relative to this path.

## Upstream dependencies

| Dataset | Role |
|---|---|
| `fills_v1` | Sole source. Every column is aggregated from fill legs in the same 10K partition. |

`fills_v1` must be fully materialized through the target partition before this dataset is produced. The producer reads only partitions whose inclusive end block is `<=` the `fills_v1` frontier. It does not read `polygon_contract_events_v3` or `token_id_map_v1`.

Fills after condition resolution are already suppressed in `fills_v1`; this dataset does not re-apply that filter.

## Parquet type contract

This dataset reuses the logical Parquet types from [`polygon_contract_events_v3`](../../raw_data/polygon_contract_events_v3/DATA_DICTIONARY.md#parquet-type-contract) — `"BLOB"` is Parquet `BYTE_ARRAY` (variable length, never `FIXED_LEN_BYTE_ARRAY`) and `INT(bitWidth=32, isSigned=false)` is an unsigned 32-bit integer — plus the following:

| Parquet logical type | Physical type | DuckDB type | Used for |
|---|---|---|---|
| `INT(bitWidth=64, isSigned=false)` | `INT64` | `UBIGINT` | Matched token and USDC amounts, and fees (6 decimal places). These values cannot be negative. |
| `DOUBLE` | `DOUBLE` (`FLOAT64`) | `DOUBLE` | Implied YES prices. Never `FLOAT` / `FLOAT32`. |

USDC and condition-token amounts use 6 decimal places (raw units; `1_000_000` = 1 token or 1 USDC), matching `fills_v1`. Non-negative amounts use unsigned 64-bit integers; they are not signed `INT64`. `fills_v1` stores some always-non-negative amounts as signed (`fee_usdc`); this dataset does not copy that.

`NULL` is not permitted in any column. Every row has at least one contributing fill, so prices and amounts are always present.

## Grain

One row per `(block_number, condition_id)` that has at least one `fills_v1` leg in that block.

A block in which a condition did not trade produces no row for that pair. A 10K partition in which no condition traded still writes a zero-row `data.parquet` plus `metadata.json`.

## Schema

| Column | Parquet logical type | Nullable | Description |
|---|---|---|---|
| `block_number` * | `INT(bitWidth=32, isSigned=false)` | no | Block these fills were mined in |
| `condition_id` * | `"BLOB"` (32 bytes) | no | CTF condition, copied from `fills_v1` |
| `open_yes_price` | `DOUBLE` | no | Implied YES price of the first fill in this block for this condition (lowest `logical_fill_index`) |
| `high_yes_price` | `DOUBLE` | no | Maximum implied YES price among fills in this block for this condition |
| `low_yes_price` | `DOUBLE` | no | Minimum implied YES price among fills in this block for this condition |
| `close_yes_price` | `DOUBLE` | no | Implied YES price of the last fill in this block for this condition (highest `logical_fill_index`) |
| `matched_yes_tokens` | `INT(bitWidth=64, isSigned=false)` | no | `sum(abs(net_yes_tokens)) / 2` over fills in this block for this condition. 6 decimals. Integer division; remainder discarded. |
| `matched_usdc` | `INT(bitWidth=64, isSigned=false)` | no | `sum(abs(gross_usdc)) / 2` over fills in this block for this condition. Micro USDC, 6 decimals. Integer division; remainder discarded. |
| `fee_usdc` | `INT(bitWidth=64, isSigned=false)` | no | `sum(fee_usdc)` over fills in this block for this condition. Micro USDC, 6 decimals. **Not** divided by two — each leg already carries its own fee. |
| `fill_count` | `INT(bitWidth=32, isSigned=false)` | no | Number of `fills_v1` legs in this block for this condition. Always ≥ 1. This is a leg count (maker + taker), not a match count. |

Column order is fixed and is part of the contract. Grain columns are marked with `*`.

## Implied YES price

`fills_v1.net_yes_tokens` is YES-equivalent **size**. It is not a YES price. The ratio `gross_usdc / net_yes_tokens` equals `+p_yes` on the YES book and `-p_no` on the NO book:

| Leg | `net_yes_tokens` | `gross_usdc` | ratio | YES price |
|---|---|---|---|---|
| buy YES | `+q` | `+c` | `+p_yes` | `p_yes` |
| sell YES | `-q` | `-c` | `+p_yes` | `p_yes` |
| buy NO | `-q` | `+c` | `-p_no` | `1 - p_no` |
| sell NO | `+q` | `-c` | `-p_no` | `1 - p_no` |

Per fill:

1. Fail if `net_yes_tokens = 0` or `gross_usdc = 0` (`fills_v1` fill amounts are positive; a zero is a producer bug).
2. `ratio = gross_usdc::DOUBLE / net_yes_tokens::DOUBLE`
3. If `gross_usdc` and `net_yes_tokens` have the same sign (YES book): `yes_price = ratio`
4. Otherwise (NO book): `yes_price = 1.0 + ratio`  (equals `1 - p_no`)

Prices are not clipped to `(0, 1]`. A fill priced through $0 or $1 is stored as computed.

OHLC uses **every** `fills_v1` leg for that `(block_number, condition_id)`, maker and taker. Complementary mint/merge legs imply the same YES price; same-book maker and taker legs may differ as the taker walks the book. `logical_fill_index` is unique within a block, so open and close are well-defined.

## Matched volume and fees

Project-wide naming (see the [data catalog](../../docs/Data%20catalog.md#column-naming-conventions)):

- `matched_*` is market-wide volume: sum of fill-leg magnitudes divided by two, because each match has a maker leg and a taker leg.
- `fee_usdc` is the sum of per-leg fees. Dividing fees by two would understate protocol take.

`fill_count` is not divided by two. It is the number of ledger rows that contributed to the bar.

## Physical sort order

Within each partition file, rows are sorted ascending by `(block_number, condition_id)`. `condition_id` is compared byte-wise. The producer enforces this with an explicit `ORDER BY` before writing.

This ordering makes each partition file row-for-row reproducible from identical `fills_v1` input.

## Partitioning

**1M/10K nested partitioning** (same scheme as `fills_v1`).

Each row is materialized in exactly the 10K partition whose block range contains its `block_number`. A 10K block range with no rows still writes a zero-row `data.parquet` plus `metadata.json`. Once written, a partition file is immutable.

The start partition is the 10K partition containing `SCRAPE_START_BLOCK`. Every consecutive 10K partition from there through the upstream frontier is materialized, with no gaps.

## Guarantees

When the producer exists, data-validation tests must assert:

- Physical types match this contract (`BLOB` as `BYTE_ARRAY` with no logical type, never `FIXED_LEN_BYTE_ARRAY`; prices are `DOUBLE` / `FLOAT64`; amounts are unsigned `INT64`; counts and `block_number` are unsigned `INT32`).
- Column order matches the schema table.
- No `NULL` values.
- `octet_length(condition_id) = 32`.
- Rows within each file are sorted by `(block_number, condition_id)`.
- `(block_number, condition_id)` is unique within each partition.
- `fill_count ≥ 1`.
- `high_yes_price ≥ low_yes_price`, `high_yes_price ≥ open_yes_price`, `high_yes_price ≥ close_yes_price`, `low_yes_price ≤ open_yes_price`, `low_yes_price ≤ close_yes_price`.
- Against `fills_v1` for the same partition: `matched_usdc = sum(abs(gross_usdc)) / 2` and `matched_yes_tokens = sum(abs(net_yes_tokens)) / 2` and `fee_usdc = sum(fee_usdc)` and `fill_count = count(*)` grouped by `(block_number, condition_id)`.
- Consecutive 10K coverage from the start partition through the landed frontier; every partition folder has `data.parquet` and `metadata.json`.

## Out of scope

- Per-token / per-book OHLC. Users of this stack think in conditions. YES and NO books are coupled by the exchange matcher; the executable price is a condition object.
- Splits, merges, redeems, converts, or any ledger other than `fills_v1`.
- Resolution outcome. That lives on `condition_by_10k_v1`.
- Account-level volume. That lives on `account_condition_by_10k_v1` / `account_by_10k_v1` as `volume_*` (not divided by two).

## Versioning

This is `v1`. The producer shall not make a material breaking change to the schema, ordering, or guarantees without incrementing the version.
