# condition_by_10k_v1 data dictionary

Per-10K-partition OHLC YES price, market-wide matched volume, fees, and resolution outcome for each condition that traded or resolved in that partition.

**This document is the data contract.** It defines the on-disk Parquet schema, the derivation of every column, the canonical row ordering, and all guarantees made by the producer. Consumers depend on these definitions.

Files are stored in `$CONDITION_BY_10K_V1_DIR`. References below to filenames are relative to this path.

Time grain is in the dataset name. Files still use the `1M=*/10K=*/` layout; that is storage, not the row grain. One row covers the whole 10K block range.

This table is built from `fills_v1` (plus resolution), not by reading `condition_by_block_v1`. The two producers are independent. OHLC identity with a roll-up of `condition_by_block_v1` is a consequence of the same fill-level formula, not a pipeline dependency.

## Upstream dependencies

| Dataset | Role |
|---|---|
| `fills_v1` | OHLC, matched volume, fees, fill count. Same implied-YES-price formula as `condition_by_block_v1`. |
| `polygon_contract_events_v3` | `ConditionalTokens/condition_resolution` rows whose `block_number` falls in this 10K partition |
| `token_id_map_v1` | Universe of Polymarket conditions. Used only to keep resolution-only rows on-protocol; the raw `condition_resolution` table includes every ConditionalTokens resolution on Polygon, not just Polymarket. |

All three must be fully materialized through the target partition. The producer includes a partition only if `partition_end(k) <= min(fills_v1 frontier, token_id_map_v1 frontier, polygon_contract_events_v3 frontier)`.

Fills after condition resolution are already suppressed in `fills_v1`.

## Parquet type contract

This dataset reuses the logical Parquet types from [`polygon_contract_events_v3`](../../raw_data/polygon_contract_events_v3/DATA_DICTIONARY.md#parquet-type-contract) — `"BLOB"` is Parquet `BYTE_ARRAY` (variable length, never `FIXED_LEN_BYTE_ARRAY`) and `INT(bitWidth=32, isSigned=false)` is an unsigned 32-bit integer — plus the following:

| Parquet logical type | Physical type | DuckDB type | Used for |
|---|---|---|---|
| `INT(bitWidth=64, isSigned=false)` | `INT64` | `UBIGINT` | Matched token and USDC amounts, and fees (6 decimal places). These values cannot be negative. |
| `DOUBLE` | `DOUBLE` (`FLOAT64`) | `DOUBLE` | Implied YES prices. Never `FLOAT` / `FLOAT32`. |
| `STRING` (UTF-8) | `BYTE_ARRAY` | `VARCHAR` | `payout_numerators`, copied from raw `condition_resolution` |

`NULL` is permitted only for price columns (when `fill_count = 0`) and for resolution columns (when the condition did not resolve in this partition). Amount and count columns are never null: they are `0` when there were no fills.

## Grain

One row per `condition_id` in a 10K partition that satisfies at least one of:

1. At least one `fills_v1` leg in this partition, or
2. A `ConditionalTokens/condition_resolution` event in this partition whose `condition_id` is present in `token_id_map_v1`.

Invariant: every row has `fill_count ≥ 1` or `resolved_block IS NOT NULL` (or both).

A condition that resolved in this 10K but never traded in this 10K still gets a row (prices null, volumes zero, resolution populated). A condition that resolved in a *different* 10K is not repeated here. A raw resolution whose `condition_id` is absent from `token_id_map_v1` is ignored (non-Polymarket ConditionalTokens activity).

The 10K partition identity is the folder (`10K={K}`), not a column. Partition-key columns are never stored inside the file.

## Schema

| Column | Parquet logical type | Nullable | Description |
|---|---|---|---|
| `condition_id` * | `"BLOB"` (32 bytes) | no | CTF condition |
| `open_yes_price` | `DOUBLE` | **yes** | Implied YES price of the first fill in this partition (lowest `(block_number, logical_fill_index)`). `NULL` iff `fill_count = 0`. |
| `high_yes_price` | `DOUBLE` | **yes** | Maximum implied YES price among fills in this partition. `NULL` iff `fill_count = 0`. |
| `low_yes_price` | `DOUBLE` | **yes** | Minimum implied YES price among fills in this partition. `NULL` iff `fill_count = 0`. |
| `close_yes_price` | `DOUBLE` | **yes** | Implied YES price of the last fill in this partition (highest `(block_number, logical_fill_index)`). `NULL` iff `fill_count = 0`. |
| `matched_yes_tokens` | `INT(bitWidth=64, isSigned=false)` | no | `sum(abs(net_yes_tokens)) / 2` over fills in this partition for this condition. `0` when there were no fills. 6 decimals. Integer division; remainder discarded. |
| `matched_usdc` | `INT(bitWidth=64, isSigned=false)` | no | `sum(abs(gross_usdc)) / 2` over fills in this partition for this condition. `0` when there were no fills. Micro USDC, 6 decimals. Integer division; remainder discarded. |
| `fee_usdc` | `INT(bitWidth=64, isSigned=false)` | no | `sum(fee_usdc)` over fills in this partition for this condition. `0` when there were no fills. **Not** divided by two. |
| `fill_count` | `INT(bitWidth=32, isSigned=false)` | no | Number of `fills_v1` legs in this partition for this condition. `0` for resolution-only rows. |
| `resolved_block` | `INT(bitWidth=32, isSigned=false)` | **yes** | `block_number` of the `condition_resolution` event if it falls in this partition; otherwise `NULL` |
| `resolved_log_index` | `INT(bitWidth=32, isSigned=false)` | **yes** | `log_index` of that event; `NULL` iff `resolved_block` is `NULL` |
| `outcome_slot_count` | `INT(bitWidth=32, isSigned=false)` | **yes** | Copied from `condition_resolution`; `NULL` iff `resolved_block` is `NULL` |
| `payout_numerators` | `STRING` | **yes** | Copied from `condition_resolution`: JSON array of uint256 decimal strings, no spaces, length equal to `outcome_slot_count`. `NULL` iff `resolved_block` is `NULL`. For a clean binary win this is `["1","0"]` (YES) or `["0","1"]` (NO). `["1","1"]` is a draw/void (each side receives half). |

Column order is fixed and is part of the contract. Grain columns are marked with `*`.

The four resolution columns are null together or populated together. Price columns are null together iff `fill_count = 0`.

## Implied YES price

Identical to [`condition_by_block_v1`](../condition_by_block_v1/DATA_DICTIONARY.md#implied-yes-price). Fail if any contributing fill has `net_yes_tokens = 0` or `gross_usdc = 0`. Do not clip to `(0, 1]`.

Open / high / low / close are computed from fill-level `yes_price` values across the whole 10K, ordered by `(block_number, logical_fill_index)`. This is the same OHLC you would get by rolling `condition_by_block_v1` bars (open of the first non-empty block, max of highs, min of lows, close of the last non-empty block).

## Resolution

A condition is resolved at most once on-chain (`ConditionalTokens.reportPayouts` cannot succeed twice for the same `condition_id`). If this partition contains more than one `condition_resolution` row for the same `condition_id`, the producer fails fast.

`payout_numerators` is stored exactly as in the raw table (JSON string of uint256 decimals). This dataset does not add a derived winner column; `["1","1"]` would not fit a boolean. Slot 0 is YES (`index_set = 1`); slot 1 is NO (`index_set = 2`). See [`fills_v1` outcome convention](../fills_v1/DATA_DICTIONARY.md#outcome-convention-yes--index_set-1-no--index_set-2).

`token_id_map_v1` is an approximation (>99% of traded tokens). A condition that is absent from the map will still appear here if it has `fills_v1` legs in this partition (those legs made it through `fills_v1`'s own map join). It will not appear as a resolution-only row.

The producer does not require `outcome_slot_count = 2` of every raw resolution on the contract — only of rows it emits. `fills_v1` already fails on traded non-binary tokens. If a map-filtered resolution-only row has `outcome_slot_count != 2`, the producer fails fast so this table stays in the YES/NO model.

## Physical sort order

Within each partition file, rows are sorted ascending by `condition_id` (byte-wise). The producer enforces this with an explicit `ORDER BY` before writing.

## Partitioning

**1M/10K nested partitioning** (same scheme as `fills_v1`).

The start partition is the 10K partition containing `SCRAPE_START_BLOCK`. Every consecutive 10K partition from there through the upstream frontier is materialized, including empty (zero-row) partitions. Once written, a partition file is immutable.

## Guarantees

When the producer exists, data-validation tests must assert:

- Physical types match this contract.
- Column order matches the schema table.
- `octet_length(condition_id) = 32`.
- Rows within each file are sorted by `condition_id`.
- `condition_id` is unique within each partition.
- `fill_count ≥ 1 OR resolved_block IS NOT NULL`.
- Price columns are `NULL` iff `fill_count = 0`; otherwise all four prices are non-null and satisfy the same high/low vs open/close inequalities as `condition_by_block_v1`.
- Resolution columns are all `NULL` or all non-null together.
- `matched_*` and `fee_usdc` are `0` when `fill_count = 0`, and match the `fills_v1` group-by-`condition_id` sums (with `/ 2` on the matched columns only) when `fill_count > 0`.
- A populated `payout_numerators` value is valid JSON with `json_array_length = outcome_slot_count`, and `outcome_slot_count = 2`.
- No emitted `condition_id` has two resolutions in the same partition.
- Consecutive 10K coverage from the start partition through the landed frontier; every partition folder has `data.parquet` and `metadata.json`.

## Out of scope

- Per-token / per-book OHLC.
- Splits, merges, redeems, converts.
- Account-level volume.
- A lifetime / as-of-now snapshot of every condition ever seen. This is a per-partition bar, not a dimension table. Conditions with neither fills nor a resolution in this 10K do not appear.

## Versioning

This is `v1`. The producer shall not make a material breaking change to the schema, ordering, or guarantees without incrementing the version.
