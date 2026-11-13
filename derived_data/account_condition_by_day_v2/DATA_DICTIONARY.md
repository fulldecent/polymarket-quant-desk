# account_condition_by_day_v2 data dictionary

Per-UTC-day summary of how one account traded one condition: fill count, account-specific volume, signed fill-flow, fees, and timing.

**This document is the data contract.** It defines the on-disk Parquet schema, the derivation of every column, the canonical row ordering, and all guarantees made by the producer. Consumers depend on these definitions.

Files are stored in `$ACCOUNT_CONDITION_BY_DAY_V2_DIR`. References below to filenames are relative to this path.

This table is fills-only. It is a cousin of a private-desk rollup that also counted splits, merges, redeems, and converts off a dead `token_and_usdc_flows` ledger. Those columns are not part of this dataset. See [Out of scope](#out-of-scope).

## Upstream dependencies

| Dataset | Role |
|---|---|
| `fills_v2` | Sole source. One group of legs per `(account, condition_id)` in the UTC day. |

`fills_v2` must be materialized through the target UTC day. The producer reads only days at or before the contiguous `fills_v2` frontier.

`fills_v2` already dropped exchange-contract marketplace legs and fills after resolution. This dataset does not re-filter those.

YES/NO token ids are not stored. Join `token_id_map_v2` on `condition_id` if a consumer needs them. Do not recompute position ids with `ct_helpers`.

## Parquet type contract

This dataset reuses the logical Parquet types from [`polygon_contract_events_v4`](../../raw_data/polygon_contract_events_v4/DATA_DICTIONARY.md#parquet-type-contract) — `"BLOB"` is Parquet `BYTE_ARRAY` (variable length, never `FIXED_LEN_BYTE_ARRAY`) and `INT(bitWidth=32, isSigned=false)` is an unsigned 32-bit integer — plus:

| Parquet logical type | Physical type | DuckDB type | Used for |
|---|---|---|---|
| `INT(bitWidth=64, isSigned=false)` | `INT64` | `UBIGINT` | Volume and fees (6 decimal places). These values cannot be negative. |
| `INT(bitWidth=64, isSigned=true)` | `INT64` | `BIGINT` | Signed fill-flow (`net_yes_tokens`) |

`NULL` is not permitted in any column. Every row has at least one contributing fill.

## Grain

One row per `(account, condition_id)` that has at least one `fills_v2` leg in this UTC day.

A manifested day with no fills still writes a zero-row `data.parquet` plus `metadata.json`. The day identity is the folder (`year=YYYY/month=MM/day=DD`), not a column.

## Schema

| Column | Parquet logical type | Nullable | Description |
|---|---|---|---|
| `account` * | `"BLOB"` (20 bytes) | no | Trader address, copied from `fills_v2` |
| `condition_id` * | `"BLOB"` (32 bytes) | no | CTF condition, copied from `fills_v2` |
| `fill_count` | `INT(bitWidth=32, isSigned=false)` | no | Number of `fills_v2` legs for this account and condition in this UTC day. Always ≥ 1. Each bilateral match contributes exactly one to each counterparty. |
| `volume_usdc` | `INT(bitWidth=64, isSigned=false)` | no | `sum(abs(gross_usdc))` for this account and condition. Account-specific volume; **never** divided by two. Micro USDC, 6 decimals. |
| `volume_yes_tokens` | `INT(bitWidth=64, isSigned=false)` | no | `sum(abs(net_yes_tokens))` for this account and condition. 6 decimals. |
| `net_yes_tokens` | `INT(bitWidth=64, isSigned=true)` | no | `sum(net_yes_tokens)` for this account and condition. Signed fill-flow in YES-equivalent tokens this UTC day (positive = net bought YES / sold NO). **Not** a wallet balance: splits, merges, redeems, converts, and transfers are absent. 6 decimals. |
| `fee_usdc` | `INT(bitWidth=64, isSigned=false)` | no | `sum(fee_usdc)` for this account and condition. Micro USDC, 6 decimals. |
| `first_fill_block` | `INT(bitWidth=32, isSigned=false)` | no | `min(block_number)` among this account's fills on this condition in this UTC day |
| `last_fill_block` | `INT(bitWidth=32, isSigned=false)` | no | `max(block_number)` among this account's fills on this condition in this UTC day |

Column order is fixed and is part of the contract. Grain columns are marked with `*`.

`volume_*` vs `matched_*`: this is an account table, so magnitudes are **not** divided by two. Summing `volume_usdc` across both counterparties of a match double-counts market volume; use `condition_by_block_v2` / `condition_by_day_v2` for market-wide figures.

`gross_usdc` in `fills_v2` is positive when the account *spends* USDC. This table does not emit a signed USDC P&L column. `volume_usdc` is unsigned notional; `net_yes_tokens` is the directional fill-flow.

## Physical sort order

Within each day file, rows are sorted ascending by `(account, condition_id)` (byte-wise on both BLOBs). The producer enforces this with an explicit `ORDER BY` before writing.

## Partitioning

**UTC day folders** `year=YYYY/month=MM/day=DD` (same scheme as `fills_v2`).

Each row is materialized in the UTC day whose block span contains its fills. The day identity is the folder, not a column. Every contiguous manifested UTC day through the upstream frontier is materialized, including days with no fills. Once written, a day folder is immutable.

This is not a running snapshot. Summing `net_yes_tokens` across days is the fills-only position change, not holdings after redemption.

## Guarantees

When the producer exists, data-validation tests must assert:

- Physical types match this contract (`account` and `condition_id` are `BYTE_ARRAY` with no logical type; never `FIXED_LEN_BYTE_ARRAY`).
- Column order matches the schema table.
- No `NULL` values.
- `octet_length(account) = 20`, `octet_length(condition_id) = 32`.
- Rows within each file are sorted by `(account, condition_id)`.
- `(account, condition_id)` is unique within each day file.
- `fill_count ≥ 1`, `first_fill_block ≤ last_fill_block`.
- Against `fills_v2` for the same UTC day, grouped by `(account, condition_id)`: `fill_count`, `volume_usdc`, `volume_yes_tokens`, `net_yes_tokens`, `fee_usdc`, `first_fill_block`, and `last_fill_block` match the obvious aggregates.
- Contiguous UTC-day coverage from the first manifested day through the landed frontier; every day folder has `data.parquet` and `metadata.json`. A missing day inside that range is a failure.

## Out of scope

Not carried over from the private rollup, and not added later without a version bump:

- `split_*`, `merge_*`, `redeem_*`, `convert_*`, `any_action_event_count`
- `did_redeem_yes_tokens`, `did_redeem_no_tokens`
- Redemption-aware `net_yes_tokens` / `net_no_tokens` (a running-balance reset at redeem)
- `yes_token_id`, `no_token_id`
- Signed `trade_usdc` / `net_usdc` P&L (the private figures were also wrong because upstream fees were overstated)

Do not recreate `token_and_usdc_flows` to recover those columns.

## Versioning

This is `v2`. The producer shall not make a material breaking change to the schema, ordering, or guarantees without incrementing the version.
