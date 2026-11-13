# account_by_day_v2 data dictionary

Per-UTC-day behavioral profile for each account that filled that day: fill count, condition breadth, volume, fees, maker/taker mix, timing, and hour-of-day entropy.

**This document is the data contract.** It defines the on-disk Parquet schema, the derivation of every column, the canonical row ordering, and all guarantees made by the producer. Consumers depend on these definitions.

Files are stored in `$ACCOUNT_BY_DAY_V2_DIR`. References below to filenames are relative to this path.

This table is fills-only and **partitioned immutable**. It is not a global mutable snapshot. A private-desk cousin wrote `{ACCOUNT_SUMMARY_DIR}/snapshot.parquet` and counted splits/merges off a dead `token_and_usdc_flows` ledger. Those properties are not part of this dataset. See [Out of scope](#out-of-scope) and [Entropy](#entropy).

## Upstream dependencies

| Dataset | Role |
|---|---|
| `fills_v2` | Sole source. `is_taker` distinguishes maker vs taker; `block_timestamp` feeds UTC hour-of-day bins. |

`fills_v2` must be materialized through the target UTC day. The producer reads only days at or before the contiguous `fills_v2` frontier.

`fills_v2` already dropped exchange-contract marketplace legs. This dataset does not re-filter the four exchange addresses.

## Parquet type contract

This dataset reuses the logical Parquet types from [`polygon_contract_events_v4`](../../raw_data/polygon_contract_events_v4/DATA_DICTIONARY.md#parquet-type-contract) — `"BLOB"` is Parquet `BYTE_ARRAY` (variable length, never `FIXED_LEN_BYTE_ARRAY`) and `INT(bitWidth=32, isSigned=false)` is an unsigned 32-bit integer — plus:

| Parquet logical type | Physical type | DuckDB type | Used for |
|---|---|---|---|
| `INT(bitWidth=64, isSigned=false)` | `INT64` | `UBIGINT` | Volume and fees (6 decimal places). These values cannot be negative. |
| `DOUBLE` | `DOUBLE` (`FLOAT64`) | `DOUBLE` | `maker_fill_ratio` and hour-of-day entropy. Never `FLOAT` / `FLOAT32`. |

`NULL` is permitted only for `taker_hour_entropy` and `maker_hour_entropy` (see [Entropy](#entropy)). Every other column is non-null. Every row has at least one contributing fill.

## Grain

One row per `account` that has at least one `fills_v2` leg in this UTC day.

Metrics are computed from **this day's fills only**. They are not lifetime-to-date and do not carry a running sidecar. A manifested day with no fills still writes a zero-row `data.parquet` plus `metadata.json`. The day identity is the folder (`year=YYYY/month=MM/day=DD`), not a column.

## Schema

| Column | Parquet logical type | Nullable | Description |
|---|---|---|---|
| `account` * | `"BLOB"` (20 bytes) | no | Trader address, copied from `fills_v2` |
| `fill_count` | `INT(bitWidth=32, isSigned=false)` | no | Number of `fills_v2` legs for this account in this UTC day. Always ≥ 1. Equals `maker_fills + taker_fills`. |
| `condition_count` | `INT(bitWidth=32, isSigned=false)` | no | `count(distinct condition_id)` for this account in this UTC day. Always ≥ 1. |
| `volume_usdc` | `INT(bitWidth=64, isSigned=false)` | no | `sum(abs(gross_usdc))` for this account in this UTC day. Account-specific; **never** divided by two. Micro USDC, 6 decimals. |
| `fee_usdc` | `INT(bitWidth=64, isSigned=false)` | no | `sum(fee_usdc)` for this account in this UTC day. Micro USDC, 6 decimals. |
| `maker_fills` | `INT(bitWidth=32, isSigned=false)` | no | Legs with `is_taker = FALSE` |
| `taker_fills` | `INT(bitWidth=32, isSigned=false)` | no | Legs with `is_taker = TRUE` |
| `maker_fill_ratio` | `DOUBLE` | no | `maker_fills / (maker_fills + taker_fills)`. Always in `[0.0, 1.0]`. Every row has `fill_count ≥ 1`, so the denominator is never 0. |
| `taker_hour_entropy` | `DOUBLE` | **yes** | Shannon entropy of this day's **taker** fills across 24 hour-of-day bins. `NULL` when the entropy preconditions fail (see [Entropy](#entropy)). |
| `maker_hour_entropy` | `DOUBLE` | **yes** | Same, for **maker** fills. |
| `first_fill_block` | `INT(bitWidth=32, isSigned=false)` | no | `min(block_number)` among this account's fills in this UTC day |
| `last_fill_block` | `INT(bitWidth=32, isSigned=false)` | no | `max(block_number)` among this account's fills in this UTC day |

Column order is fixed and is part of the contract. Grain columns are marked with `*`.

## Entropy

Hour-of-day is the UTC hour of `fills_v2.block_timestamp`:

- `SECONDS_PER_HOUR = 3600`
- `SECONDS_PER_DAY = 86400`
- `hour_slot = floor( (block_timestamp % SECONDS_PER_DAY) / SECONDS_PER_HOUR )` → integer `0..23`

The bin identity is UTC clock hour. Polygon PoS rejects a header whose timestamp is below parent.Time plus the minimum block period (Bor `ErrInvalidTimestamp`, commit `9c445ef`, `consensus/bor/bor.go` lines 131–133). Mainnet period is never 0, so header time strictly increases. Within a UTC day, a later block cannot fall in an earlier UTC hour. The histogram itself is **not** stored; only `H` is.

For one account and one role (maker or taker), using only that role's fills in **this UTC day**:

\[
H = -\sum_{h:\,n_h>0} p_h \ln(p_h), \quad p_h = n_h / N
\]

`ln` is natural log. Empty bins do not contribute (`0 ln 0` is treated as 0).

- All fills in one hour: `H = 0` (most concentrated).
- Uniform across `k` occupied hours: `H = ln(k)`.
- Uniform across all 24 hours: `H = ln(24) ≈ 3.178`. A UTC day can occupy all 24 hours, so `H` can reach `ln(24)`.

`H` is computed for a role only when **both** hold:

1. The day's `min_block` is `≥ 70_000_000` (`MIN_BLOCK_FOR_ENTROPY`). A day whose `min_block` is earlier has both entropy columns `NULL`, even when that day extends past block 70,000,000.
2. That role has `≥ 100` fills in this UTC day (`MIN_FILLS_FOR_ENTROPY`). Maker and taker are tested separately.

Otherwise the role's entropy column is `NULL`.

Lifetime / 24-hour entropy is a query against `fills_v2` (or a future roll-up of per-day hour histograms, which this v2 does not emit).

## Physical sort order

Within each day file, rows are sorted ascending by `account` (byte-wise). The producer enforces this with an explicit `ORDER BY` before writing.

## Partitioning

**UTC day folders** `year=YYYY/month=MM/day=DD` (same scheme as `fills_v2`).

The day identity is the folder, not a column. Hive keys stay out of the parquet file.

Do not emit `snapshot.parquet`. Every contiguous manifested UTC day through the upstream frontier is materialized, including days with no fills. Once written, a day folder is immutable.

## Guarantees

When the producer exists, data-validation tests must assert:

- Physical types match this contract (`account` is `BYTE_ARRAY` with no logical type; never `FIXED_LEN_BYTE_ARRAY`; ratios and entropy are `DOUBLE` / `FLOAT64`).
- Column order matches the schema table.
- `NULL` appears only in `taker_hour_entropy` and `maker_hour_entropy`.
- `octet_length(account) = 20`.
- Rows within each file are sorted by `account`.
- `account` is unique within each day file.
- `fill_count = maker_fills + taker_fills`, `fill_count ≥ 1`, `condition_count ≥ 1`, `first_fill_block ≤ last_fill_block`.
- `maker_fill_ratio` is in `[0.0, 1.0]` and equals `maker_fills / fill_count`.
- Entropy is `NULL` whenever the day's `min_block` is `< 70_000_000` or the role's fill count in this day is `< 100`; otherwise it is non-null, finite, and in `[0, ln(24)]` (with floating slack).
- Against `fills_v2` for the same UTC day, grouped by `account`: counts, volume, fees, and first/last block match.
- Contiguous UTC-day coverage from the first manifested day through the landed frontier; every day folder has `data.parquet` and `metadata.json`. A missing day inside that range is a failure.

## Out of scope

Not carried over from the private snapshot, and not added later without a version bump:

- `total_flow_events`, `merge_split_count`, `merge_split_ratio` — those counted CTF splits/merges from `token_and_usdc_flows`
- A single global `snapshot.parquet`
- Lifetime entropy, or a 24-bin histogram column
- Per-condition breakdown (see `account_condition_by_day_v2`)
- Signed P&L / net token holdings

## Versioning

This is `v2`. The producer shall not make a material breaking change to the schema, ordering, or guarantees without incrementing the version.
