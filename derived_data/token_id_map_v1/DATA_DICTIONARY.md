# token_id_map_v1 data dictionary

Lookup table: `(collateral_token, parent_collection_id, condition_id, index_set)` → `token_id`.

## Scope

Every condition that appears in `CTFExchange/token_registered` or `NegRiskCtfExchange/token_registered`.

- **CT rows:** `collateral_token` varies (from `ConditionalTokens/position_split`, `positions_merge`, `payout_redemption`).
- **NR rows:** `(resolved_collateral_token, ZERO32, condition, 1)` and `(resolved_collateral_token, ZERO32, condition, 2)`, where `resolved_collateral_token` is determined by matching `NegRiskCtfExchange/token_registered.token0/token1` against position IDs derived from the three known Polymarket collaterals: USDC.e (`2791bca1…a84174`), the NegRisk wrapped collateral (`3a3bd7bb…002e2`), and the v2 base collateral (`c011a7e1…2e82dfb`). Native USDC is not used as CTF collateral and is excluded. Exactly one collateral must reproduce the registered token pair; zero or multiple matches is a fatal error. Seven known orphan conditions — emitted to `token_registered` but with no usable `condition_preparation` and no trades on any exchange — match none of the three collaterals and are skipped (enumerated in `KNOWN_UNRESOLVABLE_NR_CONDITIONS`).

## Grain

One row per unique `(collateral_token, parent_collection_id, condition_id, index_set)`.

## Schema

| Column | Parquet logical type | Nullable | Description |
|---|---|---|---|
| `collateral_token` | `"BLOB"` (20 bytes) | no | ERC-20 collateral address |
| `parent_collection_id` | `"BLOB"` (32 bytes) | no | Parent collection (ZERO32 for root) |
| `condition_id` | `"BLOB"` (32 bytes) | no | CTF condition ID |
| `index_set` | `INT(bitWidth=32, isSigned=false)` | no | Outcome index set (bit position, > 0) |
| `token_id` | `"BLOB"` (32 bytes) | no | Computed ERC-1155 token ID |

## Physical sort order

Within each partition file, rows are sorted ascending by the full grain key, in this column order:

1. `collateral_token` (byte-wise ascending)
2. `parent_collection_id` (byte-wise ascending)
3. `condition_id` (byte-wise ascending)
4. `index_set` (numeric ascending)

`token_id` is functionally determined by the grain key and is not part of the sort. This total ordering over the grain key makes each partition file row-for-row reproducible from identical source data.

## Partitioning

**1M/10K nested partitioning** (same scheme as other derived tables).

Each 4-tuple is materialized in **exactly one partition** — the 10K partition in which it was *first seen* in any Polymarket-scoped event (`token_registered`, or a CT split/merge/redeem for a Polymarket condition).

- Partitions are processed in strict ascending block order.
- When materializing a 10K partition, the job loads all previously materialized `token_id_map_v1` partitions (or a compact "seen" representation) to suppress duplicates.
- Once a partition file (`data.parquet` + `metadata.json`) is written, it is **immutable** — it is never modified.
- The global set of all partitions is **globally unique** (no 4-tuple appears in more than one partition) while each individual partition file remains immutable.

This design allows incremental materialization while preserving the immutability contract required for derived data.

## Guarantees

- `parent_collection_id = ZERO32` for all rows (4 known exceptions in CT `position_split` are excluded; see `raw_data/polygon_contract_events_v3/tests/data_validation/test_token_registered_parent_is_zero32.py`).
- `index_set > 0`.
- No duplicate 4-tuples across the entire dataset.
- All `condition_id` values appear in `token_registered` from at least one Polymarket exchange.

## Event sources (for CT rows)

| Event | Contract | Purpose |
|---|---|---|
| `position_split` | `ConditionalTokens` | Discover `(collateral, parent, condition, index_set)` tuples from `partition` array |
| `positions_merge` | `ConditionalTokens` | Same as split (inverse operation) |
| `payout_redemption` | `ConditionalTokens` | Same as split (from `index_sets` array) |

NR rows are sourced from `NegRiskCtfExchange/token_registered` (token0/token1 for index_set 1/2), with collateral resolved by matching those token IDs against position IDs derived from the three allowlisted Polymarket collaterals.

## Lookup tables (used but not row contributors)

| Contract/event table | Purpose |
|---|---|
| `CTFExchange/token_registered` | Polymarket condition allow-list + NR token0/token1 values |
| `NegRiskCtfExchange/token_registered` | Polymarket condition allow-list + NR token0/token1 values |

## Versioning

This is `v1`. The producer shall not make a material breaking change to the schema or guarantees without incrementing the version.
