# token_id_map_v1 data dictionary

Lookup table: `(collateral_token, parent_collection_id, condition_id, index_set)` → `token_id`.

## Scope

Every condition that appears in `CTFExchange/token_registered` or `NegRiskCtfExchange/token_registered`.

- **CT rows:** `collateral_token` varies (from `ConditionalTokens/position_split`, `positions_merge`, `payout_redemption`).
- **NR rows:** Always `(USDC_E, ZERO32, condition, 1)` and `(USDC_E, ZERO32, condition, 2)` sourced directly from `token_registered` (no keccak).

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

NR rows are sourced directly from `NegRiskCtfExchange/token_registered` (token0/token1 for index_set 1/2).

## Lookup tables (used but not row contributors)

| Contract/event table | Purpose |
|---|---|
| `CTFExchange/token_registered` | Polymarket condition allow-list + NR token0/token1 values |
| `NegRiskCtfExchange/token_registered` | Polymarket condition allow-list + NR token0/token1 values |

## Versioning

This is `v1`. The producer shall not make a material breaking change to the schema or guarantees without incrementing the version.
