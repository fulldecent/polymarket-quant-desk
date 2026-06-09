# token_id_map_v1

Maps `(collateral_token, parent_collection_id, condition_id, index_set)` → `token_id` for all Polymarket-registered conditions.

## Scope

- All conditions registered in `CTFExchange/token_registered` or `NegRiskCtfExchange/token_registered`.
- CT rows: varying `collateral_token` from `ConditionalTokens/position_split`, `positions_merge`, `payout_redemption`.
- NR rows: always `(USDC_E, ZERO32, condition, 1/2)` sourced directly from `token_registered` (no keccak).

## Schema

| Column | Type | Description |
|--------|------|-------------|
| `collateral_token` | `BLOB(20)` | ERC-20 collateral address |
| `parent_collection_id` | `BLOB(32)` | Parent collection (ZERO32 for root) |
| `condition_id` | `BLOB(32)` | CTF condition ID |
| `index_set` | `UINT32` | Outcome index set (bit position) |
| `token_id` | `BLOB(32)` | Computed ERC-1155 token ID |

**Grain:** One row per unique 4-tuple. No duplicates.

## Partitioning

`1M={N}/10K={K}/data.parquet` (same scheme as other derived tables).

## How to run

```sh
source .venv/bin/activate
python derived_data/token_id_map_v1/main.py [options]
```

## How to test

```sh
source .venv/bin/activate
python -m pytest derived_data/token_id_map_v1/tests/ -v
```
