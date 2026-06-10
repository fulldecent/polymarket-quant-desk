# token_id_map_v1

Maps `(collateral_token, parent_collection_id, condition_id, index_set)` → `token_id` for every outcome token that trades on a Polymarket exchange, plus an optional `market_id` for NegRisk conditions.

## Scope

A `ConditionalTokens` split/merge is treated as Polymarket-related only when it shares a transaction with an exchange `orders_matched` event (v1 or v2, standard or NegRisk: `CTFExchange`, `CTFExchangeV2`, `NegRiskCtfExchange`, `NegRiskCtfExchangeV2`). Each such `position_split`/`positions_merge` carries the collateral, parent collection, condition, and `partition` (index sets) that fully determine each `token_id`, which is derived with the same `getCollectionId`/`getPositionId` math as the contract (`lib/ct_helpers.py`). NegRisk markets need no special handling because the adapter splits/merges against the wrapped collateral, so the underlying `ConditionalTokens` event already carries the correct collateral. There is no dependency on `token_registered` and no collateral-resolution heuristic.

Both splits and merges are scanned. On-chain a merge always follows a split, but discovery is filtered to *trade-linked* CT operations, and a token's split can be non-trade-linked while its first trade-linked operation is a merge — scanning splits only would miss 10 such 4-tuples across the dataset. See `DATA_DICTIONARY.md` (assumption 2) for details.

`market_id` is resolved per `condition_id` from `ConditionalTokens/condition_preparation`: NegRisk conditions (oracle = NegRiskAdapter) get `market_id = question_id & ~0xFF` (`NegRiskIdLib.getMarketId`); all other conditions get `NULL`.

## Schema

| Column | Type | Description |
|--------|------|-------------|
| `collateral_token` | `BLOB(20)` | ERC-20 collateral address |
| `parent_collection_id` | `BLOB(32)` | Parent collection (ZERO32 for root) |
| `condition_id` | `BLOB(32)` | CTF condition ID |
| `index_set` | `UINT32` | Outcome index set (bit position) |
| `token_id` | `BLOB(32)` | Computed ERC-1155 token ID |
| `market_id` | `BLOB(32)`, nullable | NegRisk market identifier; `NULL` for standard binary / UMA conditions |

**Grain:** One row per unique 4-tuple. No duplicates.

## Partitioning

`1M={N}/10K={K}/data.parquet` (same scheme as other derived tables).

## How to run

```sh
source .venv/bin/activate
python derived_data/token_id_map_v1/main.py [options]
```

## Coverage

This map is an approximation: a token is included only when its split/merge shares a transaction with an exchange `orders_matched` event (the "trade-linkage" simplification). We do not enumerate every theoretically derivable token, only the ones that actually trade. Measured across the full dataset, this resolves **~99.90% of order-fill volume** and **~99.39% of distinct traded tokens**. A token is missing only if it is never seen in a trade-linked split/merge — e.g. it is only ever minted via a non-trade-linked `splitPosition`, only via NegRisk `positions_converted`, only traded after the materialized frontier, or never matched on an exchange at all. The >99% guarantee is enforced by `tests/data_validation/test_exchange_token_coverage.py`; see `DATA_DICTIONARY.md` "Coverage" for the full breakdown.

## How to test

```sh
source .venv/bin/activate
python -m pytest derived_data/token_id_map_v1/tests/ -v
```
