# fills_v1 data dictionary

One row per account leg of every matched fill on the four Polymarket exchanges, enriched with the condition (and NegRisk market) the outcome token belongs to and a running per-account position.

**This document is the data contract.** It defines the on-disk Parquet schema, the derivation of every column, the canonical row ordering, and all guarantees made by the producer. Consumers depend on these definitions.

Files are stored in `$FILLS_V1_DIR`. References below to filenames are relative to this path.

## Upstream dependencies

| Dataset | Role |
|---|---|
| `polygon_contract_events_v3` | Primary source — `order_filled` (all four exchanges: `CTFExchange`, `CTFExchangeV2`, `NegRiskCtfExchange`, `NegRiskCtfExchangeV2`), plus the FeeModule `fee_refunded` events (`FeeModuleCTF`, `FeeModuleNegRisk`) used to net v1 fees |
| `token_id_map_v1` | Lookup — resolves `token_id → condition_id` and `token_id → market_id`. v1 `order_filled` rows carry outcome-token IDs as `maker_asset_id` / `taker_asset_id` and v2 rows as `token_id`, never the condition, so the map is required to attribute a fill to its condition. The map is an approximation (a token enters only when a ConditionalTokens split/merge shares a transaction with an exchange `orders_matched`); coverage exceeds 99% by both row-weighted and distinct-token metrics (see its DATA_DICTIONARY.md). `fills_v1` only materializes legs whose token_id is present in the map; legs for tokens absent from the map are dropped. |

Both upstream datasets must be fully materialized through the target partition before this dataset is produced.

## Parquet type contract

This dataset reuses the logical Parquet types from [`polygon_contract_events_v3`](../../raw_data/polygon_contract_events_v3/DATA_DICTIONARY.md#parquet-type-contract) — `"BLOB"` is Parquet `BYTE_ARRAY` (variable length, never `FIXED_LEN_BYTE_ARRAY`) and `INT(bitWidth=32, isSigned=false)` is an unsigned 32-bit integer — plus the following:

| Parquet logical type | Physical type | DuckDB type | Used for |
|---|---|---|---|
| `INT(bitWidth=64, isSigned=true)` | `INT64` | `BIGINT` | Signed condition-token and USDC amounts (6 decimal places) |
| `BOOLEAN` | `BOOLEAN` | `BOOLEAN` | Boolean flags |

Condition-token amounts and USDC amounts both use 6 decimal places (raw units; `1_000_000` = 1 token or 1 USDC). All amount columns fit comfortably in signed 64 bits. `NULL` is permitted only for `market_id`; all other columns are guaranteed non-null.

## Grain

One row per **account leg** of every `order_filled` event, identified by `(block_number, logical_fill_index)`.

Every `order_filled` row whose outcome token is present in `token_id_map_v1` maps to exactly one fills_v1 leg. The exchange contract's own marketplace legs (where maker or taker equals an exchange address) are excluded; every row represents a beneficial trader's side of the fill. Legs whose `token_id` is absent from `token_id_map_v1` are dropped (the map covers >99% of traded tokens).

## Schema

| Column | Parquet logical type | Nullable | Description |
|---|---|---|---|
| `block_number` | `INT(bitWidth=32, isSigned=false)` | no | Block in which the fill was mined |
| `logical_fill_index` | `INT(bitWidth=32, isSigned=false)` | no | Dense 0-based index that totally orders all fill legs within the block (see physical sort order). Ordered by `transaction_index`, then by atomic matched order within the transaction, then taker leg first followed by maker legs from best price to worst price, ties broken by `log_index`. |
| `transaction_index` | `INT(bitWidth=32, isSigned=false)` | no | Index of the transaction within the block |
| `log_index` | `INT(bitWidth=32, isSigned=false)` | no | Log index of the source `order_filled` |
| `account` | `"BLOB"` (20 bytes) | no | Trader (wallet) address for this leg |
| `token_id` | `"BLOB"` (32 bytes) | no | Outcome token ID traded in this leg |
| `condition_id` | `"BLOB"` (32 bytes) | no | CTF condition the outcome token belongs to, from `token_id_map_v1` |
| `market_id` | `"BLOB"` (32 bytes) | **yes** | NegRisk market identifier from `token_id_map_v1`; `NULL` for standard (binary CTF / UMA) conditions |
| `is_taker` | `BOOLEAN` | no | `TRUE` for the taker leg of the matched order, `FALSE` for a maker leg |
| `net_yes_tokens` | `INT(bitWidth=64, isSigned=true)` | no | Signed equivalent change in the condition's YES tokens for this account (fees do not apply to condition tokens): positive for buy YES, negative for sell YES, positive for sell NO, negative for buy NO. 6 decimals. |
| `gross_usdc` | `INT(bitWidth=64, isSigned=true)` | no | Signed nominal USDC amount before fees: positive for buys (account spends USDC), negative for sells (account receives USDC). Micro USDC, 6 decimals. |
| `fee_usdc` | `INT(bitWidth=64, isSigned=true)` | no | Net USDC fee attributed to this leg (always ≥ 0). Micro USDC, 6 decimals. See fee mechanics. |
| `net_yes_position_after` | `INT(bitWidth=64, isSigned=true)` | no | Cumulative sum of `net_yes_tokens` for this `(account, condition_id)` across all fills up to and including this one (running balance, spans partitions). 6 decimals. |

Column order is fixed and is part of the contract.

## Physical sort order

Within each partition file, rows are sorted ascending by `(block_number, logical_fill_index)`. `logical_fill_index` is assigned to make this ordering canonical and is derived, within each block, by ordering legs as follows:

1. `transaction_index` ascending.
2. Within a transaction, atomic matches are grouped by `order_hash` (the match key); each match is ordered by the minimum `log_index` among its `order_filled` rows.
3. Within a match: the taker leg first (`is_taker = TRUE`), then the maker legs.
4. Maker legs ordered by implied price, best-for-taker first: when the taker is buying outcome tokens, lowest price first; when the taker is selling, highest price first.
5. Ties (makers at the same price) broken by `log_index` ascending.

This ordering makes each partition file row-for-row reproducible from identical source data.

## Partitioning

**1M/10K nested partitioning** (same scheme as the raw tables and `token_id_map_v1`).

Each fill is materialized in exactly the 10K partition whose block range contains its `block_number`. A 10K block range with no fills still writes a zero-row `data.parquet` plus `metadata.json`. Once written, a partition file is immutable.

## Fee mechanics

Fees are charged only in selected markets and the mechanism differs by exchange generation. See [How Polymarket transactions work](../../docs/How%20Polymarket%20transactions%20work.md) for the on-chain semantics.

- **V2 exchanges (CTFExchangeV2, NegRiskCtfExchangeV2):** the `order_filled` row carries a final `fee` field denominated in the asset the order's signer receives (USDC when selling, outcome tokens when buying). `fee_usdc` records only the USDC-denominated fees; buy-side (token-denominated) fees contribute 0.
- **V1 exchanges (CTFExchange, NegRiskCtfExchange):** the raw `order_filled.fee` is gross. The net fee is `fee_refunded.fee_charged` where a refund row exists (joined on `(transaction_hash, order_hash)`) and the raw `order_filled.fee` otherwise; only the USDC-denominated fee (`fee_refunded.token_id = ZERO32`) contributes to `fee_usdc`. Buy-side fees (token-denominated) contribute 0 to `fee_usdc`.

`gross_usdc` and `net_yes_tokens` always record the nominal pre-fee fill size; the fee is reported separately in `fee_usdc` so consumers can choose how to apply it.

## Outcome convention (YES = index_set 1, NO = index_set 2)

Per Polymarket's CTF documentation (<https://github.com/Polymarket/agent-skills/blob/main/ctf-operations.md>: "partition [1, 2] for binary (Yes=1, No=2)"), the YES outcome is index_set 1 and NO is index_set 2. `net_yes_tokens` expresses every leg in YES-equivalent terms. The producer asserts that every traded token has index_set ∈ {1, 2} (binary conditions only) and fails fast on violation.

## Row suppression: fills after condition resolution

Fills that occur after the resolution of their condition are suppressed from output and do not appear in any partition file. A fill is suppressed if its `(block_number, log_index)` is strictly after the `(block_number, log_index)` of the `ConditionalTokens/condition_resolution` event for its `condition_id`.

This suppression is deterministic and depends only on the condition's resolution timestamp (on-chain), not on partition boundaries or producer state. The same fill will always be suppressed or included across all runs. Consumers should never observe fills with `condition_id` values in resolved states at their respective `(block_number, log_index)` tuples.

## Versioning

This is `v1`. The producer shall not make a material breaking change to the schema, ordering, or guarantees without incrementing the version.
