# token_id_map_v1 data dictionary

Lookup table: `(collateral_token, parent_collection_id, condition_id, index_set)` → `token_id`, with an optional `market_id` populated for NegRisk conditions.

## Scope

Every outcome token that trades on a Polymarket exchange.

A ConditionalTokens split/merge is treated as Polymarket-related only when it shares a transaction with an exchange `orders_matched` event from any of the four exchanges (`CTFExchange`, `CTFExchangeV2`, `NegRiskCtfExchange`, `NegRiskCtfExchangeV2`). Both legs live in the same transaction — and therefore the same block and 10K partition — so the join is partition-local. Each qualifying `ConditionalTokens/position_split` or `ConditionalTokens/positions_merge` row carries the `collateral_token`, `parent_collection_id`, `condition_id`, and `partition` (index sets) that fully determine each `token_id`. NegRisk markets are covered without special handling: the NegRiskAdapter performs its split/merge against the wrapped collateral, so the underlying `ConditionalTokens` event already carries the correct collateral.

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
| `market_id` | `"BLOB"` (32 bytes) | **yes** | NegRisk market identifier; `NULL` for standard (binary CTF / UMA) conditions |

## Physical sort order

Within each partition file, rows are sorted ascending by the full grain key, in this column order:

1. `collateral_token` (byte-wise ascending)
2. `parent_collection_id` (byte-wise ascending)
3. `condition_id` (byte-wise ascending)
4. `index_set` (numeric ascending)

`token_id` is functionally determined by the grain key and is not part of the sort. `market_id` is functionally determined by `condition_id` and is likewise not part of the sort. This total ordering over the grain key makes each partition file row-for-row reproducible from identical source data.

## Partitioning

**1M/10K nested partitioning** (same scheme as other derived tables).

Each 4-tuple is materialized in **exactly one partition** — the 10K partition in which it was *first seen* in a trade-linked `ConditionalTokens` split/merge (a split/merge sharing a transaction with an exchange `orders_matched` event).

- Partitions are processed in strict ascending block order.
- When materializing a 10K partition, the job loads all previously materialized `token_id_map_v1` partitions (or a compact "seen" representation) to suppress duplicates.
- Once a partition file (`data.parquet` + `metadata.json`) is written, it is **immutable** — it is never modified.
- The global set of all partitions is **globally unique** (no 4-tuple appears in more than one partition) while each individual partition file remains immutable.

This design allows incremental materialization while preserving the immutability contract required for derived data.

## Guarantees

- `parent_collection_id = ZERO32` for all rows. Polymarket conditions are flat (root parent); a non-root split never shares a transaction with an exchange match, so it is naturally excluded. Verified: zero trade-linked split tuples have a non-ZERO32 parent across the dataset.
- `index_set > 0`.
- No duplicate 4-tuples across the entire dataset.
- Every `token_id` is derived from the on-chain `ConditionalTokens` split/merge that introduced it, using the same `getCollectionId`/`getPositionId` math as the contract (see `lib/ct_helpers.py`).
- `market_id` is populated **if and only if** the `condition_id` is a NegRisk condition (its `ConditionalTokens/condition_preparation` row carries `oracle = NegRiskAdapter`). For all other conditions `market_id` is `NULL`.

## Attaching `market_id`

`market_id` is resolved per `condition_id` from `ConditionalTokens/condition_preparation` and embedded directly in each partition (no separate post-join step). The derivation is:

1. Look up the condition's single `condition_preparation` row (one row per `condition_id`; verified unique across the dataset).
2. If `oracle = NegRiskAdapter` (`0xd91E80cF2E7be2e162c6513ceD06f1dD0dA35296`), the row's `question_id` is the NegRisk questionId and `market_id = question_id` with its final byte cleared (`questionId & ~0xFF`, i.e. `NegRiskIdLib.getMarketId`).
3. Otherwise (standard binary / UMA condition) `market_id = NULL`.

This derives `market_id` straight from `condition_preparation` rather than `NegRiskAdapter/question_prepared`. The two are equivalent where both exist (verified: every `question_prepared` row satisfies `question_id & ~0xFF = market_id`, and the `question_id → market_id` map is single-valued), but deriving from `condition_preparation` also covers the handful of NegRisk conditions whose `question_prepared` event is not present.

## Assumptions (with source-code references)

These are enforced on-chain and therefore hold for any data that reaches an exchange.

1. **Preparation precedes any split or merge.** `ConditionalTokens.splitPosition` / `mergePositions` require the `conditionId` to have been prepared (`prepareCondition`). For NegRisk, `NegRiskAdapter.prepareQuestion` calls `ctf.prepareCondition(address(this), questionId, 2)` before any split is possible — see `NegRiskAdapter.sol:391` (in `raw_data/polygon_contract_events_v3/deployed_contract_source_code/NegRiskAdapter-0xd91E80cF2E7be2e162c6513ceD06f1dD0dA35296/NegRiskAdapter/src/NegRiskAdapter.sol`). Consequently every `condition_id` in this dataset has exactly one `condition_preparation` row, making the `market_id` lookup a total function.

2. **A merge is always preceded by a split for the same condition.** You cannot merge tokens you do not hold; the first balance of any `(condition_id, index_set)` is minted by a `splitPosition` (or a NegRisk conversion). **However, this does NOT let us scan splits only.** Discovery is restricted to CT operations that share a transaction with an exchange match ("trade-linked"). A token's split can be non-trade-linked (e.g. a market maker calling `splitPosition` directly) while its first trade-linked CT operation is a merge. Scanning splits only would therefore miss exactly **10** such 4-tuples (5 conditions × index sets 1/2; all USDC.e collateral; none NegRisk) measured across the full dataset. Both `position_split` and `positions_merge` are scanned to guarantee complete coverage.

3. **NegRisk `market_id` is the masked `question_id`.** `NegRiskIdLib.getMarketId(bytes32 questionId)` returns `questionId & MASK` where `MASK` clears the low 8 bits (see `.../NegRiskAdapter/src/libraries/NegRiskIdLib.sol`). Verified across the dataset: every `NegRiskAdapter/question_prepared` row satisfies this relation exactly.

## Event sources

| Event | Contract | Role |
|---|---|---|
| `orders_matched` | `CTFExchange`, `CTFExchangeV2`, `NegRiskCtfExchange`, `NegRiskCtfExchangeV2` | Transaction filter: a CT split/merge counts only if it shares a transaction with one of these |
| `position_split` | `ConditionalTokens` | Row source: `(collateral, parent, condition, index_set)` tuples from the `partition` array |
| `positions_merge` | `ConditionalTokens` | Row source: same as split (inverse operation); required for full coverage — see assumption 2 |
| `condition_preparation` | `ConditionalTokens` | `market_id` resolution: one row per condition gives `oracle` and `question_id` |

No other event tables are read.

## Versioning

This is `v1`. The producer shall not make a material breaking change to the schema or guarantees without incrementing the version.
