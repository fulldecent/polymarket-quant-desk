# token_id_map_v1 data dictionary

Lookup table: `(collateral_token, parent_collection_id, condition_id, index_set)` → `token_id`, with an optional `market_id` populated for NegRisk conditions.

## Scope

Every outcome token that trades on a Polymarket exchange. (See [coverage note](#coverage) below.)

A ConditionalTokens split/merge is treated as Polymarket-related only when it shares a transaction with an exchange `orders_matched` event from any of the four exchanges (`CTFExchange`, `CTFExchangeV2`, `NegRiskCtfExchange`, `NegRiskCtfExchangeV2`). Both legs live in the same transaction — and therefore the same block and 10K partition — so the join is partition-local. Each qualifying `ConditionalTokens/position_split` or `ConditionalTokens/positions_merge` row carries the `collateral_token`, `parent_collection_id`, `condition_id`, and `partition` (index sets) that fully determine each `token_id`. NegRisk markets are covered without special handling: the NegRiskAdapter performs its split/merge against the wrapped collateral, so the underlying `ConditionalTokens` event already carries the correct collateral.

## Grain

One row per `token_id`.

Each `(collateral_token, parent_collection_id, condition_id, index_set)` is also unique (assuming no Keccak-256 collisions).

## Schema

| Column | Parquet logical type | Nullable | Description |
|---|---|---|---|
| `token_id` | `"BLOB"` (32 bytes) | no | Computed ERC-1155 token ID |
| `collateral_token` | `"BLOB"` (20 bytes) | no | ERC-20 collateral address |
| `parent_collection_id` | `"BLOB"` (32 bytes) | no | Parent collection (ZERO32 for root) |
| `condition_id` | `"BLOB"` (32 bytes) | no | CTF condition ID |
| `index_set` | `INT(bitWidth=32, isSigned=false)` | no | Outcome index set (bit position, > 0) |
| `market_id` | `"BLOB"` (32 bytes) | **yes** | NegRisk market identifier; `NULL` for standard (binary CTF / UMA) conditions |

## Physical sort order

Within each partition file, rows are sorted ascending by `token_id` (byte-wise ascending).

## Partitioning

**1M/10K nested partitioning** (same scheme as raw tables).

Each `token_id` is materialized in **exactly one partition** — the 10K partition in which it was *first seen* in a trade-linked `ConditionalTokens` split/merge (a split/merge sharing a transaction with an exchange `orders_matched` event).

- Partitions are processed in strict ascending block order.
- When materializing a 10K partition, the job loads all previously materialized `token_id_map_v1` partitions (or a compact "seen" representation) to suppress duplicates.
- Once a partition file (`data.parquet` + `metadata.json`) is written, it is **immutable** — it is never modified.
- The global set of all partitions is **globally unique** (no `token_id` or 4-tuple appears in more than one partition) while each individual partition file remains immutable.

This design allows incremental materialization while preserving the immutability contract required for derived data.

## Guarantees and validation

Validation scripts live in `tests/data_validation/`. Each guarantee below links to its validation script and states scope.

### Identity, schema, and sort guarantees

- [test_schema_and_sort_order.py](tests/data_validation/test_schema_and_sort_order.py) — no duplicate `token_id` or 4-tuple (`collateral_token`, `parent_collection_id`, `condition_id`, `index_set`) across the entire dataset. **Scope:** all landed partitions.
- [test_schema_and_sort_order.py](tests/data_validation/test_schema_and_sort_order.py) — physical schema matches contract types (`BLOB` as Parquet `BYTE_ARRAY`, `index_set` as unsigned `INT32`) and rows are sorted by `token_id` (byte-wise ascending). **Scope:** all landed partitions.
- [test_schema_and_sort_order.py](tests/data_validation/test_schema_and_sort_order.py) — `parent_collection_id = ZERO32` for all rows. **Scope:** all landed partitions.
- [test_schema_and_sort_order.py](tests/data_validation/test_schema_and_sort_order.py) — `index_set > 0` for all rows. **Scope:** all landed partitions.

### NegRisk enrichment guarantees

- [test_schema_and_sort_order.py](tests/data_validation/test_schema_and_sort_order.py) — `market_id` is populated if and only if the condition oracle is `NegRiskAdapter`; otherwise `market_id` is `NULL`. **Scope:** all map rows joined to `ConditionalTokens/condition_preparation`.
- [test_schema_and_sort_order.py](tests/data_validation/test_schema_and_sort_order.py) — non-null `market_id` values are 32 bytes with final byte cleared, and each `condition_id` maps to a single `market_id`. **Scope:** all landed partitions.

### Partition and coverage guarantees

- [test_partition_coverage.py](tests/data_validation/test_partition_coverage.py) — partitions are consecutive from the start partition, each partition has `data.parquet` plus `metadata.json`, and each `10K` partition sits under the correct `1M` parent. **Scope:** landed partition folders.
- [test_exchange_token_coverage.py](tests/data_validation/test_exchange_token_coverage.py) — exchange outcome-token coverage stays above 99% by both row-weighted and distinct-token metrics. **Scope:** all supported exchange `order_filled` tables against the full token map.

## Attaching `market_id`

`market_id` is resolved per `condition_id` from `ConditionalTokens/condition_preparation` and embedded directly in each partition (no separate post-join step). The derivation is:

1. Look up the condition's single `condition_preparation` row (one row per `condition_id`; verified unique across the dataset).
2. If `oracle = NegRiskAdapter` (`0xd91E80cF2E7be2e162c6513ceD06f1dD0dA35296`), the row's `question_id` is the NegRisk questionId and `market_id = question_id` with its final byte cleared (`questionId & ~0xFF`, i.e. [NegRiskIdLib.getMarketId](../../raw_data/polygon_contract_events_v3/deployed_contract_source_code/NegRiskAdapter-0xd91E80cF2E7be2e162c6513ceD06f1dD0dA35296/NegRiskAdapter/src/libraries/NegRiskIdLib.sol#L25)).
3. Otherwise (standard binary / UMA condition) `market_id = NULL`.

This derives `market_id` straight from `condition_preparation` rather than `NegRiskAdapter/question_prepared`. The two are equivalent where both exist (verified: every `question_prepared` row satisfies `question_id & ~0xFF = market_id`, and the `question_id → market_id` map is single-valued), but deriving from `condition_preparation` also covers the handful of NegRisk conditions whose `question_prepared` event is not present.

## Assumptions (with source-code references)

These are enforced on-chain and therefore hold for any data that reaches an exchange.

1. **Preparation precedes any split or merge.** [ConditionalTokens.splitPosition](../../raw_data/polygon_contract_events_v3/deployed_contract_source_code/ConditionalTokens-0x4D97DCd97eC945f40cF65F87097ACe5EA0476045/ConditionalTokens/Contract.sol#L1297) and [ConditionalTokens.mergePositions](../../raw_data/polygon_contract_events_v3/deployed_contract_source_code/ConditionalTokens-0x4D97DCd97eC945f40cF65F87097ACe5EA0476045/ConditionalTokens/Contract.sol#L1357) require the `conditionId` to have been prepared via [ConditionalTokens.prepareCondition](../../raw_data/polygon_contract_events_v3/deployed_contract_source_code/ConditionalTokens-0x4D97DCd97eC945f40cF65F87097ACe5EA0476045/ConditionalTokens/Contract.sol#L1257). For NegRisk, [NegRiskAdapter.prepareQuestion](../../raw_data/polygon_contract_events_v3/deployed_contract_source_code/NegRiskAdapter-0xd91E80cF2E7be2e162c6513ceD06f1dD0dA35296/NegRiskAdapter/src/NegRiskAdapter.sol#L385) calls `ctf.prepareCondition(address(this), questionId, 2)` at [NegRiskAdapter.prepareQuestion call site](../../raw_data/polygon_contract_events_v3/deployed_contract_source_code/NegRiskAdapter-0xd91E80cF2E7be2e162c6513ceD06f1dD0dA35296/NegRiskAdapter/src/NegRiskAdapter.sol#L391) before any split is possible. Consequently every `condition_id` in this dataset has exactly one `condition_preparation` row, making the `market_id` lookup a total function.

2. **A merge is always preceded by a split for the same condition.** You cannot merge tokens you do not hold; the first balance of any `(condition_id, index_set)` is minted by a `splitPosition` (or a NegRisk conversion). **However, this does NOT let us scan splits only.** Discovery is restricted to CT operations that share a transaction with an exchange match ("trade-linked"). A token's split can be non-trade-linked (e.g. a market maker calling `splitPosition` directly) while its first trade-linked CT operation is a merge. Scanning splits only would therefore miss exactly **10** such 4-tuples (5 conditions × index sets 1/2; all USDC.e collateral; none NegRisk) measured across the full dataset. Both `position_split` and `positions_merge` are scanned to guarantee complete coverage.

3. **NegRisk `market_id` is the masked `question_id`.** [NegRiskIdLib.getMarketId](../../raw_data/polygon_contract_events_v3/deployed_contract_source_code/NegRiskAdapter-0xd91E80cF2E7be2e162c6513ceD06f1dD0dA35296/NegRiskAdapter/src/libraries/NegRiskIdLib.sol#L25) returns `questionId & MASK` where `MASK` clears the low 8 bits. Verified across the dataset: every `NegRiskAdapter/question_prepared` row satisfies this relation exactly.

## Event sources

| Event | Contract | Role |
|---|---|---|
| `orders_matched` | `CTFExchange[V2]`, `NegRiskCtfExchange[V2]` | Transaction filter: a CT split/merge counts only if it shares a transaction with one of these |
| `position_split` | `ConditionalTokens` | Row source: `(collateral, parent, condition, index_set)` tuples from the `partition` array |
| `positions_merge` | `ConditionalTokens` | Row source: same as split (inverse operation); required for full coverage — see assumption 2 |
| `condition_preparation` | `ConditionalTokens` | `market_id` resolution: one row per condition gives `oracle` and `question_id` |

No other event tables are read.

## Coverage (the approximation and its limits)

This dataset is an **approximation** of "every outcome token a consumer might encounter". The simplification is the trade-linkage filter described under "Scope": a token enters the map only when its `condition_id`/`index_set` is observed in a `ConditionalTokens` split/merge that **shares a transaction with an exchange `orders_matched` event**. We deliberately do not enumerate every token that could theoretically exist (e.g. by hashing every prepared condition × every index set), because the goal is to cover tokens that actually trade.

Measured across the full dataset, coverage of exchange `order_filled` outcome-token legs is:

- **Row-weighted (trade-volume) coverage: ~99.90%** — of ~1.31 billion order-fill outcome-token legs, ~99.90% reference a `token_id` present in the map.
- **Distinct-token coverage: ~99.39%** — of ~2.01 million distinct outcome tokens ever traded, ~99.39% are present.

The contract is that **both metrics stay above 99%**, asserted by `tests/data_validation/test_exchange_token_coverage.py`. If a future rebuild drops below this, the test fails and a human must investigate.

### What produces a not-covered outcome token

A traded token is absent from the map only when its grain tuple is never observed in a *trade-linked* split/merge. The known scenarios:

1. **No split/merge co-occurs with a trade in the scanned data.** A token can be minted by a `splitPosition` in a transaction that contains no exchange `orders_matched` event (e.g. a market maker splits collateral directly, then later sells), and if every subsequent split/merge for that tuple is likewise non-trade-linked, the tuple is never discovered. (Note: a token whose split is non-trade-linked but whose *merge* is trade-linked **is** covered — that is exactly why both splits and merges are scanned; see assumption 2.)
2. **Conversions instead of splits/merges.** NegRisk `positions_converted` mints/burns positions without a `ConditionalTokens` split/merge. A token reachable only via conversion (never via a trade-linked split/merge) is not discovered.
3. **Trades outside the materialized block range.** Discovery only runs up to the frontier; a token whose only trade-linked split/merge falls after the frontier is not yet present (it will appear when later partitions are materialized).
4. **Off-exchange-only tokens.** Tokens that are split/merged and transferred but never matched on any of the four exchanges are intentionally out of scope; they are not "traded" by this dataset's definition and their absence is expected, not a defect.

These are inherent to the trade-linkage approximation. The >99% coverage guarantee bounds their aggregate impact: the uncovered tokens are a low-volume long tail (the missing ~0.61% of distinct tokens account for only ~0.10% of trades).

## Versioning

This is `v1`. The producer shall not make a material breaking change to the schema or guarantees without incrementing the version.
