# Data dictionary — polygon_contract_events_v2

On-chain events from Polymarket's core contracts on Polygon, scraped via JSON-RPC `eth_getLogs` and exported to partitioned Parquet files.

## Data type strategy

Raw tables must be a lossless transcript of what the blockchain emitted. Downstream tables can downcast to smaller, faster types with assertions that fail loudly if a value falls outside expected bounds.

| Value category | Type in Parquet | Rationale |
|---|---|---|
| 32-byte identifiers (condition_id, market_id, order_hash, token IDs, etc.) | BLOB (exactly 32 bytes) | Opaque names, not quantities. The JSON-RPC API returns some as hex and others as decimal — once parsed, they are all fixed-size byte arrays. |
| 20-byte addresses (maker, taker, oracle, etc.) | BLOB (exactly 20 bytes) | Same rationale. |
| uint256 amounts (amount, fee, maker_amount_filled, payout, etc.) | VARCHAR (decimal string) | Lossless for any uint256. The EVM can produce values up to $2^{256} - 1$. In practice every real Polymarket amount fits in uint64, but storing as VARCHAR avoids silent truncation. Downstream layers downcast to BIGINT with fail-fast assertions. |
| Small integers (block_number, outcome_slot_count, fee_bips, etc.) | UINTEGER (uint32) | Bounded values. Block numbers are ~85M; uint32 covers ~$4.3 \times 10^{9}$ (~270 years at 1 block / 2 s). |
| Arrays of uint256 (payout_numerators, partition, index_sets, etc.) | VARCHAR (JSON) | JSON array of decimal strings. No good native DuckDB / Parquet type for variable-length uint256 arrays. |

## Location

- **Hot (SQLite):** `/Volumes/Untitled/raw_data/hot/polygon_contract_events_v2.db`
- **Cold (Parquet):** `/Volumes/Untitled/raw_data/cold/polygon_contract_events_v2/{contract}/{event}/1M={N}/10K={N}/data.parquet`

## Partitioning

- **Index column:** `block_number`
- **Scheme:** `1M={N}/10K={N}/data.parquet` — 1M-block directories containing 100 10K-block partitions
- **A complete 1M partition** has exactly 100 10K sub-directories
- **Example:** `CTFExchange/order_filled/1M=83000000/10K=83010000/data.parquet` covers blocks 83,010,000–83,019,999

## Physical sort order

All partitions are sorted by `block_number, transaction_index, log_index`.

## Unique key

`(transaction_hash, log_index)` — enforced by UNIQUE constraint in SQLite and validated by [test_transaction_log_index_globally_unique](assertions/test_transaction_log_index_globally_unique.py).

---

## Common columns

All tables share these columns in this order, before event-specific columns:

| Column | DuckDB type | Description | Guarantees |
|---|---|---|---|
| `block_number` | UINTEGER | Polygon block number | Strictly within partition bounds |
| `transaction_index` | UINTEGER | Position of the transaction within the block | Non-negative |
| `transaction_hash` | BLOB (32 bytes) | Transaction hash | 0x + 64 hex chars in source |
| `log_index` | UINTEGER | Position of the log within the block | Non-negative |

The emitting **contract address** is not stored as a column — it is implicit from the directory path (e.g., `CTFExchange/order_filled/`).

---

## Contracts

| Contract | Address | Directory | Role |
|---|---|---|---|
| Gnosis ConditionalTokens | `0x4D97DCd97eC945f40cF65F87097ACe5EA0476045` | `ConditionalTokens/` | Outcome token framework — manages conditions, splits, merges, redemptions |
| CTFExchange | `0x4bFb41d5B3570DeFd03C39a9A4D8dE6Bd8B8982E` | `CTFExchange/` | Binary market orderbook exchange |
| NegRiskCtfExchange | `0xC5d563A36AE78145C45a50134d48A1215220f80a` | `NegRiskCtfExchange/` | Multi-outcome (NegRisk) market orderbook exchange |
| NegRiskAdapter | `0xd91E80cF2E7be2e162c6513ceD06f1dD0dA35296` | `NegRiskAdapter/` | Wraps CTF for NegRisk position actions |
| UmaCtfAdapter | `0x157Ce2d672854c848c9b79C49a8Cc6cc89176a49` | `UmaCtfAdapter/` | UMA oracle integration for market resolution |

---

## Events by contract

### ConditionalTokens

#### condition_preparation

Market condition created. One row per condition.

| Column | DuckDB type | Description |
|---|---|---|
| `condition_id` | BLOB (32 bytes) | The condition identifier |
| `oracle` | BLOB (20 bytes) | The oracle that can resolve this condition |
| `question_id` | BLOB (32 bytes) | The question identifier |
| `outcome_slot_count` | UINTEGER | Number of outcome slots (typically 2 for binary) |

#### condition_resolution

Condition resolved with payout distribution. Emitted for both binary CTF markets and NegRisk conditions (the NegRiskAdapter calls `reportPayouts` on ConditionalTokens).

| Column | DuckDB type | Description |
|---|---|---|
| `condition_id` | BLOB (32 bytes) | The condition identifier |
| `oracle` | BLOB (20 bytes) | The resolving oracle |
| `question_id` | BLOB (32 bytes) | The question identifier |
| `outcome_slot_count` | UINTEGER | Number of outcome slots |
| `payout_numerators` | VARCHAR | JSON array of uint256 strings (length = outcome_slot_count) |

`payout_denominator = sum(payout_numerators)` per Gnosis CTF semantics. For a clean win/loss this is `["1","0"]` or `["0","1"]` (denom=1). A `["1","1"]` resolution (denom=2) indicates a draw or voided market where each side receives half. Some markets use 10^18-scaled values.

#### position_split

Collateral split into outcome tokens. Locks USDC.e and mints one token per outcome.

| Column | DuckDB type | Description |
|---|---|---|
| `stakeholder` | BLOB (20 bytes) | The account splitting |
| `collateral_token` | BLOB (20 bytes) | The collateral token address (usually USDC.e) |
| `parent_collection_id` | BLOB (32 bytes) | Parent collection (0x00...00 for root splits) |
| `condition_id` | BLOB (32 bytes) | The condition being split on |
| `partition` | VARCHAR | JSON array of uint256 strings — the outcome index sets |
| `amount` | VARCHAR | uint256 decimal string — collateral amount locked |

#### positions_merge

Outcome tokens merged back into collateral. Inverse of position_split — burns equal quantities of all outcome tokens.

| Column | DuckDB type | Description |
|---|---|---|
| `stakeholder` | BLOB (20 bytes) | The account merging |
| `collateral_token` | BLOB (20 bytes) | The collateral token address |
| `parent_collection_id` | BLOB (32 bytes) | Parent collection |
| `condition_id` | BLOB (32 bytes) | The condition being merged on |
| `partition` | VARCHAR | JSON array of uint256 strings — the outcome index sets |
| `amount` | VARCHAR | uint256 decimal string — collateral amount returned |

#### payout_redemption

Winning outcome tokens redeemed for collateral after resolution.

| Column | DuckDB type | Description |
|---|---|---|
| `redeemer` | BLOB (20 bytes) | The account redeeming |
| `collateral_token` | BLOB (20 bytes) | The collateral token address |
| `parent_collection_id` | BLOB (32 bytes) | Parent collection |
| `condition_id` | BLOB (32 bytes) | The resolved condition |
| `index_sets` | VARCHAR | JSON array of uint256 strings — the outcome index sets being redeemed |
| `payout` | VARCHAR | uint256 decimal string — total collateral returned |

---

### CTFExchange / NegRiskCtfExchange

These two contracts emit identical event signatures. Data is stored in separate directories: `CTFExchange/` for binary markets, `NegRiskCtfExchange/` for NegRisk markets.

#### order_filled

Order fill (partial or complete). One row per maker order filled in a match.

| Column | DuckDB type | Description |
|---|---|---|
| `order_hash` | BLOB (32 bytes) | The order being filled |
| `maker` | BLOB (20 bytes) | Maker wallet address |
| `taker` | BLOB (20 bytes) | Taker wallet address |
| `maker_asset_id` | BLOB (32 bytes) | Token ID of maker's asset (32 zero bytes = USDC side of trade) |
| `taker_asset_id` | BLOB (32 bytes) | Token ID of taker's asset (32 zero bytes = USDC side of trade) |
| `maker_amount_filled` | VARCHAR | uint256 decimal string — amount of maker asset filled (6 decimals for USDC) |
| `taker_amount_filled` | VARCHAR | uint256 decimal string — amount of taker asset filled (6 decimals for USDC) |
| `fee` | VARCHAR | uint256 decimal string — fee charged on this fill |

Exactly one of `maker_asset_id` / `taker_asset_id` is zero bytes (USDC). This invariant is validated by [test_order_filled_one_side_is_collateral](assertions/test_order_filled_one_side_is_collateral.py).

Implied price = USDC amount / outcome token amount. Validated > 0 by [test_order_filled_implied_price_in_range](assertions/test_order_filled_implied_price_in_range.py).

#### orders_matched

Summary of a taker order match. One row per `matchOrders` call, aggregating the taker's view of all constituent fills.

| Column | DuckDB type | Description |
|---|---|---|
| `taker_order_hash` | BLOB (32 bytes) | The taker order being matched |
| `taker_order_maker` | BLOB (20 bytes) | The taker order's maker address |
| `maker_asset_id` | BLOB (32 bytes) | Token ID (or zero bytes for USDC) |
| `taker_asset_id` | BLOB (32 bytes) | Token ID (or zero bytes for USDC) |
| `maker_amount_filled` | VARCHAR | uint256 decimal string |
| `taker_amount_filled` | VARCHAR | uint256 decimal string |

The taker's `order_filled` row has identical amounts — validated by [test_order_filled_taker_matches_orders_matched](assertions/test_order_filled_taker_matches_orders_matched.py).

#### fee_charged

Fee deducted from a trade.

| Column | DuckDB type | Description |
|---|---|---|
| `receiver` | BLOB (20 bytes) | The fee recipient address |
| `token_id` | BLOB (32 bytes) | The token the fee is denominated in |
| `amount` | VARCHAR | uint256 decimal string — fee amount |

Per-transaction fee identity: `SUM(order_filled.fee) = SUM(fee_charged.amount)`. Validated by [test_fee_reconciliation_per_tx](assertions/test_fee_reconciliation_per_tx.py).

#### token_registered

Token pair registered for trading on the exchange.

| Column | DuckDB type | Description |
|---|---|---|
| `token0` | BLOB (32 bytes) | First outcome token ID |
| `token1` | BLOB (32 bytes) | Second outcome token ID |
| `condition_id` | BLOB (32 bytes) | The condition these tokens belong to |

Each registration emits two rows: (token0=A, token1=B) and (token0=B, token1=A). Validated by [test_binary_conditions_have_two_tokens](assertions/test_binary_conditions_have_two_tokens.py).

#### order_cancelled

Order cancelled by maker.

| Column | DuckDB type | Description |
|---|---|---|
| `order_hash` | BLOB (32 bytes) | The cancelled order identifier |

---

### FeeModuleCTF / FeeModuleNegRisk

#### fee_refunded

Fee refund emitted by the fee module after each `OrderFilled`. The `fee` field in `OrderFilled` is the gross fee; most of it is refunded back to the trader. The actual net fee is `fee_charged`.

| Column | DuckDB type | Description |
|---|---|---|
| `order_hash` | BLOB (32 bytes) | Matches the order hash from `OrderFilled` (1:1 per fill) |
| `receiver` | BLOB (20 bytes) | Trader receiving the refund (Solidity field: `to`) |
| `token_id` | BLOB (32 bytes) | Token the refund is denominated in (0 for USDC) |
| `refund` | VARCHAR | uint256 decimal string — amount refunded to the trader |
| `fee_charged` | VARCHAR | uint256 decimal string — actual net fee retained |

Invariant: `refund + fee_charged = order_filled.fee` for the corresponding fill.

---

### NegRiskAdapter

#### market_prepared

NegRisk market created.

| Column | DuckDB type | Description |
|---|---|---|
| `market_id` | BLOB (32 bytes) | The NegRisk market identifier |
| `oracle` | BLOB (20 bytes) | The oracle for this market |
| `fee_bips` | UINTEGER | Fee in basis points (0–10,000) |
| `data` | VARCHAR | Additional market data (hex string) |

#### question_prepared

Question added to a NegRisk market.

| Column | DuckDB type | Description |
|---|---|---|
| `market_id` | BLOB (32 bytes) | The parent NegRisk market |
| `question_id` | BLOB (32 bytes) | The question identifier |
| `index_val` | UINTEGER | Question index within the market |
| `data` | VARCHAR | Additional question data (hex string) |

#### outcome_reported

Outcome reported for a NegRisk question. In almost all cases a corresponding `condition_resolution` event fires on ConditionalTokens in the same transaction.

| Column | DuckDB type | Description |
|---|---|---|
| `market_id` | BLOB (32 bytes) | The NegRisk market |
| `question_id` | BLOB (32 bytes) | The question being reported on |
| `outcome` | UINTEGER | 1 = YES, 0 = NO |

To find the corresponding `condition_id`, join via `condition_preparation` on `question_id`.

#### position_split

NegRisk position split (via NegRiskAdapter, not ConditionalTokens directly).

| Column | DuckDB type | Description |
|---|---|---|
| `stakeholder` | BLOB (20 bytes) | The account splitting |
| `condition_id` | BLOB (32 bytes) | The condition being split on |
| `amount` | VARCHAR | uint256 decimal string — amount split |

#### positions_merge

NegRisk positions merged (via NegRiskAdapter).

| Column | DuckDB type | Description |
|---|---|---|
| `stakeholder` | BLOB (20 bytes) | The account merging |
| `condition_id` | BLOB (32 bytes) | The condition being merged on |
| `amount` | VARCHAR | uint256 decimal string — amount merged |

#### positions_converted

Positions converted between NegRisk formats.

| Column | DuckDB type | Description |
|---|---|---|
| `stakeholder` | BLOB (20 bytes) | The account converting |
| `market_id` | BLOB (32 bytes) | The NegRisk market |
| `index_set` | VARCHAR | uint256 decimal string — index set |
| `amount` | VARCHAR | uint256 decimal string — amount converted |

#### payout_redemption

NegRisk payout redeemed.

| Column | DuckDB type | Description |
|---|---|---|
| `redeemer` | BLOB (20 bytes) | The account redeeming |
| `condition_id` | BLOB (32 bytes) | The resolved condition |
| `amounts` | VARCHAR | JSON array of uint256 strings — amounts per outcome |
| `payout` | VARCHAR | uint256 decimal string — total collateral returned |

---

### UmaCtfAdapter

UMA oracle integration for market resolution.

#### question_initialized

Question submitted to the UMA oracle.

| Column | DuckDB type | Description |
|---|---|---|
| `question_id` | BLOB (32 bytes) | The question identifier |
| `request_timestamp` | UINTEGER | Unix timestamp of the UMA request |
| `creator` | BLOB (20 bytes) | Who created the question |
| `ancillary_data` | VARCHAR | Question text/metadata (hex string) |
| `reward_token` | BLOB (20 bytes) | The reward token address |
| `reward` | VARCHAR | uint256 decimal string — reward amount |
| `proposal_bond` | VARCHAR | uint256 decimal string — bond required to propose |

#### question_resolved

Question resolved by UMA.

| Column | DuckDB type | Description |
|---|---|---|
| `question_id` | BLOB (32 bytes) | The question identifier |
| `settled_price` | VARCHAR | int256 decimal string — settlement price |
| `payouts` | VARCHAR | JSON array of uint256 strings — payout per outcome |

#### question_reset

Question reset for re-resolution.

| Column | DuckDB type | Description |
|---|---|---|
| `question_id` | BLOB (32 bytes) | The question that was reset |

#### question_flagged

Question flagged for dispute.

| Column | DuckDB type | Description |
|---|---|---|
| `question_id` | BLOB (32 bytes) | The question that was flagged |

#### question_emergency_resolved

Emergency resolution by admin.

| Column | DuckDB type | Description |
|---|---|---|
| `question_id` | BLOB (32 bytes) | The question resolved |
| `payouts` | VARCHAR | JSON array of uint256 strings — payout per outcome |

---

## Collection guarantees

- **Source:** Polygon JSON-RPC `eth_getLogs` via any compliant provider
- **Deduplication:** `UNIQUE(transaction_hash, log_index)` constraint in SQLite
- **Completeness:** `completed_ranges` table in SQLite tracks scraped block ranges; gaps trigger re-scrape
- **Start block:** 33,605,403 (CTFExchange deployment). Fee module contracts deployed later at blocks 75,253,526 (FeeModuleCTF) and 75,253,721 (FeeModuleNegRisk).
- **Timezone:** all timestamps UTC (Unix epoch seconds)
- **Versioning:** schema changes bump to a new versioned folder

## Known limitations

- Large uint256 values stored as decimal strings (exceeds 64-bit integer range)
- JSON arrays (e.g., `payout_numerators`) are stored as JSON-encoded strings, not native arrays
- Some conditions prepared before block 33,605,403 exist on-chain but are not in this dataset — ~84 orphaned conditions are tolerated (see [test_token_registered_conditions_have_preparation](assertions/test_token_registered_conditions_have_preparation.py))
- ~54 fills on CTFExchange occur after condition resolution — an off-chain invariant the contracts do not enforce (see [test_no_trading_after_resolution](assertions/test_no_trading_after_resolution.py))
