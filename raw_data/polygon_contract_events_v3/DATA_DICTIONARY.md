# Data dictionary — polygon_contract_events_v3

Core on-chain events from Polymarket's contracts on Polygon, scraped via JSON-RPC `eth_getLogs` and exported to partitioned Parquet files.

**This document is the data contract.** It defines every on-disk Parquet schema for every event and other guarantees from the producer. These schemas and guarantees are what consumers depend on, so this is the source of truth.

Files are stored in `$POLYGON_CONTRACT_EVENTS_V3_DIR`. References below to filenames are relative to this path.

## File locations

- `{contract}/{event}/1M={N}/10K={N}/`
  - `data.parquet` — specified per contract/event below
  - `metadata.json` — see [Metadata files specification](../../docs/Metadata%20files.md)
- `manifests/1M={N}/10K={N}/`
  - `_SUCCESS` — empty file (i.e. zero file length)

## Parquet type contract

Every column in every schema uses one of three Parquet logical types — `"BLOB"` (none), `STRING`, or `INT(bitWidth=32, isSigned=false)`. No column in any schema contains any `NULL` value.

Equivalent DuckDB types are shown below as an implementation note, but these are not part of the contract.

| Parquet logical type | Parquet physical type | DuckDB type (implementation) | Used for |
|---|---|---|---|
| `"BLOB"` (20 bytes), `"BLOB"` (32 bytes) | `BYTE_ARRAY` | `BLOB` | Identifiers and addresses — raw bytes with no character-set interpretation. 32 bytes for hashes / token IDs / condition IDs / etc., 20 bytes for addresses. |
| `STRING` (UTF-8) | `BYTE_ARRAY` | `VARCHAR` | uint256 amounts (decimal strings), JSON arrays of uint256 decimal strings, and free-form text or hex fields. |
| `INT(bitWidth=32, isSigned=false)` | `INT32` | `UINTEGER` | Bounded small integers: `block_number`, `transaction_index`, `log_index`, `outcome_slot_count`, `fee_bips`, `index_val`, `request_timestamp`, `outcome`, `side`. |

Notes:

- **We introduce the term `"BLOB"` to represent the logical Parquet type for binary data stored with the `BYTE_ARRAY` physical type.** The Parquet specification does not include a keyword for this logical type and calls it an "unspecified type". DuckDB happens to call this type `BLOB`; we borrow the word but keep it in quotes to signal that Parquet does not use that word officially.
- **Our `"BLOB"` columns use variable length physical type (`BYTE_ARRAY`).** Each column is specified as exactly 20 or 32 bytes and we would prefer to use `FIXED_LEN_BYTE_ARRAY` length guarantees, but our writer implementation [does not support this](https://github.com/duckdb/duckdb/blob/2cda70b1eb522ea29831904613b29a7eda1f333e/extension/parquet/parquet_writer.cpp#L115-L116).
- **uint256 values are strings, not integers.** EVM uint256 exceeds the range of any native Parquet integer. Strings have capacity to store every value the chain emits.
- **Variable-length arrays are JSON strings.** Variable length JSON arrays must be valid JSON arrays, encoded without spaces. Each element MUST be the JSON-string representation of a non-negative uint256 written in base-10. Examples:  `[]` (empty array), `["1","2"]`. We also considered Parquet's `LIST` format but found this to be more useful.
- **Small bounded integers use unsigned `INT32`.** Block are created every two seconds, so block numbers will fit in unsigned `INT32` until year ~2300. We use unsigned `INT32` here and for some other values that have natural or technical guarantees to fit in this column type.

## Partitioning

- **Index column:** `block_number`
- **Scheme:** `1M={N}/10K={N}/data.parquet` — 1M-block directories containing 100 10K-block partitions
- **Example:** `CTFExchange/order_filled/1M=83000000/10K=83010000/data.parquet` covers blocks 83,010,000–83,019,999

## Physical sort order

Each partition is sorted internally by `(block_number, transaction_index, log_index)`.

## Uniqueness

Each tuple `(block_number, transaction_index, log_index)` is globally unique across the entire dataset.

Across the entire dataset, no two rows with the same `transaction_hash` shall have different `block_number`.

## File immutability and atomic visibility

**In plain words: the `manifests/**/_SUCCESS` files form a contiguous range starting at the launch of Polymarket up to the sunk "frontier", and the corresponding `{contract}/{event}/**/*` files form a complete, consistent and immutable dataset.**

Following is a formalization of those guarantees.

The producer makes exactly the following guarantees regarding file visibility and immutability. Such guarantees are absolute, even in the event of race conditions, power outages and errors writing to disk.

- `{contract}/{event}/1M={N}/10K={N}/`
  - Any folder existing with this path pattern shall contain exactly two files: `data.parquet` and `metadata.json`.
- `manifests/1M={N}/10K={N}/`
  - Any folder existing with this path pattern shall contain exactly one file: `_SUCCESS`. **The producer shall never modify, replace or delete this file.**
  - Its `1M={N}/10K={N}` path part shall be lexicographically greater than or equal to `1M=33000000/10K=33600000` (this is the partition containing `SCRAPE_START_BLOCK`).
  - For every pattern  `1M={N}/10K={N}` lexicographically between `1M=33000000/10K=33600000` and this folder (inclusive), a folder at that path `manifests/1M={N}/10K={N}/` shall also exist.
  - Let the "block lower bound" for this partition be the number `N` in  `10K={N}`.
  - For every (contract, event) [specified below](#events-by-contract) with deployment block less than or equal to the block lower bound, the folder `{contract}/{event}/1M={N}/10K={N}/` shall exist. **The producer shall never modify, replace or delete its two files.**

The folder existing with the lexicographically highest pattern `manifests/1M={N}/10K={N}/` is called the "frontier". For the avoidance of doubt, no guarantees are made regarding the existance, completeness or immutability of files in ``{contract}/{event}/1M={N}/10K={N}/`` folders which are lexicographically higher than that.

## Known limitations

- Some conditions prepared before block 33,605,403 exist on-chain but are not in this dataset — exactly 84 orphaned conditions are known and tolerated (see [test_token_registered_conditions_have_preparation](tests/data_validation/test_token_registered_conditions_have_preparation.py)).
- Some fills  on CTFExchange occur after condition resolution, this is an off-chain invariant the contracts do not enforce — exactly 54 such cases are known and tolerated (see [test_no_trading_after_resolution](tests/data_validation/test_no_trading_after_resolution.py)).
- **Not included in this dataset:** ERC-20 token transfers (USDC.e and other tokens), ERC-1155 outcome-token transfers from `ConditionalTokens` (`TransferSingle` / `TransferBatch`), proxy-wallet creation/management events, generic NegRisk wrapped-collateral transfers, and any events from contracts not listed in the [Contracts](#contracts) table. These flows are relevant to the Polymarket ecosystem but live outside the events scraped here.

## Schema common columns

All tables share these four columns, in this order, **before any event-specific columns**. The ordering is part of the contract — both the four common columns and the event-specific columns that follow appear in the order documented here.

| Column | Parquet logical type | Description | Guarantees |
|---|---|---|---|
| `block_number` | `INT(bitWidth=32, isSigned=false)` | Polygon block number | Strictly within partition bounds; fits in uint32 |
| `transaction_index` | `INT(bitWidth=32, isSigned=false)` | Position of the transaction within the block | Fits in uint32 |
| `transaction_hash` | `"BLOB"` (32 bytes) | Transaction hash | Exactly 32 bytes |
| `log_index` | `INT(bitWidth=32, isSigned=false)` | Position of the log within the block | Fits in uint32 |

The emitting contract address is **not** stored as a column — it is implicit from the directory path (e.g., `CTFExchange/order_filled/`).

The partition values (`1M=…`, `10K=…`) are encoded in the directory path. They **must not appear as columns inside the Parquet file** — they are Hive-style partition keys, not data.

## Contracts

Each contract has a `deployment_block`, specified below, and it has no events before that block.

| Contract | Address | Directory | Deployment block | Role |
|---|---|---|---|---|
| Gnosis ConditionalTokens | `0x4D97DCd97eC945f40cF65F87097ACe5EA0476045` | `ConditionalTokens/` | 33,605,403 | Outcome token framework — manages conditions, splits, merges, redemptions |
| CTFExchange | `0x4bFb41d5B3570DeFd03C39a9A4D8dE6Bd8B8982E` | `CTFExchange/` | 33,605,743 | Binary market orderbook exchange |
| NegRiskCtfExchange | `0xC5d563A36AE78145C45a50134d48A1215220f80a` | `NegRiskCtfExchange/` | 45,169,177 | Multi-outcome (NegRisk) market orderbook exchange |
| CTFExchangeV2 | `0xE111180000d2663C0091e4f400237545B87B996B` | `CTFExchangeV2/` | 84,902,353 | Next-generation binary market exchange |
| NegRiskCtfExchangeV2 | `0xe2222d279d744050d28e00520010520000310F59` | `NegRiskCtfExchangeV2/` | 85,058,176 | Next-generation NegRisk exchange |
| NegRiskAdapter | `0xd91E80cF2E7be2e162c6513ceD06f1dD0dA35296` | `NegRiskAdapter/` | 45,169,177 | Wraps CTF for NegRisk position actions |
| UmaCtfAdapter | `0x157Ce2d672854c848c9b79C49a8Cc6cc89176a49` | `UmaCtfAdapter/` | 33,605,574 | UMA oracle integration for market resolution |
| FeeModuleCTF | `0xE3f18aCc55091e2c48d883fc8C8413319d4Ab7b0` | `FeeModuleCTF/` | 75,253,526 | Fee refund module for CTFExchange |
| FeeModuleNegRisk | `0xB768891e3130F6dF18214Ac804d4DB76c2C37730` | `FeeModuleNegRisk/` | 75,253,721 | Fee refund module for NegRiskCtfExchange |

## Events by contract

Each contract/event is saved in Parquet files with the common columns and the below specified columns.

### ConditionalTokens

#### condition_preparation

Market condition created. One row per condition.

Solidity event: `ConditionPreparation(bytes32 indexed conditionId, address indexed oracle, bytes32 indexed questionId, uint256 outcomeSlotCount)`

| Column | Parquet logical type | Description |
|---|---|---|
| `condition_id` | `"BLOB"` (32 bytes) | The condition identifier |
| `oracle` | `"BLOB"` (20 bytes) | The oracle that can resolve this condition |
| `question_id` | `"BLOB"` (32 bytes) | The question identifier |
| `outcome_slot_count` | `INT(bitWidth=32, isSigned=false)` | Number of outcome slots — in `[1, 256]` (Gnosis CTF caps `outcomeSlotCount` at 256 in `prepareCondition`). Typically 2 for binary conditions; NegRisk markets use 2 as well, with multi-outcome questions decomposed across many conditions. |

Additional guarantee from the Solidity contract (`ConditionalTokens.prepareCondition`): a `condition_id` is unique across the entire dataset — a single `(contract, oracle, question_id, outcome_slot_count)` tuple can only successfully prepare once. So `condition_id` is unique within `ConditionalTokens/condition_preparation` and across all sources of `condition_id`.

#### condition_resolution

Condition resolved with payout distribution. Emitted for both binary CTF markets and NegRisk conditions (the NegRiskAdapter calls `reportPayouts` on ConditionalTokens).

Solidity event: `ConditionResolution(bytes32 indexed conditionId, address indexed oracle, bytes32 indexed questionId, uint256 outcomeSlotCount, uint256[] payoutNumerators)`

| Column | Parquet logical type | Description |
|---|---|---|
| `condition_id` | `"BLOB"` (32 bytes) | The condition identifier |
| `oracle` | `"BLOB"` (20 bytes) | The resolving oracle |
| `question_id` | `"BLOB"` (32 bytes) | The question identifier |
| `outcome_slot_count` | `INT(bitWidth=32, isSigned=false)` | Number of outcome slots — in `[1, 256]`, identical to the value emitted in the prior `condition_preparation` for this `condition_id` |
| `payout_numerators` | `STRING` | JSON array of uint256 decimal strings; `json_array_length(payout_numerators) == outcome_slot_count` per the Gnosis CTF `reportPayouts` contract |

`payout_denominator = sum(payout_numerators)` per Gnosis CTF semantics. For a clean win/loss this is `["1","0"]` or `["0","1"]` (denominator = 1). A `["1","1"]` resolution (denominator = 2) indicates a draw or voided market where each side receives half.

#### position_split

Collateral split into outcome tokens. Locks collateral and mints one token per outcome.

Solidity event: `PositionSplit(address indexed stakeholder, IERC20 collateralToken, bytes32 indexed parentCollectionId, bytes32 indexed conditionId, uint256[] partition, uint256 amount)`

| Column | Parquet logical type | Description |
|---|---|---|
| `stakeholder` | `"BLOB"` (20 bytes) | The account splitting |
| `collateral_token` | `"BLOB"` (20 bytes) | The collateral token address (USDC.e for the vast majority of Polymarket markets) |
| `parent_collection_id` | `"BLOB"` (32 bytes) | Parent collection (32 zero bytes for root splits) |
| `condition_id` | `"BLOB"` (32 bytes) | The condition being split on |
| `partition` | `STRING` | JSON array of uint256 decimal strings — the outcome index sets |
| `amount` | `STRING` | uint256 decimal string — collateral amount locked, in raw token units (e.g. 6-decimal USDC.e) |

#### positions_merge

Outcome tokens merged back into collateral. Inverse of `position_split` — burns equal quantities of all outcome tokens.

Solidity event: `PositionsMerge(address indexed stakeholder, IERC20 collateralToken, bytes32 indexed parentCollectionId, bytes32 indexed conditionId, uint256[] partition, uint256 amount)`

| Column | Parquet logical type | Description |
|---|---|---|
| `stakeholder` | `"BLOB"` (20 bytes) | The account merging |
| `collateral_token` | `"BLOB"` (20 bytes) | The collateral token address |
| `parent_collection_id` | `"BLOB"` (32 bytes) | Parent collection |
| `condition_id` | `"BLOB"` (32 bytes) | The condition being merged on |
| `partition` | `STRING` | JSON array of uint256 decimal strings |
| `amount` | `STRING` | uint256 decimal string — collateral amount returned, in raw token units |

#### payout_redemption

Winning outcome tokens redeemed for collateral after resolution.

Solidity event: `PayoutRedemption(address indexed redeemer, IERC20 indexed collateralToken, bytes32 indexed parentCollectionId, bytes32 conditionId, uint256[] indexSets, uint256 payout)`

| Column | Parquet logical type | Description |
|---|---|---|
| `redeemer` | `"BLOB"` (20 bytes) | The account redeeming |
| `collateral_token` | `"BLOB"` (20 bytes) | The collateral token address |
| `parent_collection_id` | `"BLOB"` (32 bytes) | Parent collection |
| `condition_id` | `"BLOB"` (32 bytes) | The resolved condition |
| `index_sets` | `STRING` | JSON array of uint256 decimal strings — outcome index sets being redeemed. May be the empty array `[]` (a no-op redeem call with no index sets); this is a valid value, not an error. |
| `payout` | `STRING` | uint256 decimal string — total collateral returned, in raw token units |

### CTFExchange / NegRiskCtfExchange

These two contracts emit identical event signatures. Data is stored in separate directories: `CTFExchange/` for binary markets, `NegRiskCtfExchange/` for NegRisk markets.

#### order_filled

Order fill (partial or complete). One row per maker order filled in a match.

Solidity event: `OrderFilled(bytes32 indexed orderHash, address indexed maker, address indexed taker, uint256 makerAssetId, uint256 takerAssetId, uint256 makerAmountFilled, uint256 takerAmountFilled, uint256 fee)`

| Column | Parquet logical type | Description |
|---|---|---|
| `order_hash` | `"BLOB"` (32 bytes) | The order being filled |
| `maker` | `"BLOB"` (20 bytes) | Maker wallet address |
| `taker` | `"BLOB"` (20 bytes) | Taker wallet address |
| `maker_asset_id` | `"BLOB"` (32 bytes) | Token ID of maker's asset; 32 zero bytes means the USDC side of the trade |
| `taker_asset_id` | `"BLOB"` (32 bytes) | Token ID of taker's asset; 32 zero bytes means the USDC side of the trade |
| `maker_amount_filled` | `STRING` | uint256 decimal string — amount of maker asset filled, in raw token units (USDC.e is 6-decimal, outcome tokens are unscaled per Gnosis CTF semantics) |
| `taker_amount_filled` | `STRING` | uint256 decimal string — amount of taker asset filled, in raw token units |
| `fee` | `STRING` | uint256 decimal string — gross fee debited on this fill, denominated in the same token as the side that pays it. Most of this is refunded; see `FeeModuleCTF` / `FeeModuleNegRisk` `fee_refunded` for the net fee. |

#### orders_matched

Summary of a taker order match. One row per `matchOrders` call, aggregating the taker's view of all constituent fills.

Solidity event: `OrdersMatched(bytes32 indexed takerOrderHash, address indexed takerOrderMaker, uint256 makerAssetId, uint256 takerAssetId, uint256 makerAmountFilled, uint256 takerAmountFilled)`

| Column | Parquet logical type | Description |
|---|---|---|
| `taker_order_hash` | `"BLOB"` (32 bytes) | The taker order being matched |
| `taker_order_maker` | `"BLOB"` (20 bytes) | The taker order's maker address |
| `maker_asset_id` | `"BLOB"` (32 bytes) | Token ID; 32 zero bytes for the USDC side |
| `taker_asset_id` | `"BLOB"` (32 bytes) | Token ID; 32 zero bytes for the USDC side |
| `maker_amount_filled` | `STRING` | uint256 decimal string — in raw token units |
| `taker_amount_filled` | `STRING` | uint256 decimal string — in raw token units |

#### fee_charged

Fee deducted from a trade (gross fee at the exchange, before any refund).

Solidity event: `FeeCharged(address indexed receiver, uint256 tokenId, uint256 amount)`

| Column | Parquet logical type | Description |
|---|---|---|
| `receiver` | `"BLOB"` (20 bytes) | The fee recipient address |
| `token_id` | `"BLOB"` (32 bytes) | The token the fee is denominated in; 32 zero bytes means USDC |
| `amount` | `STRING` | uint256 decimal string — fee amount in raw token units |

#### token_registered

Token pair registered for trading on the exchange.

Solidity event: `TokenRegistered(uint256 indexed token0, uint256 indexed token1, bytes32 conditionId)`

| Column | Parquet logical type | Description |
|---|---|---|
| `token0` | `"BLOB"` (32 bytes) | First outcome token ID |
| `token1` | `"BLOB"` (32 bytes) | Second outcome token ID |
| `condition_id` | `"BLOB"` (32 bytes) | The condition these tokens belong to |

Each registration call emits two rows: `(token0=A, token1=B)` and `(token0=B, token1=A)`.

#### order_cancelled

Order cancelled by maker.

Solidity event: `OrderCancelled(bytes32 indexed orderHash)`

| Column | Parquet logical type | Description |
|---|---|---|
| `order_hash` | `"BLOB"` (32 bytes) | The cancelled order identifier |

### CTFExchangeV2 / NegRiskCtfExchangeV2

Next-generation versions of the exchanges. The event schema differs from the v1 exchanges: fills carry a single `token_id` plus a `side` flag (instead of separate maker/taker asset IDs), there is no `token_id` on `fee_charged`, there are no `token_registered` or `order_cancelled` events, and orders can be pre-approved on-chain.

These contracts also do not have an associated FeeModule, so v2 fills have no corresponding `fee_refunded` rows. A consumer that wants to unify trade rows across v1 and v2 must compute the USDC side from `side` and the outcome side from `token_id`; `side=0` (BUY) means the taker is buying the outcome token (taker pays USDC, receives `token_id`), and `side=1` (SELL) means the taker is selling the outcome token.

#### order_filled

Solidity event: `OrderFilled(bytes32 indexed orderHash, address indexed maker, address indexed taker, uint8 side, uint256 tokenId, uint256 makerAmountFilled, uint256 takerAmountFilled, uint256 fee, bytes32 builder, bytes32 metadata)`

| Column | Parquet logical type | Description |
|---|---|---|
| `order_hash` | `"BLOB"` (32 bytes) | The order being filled |
| `maker` | `"BLOB"` (20 bytes) | Maker wallet address |
| `taker` | `"BLOB"` (20 bytes) | Taker wallet address |
| `side` | `INT(bitWidth=32, isSigned=false)` | 0 = BUY (taker buys outcome token with USDC), 1 = SELL (taker sells outcome token for USDC) |
| `token_id` | `"BLOB"` (32 bytes) | Outcome token ID; the USDC side is implicit from `side` |
| `maker_amount_filled` | `STRING` | uint256 decimal string — in raw token units |
| `taker_amount_filled` | `STRING` | uint256 decimal string — in raw token units |
| `fee` | `STRING` | uint256 decimal string — fee on this fill, in raw token units of the side that pays it |
| `builder` | `"BLOB"` (32 bytes) | Builder identifier (opaque) |
| `metadata` | `"BLOB"` (32 bytes) | Opaque metadata field |

#### orders_matched

Solidity event: `OrdersMatched(bytes32 indexed takerOrderHash, address indexed takerOrderMaker, uint8 side, uint256 tokenId, uint256 makerAmountFilled, uint256 takerAmountFilled)`

| Column | Parquet logical type | Description |
|---|---|---|
| `taker_order_hash` | `"BLOB"` (32 bytes) | The taker order being matched |
| `taker_order_maker` | `"BLOB"` (20 bytes) | The taker order's maker address |
| `side` | `INT(bitWidth=32, isSigned=false)` | 0 = BUY, 1 = SELL (same convention as `order_filled.side`) |
| `token_id` | `"BLOB"` (32 bytes) | Outcome token ID |
| `maker_amount_filled` | `STRING` | uint256 decimal string — in raw token units |
| `taker_amount_filled` | `STRING` | uint256 decimal string — in raw token units |

#### fee_charged

Solidity event: `FeeCharged(address indexed receiver, uint256 amount)`

| Column | Parquet logical type | Description |
|---|---|---|
| `receiver` | `"BLOB"` (20 bytes) | The fee recipient address |
| `amount` | `STRING` | uint256 decimal string — fee amount in raw token units of whichever side paid it |

#### order_preapproved

Solidity event: `OrderPreapproved(bytes32 indexed orderHash)`

| Column | Parquet logical type | Description |
|---|---|---|
| `order_hash` | `"BLOB"` (32 bytes) | The pre-approved order hash |

#### order_preapproval_invalidated

Solidity event: `OrderPreapprovalInvalidated(bytes32 indexed orderHash)`

| Column | Parquet logical type | Description |
|---|---|---|
| `order_hash` | `"BLOB"` (32 bytes) | The order whose pre-approval was invalidated |

### FeeModuleCTF / FeeModuleNegRisk

These contracts pair 1:1 with their corresponding v1 exchange (FeeModuleCTF ↔ CTFExchange, FeeModuleNegRisk ↔ NegRiskCtfExchange). The v2 exchanges do not have a FeeModule; fills on `CTFExchangeV2` / `NegRiskCtfExchangeV2` have no corresponding `fee_refunded` row.

#### fee_refunded

Fee refund emitted by the fee module after each v1 `OrderFilled`. The `fee` field in v1 `OrderFilled` is the gross fee; most of it is refunded back to the trader. The actual net fee retained by the protocol is `fee_charged`.

Solidity event: `FeeRefunded(bytes32 indexed orderHash, address indexed to, uint256 tokenId, uint256 refund, uint256 feeCharged)`

| Column | Parquet logical type | Description |
|---|---|---|
| `order_hash` | `"BLOB"` (32 bytes) | The order hash from the corresponding `OrderFilled` (1:1 per fill) |
| `receiver` | `"BLOB"` (20 bytes) | Trader receiving the refund (Solidity field: `to`) |
| `token_id` | `"BLOB"` (32 bytes) | Token the refund is denominated in; 32 zero bytes means USDC |
| `refund` | `STRING` | uint256 decimal string — amount refunded to the trader, in raw token units |
| `fee_charged` | `STRING` | uint256 decimal string — net fee retained by the protocol, in raw token units |

Invariant: `refund + fee_charged = order_filled.fee` for the corresponding v1 fill.

### NegRiskAdapter

#### market_prepared

Solidity event: `MarketPrepared(bytes32 indexed marketId, address indexed oracle, uint256 feeBips, bytes data)`

| Column | Parquet logical type | Description |
|---|---|---|
| `market_id` | `"BLOB"` (32 bytes) | The NegRisk market identifier |
| `oracle` | `"BLOB"` (20 bytes) | The oracle for this market |
| `fee_bips` | `INT(bitWidth=32, isSigned=false)` | Fee in basis points, in `[0, 10_000]` |
| `data` | `STRING` | Additional market data, hex-encoded (no `0x` prefix); may be the empty string when the on-chain `data` was empty |

#### question_prepared

Solidity event: `QuestionPrepared(bytes32 indexed marketId, bytes32 indexed questionId, uint256 index, bytes data)`

| Column | Parquet logical type | Description |
|---|---|---|
| `market_id` | `"BLOB"` (32 bytes) | The parent NegRisk market |
| `question_id` | `"BLOB"` (32 bytes) | The question identifier |
| `index_val` | `INT(bitWidth=32, isSigned=false)` | Question index within the market (fits in uint32) |
| `data` | `STRING` | Additional question data, hex-encoded (no `0x` prefix); may be the empty string |

#### outcome_reported

Outcome reported for a NegRisk question. In almost all cases a corresponding `ConditionalTokens/condition_resolution` event fires in the same transaction.

Solidity event: `OutcomeReported(bytes32 indexed marketId, bytes32 indexed questionId, bool outcome)`

| Column | Parquet logical type | Description |
|---|---|---|
| `market_id` | `"BLOB"` (32 bytes) | The NegRisk market |
| `question_id` | `"BLOB"` (32 bytes) | The question being reported on |
| `outcome` | `INT(bitWidth=32, isSigned=false)` | 1 = YES, 0 = NO (the Solidity `bool` is encoded as the uint8 value `0` or `1`) |

To find the corresponding `condition_id`, join via `ConditionalTokens/condition_preparation` on `question_id`.

#### position_split

NegRisk position split (via NegRiskAdapter, not ConditionalTokens directly).

Solidity event: `PositionSplit(address indexed stakeholder, bytes32 indexed conditionId, uint256 amount)`

| Column | Parquet logical type | Description |
|---|---|---|
| `stakeholder` | `"BLOB"` (20 bytes) | The account splitting |
| `condition_id` | `"BLOB"` (32 bytes) | The condition being split on |
| `amount` | `STRING` | uint256 decimal string — amount split, in raw token units |

#### positions_merge

Solidity event: `PositionsMerge(address indexed stakeholder, bytes32 indexed conditionId, uint256 amount)`

| Column | Parquet logical type | Description |
|---|---|---|
| `stakeholder` | `"BLOB"` (20 bytes) | The account merging |
| `condition_id` | `"BLOB"` (32 bytes) | The condition being merged on |
| `amount` | `STRING` | uint256 decimal string — amount merged, in raw token units |

#### positions_converted

Positions converted between NegRisk formats.

Solidity event: `PositionsConverted(address indexed stakeholder, bytes32 indexed marketId, uint256 indexSet, uint256 amount)`

| Column | Parquet logical type | Description |
|---|---|---|
| `stakeholder` | `"BLOB"` (20 bytes) | The account converting |
| `market_id` | `"BLOB"` (32 bytes) | The NegRisk market |
| `index_set` | `STRING` | uint256 decimal string — index set (a bitmask over the market's questions) |
| `amount` | `STRING` | uint256 decimal string — amount converted, in raw token units |

#### payout_redemption

Solidity event: `PayoutRedemption(address indexed redeemer, bytes32 indexed conditionId, uint256[] amounts, uint256 payout)`

| Column | Parquet logical type | Description |
|---|---|---|
| `redeemer` | `"BLOB"` (20 bytes) | The account redeeming |
| `condition_id` | `"BLOB"` (32 bytes) | The resolved condition |
| `amounts` | `STRING` | JSON array of uint256 decimal strings — amounts per outcome |
| `payout` | `STRING` | uint256 decimal string — total collateral returned, in raw token units |

### UmaCtfAdapter

UMA oracle integration for market resolution. `UmaCtfAdapter.question_id` is the same `bytes32` value as `ConditionalTokens/condition_preparation.question_id`: a consumer joins UMA events to CTF conditions on `question_id`, and from there to outcome tokens via `CTFExchange/token_registered.condition_id` (or `NegRiskCtfExchange/token_registered.condition_id`).

#### question_initialized

Solidity event: `QuestionInitialized(bytes32 indexed questionId, uint256 indexed requestTimestamp, address indexed creator, bytes ancillaryData, address rewardToken, uint256 reward, uint256 proposalBond)`

| Column | Parquet logical type | Description |
|---|---|---|
| `question_id` | `"BLOB"` (32 bytes) | The question identifier |
| `request_timestamp` | `INT(bitWidth=32, isSigned=false)` | Unix timestamp (UTC) of the UMA request, in seconds; fits in uint32 |
| `creator` | `"BLOB"` (20 bytes) | Who created the question |
| `ancillary_data` | `STRING` | Question text / metadata, hex-encoded (no `0x` prefix) |
| `reward_token` | `"BLOB"` (20 bytes) | The reward token address |
| `reward` | `STRING` | uint256 decimal string — reward amount, in raw token units of `reward_token` |
| `proposal_bond` | `STRING` | uint256 decimal string — bond required to propose, in raw token units of `reward_token` |

#### question_resolved

Solidity event: `QuestionResolved(bytes32 indexed questionId, int256 indexed settledPrice, uint256[] payouts)`

| Column | Parquet logical type | Description |
|---|---|---|
| `question_id` | `"BLOB"` (32 bytes) | The question identifier |
| `settled_price` | `STRING` | int256 decimal string — settlement price (may be negative; consumers must parse as signed) |
| `payouts` | `STRING` | JSON array of uint256 decimal strings — payout per outcome |

#### question_reset

Solidity event: `QuestionReset(bytes32 indexed questionId)`

| Column | Parquet logical type | Description |
|---|---|---|
| `question_id` | `"BLOB"` (32 bytes) | The question that was reset |

#### question_flagged

Solidity event: `QuestionFlagged(bytes32 indexed questionId)`

| Column | Parquet logical type | Description |
|---|---|---|
| `question_id` | `"BLOB"` (32 bytes) | The question that was flagged |

#### question_unflagged

Solidity event: `QuestionUnflagged(bytes32 indexed questionId)`

| Column | Parquet logical type | Description |
|---|---|---|
| `question_id` | `"BLOB"` (32 bytes) | The question that was unflagged |

#### question_paused

Solidity event: `QuestionPaused(bytes32 indexed questionId)`

| Column | Parquet logical type | Description |
|---|---|---|
| `question_id` | `"BLOB"` (32 bytes) | The question that was paused |

#### question_unpaused

Solidity event: `QuestionUnpaused(bytes32 indexed questionId)`

| Column | Parquet logical type | Description |
|---|---|---|
| `question_id` | `"BLOB"` (32 bytes) | The question that was unpaused |

#### question_emergency_resolved

Emergency resolution by admin.

Solidity event: `QuestionEmergencyResolved(bytes32 indexed questionId, uint256[] payouts)`

| Column | Parquet logical type | Description |
|---|---|---|
| `question_id` | `"BLOB"` (32 bytes) | The question resolved |
| `payouts` | `STRING` | JSON array of uint256 decimal strings — payout per outcome |

## Guarantees and validation

The `tests/data_validation/` folder contains pytest-style checks that run against the on-disk parquet. Each guarantee below links to its validation script and states scope so producer and consumer can agree on exactly what is and is not checked.

Most guarantees written before the v2 exchanges existed reference columns (`maker_asset_id`, `taker_asset_id`, v1 `fee_charged.token_id`) that the v2 exchanges do not emit; those guarantees are scoped to the v1 exchanges (`CTFExchange`, `NegRiskCtfExchange`) only.

### Row identity (every contract, every event)

- [test_transaction_log_index_unique_per_partition](tests/data_validation/test_transaction_log_index_unique_per_partition.py) — within every 10K partition, `(transaction_hash, log_index)` is unique across every event row from every contract.
- [test_block_transaction_log_index_unique_per_partition](tests/data_validation/test_block_transaction_log_index_unique_per_partition.py) — within every 10K partition, `(block_number, transaction_index, log_index)` is unique across every event row from every contract.
- [test_transaction_hash_maps_to_block_number_index_per_partition](tests/data_validation/test_transaction_hash_maps_to_block_number_index_per_partition.py) — within every 10K partition, every `transaction_hash` maps to a single `(block_number, transaction_index)` pair across every event row from every contract.
- [test_transaction_log_index_globally_unique](tests/data_validation/test_transaction_log_index_globally_unique.py) **(skipped)** — same invariant as the per-partition test above, evaluated as one dataset-wide GROUP BY. Skipped because the query needs tens of GB of memory and hours of CPU; the partition-scoped test plus the partition-bounds check imply it.
- [test_transaction_hash_maps_to_block_number_index](tests/data_validation/test_transaction_hash_maps_to_block_number_index.py) **(skipped)** — same invariant as the per-partition test above, evaluated as one dataset-wide GROUP BY. Skipped for the same reason.
- [test_no_null_values_anywhere](tests/data_validation/test_no_null_values_anywhere.py) — no column in any event-table Parquet file contains a NULL value. **Scope:** every column of every event table from every contract.
- [test_rows_sorted_within_each_file](tests/data_validation/test_rows_sorted_within_each_file.py) — every Parquet file is sorted internally by `(block_number, transaction_index, log_index)`. **Scope:** every event-table Parquet file from every contract.

### Schema and format

- [test_address_hash_format_valid](tests/data_validation/test_address_hash_format_valid.py) — every 20-byte address column is exactly 20 bytes and every 32-byte hash column is exactly 32 bytes. **Scope:** v1 exchange directories (`CTFExchange/{order_filled, orders_matched, fee_charged}`, `NegRiskCtfExchange/{order_filled, orders_matched, fee_charged}`) plus `*/token_registered` (every exchange that emits it). Does not cover v2 exchanges or non-exchange contracts; rewriting it to cover those is open work.
- [test_v2_exchange_tables](tests/data_validation/test_v2_exchange_tables.py) — for v2 exchanges, `order_filled` and `orders_matched` rows have `side ∈ {0, 1}` and `fee_charged` has no `token_id` column. **Scope:** v2 exchanges only (`CTFExchangeV2`, `NegRiskCtfExchangeV2`) by construction.
- [test_no_partition_key_columns_in_files](tests/data_validation/test_no_partition_key_columns_in_files.py) — no Parquet file contains a column named `1M` or `10K` (those values live in the directory path, not as data). **Scope:** every event-table Parquet file from every contract.
- [test_array_columns_well_formed](tests/data_validation/test_array_columns_well_formed.py) — every JSON-array column (`payout_numerators`, `partition`, `index_sets`, `amounts`, `payouts`) parses as a valid JSON array (the empty array `[]` is allowed; NULL is not), and additionally `json_array_length(condition_resolution.payout_numerators) == outcome_slot_count` per Gnosis CTF's `reportPayouts` contract. **Scope:** every directory that emits each column.
- [test_partition_paths_have_no_gaps](tests/data_validation/test_partition_paths_have_no_gaps.py) — for each `(contract, event)`, the 10K partitions on disk form a contiguous chain from `floor(deployment_block / 10_000) * 10_000` upward, with no missing 10K cells. **Scope:** every `(contract, event)` directory; filename-only check.
- [test_no_partition_before_contract_deployment](tests/data_validation/test_no_partition_before_contract_deployment.py) — no `(contract, event)` partition exists with an end block earlier than the contract's `deployment_block`. **Scope:** every `(contract, event)` directory; filename-only check.

### Numeric value sanity

- [test_amounts_are_non_negative_integers](tests/data_validation/test_amounts_are_non_negative_integers.py) — every amount column parses as a non-negative integer. **Scope:** every directory that emits the column — covers v1 and v2 exchanges' `order_filled` / `orders_matched`, every `*/fee_charged`, every `*/payout_redemption.payout`, every `*/position_split.amount`, every `*/positions_merge.amount`.
- [test_order_filled_amounts_positive](tests/data_validation/test_order_filled_amounts_positive.py) — `maker_amount_filled > 0` and `taker_amount_filled > 0` on every fill. **Scope:** every `*/order_filled` directory, including v2 exchanges.
- [test_order_filled_implied_price_in_range](tests/data_validation/test_order_filled_implied_price_in_range.py) — implied price > 0 (warns on price ≥ 1). **Scope:** v1 exchanges only (`CTFExchange/order_filled`, `NegRiskCtfExchange/order_filled`); depends on `maker_asset_id` / `taker_asset_id` to know which side is USDC.
- [test_condition_resolution_payout_sums](tests/data_validation/test_condition_resolution_payout_sums.py) — for binary conditions, `payout_numerators` sums to a sensible value. **Scope:** `ConditionalTokens/condition_resolution`. (Not exchange-specific.)
- [test_outcome_reported_value_is_bool](tests/data_validation/test_outcome_reported_value_is_bool.py) — `outcome_reported.outcome ∈ {0, 1}`. **Scope:** `NegRiskAdapter/outcome_reported` only.
- [test_outcome_slot_count_in_range](tests/data_validation/test_outcome_slot_count_in_range.py) — `outcome_slot_count ∈ [1, 256]` (Gnosis CTF caps `outcomeSlotCount` at 256 in `prepareCondition`). **Scope:** `ConditionalTokens/condition_preparation` and `ConditionalTokens/condition_resolution`.
- [test_fee_bips_in_range](tests/data_validation/test_fee_bips_in_range.py) — `fee_bips ∈ [0, 10_000]`. **Scope:** `NegRiskAdapter/market_prepared` only.
- [test_request_timestamp_range](tests/data_validation/test_request_timestamp_range.py) — `request_timestamp` falls in `[2020-01-01T00:00:00Z, 2030-01-01T00:00:00Z]` Unix seconds, a generous sanity range. **Scope:** `UmaCtfAdapter/question_initialized` only.

### Exchange / trade invariants

- [test_order_filled_one_side_is_collateral](tests/data_validation/test_order_filled_one_side_is_collateral.py) — exactly one of `maker_asset_id` / `taker_asset_id` is 32 zero bytes (USDC side). **Scope:** v1 exchanges only. The analogous v2 invariant ("`side ∈ {0, 1}`") is enforced by `test_v2_exchange_tables` above.
- [test_order_filled_taker_matches_orders_matched](tests/data_validation/test_order_filled_taker_matches_orders_matched.py) — for every `orders_matched` row the taker's `order_filled` row has identical amounts. **Scope:** v1 exchanges only.
- [test_orders_matched_bundle_consistency](tests/data_validation/test_orders_matched_bundle_consistency.py) — within every transaction, no maker `order_hash` is filled more than once. **Scope:** v1 exchanges only.
- [test_no_fill_after_cancel](tests/data_validation/test_no_fill_after_cancel.py) — no `order_filled` row has a `block_number` after its `order_cancelled` block. **Scope:** v1 exchanges only. The v2 exchanges have no `order_cancelled` event, so the v1 scope is the full scope today.
- [test_no_trading_after_resolution](tests/data_validation/test_no_trading_after_resolution.py) — no `order_filled` row uses an outcome token whose condition has already resolved. **Scope:** v1 exchanges only.

### Token registration and identity

`*/token_registered` is read via union across every exchange directory that emits it. Joins against `order_filled` / `orders_matched` are scoped to whichever exchanges the test reads on the trade side.

- [test_binary_conditions_have_two_tokens](tests/data_validation/test_binary_conditions_have_two_tokens.py) — every `token_registered` row has its symmetric counterpart `(A, B) ↔ (B, A)`. **Scope:** every exchange directory that emits `token_registered` (v1 only — `CTFExchange`, `NegRiskCtfExchange`).
- [test_token_registered_has_condition_id](tests/data_validation/test_token_registered_has_condition_id.py) — every `token_registered` row has a non-null, non-empty `condition_id`. **Scope:** every exchange that emits `token_registered` (v1 only).
- [test_token_belongs_to_one_condition](tests/data_validation/test_token_belongs_to_one_condition.py) — every outcome token belongs to exactly one `condition_id`. **Scope:** every exchange that emits `token_registered` (v1 only).
- [test_token_registered_before_first_trade](tests/data_validation/test_token_registered_before_first_trade.py) — every traded token was registered before its first fill. **Scope:** v1 exchanges only (joins `token_registered` against `CTFExchange/order_filled` + `NegRiskCtfExchange/order_filled`).
- [test_token_registered_conditions_have_preparation](tests/data_validation/test_token_registered_conditions_have_preparation.py) — every `condition_id` in `token_registered` exists in `condition_preparation`. **Scope:** every exchange that emits `token_registered` (v1 only) joined against `ConditionalTokens/condition_preparation`.
- [test_order_filled_tokens_are_registered](tests/data_validation/test_order_filled_tokens_are_registered.py) — every non-collateral token in `order_filled` exists in `token_registered`. **Scope:** v1 exchanges only.
- [test_orders_matched_tokens_are_registered](tests/data_validation/test_orders_matched_tokens_are_registered.py) — every non-collateral token in `orders_matched` exists in `token_registered`. **Scope:** v1 exchanges only.
- [test_fee_charged_tokens_are_registered](tests/data_validation/test_fee_charged_tokens_are_registered.py) — every non-zero token in `fee_charged` exists in `token_registered`. **Scope:** v1 exchanges only; v2 `fee_charged` has no `token_id` column, so the test is structurally inapplicable to v2.
- [test_ctf_negrisk_token_ids_disjoint](tests/data_validation/test_ctf_negrisk_token_ids_disjoint.py) — the set of token IDs traded on `CTFExchange` is disjoint from those traded on `NegRiskCtfExchange`. **Scope:** v1 exchanges only.
- **TODO** — extend `test_ctf_negrisk_token_ids_disjoint` to include `CTFExchangeV2` and `NegRiskCtfExchangeV2`: the four token-ID sets across `CTFExchange`, `NegRiskCtfExchange`, `CTFExchangeV2`, `NegRiskCtfExchangeV2` should be pairwise disjoint. **Scope:** all four exchanges. (Test file not yet written.)

### Condition lifecycle and resolution

Not exchange-specific.

- [test_condition_resolution_has_preparation](tests/data_validation/test_condition_resolution_has_preparation.py) — `condition_resolution` rows reference a known `condition_preparation`. **Scope:** `ConditionalTokens/{condition_resolution, condition_preparation}` (NegRisk conditions go through `ConditionalTokens` too via `NegRiskAdapter.reportPayouts`, so this covers both flavors).
- [test_payout_redemption_has_resolution](tests/data_validation/test_payout_redemption_has_resolution.py) — every `condition_id` in `payout_redemption` (within scrape window) has a prior `condition_resolution`. **Scope:** `ConditionalTokens/{payout_redemption, condition_resolution}`.

### NegRisk lifecycle (NegRiskAdapter only)

- [test_neg_risk_questions_have_market](tests/data_validation/test_neg_risk_questions_have_market.py) — every `market_id` in `question_prepared` exists in `market_prepared`. **Scope:** `NegRiskAdapter/{question_prepared, market_prepared}`.
- [test_outcome_reported_has_question](tests/data_validation/test_outcome_reported_has_question.py) — every `question_id` in `outcome_reported` exists in `question_prepared`. **Scope:** `NegRiskAdapter/{outcome_reported, question_prepared}`.
- [test_converted_market_has_questions_and_conditions](tests/data_validation/test_converted_market_has_questions_and_conditions.py) — every `market_id` in `positions_converted` exists in `question_prepared`, and every such question has a corresponding `condition_preparation`. **Scope:** `NegRiskAdapter/{positions_converted, question_prepared}` joined with `ConditionalTokens/condition_preparation`.

### Fee reconciliation

- [test_fee_reconciliation_per_tx](tests/data_validation/test_fee_reconciliation_per_tx.py) — `SUM(order_filled.fee) = SUM(fee_charged.amount)` per transaction. **Scope:** v1 exchanges only (`CTFExchange/{order_filled, fee_charged}`, `NegRiskCtfExchange/{order_filled, fee_charged}`); v2 `fee_charged` lacks `token_id` and has not been ported.
- [test_fee_refunded_known_transactions](tests/data_validation/test_fee_refunded_known_transactions.py) — two known transactions have the expected `fee_refunded` rows with `refund + fee_charged = gross fee` totals. **Scope:** `FeeModuleCTF/fee_refunded`, `FeeModuleNegRisk/fee_refunded`, `CTFExchange/order_filled`, `NegRiskCtfExchange/order_filled` (fee modules apply only to v1 fills).

## Versioning

This is `v3`. The producer shall not make a material breaking change to the schema or guarantees without incrementing the version.
