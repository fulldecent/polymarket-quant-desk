# token_and_usdc_flows_v2 data dictionary

Track every USDC and outcome-token movements across Polymarket V1 and V2 operations (trades, splits, merges, redeems, conversions). Each row represents a single USDC and/or token delta for one account from a single on-chain event. This table is the foundation for:

- Running-sum token holdings per account
- Realized PnL (sum of `net_usdc` per account/market)
- Trade volume and price analysis
- Unrealized value estimation (via current holdings × latest price)

## Scope

Every transfers from Polymarket V1 (CTFExchange, NegRiskCtfExchange, ConditionalTokens, NegRiskAdapter) AND Polymarket V2 (CTFExchangeV2, NegRiskCtfExchangeV2). Covers all blocks from the finalized partitions (up to "frontier") of the `polymarket_contract_events_v3` dataset.

Excluded scope is token transfers (even if involving the USDC and outcome tokens we care about) initiated outside of the Polymarket smart contracts (i.e. transfers initiated directly on the USDC and the Gnosis Conditional Token Framework smart contracts).

## Grain

One row per (`block_number`, `log_index`, `sub_index`). Each on-chain event may produce multiple rows:

- Trade fills: 2 rows (buyer + seller)
- CT splits/merges: 1 + len(partition) rows (1 USDC + N token rows)
- NR splits/merges: 3 rows (1 USDC + YES + NO)
- CT redeems: 1 + len(index_sets) rows
- NR redeems: 1 + count(non-zero amounts) rows
- Converts: 1 row per condition in the NegRisk market

## Schema

| Column | Parquet logical type | Nullable | Description |
|---|---|---|---|
| `block_number` | `INT(bitWidth=32, isSigned=false)` | no | Polygon block number (for 10K partitioning) |
| `transaction_index` | `INT(bitWidth=32, isSigned=false)` | no | Transaction position within the block |
| `transaction_hash` | `"BLOB"` (32 bytes) | no | Transaction hash as raw bytes |
| `log_index` | `INT(bitWidth=32, isSigned=false)` | no | Log position within the block |
| `sub_index` | `INT(bitWidth=32, isSigned=false)` | no | Row index within a single event: 0 = USDC/collateral flow or buyer; 1+ = per-token flows or seller |
| `raw_source` | `STRING` | no | Which raw event produced this row (e.g. `CTFExchange/order_filled`). Dictionary-encoded in Parquet for compression |
| `account` | `"BLOB"` (20 bytes) | no | Trader's Ethereum address (account) |
| `token_id` | `"BLOB"` (32 bytes) | yes | CTF ERC-1155 token ID; NULL on pure USDC rows (sub_index=0 for splits/merges/redeems) |
| `condition_id` | `"BLOB"` (32 bytes) | no | CTF condition ID (from raw event or via token_registered lookup) |
| `flow_type` | `STRING` | no | One of: `trade_buy`, `trade_sell`, `split`, `merge`, `redeem`, `convert`. Dictionary-encoded |
| `net_usdc` | `INT(bitWidth=64, isSigned=true)` | no | Signed micro-USDC change for this account. Positive = USDC received, negative = USDC spent. Clamped ±100M |
| `net_tokens` | `INT(bitWidth=64, isSigned=true)` | yes | Signed micro-token change. NULL means "unknown negative quantity burned" (see semantics below) |
| `price_1e18` | `INT(bitWidth=64, isSigned=false)` | yes | Implied price on trade rows: abs(net_usdc) × 10^18 / abs(net_tokens). NULL on non-trade rows |
| `collateral_token` | `"BLOB"` (20 bytes) | no | Usually USDC.e. From CT events or constant USDC.e on NR events |

Guarantees

- These first four columns faithfully match the upstream first four columns.

## Event sources

| Event | Contract | Produces rows | Deployed block |
|---|---|---|---|
| `order_filled` | `CTFExchange` | trade_buy, trade_sell | 33,605,743 |
| `order_filled` | `NegRiskCtfExchange` | trade_buy, trade_sell | 45,169,177 |
| `order_filled` | `CTFExchangeV2` | trade_buy, trade_sell | 84,902,353 |
| `order_filled` | `NegRiskCtfExchangeV2` | trade_buy, trade_sell | 85,058,176 |
| `position_split` | `ConditionalTokens` | split | 33,605,403 |
| `position_split` | `NegRiskAdapter` | split | 45,169,177 |
| `positions_merge` | `ConditionalTokens` | merge | 33,605,403 |
| `positions_merge` | `NegRiskAdapter` | merge | 45,169,177 |
| `payout_redemption` | `ConditionalTokens` | redeem | 33,605,403 |
| `payout_redemption` | `NegRiskAdapter` | redeem | 45,169,177 |
| `positions_converted` | `NegRiskAdapter` | convert | 45,169,177 |

## Lookup tables (used but not row contributors)

| Contract/event table | Purpose |
|---|---|
| `CTFExchange/token_registered` | Maps token_id → condition_id for CTF trade rows |
| `NegRiskCtfExchange/token_registered` | Maps token_id → condition_id for NegRisk trade rows |
| `ConditionalTokens/condition_preparation` | Provides outcome_slot_count per condition for token_id computation |
| `NegRiskAdapter/question_prepared` | Maps market_id → (question_id, index_val) for convert row generation |

## flow_type enum

| Value | Source event(s) | net_usdc | net_tokens | Notes |
|---|---|---|---|---|
| `trade_buy` | `{CTF,NegRisk}CTFExchange{,V2}/order_filled` | negative | positive | Sub_index=0. Account pays USDC, receives tokens |
| `trade_sell` | same | positive | negative | Sub_index=1. Account receives USDC (net of fee), gives tokens |
| `split` | `{ConditionalTokens,NegRiskAdapter}/position_split` | negative (row 0) or 0 (rows 1+) | 0 (row 0) or positive (rows 1+) | USDC locked, outcome tokens minted |
| `merge` | `{ConditionalTokens,NegRiskAdapter}/positions_merge` | positive (row 0) or 0 (rows 1+) | 0 (row 0) or negative (rows 1+) | Outcome tokens burned, USDC returned |
| `redeem` | `{ConditionalTokens,NegRiskAdapter}/payout_redemption` | positive (row 0) or 0 (rows 1+) | 0 (row 0) or negative/NULL (rows 1+) | Winning tokens redeemed for USDC after resolution |
| `convert` | `NegRiskAdapter/positions_converted` | 0 | signed | Pure token swap within a NegRisk market (NO→YES or YES→NO) |

## net_tokens NULL semantics

NULL in `net_tokens` has exactly one meaning: **"unknown negative (full position burned)."** This occurs on ConditionalTokens `payout_redemption` token rows where the exact burn quantity cannot be recovered from the event data. The smart contract burns the caller's entire ERC-1155 balance for that token ID.

Any running-balance computation that encounters a NULL must reset the position to 0 — not treat it as zero change. These rows are identifiable by:

```sql
flow_type = 'redeem' 
AND raw_source = 'ConditionalTokens/payout_redemption' 
AND net_tokens IS NULL 
AND sub_index > 0
```

Rows with no token movement use `net_tokens = 0`, never NULL.

## Trade row detail

**Exchange-as-intermediary filtering:** During `matchOrders`, the contract emits an extra `OrderFilled` event with `taker = address(this)` (the exchange contract itself). These rows are filtered out at SQL time because the real economic flows are already captured in the per-maker-order fills.

Filtered addresses (no real traders):

- `CTFExchange`: `0x4bFb41d5B3570DeFd03C39a9A4D8dE6Bd8B8982E`
- `NegRiskCtfExchange`: `0xC5d563A36AE78145C45a50134d48A1215220f80a`
- `CTFExchangeV2`: `0xE111180000d2663C0091e4f400237545B87B996B`
- `NegRiskCtfExchangeV2`: `0xe2222d279d744050d28e00520010520000310F59`

**Fee placement (V1, CTFExchange / NegRiskCtfExchange):**

- The fee is denominated in the **taker asset** — the asset the maker *receives*. BUY order → fee in outcome tokens; SELL order → fee in USDC. Order side is read from `maker_asset_id` (all-zero = BUY).
- `OrderFilled.maker_amount_filled` / `taker_amount_filled` are **gross**. The maker actually receives `taker_amount_filled - net_fee_charged` of the taker asset.
- `net_fee_charged` comes from `FeeModule` via a **deterministic join on `(transaction_hash, order_hash)`** — this pair is unique in both the `order_filled` and `fee_refunded` tables. Use `fee_refunded.fee_charged` when a row exists, otherwise fall back to `order_filled.fee` (no `FeeRefunded` is emitted when `refund = 0`).
- Never join on `order_hash` alone or by block/log proximity: a single `order_hash` is filled many times (partial fills) across different transactions and blocks, and each fill carries its own gross fee and refund.
- Airtight invariant per fill: `sum(net_usdc) + sum(net_tokens valued at price)` conserves value; the only permitted imbalance is `net_fee_charged`, which drains in the taker asset (tokens on BUY, USDC on SELL).

**Behavioral assumption — uniqueness of the fee join key (NOT enforced on-chain):**

- The `(transaction_hash, order_hash)` join above is correct only if, within a single transaction, an order is filled **at most once** and the `FeeModule` emits **at most one** `FeeRefunded` for it.
- This is **not guaranteed by the smart contracts.** The V1 `CTFExchange` allows an order to be filled incrementally across many transactions (partial fills); `_updateOrderStatus` only decrements the order's remaining amount and does not forbid two fills of the same `order_hash` inside one transaction. An operator using `exec_many` (several `matchOrders` / `fillOrder` calls batched into one transaction) could in principle place two fills of the same order in a single transaction.
- If that ever occurred, the join would become a cartesian product — wrong fee attribution and duplicate `(block_number, log_index, sub_index)` grain (silent corruption).
- The producer therefore **verifies this assumption on every partition and aborts (fail-fast) if it is violated**, rather than emitting corrupt data. See `_assert_unique_fills_per_tx` in main.py.

See README.md for detailed V1 vs V2 fee mechanics and worked examples.

## Live examples from block 82000130

All examples below are extracted from real on-chain data. Block 82000130 (a V1-era block) contains representative events for each source type.

### Example 1: CTFExchange/order_filled (V1 BUY trade, one fill of a partially-filled order)

**Raw upstream event** (`CTFExchange/order_filled`):

```
block_number=82000130, transaction_index=362, log_index=665
transaction_hash=0xDA83FB49079D7BB1D64EA0F5AD9643FAF6DBD331AD3DF96AB59BF6548BD1FB8E
order_hash=0x2677ED408E367691E8920F21947CA175DB755E935FD069903720898A958F9FAD
maker=0x330EFA1EAEBFC30B51686879A846885ED546BF6C
taker=0x8A2B9679724FB79EDD1D8E3AD7A311F370D4BBAC
maker_asset_id=0x0000000000000000000000000000000000000000000000000000000000000000 (USDC)
taker_asset_id=0x59938969E08F19893FBB1587C4DA073CA193232286896602713C413EF3E04E6F (outcome token)
maker_amount_filled=950000   (the maker GIVES this — µUSDC)
taker_amount_filled=5000000  (the maker RECEIVES this, GROSS before fee — µtoken)
fee=500000                   (gross fee, charged in the taker asset = µtoken here)
```

**Order side:** `maker_asset_id` is all-zero, so the maker pays USDC and receives outcome tokens. This is a **BUY order**.

**Where the fee lives (verified in `Trading.sol`):** The exchange charges the fee in the **taker asset** — the asset the maker receives. The `OrderFilled` amounts are gross; the maker actually receives `taking - fee` of the taker asset:

```solidity
fee = calculateFee(feeRateBps, side == BUY ? taking : making, ...);
_transfer(msg.sender, order.maker, takerAssetId, taking - fee); // maker gets taking - fee
emit OrderFilled(..., making, taking, fee);
```

- BUY order → taker asset is the **outcome token**, so the fee is denominated in **tokens**.
- SELL order → taker asset is **USDC**, so the fee is denominated in **USDC**.

So before any refund the maker here receives `5000000 - 500000 = 4500000` tokens, and the 500000-token fee goes to the operator.

**Net fee via the FeeModule — deterministic join on `(transaction_hash, order_hash)`:**

`FeeModule.FeeRefunded` is emitted in the **same transaction** as the fill, and only when `refund > 0` (`_refundFee` guards `if (refund > 0)`). The pair `(transaction_hash, order_hash)` is unique in both `CTFExchange/order_filled` and `FeeModuleCTF/fee_refunded`, so this is a clean 1-to-(0 or 1) join. Do **not** join on `order_hash` alone, and do **not** match by block/log proximity.

For this fill the matching refund row (same tx, `log_index=674`) is:

```
refund=500000, fee_charged=0   (full refund; operator kept nothing on this fill)
```

`net_fee_charged = fee_refunded.fee_charged` when a refund row exists, else `order_filled.fee`. Here `net_fee_charged = 0`, so the maker keeps the full 5000000 tokens.

**Output rows** (one `order_filled` → exactly 2 rows; the fee drains in the taker asset):

| sub_index | account | token_id | flow_type | net_usdc | net_tokens | price_1e18 |
|---|---|---|---|---|---|---|
| 0 | 0x330EFA1E...BF6C | 0x5993896...3E04E6F | trade_buy | -950000 | +5000000 (= 5000000 - net_fee_charged) | 950000 × 10^18 / 5000000 |
| 1 | 0x8A2B967...BBAC | 0x5993896...3E04E6F | trade_sell | +950000 | -5000000 | 950000 × 10^18 / 5000000 |

**Airtight check:** `sum(net_usdc) = 0` and `sum(net_tokens) = -net_fee_charged`. With `net_fee_charged = 0` here, both sums are 0 — no value created or destroyed. If the operator had kept a fee, the buyer's `net_tokens` would be `5000000 - net_fee_charged` and `sum(net_tokens)` would equal `-net_fee_charged` (the visible token drain to the operator). For a SELL order the drain appears in `net_usdc` instead.

**Why one order_hash maps to many refund rows (the partial-fill pattern):** This maker order (`0x2677ED40…`) was filled in **5 separate transactions** across blocks 82000129–82000130. Each fill is its own `OrderFilled` + `FeeRefunded` pair, joined within its own transaction:

| block | tx_index | of_log | maker_amount_filled | taker_amount_filled | raw fee | fr_log | refund | fee_charged |
|---|---|---|---|---|---|---|---|---|
| 82000129 | 119 | 1191 | 617500 | 3250000 | 325000 | 1202 | 325000 | 0 |
| 82000129 | 130 | 1386 | 1117200 | 5880000 | 588000 | 1395 | 588000 | 0 |
| 82000130 | 362 | 665 | 950000 | 5000000 | 500000 | 674 | 500000 | 0 |
| 82000130 | 367 | 731 | 58900 | 310000 | 31000 | 754 | 31000 | 0 |
| 82000130 | 370 | 825 | 1056400 | 5560000 | 556000 | 834 | 556000 | 0 |

Each `fr_log` immediately follows its `of_log` inside the same transaction. The five refund rows are **not** ambiguous candidates for this one fill — only the row in `transaction_hash=0xDA83FB49…` belongs to it. Encode each fill independently; never aggregate all refunds for an `order_hash`.

### Example 2: ConditionalTokens/position_split (splitting collateral into outcome tokens)

**Raw upstream event:**

```
block_number=82000130, transaction_index=X, log_index=Y
stakeholder=0xD91E80CF2E7BE2E162C6513CED06F1DD0DA35296
collateral_token=0x3A3BD7BB9528E159577F7C2E685CC81A765002E2
parent_collection_id=0x00...00
condition_id=0xF01D7E39FA70AF944D5A6B2375E99129B47E18204659BC957FAB01B2BD22BCF9
partition=[1, 2]  (binary outcome: YES=1, NO=2)
amount=100000000 µcollateral
```

**Output rows:**

| sub_index | account | token_id | condition_id | flow_type | net_usdc | net_tokens | collateral_token |
|---|---|---|---|---|---|---|---|
| 0 | 0xD91E80CF... | NULL | 0xF01D7E39... | split | -100000000 | 0 | 0x3A3BD7BB... |
| 1 | 0xD91E80CF... | token_id(1) | 0xF01D7E39... | split | 0 | +100000000 | 0x3A3BD7BB... |
| 2 | 0xD91E80CF... | token_id(2) | 0xF01D7E39... | split | 0 | +100000000 | 0x3A3BD7BB... |

Interpretation: 100M collateral locked (negative on row 0), 100M of each outcome token minted (positive on rows 1–2).

### Example 3: ConditionalTokens/payout_redemption (redeeming winning tokens post-resolution)

**Raw upstream event:**

```
block_number=82000130, transaction_index=X, log_index=Z
redeemer=0xB9EB10BA936F35D5BDCCA9B1CF477D6B152ABFED
condition_id=0xF01D7E39FA70AF944D5A6B2375E99129B47E18204659BC957FAB01B2BD22BCF9
index_sets=[1, 2]  (outcome 1 and outcome 2 token indices)
payout=60810214 µUSDC
```

**Output rows:**

| sub_index | account | token_id | flow_type | net_usdc | net_tokens |
|---|---|---|---|---|---|
| 0 | 0xB9EB10BA... | NULL | redeem | +60810214 | 0 |
| 1 | 0xB9EB10BA... | token_id(1) | redeem | 0 | NULL |
| 2 | 0xB9EB10BA... | token_id(2) | redeem | 0 | NULL |

The `net_tokens=NULL` indicates the smart contract burned the caller's entire ERC-1155 balance for each token ID. Downstream consumers must interpret NULL as "position reset to 0" rather than "no change."

### Example 4: NegRiskAdapter/position_split (splitting collateral into YES+NO pair)

**Raw upstream event:**

```
block_number=82000130, transaction_index=X, log_index=Y
stakeholder=0xC5D563A36AE78145C45A50134D48A1215220F80A
condition_id=0xF01D7E39FA70AF944D5A6B2375E99129B47E18204659BC957FAB01B2BD22BCF9
amount=100000000 µcollateral
```

**Output rows:**

| sub_index | account | token_id | condition_id | flow_type | net_usdc | net_tokens | collateral_token |
|---|---|---|---|---|---|---|---|
| 0 | 0xC5D563A3... | NULL | 0xF01D7E39... | split | -100000000 | 0 | 0x2791BCA1... (USDC.e) |
| 1 | 0xC5D563A3... | token_id(YES=1) | 0xF01D7E39... | split | 0 | +100000000 | 0x2791BCA1... |
| 2 | 0xC5D563A3... | token_id(NO=2) | 0xF01D7E39... | split | 0 | +100000000 | 0x2791BCA1... |

Identical structure to CT split, but collateral_token is always USDC.e (not a custom wrapped token from the upstream event).

### Example 5: NegRiskCtfExchange/order_filled (V1 NegRisk trade)

**Raw upstream event:**

```
block_number=82000130, transaction_index=361, log_index=649
maker=0xFA854FCB19F0CEF961992AE8AE267CBBCD1AFE60
taker=0x204F72F35326DB932158CBA6ADFF0B9A1DA95E14
maker_amount_filled=5000000
taker_amount_filled=3000000
fee=300000
[...similar to CTFExchange, with fee module handling...]
```

**Output rows:**
Same structure as CTFExchange trade_buy and trade_sell, with fees applied per the upstream FeeModule.FeeRefunded lookup.

## Partitioning and frontier

- **File path**: `{TOKEN_AND_USDC_FLOWS_V2_DIR}/1M={N}/10K={K}/data.parquet` (+ `metadata.json`)
- **Partition scheme**: 1M-block directories containing 100 10K-block partitions
- **Block range per partition**: `10K={K}` covers blocks `[K, K+9999]` inclusive
- **Sort order within partition**: `(block_number, log_index, sub_index)`

**Frontier immutability:** Partitions are produced strictly in order from genesis up to the **sunk frontier** reported by the upstream `polygon_contract_events_v3` producer. The frontier is determined by the highest block number with a manifest `_SUCCESS` file. **No guarantees exist for partitions beyond the frontier** — they may be incomplete, mutated, or deleted.

**Atomic writes:** Both the data file and metadata sidecar are written atomically into a temp-named folder, then the folder is renamed to the final `10K={K}` name. Crash recovery on startup deletes any incomplete temp-named folders.

## Invariants

1. **Unique grain**: Each tuple `(block_number, log_index, sub_index)` appears exactly once.
2. **Unique per transaction**: Each tuple `(transaction_hash, log_index, sub_index)` appears exactly once.
3. **net_usdc bounds**: Every `net_usdc` is between ±100,000,000,000,000 (±100M USDC in micro-USDC). This threshold was set to accommodate high-volume redemptions and merges observed on-chain.
4. **Byte lengths**: Every BLOB column is exactly the specified byte count (20 or 32).
5. **Not NULL except where allowed**: BLOB columns are NOT NULL except `token_id` and `net_tokens`.

## Known limitations and tolerated anomalies

- 84 conditions prepared before the deployment block (33,605,403) exist on-chain but are missing from the dataset.
- 54 order fills on CTFExchange occur after the corresponding condition resolution — an off-chain invariant not enforced by contracts.
- NegRiskAdapter operations that trigger underlying ConditionalTokens events produce separate rows with different collateral_tokens (WrappedCollateral for CT vs USDC.e for NR). Downstream consumers must account for this when aggregating across sources.

## Metadata schema

Each partition's `metadata.json` follows the specification in `docs/Metadata files.md`.

## Reproducibility

Fully deterministic data given identical source data. Metadata embeds `git_commit` and the producer rejects startup if the working tree is dirty (unless `--run-dirty` flag is set).

## Contract accounts to exclude

Three Polymarket smart contracts appear as `account` in split/merge/redeem rows (not real traders):

| Account (BLOB 20 bytes) | Contract | Row count estimate |
|---|---|---|
| `0xD91E80CF2E7BE2E162C6513CED06F1DD0DA35296` | NegRiskAdapter | ~144M rows |
| `0xC5D563A36AE78145C45A50134D48A1215220F80A` | NegRiskCtfExchange | ~102M rows |
| `0x4BFB41D5B3570DEFD03C39A9A4D8DE6BD8B8982E` | CTFExchange | ~87M rows |

Filter these out of any trader-level analysis (PnL rollups, leaderboards, signals, etc.).
