## Order types

| Order Type | Full Name          | Type   | Key Parameters                                               | Behavior                                                     | Typical Use Case                    |
| ---------- | ------------------ | ------ | ------------------------------------------------------------ | ------------------------------------------------------------ | ----------------------------------- |
| **GTC**    | Good-Til-Cancelled | Limit  | `tokenID`, `price`, `size`, `side`                           | Rests on the book until filled or manually cancelled         | Default passive limit orders        |
| **GTD**    | Good-Til-Date      | Limit  | `tokenID`, `price`, `size`, `side`, **`expiration`** (Unix timestamp in seconds) | Rests on the book until filled, cancelled, **or reaches expiration time** | Orders you want to auto-expire      |
| **FOK**    | Fill-Or-Kill       | Market | `tokenID`, `side`, `amount`, `price` (worst-price protection) | Must fill **entirely and immediately** or the **whole order is cancelled**. No partial fills. | All-or-nothing aggressive execution |
| **FAK**    | Fill-And-Kill      | Market | `tokenID`, `side`, `amount`, `price` (worst-price protection) | Fills as much as possible immediately, then cancels the remainder. Partial fills allowed. | Immediate-or-Cancel style execution |

Notes:

- When you sign an FAK or FOK order you are relying on the good graces of Polymarket to NOT execute that order later. If they DID sit on that order and execute it later, the smart contract would still accept it.

Unanswered questions (we could answer these with analysis of the source code or make a decent guess by testing data assertions):

- Can a FAK/FOK order cross another FAK/FOK order?

- the user interface on the website shows that you can spend a specific amount of money to buy tokens. But the order that actually executes as a worst price versus a specified number of tokens. **When you place a market buy order for $25, the actual amount you spend can (and often will) be less than $25.** you will only spend exactly $25 if the counterparty has that exact price too based on the price you picked when you signed the order. you will spend less than $25 if the price is different or does not match a diophantine equation based on the tick size.

**Here is the precise, mathematically accurate explanation:**

**When you enter “Buy $25 of YES” on the Polymarket website:**

1. The website immediately calculates the **largest integer number of tokens** *N* such that buying *N* tokens at the current best ask price (or your chosen worst acceptable price) would cost **≤ $25**.

2. You are then asked to **sign an order** for **exactly *N* tokens** (an integer quantity) at a specified worst acceptable price.

3. Once signed and submitted, the order executes as a market order (FAK/FOK).

**What you actually pay:**

You will spend **$25 or less** — specifically, *N* × (the actual average fill price per token).

- You will spend **exactly $25** only in the rare case where the final fill price results in a product that is mathematically equal to exactly 25.0000000000000 dollars (i.e., the integer quantity *N* and the executed price align perfectly with the tick size).
- In the vast majority of cases, you will spend **strictly less than $25**, because either:
  - Not all *N* tokens can be filled at acceptable prices, or
  - The executed price × *N* does not land exactly on 25.0000000000000 due to the discrete tick size and integer token quantities.

You will **never** spend more than the $25 you entered.

## YES/NO cross orders

There is a special case for cross orders between YES and NO tokens of the same condition. And this violates a very reasonable assumption about hov limit orders work.

Assumption (wrong): a limit order for asset X at price P will fill before a different limit order for the same asset X at a worse price P.

Reality: a cross order between YES and NO tokens of the same condition can fill at the same price as an existing limit order, even if that price is worse than the existing order's price. Because cross orders only fill exactly if they add up exactly to $1.

## Minimum order size

The CLOB enforces a per-market minimum order size. The order book response includes a `min_order_size` field that specifies the minimum number of shares required for any order on that market. Orders below this threshold are rejected with the error code `INVALID_ORDER_MIN_SIZE`.

As of March 2026, all observed markets have `min_order_size` of 5 shares. This is not a global constant — it is returned per-market by the order book endpoint and could vary.

This affects positions acquired in small amounts (e.g. several $1.30 trades that each yield ~3 shares). Even though the position value may exceed $1, you cannot place a limit sell if the share count is below the minimum.

Sources:

- Order book response schema: <https://docs.polymarket.com/trading/orderbook>
- Error codes: <https://docs.polymarket.com/trading/orders/overview>

## Sell-side rounding and dust

The `py_clob_client` order builder applies `round_down(amount, size_decimals)` to the amount field of every order. For 0.01-tick markets, `size_decimals = 2`. This creates an asymmetry between buying and selling:

| Step | Field being rounded | Decimal places | Effect |
|---|---|---|---|
| Buy | USDC amount | 2 | $1.30 stays $1.30 — no loss |
| Buy (result) | Shares received | 4 | e.g. 2.8888 shares from $1.30 / $0.45 |
| Sell | Share amount | 2 | 2.8888 truncated to 2.88 — 0.0088 shares left behind |

The leftover shares are permanently stuck: they're too small to sell (round to 0.00 shares) and too small to be worth the gas to merge/redeem.

Worst-case dust per trade: up to 0.0099 shares. At a price of $0.50, that's ~$0.005 per trade. The dust fraction is inversely proportional to order size:

| Order size | Worst-case dust | Dust as % of order |
|---|---|---|
| $1.30 | ~$0.005 | ~0.38% |
| $13.00 | ~$0.005 | ~0.038% |
| $130.00 | ~$0.005 | ~0.0038% |

The rounding config comes from `ROUNDING_CONFIG` in `py_clob_client.order_builder.builder`:

```
tick_size  price_decimals  size_decimals  amount_decimals
0.1        1               2              3
0.01       2               2              4
0.001      3               2              5
0.0001     4               2              6
```

`size_decimals` is always 2 regardless of tick size, so the dust issue affects all markets equally.

## On-chain settlement architecture

### Roles

| Role | Who | How identified |
|---|---|---|
| **Operator** | The relayer that submits transactions on-chain. Registered via `addOperator()` by the exchange admin. Collects all fees. | `msg.sender` of `matchOrders()` / `fillOrder()`. Must pass `onlyOperator` modifier. Not recorded in OrderFilled events. |
| **Taker** | The person whose signed order is being actively matched. In `matchOrders()`, this is `takerOrder.maker`. | In maker OrderFilled events: `taker` field = `takerOrder.maker`. In the taker's own OrderFilled: `taker` field = `address(this)` (exchange contract). |
| **Maker** | A person whose resting limit order is being filled. Each maker gets their own OrderFilled event. | `maker` field in OrderFilled. The account that signed the order. |

The operator is not the taker. The operator is a privileged relayer that submits the on-chain transaction; the taker is the person whose market/aggressive order is being matched against resting maker orders.

### `matchOrders()` execution flow

One `matchOrders()` call = one on-chain transaction. The operator submits:

- One taker order (the aggressive order)
- N maker orders (resting limit orders being consumed)
- Fill amounts for each

Execution steps:

1. Taker's `makerAsset` (what the taker is giving up) is transferred from taker to exchange contract.
2. For each maker order `i`:
   - Maker sends their `makerAsset` to exchange.
   - Exchange may mint/merge if needed (BUY-vs-BUY or SELL-vs-SELL match types).
   - Exchange sends `takerAmountFilled_i - fee_i` to maker (maker receives net of their own fee).
   - Fee_i is transferred from exchange to operator.
   - **OrderFilled emitted:** `maker = makerOrder[i].maker`, `taker = takerOrder.maker`, amounts = maker's fill.
3. After all maker fills:
   - Exchange sends accumulated `takerAsset` to taker, minus taker's fee.
   - Taker's fee is transferred from exchange to operator.
   - **OrderFilled emitted:** `maker = takerOrder.maker`, `taker = address(this)` (exchange contract), amounts = taker's total fill.
   - **OrdersMatched emitted** (summary event, not used in our data pipeline).
4. Any leftover `makerAsset` is refunded to the taker.

### OrderFilled event fields

```
event OrderFilled(
    bytes32 indexed orderHash,   // hash of the filled order
    address indexed maker,       // the order's signer
    address indexed taker,       // counterparty (or exchange contract for taker's own event)
    uint256 makerAssetId,        // what maker sends (0 = collateral/USDC)
    uint256 takerAssetId,        // what maker receives (0 = collateral/USDC)
    uint256 makerAmountFilled,   // amount maker SENDS (pre-fee, fee is never on the sending side)
    uint256 takerAmountFilled,   // amount maker would RECEIVE, BEFORE fee deduction
    uint256 fee                  // deducted from takerAmountFilled; maker actually receives (takerAmountFilled - fee)
)
```

Key facts:

- `makerAmountFilled` is the exact amount the order's maker **sends**. No fee is applied to the sending side.
- `takerAmountFilled` is the amount the order's maker would receive **before** fee deduction. The maker actually receives `takerAmountFilled - feeCharged` (not `takerAmountFilled - fee`).
- `fee` is the **gross fee** initially collected — the actual net fee is `feeCharged` from the subsequent `FeeRefunded` event emitted by the fee module contract. See "Fee refund mechanism" below.
- `fee` is always denominated in `takerAssetId` (the asset the maker receives).
- For a BUY order: `makerAssetId = 0` (USDC), `takerAssetId = tokenId` (tokens), fee is in tokens.
- For a SELL order: `makerAssetId = tokenId` (tokens), `takerAssetId = 0` (USDC), fee is in USDC.

### Which OrderFilled events appear per `matchOrders()` call

A `matchOrders()` with 1 taker and N makers emits **N+1** OrderFilled events:

- N events for the maker orders (one per maker). `taker` field = `takerOrder.maker`.
- 1 event for the taker order. `taker` field = `address(this)` (exchange contract).

Our `token_and_usdc_flows` materializer **filters out** events where maker or taker is the exchange contract (`WHERE of.maker != {exch} AND of.taker != {exch}`). This means **only the N maker events are materialized**. The taker's summary OrderFilled is excluded.

This has an important consequence: each maker fill creates two rows (buyer + seller). The "seller" row attributed to the taker in a maker's event does **not** have the taker's own fee deducted — the taker's fee was in their own (excluded) OrderFilled event. So:

- The **maker's row** correctly reflects their net position (fee deducted from what they receive).
- The **taker's counterparty row** shows gross amounts (no taker fee deducted).

For the taker, summing all their counterparty rows across the N maker events gives their total gross position; the taker's fee is not visible in any materialized row. This is a known limitation.

### Fee calculation

```solidity
fee = feeRateBps × min(price, 1 - price) × outcomeTokens / (BPS_DIVISOR × ...)
```

- `feeRateBps` is set per order by the signer (capped at `getMaxFeeRate()`).
- `min(price, 1 - price)` makes fees proportional to the lopsidedness: near 50¢ the fee is highest; near 1¢ or 99¢ the fee approaches zero.
- For BUY orders: fee is in tokens, computed as `(feeRate × min(price, 1-price) × tokens) / price`.
- For SELL orders: fee is in USDC, computed as `(feeRate × min(price, 1-price) × tokens)`.
- Many markets have zero fees — see "which markets charge fees" below.

### Fee refund mechanism (gross fee vs. actual fee)

The `fee` field in `OrderFilled` is a **gross fee** — the maximum amount initially collected from the trader. A separate fee module contract then refunds most of it back, keeping only the actual net fee.

The fee modules are:

| Contract | Address | Used by |
|---|---|---|
| FeeModule | `0xE3f18aCc55091e2c48d883fc8C8413319d4Ab7b0` | CTFExchange |
| NegRiskFeeModule | `0xB768891e3130F6dF18214Ac804d4DB76c2C37730` | NegRiskCtfExchange |

After each `OrderFilled`, the fee module emits:

```
event FeeRefunded(
    bytes32 indexed orderHash,   // matches OrderFilled.orderHash (1:1 join key)
    address indexed to,          // trader receiving the refund
    uint256 id,                  // token ID (or 0 for USDC)
    uint256 refund,              // amount returned to the trader
    uint256 indexed feeCharged   // actual net fee retained by the protocol
)
```

The invariant is: `refund + feeCharged = OrderFilled.fee`

In practice:

| Party | Typical gross fee (from OrderFilled) | Typical refund | Typical actual fee (feeCharged) |
|---|---|---|---|
| Maker | ~0.87% of fill (at max rate) | 100% | **0** (makers pay nothing) |
| Taker | ~10% of fill (at max rate) | ~98.6% | **~1.35% of fill** |

The gross fee overstates the actual cost by ~7.5x for takers and ∞ for makers. Any downstream calculation that uses `OrderFilled.fee` directly will be wrong.

**Which markets charge fees:** fees are not universal. Polymarket only charges taker fees on **crypto price markets** (Bitcoin/Ethereum/Solana up-or-down) and **certain sporting events**. Most markets (politics, geopolitics, entertainment, general prediction) have `OrderFilled.fee = 0` and emit no `FeeRefunded` event. This is a per-condition binary property — every fill within a condition is either always fee-charging or always fee-free.

How to detect fees:

- **Production (before trading):** call `GET {CLOB_API_URL}/fee-rate?token_id={clob_token_id}`. Returns `{"base_fee": 135}` for fee markets or `{"base_fee": 0}` for free markets. The `py_clob_client` does this automatically via `client.get_fee_rate_bps(token_id)`. Alternatively, the gamma API markets endpoint has `feesEnabled` (boolean) and `feeType` fields.
- **Backtesting (from on-chain data):** check `OrderFilled.fee` for any fill in the condition. If 0, the entire condition is fee-free. See [analysis/fees/analyze_fees.py](../../analysis/fees/analyze_fees.py) for empirical verification.

The fee rate lives in the signed `Order.feeRateBps` field. The CLOB API tells the client what value to sign. The exchange caps it at 1000 bps (10%), but the FeeModule refunds down to ~135 bps for takers and 0 for makers.

**Verified test cases:**

1. TX `0x86F87CB694B82A63CDB70F604C9923E12210BBC8F7E36BA2118751B0D36D6115` (block 83851893, NegRiskCTFExchange) — BTC < $60K on March 13 market, MINT match:
   - Maker `DE17F7`: gross fee 3,478,260 NO tokens → refund 3,478,260, feeCharged = **0**
   - Taker `6DDC4B`: gross fee 40,000,000 YES tokens → refund 39,458,310, feeCharged = **541,690**

2. TX `0x7C2960BDFFF1CA09D4E2FE37B58BEBC18BBEA0FC95D2A2D0B97C4DF32EFE7D1B` (block 83754485, CTFExchange) — Colombian elections, 5 makers + 1 taker:
   - All 5 makers: feeCharged = **0** (100% refunded)
   - Taker: gross fee 900,000 → refund 898,100, feeCharged = **1,900**

### How our flows materializer decomposes each OrderFilled

Each (non-exchange) OrderFilled event produces exactly two rows in `token_and_usdc_flows`:

**Row 0 (trade_buy):** the buyer's perspective.

| Field | If maker_asset_id = 0 (maker bought) | If maker_asset_id ≠ 0 (taker bought) |
|---|---|---|
| account | maker | taker |
| net_usdc | -maker_amount_filled | -taker_amount_filled |
| net_tokens | +(taker_amount_filled - fee) | +maker_amount_filled |

**Row 1 (trade_sell):** the seller's perspective.

| Field | If maker_asset_id = 0 (taker sold) | If maker_asset_id ≠ 0 (maker sold) |
|---|---|---|
| account | taker | maker |
| net_usdc | +maker_amount_filled | +(taker_amount_filled - fee) |
| net_tokens | -taker_amount_filled | -maker_amount_filled |

The fee is always deducted from the `takerAmountFilled` side. In row 0, this reduces the buyer's `net_tokens` (if maker bought) or the seller's `net_usdc` (if maker sold). The fee never appears as its own row — it disappears into the gap between what one side sends and the other receives.

### Inferring book state from the last transaction in a block

Assuming the operator acts honestly and fills orders at the best available prices:

1. Each `matchOrders()` call consumes the best available liquidity on the side being filled.
2. The worst maker price in the transaction is the marginal clearing price.
3. After the last transaction in a block, there is no better liquidity remaining than the worst maker from that transaction.

For a taker selling (consuming bids): the worst maker = the lowest buy price in the transaction. All remaining bids are at or below this price.

For a taker buying (consuming asks): the worst maker = the highest sell price in the transaction. All remaining asks are at or above this price.

In practice, most multi-maker transactions consume a single price level (all makers at the same price), so the worst and best maker prices are equal. When they span multiple levels, the worst maker is the meaningful one for inferring remaining liquidity.

## Order-to-settlement timing (Polynode)

When using Polynode as the RPC provider, you can observe confirmed transactions in the mempool before they are included in a block. This creates a predictable pipeline for reactive trading:

1. **Block B settles** — Polynode surfaces the confirmed fills from block B in the mempool.
2. **We observe and react** — our bot sees those fills and submits a market order (FOK/FAK) to the CLOB API.
3. **Block B+1 settles** — our order is very often included in the next block.

The result is a reliable one-block latency from observation to settlement. Polygon produces blocks roughly every 2 seconds, so the end-to-end time from seeing a fill to having our own fill on-chain is typically ~2 seconds.

### When B+1 is not achieved

The B+1 model is a best case that holds most of the time, but is not guaranteed. Reasons our order may land in B+2 or later:

- CLOB API latency (order signing, submission, matching) exceeds the remaining time in block B+1.
- The operator batches our fill into a later `matchOrders()` call.
- Network congestion or Polynode propagation delays.

For backtesting purposes, assuming B+1 settlement is reasonable for Polynode-connected strategies. For non-Polynode RPC providers, block observation is delayed and the latency model is less predictable.

### Cancellation timing

Cancellation follows the same pipeline but in reverse. If we observe an adverse fill in block B and immediately cancel our resting limit order via the CLOB API, the cancellation takes effect off-chain — the CLOB simply stops matching against our order. Since limit orders are matched off-chain before being submitted on-chain, a cancel request that reaches the CLOB before the next `matchOrders()` batch effectively prevents our order from being included. The on-chain settlement of someone else's fill in block B does not affect our ability to cancel, because our order was never on-chain to begin with — it only existed on the CLOB's off-chain order book.
