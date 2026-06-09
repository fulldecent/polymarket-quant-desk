# token_and_usdc_flows_v2

Per-account, per-event USDC and outcome-token movements across all Polymarket operations (trades, splits, merges, redeems, conversions).

Reads from `polygon_contract_events_v3` raw data (Polymarket V1 and V2 combined). Produces partitions in strict block order up to the upstream frontier, with atomic writes and crash recovery.

See [DATA_DICTIONARY.md](DATA_DICTIONARY.md) for full schema, invariants, and immutability guarantees.

## Status

**Under development.** This dataset is being rewritten from `token_and_usdc_flows` (v1) to upgrade to the v3 raw source, adopt the frontier-aware ordering, and use BLOB types to match the upstream gold standard.

## How to run it

```sh
source .venv/bin/activate
python derived_data/token_and_usdc_flows_v2/main.py [options]
```

Options:

- `--dry-run` — print work plan without writing
- `--partitions N` — stop after processing N partitions

## How to test it

```sh
source .venv/bin/activate
python -m pytest derived_data/token_and_usdc_flows_v2/tests/unit_tests -v
python -m pytest derived_data/token_and_usdc_flows_v2/tests/data_validation -v
```

## Fee semantics

Polymarket applies fees to some trades. You can identify if a fee has occured if

## Fee semantics: Polymarket V1 vs V2

The net fee visible in the output depends on the exchange and version.

### Polymarket V1 (CTFExchange / NegRiskCtfExchange)

**Upstream structure:**

- `OrderFilled` event carries a **gross fee** (debited, total going to protocol)
- A matching `FeeModule.FeeRefunded` event carries the **actual net fee** (what the taker really pays; maker is refunded)
- The materializer uses `COALESCE(fee_refunded.fee_charged, order_filled.fee)` to get the net actual

**Fee placement** (deducted from the receiving asset):

- **BUY order** (`maker_asset_id == 0`): Maker pays USDC (collateral), receives tokens. Fee is deducted from tokens received.
  - Buyer (sub_index=0): `net_tokens = taker_amount_filled - actual_fee`; `net_usdc = -maker_amount_filled`
  - Seller (sub_index=1): `net_tokens = -taker_amount_filled`; `net_usdc = +maker_amount_filled` (no fee)
- **SELL order** (`maker_asset_id != 0`): Maker pays tokens, receives USDC (collateral). Fee is deducted from USDC received.
  - Seller (sub_index=1): `net_usdc = taker_amount_filled - actual_fee`; `net_tokens = -maker_amount_filled`
  - Buyer (sub_index=0): `net_usdc = -taker_amount_filled`; `net_tokens = +maker_amount_filled` (no fee)

**Example: V1 BUY order** (binary market, USDC.e × tokens)

```
Raw upstream (CTFExchange/order_filled):
  maker=0xAlice, taker=0xBob
  maker_asset_id=0x00...00 (USDC), taker_asset_id=0xTok1 (outcome token)
  maker_amount_filled=1_000_000 µUSDC, taker_amount_filled=1_000_000 µtoken
  fee=10_000 µUSDC (gross)

Raw upstream (FeeModule.FeeRefunded):
  order_hash=0xABC..., fee_charged=1_500 µUSDC (net actual, taker pays)

Output rows:
  [0] buyer (Alice, sub_index=0):
      net_usdc = -1_000_000 (pays USDC)
      net_tokens = 1_000_000 - 1_500 = 998_500 (receives tokens, net of fee)
      price_1e18 = 1_000_000 × 1e18 / 998_500 ≈ 1.001502...
      
  [1] seller (Bob, sub_index=1):
      net_usdc = +10_000 (receives unilaterally from pool)  <-- NO fee; stays positive
      net_tokens = -1_000_000 (gives tokens)
      price_1e18 = (same as [0])
```

### Polymarket V2 (CTFExchangeV2 / NegRiskCtfExchangeV2)

**Upstream structure:**

- `OrderFilled` event carries `side` (0=BUY, 1=SELL) + a direct `fee` field
- No fee module; `fee` in the event is the net actual fee
- Side semantics: `side=0` → taker buys outcome token (pays USDC), `side=1` → taker sells outcome token (receives USDC)

**Fee placement** (same logic as V1: deducted from the receiving asset):

- **BUY** (`side=0`): Fee deducted from tokens.
- **SELL** (`side=1`): Fee deducted from USDC.

**Example: V2 SELL order** (binary market, USDC.e × tokens)

```
Raw upstream (CTFExchangeV2/order_filled):
  maker=0xCarol, taker=0xDave
  side=1 (taker is SELLING outcome token)
  token_id=0xTok2 (outcome token)
  maker_amount_filled=500_000 µtoken (maker buys)
  taker_amount_filled=500_000 µUSDC (taker receives)
  fee=750 µUSDC (net, taker pays)

Output rows:
  [0] seller/taker (Dave, sub_index=0, "trade_buy" semantically):
      Account is BUYING tokens via the side field mapping.
      BUT the flow direction is taker receives USDC (seller side).
      net_usdc = -500_000 + 750 = -499_250
      net_tokens = +500_000
      price_1e18 = 499_250 × 1e18 / 500_000
      
  [1] buyer/maker (Carol, sub_index=1, "trade_sell" semantically):
      net_usdc = +500_000 (receives USDC, no fee here)
      net_tokens = -500_000 (gives tokens)
```

**Key difference from V1:** V2 has no separate fee-refund event; the `fee` in the `order_filled` is always the net actual.

## Running balance: computing net holdings

To compute a trader's current token holdings for a specific `token_id`:

1. Find the latest `redeem` row for that `token_id` and `account`.
2. If found, post-redeem balance = **0** (full position burned).
3. Sum only `net_tokens` from rows **after** that redeem.
4. If no redeem row, sum all `net_tokens` from the beginning.

**Special case:** ConditionalTokens payout_redemption rows have `net_tokens = NULL` (unknown negative). This signals a full position burn. Reset balance to 0 and continue from after that row.

## Architecture

1. **Startup**: Git-clean check (exit if working tree is dirty); load frontier from upstream manifest; enumerate partitions in block order.
2. **Recovery**: Clean up any incomplete (temp-named) partition folders from prior interrupted runs.
3. **Per-1M precomputation**: Scan all splits/merges/redeems in the 1M range; compute CTF keccak/ECC token IDs; cache for all 10K chunks in that range.
4. **Per-10K chunk processing**:
   - Execute SQL UNIONs across all 11 event types (4 trades + 7 position operations).
   - Compute `price_1e18` on trade rows.
   - Convert output to Arrow table; cast to output schema (BLOB types).
   - Fail-fast validations: unique grain, bounds, byte lengths.
   - Atomically write: create temp-named folder, write both files, rename to final.
   - Write metadata per `docs/Metadata files.md` (including `input_hashes`).

## Data types

**Blob columns** (raw bytes, not hex strings):

- `transaction_hash`: 32 bytes
- `account`: 20 bytes
- `token_id`: 32 bytes (nullable)
- `condition_id`: 32 bytes
- `collateral_token`: 20 bytes

**Numeric types**:

- `net_usdc`: signed int64 (micro-USDC, clamped ±100M)
- `net_tokens`: signed int64 (nullable; NULL means "unknown negative burn")
- `price_1e18`: unsigned int64 (on trades only; NULL elsewhere)

## Dependencies

Raw data from `polygon_contract_events_v3`:

- `CTFExchange/order_filled`, `{CTFExchangeV2,NegRiskCtfExchange,NegRiskCtfExchangeV2}/order_filled`
- `{ConditionalTokens,NegRiskAdapter}/position_split`, `positions_merge`, `payout_redemption`
- `NegRiskAdapter/positions_converted`
- Lookup tables: `token_registered`, `condition_preparation`, `question_prepared`

Python library: `lib/ct_helpers.py` for deterministic token ID computation.

## Robustness guarantees

- **Ordered production**: Partitions produced strictly in block order from genesis up to and including the upstream frontier.
- **Atomic writes**: Both files (data.parquet + metadata.json) written into a temp-named folder, then atomically renamed to the final `10K={k}` folder.
- **Crash recovery**: On startup, any incomplete (temp-named) partition folder is deleted before resuming.
- **Provenance**: Metadata embeds `git_commit` and rejected if the working tree is dirty.
- **Immutability**: Completed partitions are never rewritten (unless `--force`).
