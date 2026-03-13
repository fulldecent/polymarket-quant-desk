# polygon_contract_events_v2

Raw Polymarket event logs scraped from Polygon via JSON-RPC, exported to partitioned Parquet files.

## Directory layout

```
<POLYGON_CONTRACT_EVENTS_V2_DIR>/<contract>/<event>/1M=<N>/10K=<N>/data.parquet
<POLYGON_CONTRACT_EVENTS_V2_DIR>/<contract>/<event>/1M=<N>/10K=<N>/metadata.json
```

Example paths:

```
.../ConditionalTokens/condition_preparation/1M=33000000/10K=33600000/data.parquet
.../CTFExchange/order_filled/1M=83000000/10K=83200000/data.parquet
.../NegRiskCtfExchange/order_filled/1M=83000000/10K=83200000/data.parquet
.../NegRiskAdapter/position_split/1M=60000000/10K=60010000/data.parquet
```

## Contracts

| Contract | Address | Directory name |
|---|---|---|
| Gnosis ConditionalTokens | `0x4D97DCd97eC945f40cF65F87097ACe5EA0476045` | `ConditionalTokens/` |
| CTFExchange | `0x4bFb41d5B3570DeFd03C39a9A4D8dE6Bd8B8982E` | `CTFExchange/` |
| NegRiskCtfExchange | `0xC5d563A36AE78145C45a50134d48A1215220f80a` | `NegRiskCtfExchange/` |
| NegRiskAdapter | `0xd91E80cF2E7be2e162c6513ceD06f1dD0dA35296` | `NegRiskAdapter/` |
| UmaCtfAdapter | `0x157Ce2d672854c848c9b79C49a8Cc6cc89176a49` | `UmaCtfAdapter/` |
| Fee Module (CTF) | `0xE3f18aCc55091e2c48d883fc8C8413319d4Ab7b0` | `FeeModuleCTF/` |
| Fee Module (NegRisk) | `0xB768891e3130F6dF18214Ac804d4DB76c2C37730` | `FeeModuleNegRisk/` |

## Scraping workflow

1. Scrape events from Polygon into SQLite:

   ```sh
   python3 polygon_contract_events_v2/scrape_events_rpc.py
   ```

2. Export SQLite rows to v2 Parquet:

   ```sh
   python3 polygon_contract_events_v2/export_parquet.py
   ```

3. Optionally delete exported rows from SQLite to free space:

   ```sh
   python3 polygon_contract_events_v2/export_parquet.py --delete
   ```

## Schema and data dictionary

Full column definitions, data types, guarantees, and known limitations are in [DATA_DICTIONARY.md](DATA_DICTIONARY.md).

## Assertions

Data integrity tests live in `assertions/` and run via pytest:

```sh
source .venv/bin/activate
python -m pytest polygon_contract_events_v2/assertions/ -v
```

Each test validates one structural invariant of the exported Parquet data. The
test helpers in `assertions/helpers.py` automatically union across contracts
when querying a logical event name (e.g., `order_filled` reads from both
`CTFExchange/order_filled` and `NegRiskCtfExchange/order_filled`).

### Test summary

| Test | What it checks |
|---|---|
| [test_address_hash_format_valid](assertions/test_address_hash_format_valid.py) | address columns are 0x + 40 hex, hash columns are 0x + 64 hex |
| [test_amounts_are_non_negative_integers](assertions/test_amounts_are_non_negative_integers.py) | every uint256-backed column parses as a non-negative big integer |
| [test_binary_conditions_have_two_tokens](assertions/test_binary_conditions_have_two_tokens.py) | every TokenRegistered row has its symmetric (token0, token1) counterpart |
| [test_condition_resolution_has_preparation](assertions/test_condition_resolution_has_preparation.py) | every resolved condition_id was previously prepared |
| [test_converted_market_has_questions_and_conditions](assertions/test_converted_market_has_questions_and_conditions.py) | every positions_converted market_id has question_prepared rows, and those question_ids have condition_preparation rows |
| [test_condition_resolution_payout_sums](assertions/test_condition_resolution_payout_sums.py) | binary condition payout numerators are non-negative with sum > 0 |
| [test_fee_charged_tokens_are_registered](assertions/test_fee_charged_tokens_are_registered.py) | every non-zero token_id in fee_charged exists in token_registered |
| [test_fee_reconciliation_per_tx](assertions/test_fee_reconciliation_per_tx.py) | SUM(order_filled.fee) = SUM(fee_charged.amount) per transaction |
| [test_neg_risk_questions_have_market](assertions/test_neg_risk_questions_have_market.py) | every question_prepared.market_id exists in market_prepared |
| [test_no_fill_after_cancel](assertions/test_no_fill_after_cancel.py) | no order_filled after the matching order_cancelled |
| [test_no_trading_after_resolution](assertions/test_no_trading_after_resolution.py) | no fills > 100 blocks after condition resolution (xfail — ~54 known) |
| [test_order_filled_amounts_positive](assertions/test_order_filled_amounts_positive.py) | maker_amount_filled > 0 and taker_amount_filled > 0 |
| [test_order_filled_implied_price_in_range](assertions/test_order_filled_implied_price_in_range.py) | implied price > 0; warns on price >= 1 |
| [test_order_filled_one_side_is_collateral](assertions/test_order_filled_one_side_is_collateral.py) | exactly one of maker/taker asset_id is "0" (USDC) |
| [test_order_filled_taker_matches_orders_matched](assertions/test_order_filled_taker_matches_orders_matched.py) | taker's order_filled amounts match orders_matched |
| [test_order_filled_tokens_are_registered](assertions/test_order_filled_tokens_are_registered.py) | every traded token exists in token_registered |
| [test_orders_matched_bundle_consistency](assertions/test_orders_matched_bundle_consistency.py) | no duplicate maker order_hash within a transaction |
| [test_orders_matched_tokens_are_registered](assertions/test_orders_matched_tokens_are_registered.py) | every traded token in orders_matched is registered |
| [test_outcome_reported_has_question](assertions/test_outcome_reported_has_question.py) | every outcome_reported.question_id exists in question_prepared |
| [test_outcome_reported_value_is_bool](assertions/test_outcome_reported_value_is_bool.py) | outcome_reported.outcome is 0 or 1 |
| [test_payout_redemption_has_resolution](assertions/test_payout_redemption_has_resolution.py) | every redeemed condition has a prior resolution |
| [test_token_belongs_to_one_condition](assertions/test_token_belongs_to_one_condition.py) | no token ID appears under more than one condition_id |
| [test_token_registered_before_first_trade](assertions/test_token_registered_before_first_trade.py) | every traded token was registered at an earlier block |
| [test_token_registered_conditions_have_preparation](assertions/test_token_registered_conditions_have_preparation.py) | every condition_id in token_registered has a preparation (tolerates <= 100 pre-scrape orphans) |
| [test_token_registered_has_condition_id](assertions/test_token_registered_has_condition_id.py) | every token_registered row has a valid non-null 32-byte condition_id |
| [test_transaction_log_index_globally_unique](assertions/test_transaction_log_index_globally_unique.py) | (transaction_hash, log_index) is unique within every event table |
