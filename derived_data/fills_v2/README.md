# fills_v2

One row per account leg of every `order_filled` event on the four Polymarket exchanges, enriched with the `condition_id` (and NegRisk `market_id`) of the outcome token and a running per-account YES position.

See [DATA_DICTIONARY.md](DATA_DICTIONARY.md) for the full schema, row ordering, and column derivation rules.

## What this dataset is for

`fills_v2` is the canonical, fully-enriched trade ledger that all the condition- and account-level summary datasets are built from. Each row is one trader's side of a matched fill, with the USDC and condition-token amounts already signed and the outcome token already resolved to its condition and market.

## How to run it

```sh
source .venv/bin/activate
python derived_data/fills_v2/main.py
```

```sh
python derived_data/fills_v2/main.py --dry-run
python derived_data/fills_v2/main.py --sample 10
```

## How to test it

```sh
source .venv/bin/activate
python -m pytest derived_data/fills_v2/tests -v
```

## Upstream dependencies

This dataset requires both upstream datasets to be materialized through the target partition:

- `polygon_contract_events_v4` — `order_filled` for all four exchanges, plus FeeModule `fee_refunded` events
- `token_id_map_v2` — resolves each outcome `token_id` to its `condition_id` and `market_id`

## Reproducibility

Given identical source data, the producer will always generate Parquet files with the same rows in the same order within each partition.
