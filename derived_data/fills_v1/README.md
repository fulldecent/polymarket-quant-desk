# fills_v1

One row per account leg of every `order_filled` event on the four Polymarket exchanges, enriched with the `condition_id` (and NegRisk `market_id`) of the outcome token and a running per-account YES position.

See [DATA_DICTIONARY.md](DATA_DICTIONARY.md) for the full schema, row ordering, and column derivation rules.

## What this dataset is for

`fills_v1` is the canonical, fully-enriched trade ledger that all the condition- and account-level summary datasets are built from. Each row is one trader's side of a matched fill, with the USDC and condition-token amounts already signed and the outcome token already resolved to its condition and market.

## Upstream dependencies

This dataset requires both upstream datasets to be materialized through the target partition:

- `polygon_contract_events_v3` — `order_filled` for all four exchanges, plus FeeModule `fee_refunded` events
- `token_id_map_v1` — resolves each outcome `token_id` to its `condition_id` and `market_id`

## Status

This folder currently defines the data contract (this README and [DATA_DICTIONARY.md](DATA_DICTIONARY.md)). The producer (`main.py`) and tests are not yet implemented.

## Reproducibility

Given identical source data, the producer will always generate Parquet files with the same rows in the same order within each partition.
