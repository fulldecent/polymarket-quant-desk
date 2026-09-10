# condition_by_10k_v1

Per-10K-partition OHLC YES price, market-wide matched volume, and resolution outcome for each condition that traded or resolved in that partition.

See [DATA_DICTIONARY.md](DATA_DICTIONARY.md) for the full schema, implied-YES-price formula, resolution join rules, row ordering, and guarantees.

## How to run it

```sh
source .venv/bin/activate
python derived_data/condition_by_10k_v1/main.py
```

```sh
python derived_data/condition_by_10k_v1/main.py --dry-run
python derived_data/condition_by_10k_v1/main.py --sample 10
```

## How to test it

```sh
source .venv/bin/activate
python -m pytest derived_data/condition_by_10k_v1/tests/unit_tests -v
python -m pytest derived_data/condition_by_10k_v1/tests/data_validation -v
```

## Upstream dependencies

This dataset requires all three to be materialized through the target partition:

- `fills_v1` — OHLC, matched volume, fees
- `polygon_contract_events_v3` — `ConditionalTokens/condition_resolution`
- `token_id_map_v1` — drop non-Polymarket resolutions from the raw ConditionalTokens table

Built from `fills_v1` directly. It does not read `condition_by_block_v1`.

## Reproducibility

Given identical source data, the producer will always generate Parquet files with the same rows in the same order within each partition.
