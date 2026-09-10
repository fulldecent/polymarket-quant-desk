# condition_by_block_v1

Per-block OHLC YES price and market-wide matched volume and fees for each condition.

See [DATA_DICTIONARY.md](DATA_DICTIONARY.md) for the full schema, implied-YES-price formula, row ordering, and guarantees.

## How to run it

```sh
source .venv/bin/activate
python derived_data/condition_by_block_v1/main.py
```

```sh
python derived_data/condition_by_block_v1/main.py --dry-run
python derived_data/condition_by_block_v1/main.py --sample 10
```

## How to test it

```sh
source .venv/bin/activate
python -m pytest derived_data/condition_by_block_v1/tests/unit_tests -v
python -m pytest derived_data/condition_by_block_v1/tests/data_validation -v
```

## Upstream dependencies

This dataset requires `fills_v1` to be materialized through the target partition.

## Reproducibility

Given identical source data, the producer will always generate Parquet files with the same rows in the same order within each partition.
