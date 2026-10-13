# account_condition_by_day_v2

Per-UTC-day summary of how one account traded one condition: fill count, account-specific volume, signed fill-flow, fees, and timing.

See [DATA_DICTIONARY.md](DATA_DICTIONARY.md) for the full schema, row ordering, and guarantees.

## How to run it

```sh
source .venv/bin/activate
python derived_data/account_condition_by_day_v2/main.py
```

```sh
python derived_data/account_condition_by_day_v2/main.py --dry-run
python derived_data/account_condition_by_day_v2/main.py --sample 10
```

## How to test it

```sh
source .venv/bin/activate
python -m pytest derived_data/account_condition_by_day_v2/tests/unit_tests -v
python -m pytest derived_data/account_condition_by_day_v2/tests/data_validation -v
```

## Upstream dependencies

This dataset requires `fills_v2` to be materialized through the target UTC day.

## Reproducibility

Given identical source data, the producer will always generate Parquet files with the same rows in the same order within each partition.
