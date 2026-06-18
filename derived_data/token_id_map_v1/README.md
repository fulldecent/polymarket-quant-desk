# token_id_map_v1

This is a globally unique look up table from `token_id` to `(collateral_token, parent_collection_id, condition_id, index_set, market_id)`. The `market_id` only applies for NegRisk markets.

See [DATA_DICTIONARY.md](DATA_DICTIONARY.md) for full schema and guarantees.

## How to run it

```sh
source .venv/bin/activate
python derived_data/token_id_map_v1/main.py
```

## How to test it

Run unit tests for the program:

```sh
source .venv/bin/activate
python -m pytest derived_data/token_id_map_v1/tests/unit_tests -v
```

Run data validation tests against the cold Parquet files:

```sh
source .venv/bin/activate
python -m pytest derived_data/token_id_map_v1/tests/data_validation -v
```

## Reproducibility

Given identical source data, this program will always generate Parquet files with the same rows in the same order within each partition.
