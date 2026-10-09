# Data catalog

This is the single source of truth for all datasets in this project.

**Base path:** configured via environment variables per dataset (see each producer's README).

## Dependency graph

```mermaid
graph TD
    polygon_contract_events_v4 --> token_id_map_v2
    polygon_contract_events_v4 --> fills_v2
    token_id_map_v2 --> fills_v2
    fills_v2 --> condition_by_block_v2
    fills_v2 --> condition_by_10k_v2
    polygon_contract_events_v4 --resolution--> condition_by_10k_v2
    token_id_map_v2 --> condition_by_10k_v2
    fills_v2 --> account_condition_by_10k_v2
    fills_v2 --> account_by_10k_v2

    classDef raw fill:#b3d9ff,stroke:#336
    classDef immutable fill:#c2f0c2,stroke:#363

    class polygon_contract_events_v4 raw
    class token_id_map_v2,fills_v2,condition_by_block_v2,condition_by_10k_v2,account_condition_by_10k_v2,account_by_10k_v2 immutable
```

**Legend:** blue = raw data, green = derived immutable (partitioned append-only, never rewritten)

## Data types

Data types are chosen carefully to make joining easy. Mirror the logical Parquet types from `polygon_contract_events_v4` exactly: `BLOB` is Parquet `BYTE_ARRAY` (variable length, no logical type) — never `FIXED_LEN_BYTE_ARRAY`. The full rule is in [data-storage-policies](../.github/instructions/data-storage-policies.md).

## Column naming conventions

These conventions apply to every dataset in this project. Individual data dictionaries follow them and do not redefine them.

- `gross_*` — nominal (pre-fee) amount; signed where the amount has a direction.
- `net_*` — amount after fees, or when a fee does not apply; signed where the amount has a direction.
- `matched_*` — market-wide volume: the sum across all fill legs divided by two (each matched fill has a maker side and a taker side, so dividing by two avoids double counting).
- `volume_*` — account-specific volume: the sum of the magnitudes of one account's own fill legs; never divided by two.
- `_count` suffix — a count.
- Grain columns are marked with `*` in each dictionary's schema table.

## Raw data

| Dataset | Path | Producer | Description | Dictionary | Sort order |
|---|---|---|---|---|---|
| polygon_contract_events_v4 | `$POLYGON_CONTRACT_EVENTS_V4_DIR` | [raw_data/polygon_contract_events_v4/](../raw_data/polygon_contract_events_v4/) | On-chain events from all Polymarket contracts on Polygon, scraped via JSON-RPC | [DATA_DICTIONARY.md](../raw_data/polygon_contract_events_v4/DATA_DICTIONARY.md) | `block_number, transaction_index, log_index` |

## Derived data

| Dataset | Path | Producer | Description | Inputs | Dictionary | Sort order |
|---|---|---|---|---|---|---|
| token_id_map_v2 | `$TOKEN_ID_MAP_V2_DIR` | [derived_data/token_id_map_v2/](../derived_data/token_id_map_v2/) | Lookup table: `token_id → (collateral_token, parent_collection_id, condition_id, index_set, market_id)` | polygon_contract_events_v4 | [DATA_DICTIONARY.md](../derived_data/token_id_map_v2/DATA_DICTIONARY.md) | `token_id` |
| fills_v2 | `$FILLS_V2_DIR` | [derived_data/fills_v2/](../derived_data/fills_v2/) | One row per account leg of every matched fill, with signed USDC and condition-token amounts, condition/market enrichment, and a running per-account YES position | polygon_contract_events_v4, token_id_map_v2 | [DATA_DICTIONARY.md](../derived_data/fills_v2/DATA_DICTIONARY.md) | `block_number, logical_fill_index` |
| condition_by_block_v2 | `$CONDITION_BY_BLOCK_V2_DIR` | [derived_data/condition_by_block_v2/](../derived_data/condition_by_block_v2/) | Per-block OHLC YES price and market-wide matched volume and fees for each condition | fills_v2 | [DATA_DICTIONARY.md](../derived_data/condition_by_block_v2/DATA_DICTIONARY.md) | `block_number, condition_id` |
| condition_by_10k_v2 | `$CONDITION_BY_10K_V2_DIR` | [derived_data/condition_by_10k_v2/](../derived_data/condition_by_10k_v2/) | Per-partition OHLC YES price, market-wide matched volume, and resolution outcome for each condition that traded or resolved in that 10K | fills_v2, polygon_contract_events_v4, token_id_map_v2 | [DATA_DICTIONARY.md](../derived_data/condition_by_10k_v2/DATA_DICTIONARY.md) | `condition_id` |
| account_condition_by_10k_v2 | `$ACCOUNT_CONDITION_BY_10K_V2_DIR` | [derived_data/account_condition_by_10k_v2/](../derived_data/account_condition_by_10k_v2/) | Per-partition summary of how one account traded one condition: fill count, account-specific volume, fees, timing | fills_v2 | [DATA_DICTIONARY.md](../derived_data/account_condition_by_10k_v2/DATA_DICTIONARY.md) | `account, condition_id` |
| account_by_10k_v2 | `$ACCOUNT_BY_10K_V2_DIR` | [derived_data/account_by_10k_v2/](../derived_data/account_by_10k_v2/) | Per-partition behavioral profile for each account: fill count, condition breadth, volume, fees, timing, and entropy metrics | fills_v2 | [DATA_DICTIONARY.md](../derived_data/account_by_10k_v2/DATA_DICTIONARY.md) | `account` |

## Adding a new dataset

1. Add a row to the appropriate table above (including inputs and sort order).
2. Create a `DATA_DICTIONARY.md` alongside the producer script.
3. Follow the partitioning, naming, metadata, and sorting rules in [data-storage-policies](../.github/instructions/data-storage-policies.md).
