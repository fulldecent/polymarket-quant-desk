# Copilot instructions

This project scrapes Polymarket event logs from Polygon blockchain and exports them to Parquet files.

## Project structure

- `polygon_contract_events_v2/` — chain event scraper
  - `scrape_events_rpc.py` — scrapes events from Polygon JSON-RPC into SQLite
  - `export_parquet.py` — exports SQLite rows to partitioned Parquet files
  - `v2_schemas.py` — event ABI definitions and schemas
  - `assertions/` — data quality tests (run with pytest)

## Code conventions

- Python 3.10+
- Use `python-dotenv` for environment configuration
- All environment variables are required and validated at startup
- Blockchain addresses are stored as lowercase hex strings with `0x` prefix
- Token IDs and amounts are stored as strings (uint256 values)
- Block numbers are integers

## Data storage

- Hot storage: SQLite database for active scraping (`RPC_DB_PATH`)
- Cold storage: Partitioned Parquet files for analysis (`POLYGON_CONTRACT_EVENTS_V2_DIR`)

## Testing

Run data quality assertions:

```sh
pytest polygon_contract_events_v2/assertions/
```
