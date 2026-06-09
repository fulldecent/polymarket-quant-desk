# Copilot instructions

This project scrapes Polymarket event logs from Polygon blockchain and exports them to Parquet files.

## Code conventions

- Python 3.10+
- Use `python-dotenv` for environment configuration
- All environment variables are required and validated at startup
- Do not use "baskwards compatibility aliases" or other BC features unless specifically requested

## Program functionality

- Programs never delete or modify finalized data. This is a production environment. See data guarantees for each product.
- Programs are considered broken if they fail to update their output for more than 2 seconds. Ensure output is updated every second and shows accurate progress updates.

## Queries

- The data set is approx 1 TB of raw data. Whenever testing queries, always use a timeout and adapt if you hit problems. If you need a lot of spill space, use the external drive.
