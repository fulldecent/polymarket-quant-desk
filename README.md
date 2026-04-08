# Polymarket quant desk

An automated suite for analyzing and executing trade strategies against Polymarket

## Setup

1. Create and activate a virtual environment:

   ```sh
   python3 -m venv .venv
   source .venv/bin/activate
   ```

2. Install dependencies:

   ```sh
   pip install -r requirements.txt
   ```

3. Configure environment variables:

   ```sh
   cp .env.example .env
   # Edit .env with your RPC endpoint and settings
   ```

   I can recommend dRPC, Infura and Chainstack as excellent RPC providers.

   You will also need an Etherscan account, which is also great and allows you to download contract source code.

4. Download contract source code

    See notes in [polygon_contract_events_v2/deployed_contract_source_code/README.md](polygon_contract_events_v2/deployed_contract_source_code/README.md)

## Hardware requirements

This project performs scraping of raw events from the Polygon blockchain, derived table analysis and trade execution.

Overall, Polymarket is a small enough dataset that you can run the entire analysis and trading suite on a MacBook Pro M5 Pro with 24 GB of RAM and an external SSD for data storage. Less RAM may be possible but will require more disk space for DuckDB spill files.

I use the Samsung T9 and it works great ([Amazon link (affiliate)](https://amzn.to/4sbBeOp)). If you are using a cloud service, note that there is a hot folder for the SQLite database, and a cold folder for the immutable Parquet files (can use object storage).

## Running

First, perform scraping to your cold storage disk. This script is optimized for catching up from scratch and also for running incrementally as new blocks arrive. As of 2026-03-13, this requires 106 GB of disk space (after the SQLite database exports to Parquet files). While you are scraping, the SQLite will be larger, but you can stop any time and periodically do the export command to do the commitment to cold storage which uses less space.

```sh
# Scrape from Polygon and export to Parquet files
# With MacBook Pro M5 Pro, gigabit internet and a paid Chainstack plan,
# try `--parallel 50` for a good experience
python3 polygon_contract_events_v2/scrape_events_rpc.py --parallel 25
# python3 -m pytest polygon_contract_events_v2/assertions/ -v
```
