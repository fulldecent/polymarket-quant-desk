# Polymarket quant desk

An automated suite for analyzing and executing trade strategies against Polymarket

## Hardware requirements

This project scrapes raw events from the Polygon blockchain, derived table analysis and trade execution.

Overall, Polymarket is a small enough dataset that you can run the entire analysis and trading suite on a MacBook Pro M5 Pro with 24 GB of RAM and an external 2TB SSD for data storage. Less RAM may be possible but will require more disk space for DuckDB spill files.

I use the Samsung T9 and it works great ([Amazon link (affiliate)](https://amzn.to/4sbBeOp)). If you are using a cloud service, note that there is a hot folder for the DuckDB database, and a cold folder for the immutable Parquet files (can use object storage).

## One-time setup

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

## Scraping and derived data

Repeat this process periodically to get new data as it is available from the market. This will allow you to run your backtesting analysis against recent data.

1. Scrape raw events

   ```sh
   source .venv/bin/activate
   python raw_data/polygon_contract_events_v3/main.py --parallel 25
   ```

2. Build the derived datasets

   Run each derived producer after the raw scrape, in dependency order (see [docs/Data catalog.md](docs/Data%20catalog.md) for the full graph):

   ```sh
   source .venv/bin/activate
   python derived_data/token_id_map_v1/main.py
   ```

   Each producer is incremental and immutable: it materializes every new 10,000-block partition up to the upstream frontier and never rewrites a landed partition. Shared building blocks for producers live in [lib/](lib/README.md).

## Explorations

Run any of the scripts in the `explorations/` folder to do ad-hoc analysis or testing.

- [polynode_inclusion_test.py](explorations/polynode_inclusion_test.py) — see how fast Polynode sees new trades compared to the RPC logs.

## Testing

Perform tests periodically and after changing scraping/derived data/trade execution code.

```sh
source .venv/bin/activate
python -m pytest -v
```
