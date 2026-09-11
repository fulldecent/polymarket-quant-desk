# Polymarket quant desk

:warning: NOTE: The fills dataset is in early access and may be subject to change. SemVer does not apply to that. The rest is production ready.

An automated suite for analyzing and executing trade strategies against Polymarket

This project is provided with NO WARRANTY, express or implied. Use at your own risk.

## Hardware requirements

This project scrapes raw events from the Polygon blockchain, derived table analysis and trade execution.

Overall, the Polymarket dataset is small enough to run the entire analysis and trading suite on a MacBook Pro M5 Pro with 24 GB of RAM and 2 TB external SSD. Less RAM may be possible with 200 GB+ of internal NVME storage for the "hot" folder.

I use the Samsung T9 and it works great ([Amazon link (affiliate)](https://amzn.to/4sbBeOp)). Advanced cloud setups with object and block storage are also documented in the .env.

## One-time setup

1. Create and activate a virtual environment:

   ```sh
   brew install uv # your python manager
   uv sync
   source .venv/bin/activate
   ```

2. Configure environment variables:

   ```sh
   [ -f .env ] || cp .env.example .env
   # Study and edit your .env file, estimated setup time: 30+ minutes
   ```

## Scrape and derive data

Repeat this process periodically to get new data as it is available from the market. This will allow you to run your backtesting analysis against recent data. Add new lines below in your pipeline as you create your own proprietary datasets.

```sh
source .venv/bin/activate
python raw_data/polygon_contract_events_v3/main.py
python derived_data/token_id_map_v1/main.py
python derived_data/fills_v1/main.py
python derived_data/condition_by_block_v1/main.py
python derived_data/condition_by_10k_v1/main.py
python derived_data/account_condition_by_10k_v1/main.py
python derived_data/account_by_10k_v1/main.py
```

The full dependency graph for your pipeline is in the [data catalog](docs/Data%20catalog.md), and links to each dataset's data dictionary. These scripts all produce data in partitions of 10,000 blockchain blocks which is immutable and reproducible.

This is information you use to find alpha!

## Trade

CLOB traffic goes through a local Tor proxy for testing. Start it in a **separate tab** and leave it running:

```sh
source .venv/bin/activate
python exchange_client/start_proxy.py
```

(`brew install tor gost caddy` once, if those are not on the machine yet.)

In another tab, inspect the account or unwind it with `liquidate`. With no action flags it prints a snapshot of positions and open orders:

```sh
source .venv/bin/activate
python traders/liquidate/main.py --exec clob --listen rpc
```

```sh
python traders/liquidate/main.py --exec clob --listen rpc --cancel-orders
python traders/liquidate/main.py --exec clob --listen rpc --market-sell --redeem --merge
```

Other trading strategies are available under [`traders/`](traders/).

## Explorations

Run any of the scripts in the `explorations/` folder to do ad-hoc analysis or testing.

- [polynode_inclusion_test.py](explorations/polynode_inclusion_test.py) — see how fast Polynode sees new trades compared to the RPC logs.

## Testing

Perform tests periodically and after changing scraping/derived data/trade execution code.

```sh
source .venv/bin/activate
python -m pytest -v
```
