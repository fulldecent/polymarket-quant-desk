# Polymarket quant desk

:warning: NOTE: The fills dataset is in early access and may be subject to change. SemVer does not apply to that. The rest is production ready.

An automated suite for analyzing and executing trade strategies against Polymarket

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
