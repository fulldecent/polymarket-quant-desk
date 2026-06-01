# polygon_contract_events_v3

Scrape raw Polymarket smart contract event logs from Polygon and store them in Parquet files.

Complete data schema is specified in [DATA_DICTIONARY.md](DATA_DICTIONARY.md). Immutability and completeness guarantees are also in there.

## How to run it

```sh
source .venv/bin/activate
python raw_data/polygon_contract_events_v3/main.py
```

## How to test it

Run unit tests for the program:

```sh
source .venv/bin/activate
python -m pytest raw_data/polygon_contract_events_v3/tests/unit_tests -v
```

Run data validation tests against the cold Parquet files:

```sh
source .venv/bin/activate
python -m pytest raw_data/polygon_contract_events_v3/tests/data_validation -v
```

## Reproducibility

The cold-tier `data.parquet` files are logical-data-stable across independent scrapes. Given the same source data and the same code, a fresh run of the scraper against a different storage location produces files whose bytes match the existing cold tier exactly.

## Deployed contract source code

You can download the smart contract source code for all the contracts we scrape. Get an Etherscan key and set `ETHERSCAN_API_KEY` in your .env. Then install [Foundry](https://www.getfoundry.sh/) (we recommend Homebrew for macOS rather than Foundry's proposed `curl | bash` installer) and run the `cast source` commands below. This is useful for auditing, debugging and understanding the events we scrape.

```sh
source .env
export SOURCE_DIR=./raw_data/polygon_contract_events_v3/deployed_contract_source_code

ADDR=0x4D97DCd97eC945f40cF65F87097ACe5EA0476045; cast source $ADDR --chain polygon -d "$SOURCE_DIR/ConditionalTokens-$ADDR"
ADDR=0x4bFb41d5B3570DeFd03C39a9A4D8dE6Bd8B8982E; cast source $ADDR --chain polygon -d "$SOURCE_DIR/CTFExchange-$ADDR"
ADDR=0xC5d563A36AE78145C45a50134d48A1215220f80a; cast source $ADDR --chain polygon -d "$SOURCE_DIR/NegRiskCtfExchange-$ADDR"
ADDR=0xd91E80cF2E7be2e162c6513ceD06f1dD0dA35296; cast source $ADDR --chain polygon -d "$SOURCE_DIR/NegRiskAdapter-$ADDR"
ADDR=0x157Ce2d672854c848c9b79C49a8Cc6cc89176a49; cast source $ADDR --chain polygon -d "$SOURCE_DIR/UmaCtfAdapter-$ADDR"
ADDR=0xE3f18aCc55091e2c48d883fc8C8413319d4Ab7b0; cast source $ADDR --chain polygon -d "$SOURCE_DIR/FeeModuleCTF-$ADDR"
ADDR=0xB768891e3130F6dF18214Ac804d4DB76c2C37730; cast source $ADDR --chain polygon -d "$SOURCE_DIR/FeeModuleNegRisk-$ADDR"
ADDR=0xE111180000d2663C0091e4f400237545B87B996B; cast source $ADDR --chain polygon -d "$SOURCE_DIR/CTFExchangeV2-$ADDR"
ADDR=0xe2222d279d744050d28e00520010520000310F59; cast source $ADDR --chain polygon -d "$SOURCE_DIR/NegRiskCtfExchangeV2-$ADDR"
```
