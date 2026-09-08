# polygon_contract_events_v3

Scrape raw Polymarket smart contract event logs from Polygon and store them in Parquet files.

See [DATA_DICTIONARY.md](DATA_DICTIONARY.md) for full schema and guarantees.

## How to run it

```sh
source .venv/bin/activate
python raw_data/polygon_contract_events_v3/main.py
```

Run it again whenever you want fresh data. It resumes from the sunk frontier, so an interrupted
run costs you nothing beyond the partition it was working on.

These options exist, and none of them is needed for a normal run:

| Option | Default | What it does |
| --- | --- | --- |
| `--sink-workers N` | 1 | Parquet partitions written concurrently. |
| `--max-calls N` | no limit | Stop after N RPC calls. Useful for a smoke test. |
| `--lag-tolerance N` | 2 | Treat the scrape as caught up within N blocks of the chain head. |

## How it tunes itself

Request rate, concurrency and block span are not configurable, because no single setting is right
for the whole chain. Event density swings by orders of magnitude — early history holds one event
per thousands of blocks, busy periods run to hundreds of events per block, and outages leave
stretches with none at all — and every provider enforces different limits.

So the scraper measures instead of assuming:

- Each request is sized from a running estimate of events per block, aimed at a result budget, so
  a request covers thousands of blocks in quiet history and a few dozen in a busy stretch.
- Provider limits are discovered from refusals and held as brackets, then narrowed by binary
  search. A refusal caused by response size is distinguished from one caused by block count, so a
  limit learned in a busy region does not hold back the sparse regions that follow.
- Concurrency ramps from a single request, doubling once per round trip, and stops when the
  provider pushes back or when latency per block starts climbing.
- After the ramp, throughput is measured in fixed-parameter epochs and the parameters are
  hill-climbed on blocks per second.

HTTP 429 and 5xx responses are treated as back-pressure: concurrency drops and a pause is
introduced between requests. Permanent errors such as a bad API key stop the run immediately
rather than burning through the backlog.

Each run writes a timestamped log to `logs/`, including every parameter change and epoch score.

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

Given identical source data (immutable blockchain and faithful RPC responses), this program will always generate Parquet files with the same rows in the same order within each partition.

## Deployed contract source code

You can download the smart contract source code for all the contracts we scrape. Get an Etherscan key and set `ETHERSCAN_API_KEY` in your .env. Then install [Foundry](https://www.getfoundry.sh/) (we recommend Homebrew for macOS rather than Foundry's proposed `curl | bash` installer) and run the `cast source` commands below. This is useful for auditing, debugging and understanding the events we scrape.

```sh
brew install foundry
source .env
export SOURCE_DIR=./raw_data/polygon_contract_events_v3/deployed_contract_source_code

set -e
[ -n "$ETHERSCAN_API_KEY" ] || { echo "Missing ETHERSCAN_API_KEY in .env"; exit 1; }

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
