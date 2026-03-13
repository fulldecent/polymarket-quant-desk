# Deployed contract source code

Verified source code for the Polygon contracts we scrape events from,
downloaded via `cast source` (Foundry) from Polygonscan.

To re-download, run from the `polygon_contract_events_v2/` directory with
`ETHERSCAN_API_KEY` set:

```sh
cast source 0x4D97DCd97eC945f40cF65F87097ACe5EA0476045 --chain polygon --etherscan-api-key "$ETHERSCAN_API_KEY" -d deployed_contract_source_code/ConditionalTokens-0x4D97DCd97eC945f40cF65F87097ACe5EA0476045

cast source 0x4bFb41d5B3570DeFd03C39a9A4D8dE6Bd8B8982E --chain polygon --etherscan-api-key "$ETHERSCAN_API_KEY" -d deployed_contract_source_code/CTFExchange-0x4bFb41d5B3570DeFd03C39a9A4D8dE6Bd8B8982E

cast source 0xC5d563A36AE78145C45a50134d48A1215220f80a --chain polygon --etherscan-api-key "$ETHERSCAN_API_KEY" -d deployed_contract_source_code/NegRiskCtfExchange-0xC5d563A36AE78145C45a50134d48A1215220f80a

cast source 0xd91E80cF2E7be2e162c6513ceD06f1dD0dA35296 --chain polygon --etherscan-api-key "$ETHERSCAN_API_KEY" -d deployed_contract_source_code/NegRiskAdapter-0xd91E80cF2E7be2e162c6513ceD06f1dD0dA35296

cast source 0x157Ce2d672854c848c9b79C49a8Cc6cc89176a49 --chain polygon --etherscan-api-key "$ETHERSCAN_API_KEY" -d deployed_contract_source_code/UmaCtfAdapter-0x157Ce2d672854c848c9b79C49a8Cc6cc89176a49

cast source 0xE3f18aCc55091e2c48d883fc8C8413319d4Ab7b0 --chain polygon --etherscan-api-key "$ETHERSCAN_API_KEY" -d deployed_contract_source_code/FeeModuleCTF-0xE3f18aCc55091e2c48d883fc8C8413319d4Ab7b0

cast source 0xB768891e3130F6dF18214Ac804d4DB76c2C37730 --chain polygon --etherscan-api-key "$ETHERSCAN_API_KEY" -d deployed_contract_source_code/FeeModuleNegRisk-0xB768891e3130F6dF18214Ac804d4DB76c2C37730
```

`ETHERSCAN_API_KEY` is set in the root `.env`.
