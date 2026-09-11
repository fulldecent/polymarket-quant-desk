# traders

No-model live traders. Shared project code lives in top-level `lib/` and `exchange_client/lib/`. Trader-only helpers live in [`lib/`](lib/) (`ui.py`, `streams.py`).

| Program | Job |
|---|---|
| [`watch_only/`](watch_only/) | Per-block fill stats from `--listen`. Optional mempool trigger feed. |
| [`buy_token/`](buy_token/) | One FOK market buy, wait on `--listen` for the fill block. |
| [`liquidate/`](liquidate/) | Cancel / sell on CLOB; redeem / merge via `--exec`; wrap USDC.e → pUSD. |
| [`follow_anything/`](follow_anything/) | Copy N settled buy fills; measure block lag. |

Flags: `--exec {clob,polynode}`, `--listen {rpc,polynode}` (always settled), optional `--trigger-polynode-mempool`.
