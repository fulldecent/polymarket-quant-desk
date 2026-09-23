# traders

Live programs. Research archive (tickets, FOK vs GTC, one-sided vs
two-sided) is [`../explorations/README.md`](../explorations/README.md)
and [`../explorations/FRAME.md`](../explorations/FRAME.md). Shared
project code lives in top-level `lib/` and `exchange_client/lib/`.
Trader-only helpers live in [`lib/`](lib/) (`ui.py`, `streams.py`).

`directional` is the only strategy that came out of that archive.
It is **not** a go-ahead. Formula is 3-of-8 / criss 5; see
[`IDEAS.md`](IDEAS.md) for what was not built.

| Program | Job |
|---|---|
| [`watch_only/`](watch_only/) | Per-block fill stats from `--listen`. Optional mempool trigger feed. |
| [`buy_token/`](buy_token/) | One FOK market buy, wait on `--listen` for the fill block. |
| [`liquidate/`](liquidate/) | Cancel / sell on CLOB; redeem / merge via `--exec`; wrap USDC.e → pUSD. |
| [`follow_anything/`](follow_anything/) | Copy N settled buy fills; measure block lag. |
| [`directional/`](directional/) | Live of `explorations/directional-v1` (3-of-8 / criss 5); max 3 open; 100-block warmup. Not a go-ahead. |

Flags: `--exec {clob,polynode}`, `--listen {rpc,polynode}` (always settled), optional `--trigger-polynode-mempool`.
