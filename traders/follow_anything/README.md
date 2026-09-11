# follow_anything

`--warmup` is one-shot: connect and watch, copy nothing. Then drop the queued fills, wait for the **next new block** on `--listen`, and FOK-copy buy fills of `--amount` until **N copies have settled**. No cooldown between copies. Each copy is one signed POST (no tick/book GETs). Measures `copy_settled_block - trigger_settled_block`.

Same loop for `--listen rpc` and `--listen polynode`. `--trigger-polynode-mempool` copies a fill **before** it has a block. If that trigger never appears on `--listen`, the run fails and stops — we already spent, and the signal was unconfirmed.

```sh
source .venv/bin/activate
python traders/follow_anything/main.py --exec {clob,polynode} --listen {rpc,polynode} \
  [--trigger-polynode-mempool] [--warmup DUR] [--amount USD] [--count N]
```

`--amount` defaults to 2 (reject `< 2`). `--count` defaults to 5 and is N **scored** settlements (integer `copy_block - trigger_block`). Within 60s both the trigger and our copy must be on `--listen` or the run fails. Skips our funder and EOA. `status=ok` only if `settled=N`.
