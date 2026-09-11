# follow_anything

After `--warmup`, copy buy fills as FOK market buys of `--amount` USDC until **N copies have settled** on `--listen`. Measures `copy_settled_block - trigger_settled_block`.

```sh
source .venv/bin/activate
python traders/follow_anything/main.py --exec {clob,polynode} --listen {rpc,polynode} \
  [--trigger-polynode-mempool] [--warmup DUR] [--amount USD] [--count N]
```

`--amount` defaults to 2 (reject `< 2`). `--count` defaults to 5 and is N **settlements**, not N POSTs. 60s wait per copy. Skips our funder and EOA. `status=ok` only if `settled=N`.
