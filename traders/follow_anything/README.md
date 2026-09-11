# follow_anything

`--warmup` is one-shot: connect and watch, copy nothing. Then drop the queued fills, wait for the **next new block** on `--listen`, and FOK-copy buy fills of `--amount` until **N copies have settled**. No cooldown between copies. Measures `copy_settled_block - trigger_settled_block`.

Same loop for `--listen rpc` and `--listen polynode`. `--trigger-polynode-mempool` fires on the next pre-block buy after warmup instead of waiting for that block.

```sh
source .venv/bin/activate
python traders/follow_anything/main.py --exec {clob,polynode} --listen {rpc,polynode} \
  [--trigger-polynode-mempool] [--warmup DUR] [--amount USD] [--count N]
```

`--amount` defaults to 2 (reject `< 2`). `--count` defaults to 5 and is N **settlements**, not N POSTs. 60s wait per copy. Skips our funder and EOA. `status=ok` only if `settled=N`.
