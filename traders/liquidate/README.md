# liquidate

Reduce exposure on our account.

Cancel / limit-sell / market-sell always use the authenticated CLOB client. Redeem / merge follow `--exec`. Then wait 60s on `--listen` for returned tx hashes.

```sh
source .venv/bin/activate
python traders/liquidate/main.py --exec {clob,polynode} --listen {rpc,polynode} \
  [--cancel-orders] [--limit-sell PCT] [--limit-sell-ttl DUR] [--market-sell] [--redeem] [--merge] [--dry-run]
```

At least one action flag is required.
