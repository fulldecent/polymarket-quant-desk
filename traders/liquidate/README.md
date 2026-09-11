# liquidate

Reduce exposure on our account.

Cancel / limit-sell / market-sell always use the authenticated CLOB client. Redeem / merge follow `--exec`. Then wait 60s on `--listen` for returned tx hashes.

```sh
source .venv/bin/activate
python traders/liquidate/main.py --exec {clob,polynode} --listen {rpc,polynode} \
  [--cancel-orders] [--limit-sell PCT] [--limit-sell-ttl DUR] [--market-sell] [--redeem] [--merge] [--dry-run]
```

With no action flags, prints a snapshot of positions, redeemable, mergeable, and open orders and exits. Listen is not opened.
