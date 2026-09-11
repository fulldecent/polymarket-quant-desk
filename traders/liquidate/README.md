# liquidate

Reduce exposure on our account.

Cancel / limit-sell / market-sell always use the authenticated CLOB client. Redeem / merge follow `--exec`. Then wait 60s on `--listen` for returned tx hashes.

```sh
source .venv/bin/activate
python traders/liquidate/main.py --exec {clob,polynode} --listen {rpc,polynode} \
  [--cancel-orders] [--limit-sell PCT] [--limit-sell-ttl DUR] [--market-sell] \
  [--redeem] [--merge] [--wrap [USD]] [--dry-run]
```

With no action flags, prints a snapshot of positions, redeemable, mergeable, and open orders and exits. Listen is not opened.

`--wrap` converts USDC.e on the Safe into pUSD (CLOB v2 collateral) via CollateralOnramp and sets v2 allowances. Omit the amount to wrap the entire USDC.e balance.
