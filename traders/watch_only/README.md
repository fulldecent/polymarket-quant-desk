# watch_only

Live fill stats from `--listen`. Optional Polynode mempool trigger feed. No orders.

```sh
source .venv/bin/activate
python traders/watch_only/main.py --listen {rpc,polynode} [--trigger-polynode-mempool]
```

Ctrl+C prints totals and exits 0.
