# buy_token

One fill-or-kill market buy of a single outcome token, then wait on `--listen` for the fill block.

```sh
source .venv/bin/activate
python traders/buy_token/main.py --exec {clob,polynode} --listen {rpc,polynode} --amount USD [--worst-price P] [--wrap] TOKEN_ID
```

`--exec`, `--listen`, `--amount`, and `TOKEN_ID` are required. `--worst-price` and `--wrap` are optional.

`TOKEN_ID` is a CLOB outcome token (Yes or No). Look one up with:

```sh
python explorations/token-search/main.py invade iran 2027
```

`--amount` must be ≥ 2. `--worst-price` is the cap passed to `buy_market(..., max_price=)`. Listen timeout is 60s.

CLOB v2 spends **pUSD**, not USDC.e. If pUSD is short, the buy fails with a wrap hint. `--wrap` wraps just enough USDC.e → pUSD (plus a 1% fee buffer) on the Safe via the builder relayer, then buys. To convert the whole USDC.e stack, use `liquidate --wrap`.
