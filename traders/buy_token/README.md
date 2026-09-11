# buy_token

One fill-or-kill market buy of a single outcome token, then wait on `--listen` for the fill block.

```sh
source .venv/bin/activate
python traders/buy_token/main.py TOKEN_ID --exec {clob,polynode} --listen {rpc,polynode} --amount USD [--worst-price P]
```

`TOKEN_ID` is a CLOB outcome token (Yes or No). Look one up with:

```sh
python explorations/token-search/main.py invade iran 2027
```

`--amount` must be ≥ 2. `--worst-price` is the cap passed to `buy_market(..., max_price=)`. Listen timeout is 60s.
