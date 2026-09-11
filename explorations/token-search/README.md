# token-search

Look up CLOB outcome token IDs and Gamma stats from a URL, slug, keywords, condition id, or token id.

```sh
source .venv/bin/activate
python explorations/token-search/main.py invade iran 2027
python explorations/token-search/main.py will-the-us-invade-iran-before-2027
python explorations/token-search/main.py 'https://polymarket.com/event/will-the-us-invade-iran-before-2027'
```

Uses `GAMMA_API_URL` if set, otherwise the local proxy (`http://127.0.0.1:9432`) when it is up, otherwise `https://gamma-api.polymarket.com`.

Results are sorted by 24h volume, then lifetime volume, then liquidity. Copy a token id into:

```sh
python traders/buy_token/main.py TOKEN_ID --exec clob --listen rpc --amount 2
```
