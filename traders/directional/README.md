# directional

Formerly `directional_fok`. Live line for the one-sided take-then-make ticket
([`one_sided_take_then_make`](../../explorations/FRAME.md)). Research:
[`../../explorations/directional-v1/`](../../explorations/directional-v1/).
**Not a go-ahead** — see session notes. Stage A here is **formula.json**,
not the 6-of-8 card in STAGE_A.md.

- Stage A: **3 of last 8** fills and criss-cross **≥ 5** (`formula.json`)
- Stage B: four high/low YES-delta heads (`stage_b.joblib`)
- Stage C: always-on decoder `in=1.0 out=0.5 dbl=4` (best OOS walk-forward cell)
- Portfolio: **max 3** outstanding bets
- Size: `1.1 × n_min`, floor **5 shares** and **$2**
- Warmup: **100 blocks** with a progress bar, then hot
- Hot path: FOK buy then GTC sell in the same turn (no wait on inventory)

```sh
source .venv/bin/activate
python traders/directional/main.py --exec {clob,polynode} --listen {rpc,polynode}
python traders/directional/main.py --exec clob --listen polynode --dry-run
```

Formula: [`formula.json`](formula.json). GTC lifetime is at least 60s (CLOB floor); leftover after ~30 blocks is FAK-dumped. Cooldown 180 blocks per condition. CTRL-C stops the run.

`--listen polynode` is preferred (condition id + outcome on the tape). RPC still trades but YES/NO mapping is weaker when `outcome` is empty.
