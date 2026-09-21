# ~100 Stage A signals (20 Sep 3-of-8 / criss 5+)

## Complements (CMM) on these names

Replay of original complementary-MM (FOK one outcome at last+1¢ in ~45s,
rest the other at (1−last)+1¢ for 15 min, merge if both).

23 unique conditions we actually bought. 9 had no complement token in
Gamma (spread legs). 14 simmable **non-crypto**:

| state | n | P&L |
|---|---:|---:|
| both | 10 | ~$0.00 (pair cost ≈ $1.00) |
| yes_only / no_only | 3 | −$0.78 |
| miss | 1 | $0 |
| **non-crypto total** | | **−$0.73** |

Same failure as the 7-day naive CMM: **one-sided mass**. Both-leg
completes do not pay enough to cover inventory. **Do not live this.**
No new Stage B/C, no CMM trader.

## Crypto take-then-make (7-day, fee’d, short-lived cids)

130k sampled FOK fills, 5.5 sh, 135 bps taker fee, TP +2¢ / 15m else dump:

| | |
|---|---:|
| take + TP-or-dump | **−$0.074 / clip** (−$9.6k) |
| fees | $0.025 / clip |
| hold to window end | −$0.009 / clip |

TP looks green in isolation; dumps and fees wipe it. Adverse selection
on 5m resolves is the rest. **Not positive. No crypto trader folder.**

## What Stage B/C did not see (feasibility)

108 FOK attempts: **24 crossed (22%), 84 did not (78%)**.

1. **Taker entry.** We never measured the ask. Cap was `last + 1.0 × predicted X+1 high`. Fills paid **within ~1¢ of that cap**. A 1¢ tighter cap would have missed **16/21** sports fills. The model’s “high” was not sitting on the book as size we could lift cheaply.

2. **Pricing.** Pair cost at last±tick is ~$1.02 signed, ~$1.00 after fill. There is **no complementary residual** on these mids. Directional FOK pays the spread (~1–2¢) as the whole edge budget.

3. **Maker exit.** GTC at +2¢ **does rest** (Taipei/Doge filled in ~20s when the tape ran). On 16/18 sports it **never traded in 4 minutes**, then FAK crossed −1 to −3¢. B’s 30-block high did not print. Criss-cross selected **flicker**, not “both books will complete a dollar.”

4. **RPC.** No market title → two Doges leaked. Gate must use slug/oracle/token, not the fill’s `market_title`.

No live deploy from this pass.
