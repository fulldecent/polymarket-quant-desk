# Directional FOK — EDA snapshot

Generated 2026-09-15T14:38:24Z by `eda.py`.

Last 24 non-empty `fills_v1` partitions, blocks 93,550,000–93,789,999, 15,276,882 legs, scan 1.0s.

Maker-sell bars are the entry-take tape (interpretation C). Taker-buy bars are the resting-sell tape. Maker-buy bars are dump bids.

| metric | count |
| --- | --- |
| (block, condition, outcome) maker-sell bars | 886,492 |
| of which legal floor (≥5 sh and ≥$1.20) | 499,007 |
| taker-buy bars | 2,772,979 |
| maker-buy bars | 2,716,411 |

Among floor bars at block X (same condition and outcome):

| follow-on | count | share of X floors |
| --- | --- | --- |
| X+1 also has a floor | 87,918 | 17.6% |
| some taker-buy in X+2..X+60 (exit window) | 346,058 | 69.3% |
| some taker-buy in X+61..X+120 (liq window) | 256,278 | 51.4% |

X floors: 499,007. This is unconditional tape, not a model trigger. A print in the window is not a fill at a signed `P_exit`.

## Naive signals per day (180-block cooldown)

A naive opportunity is a `(block, condition)` with a legal maker-sell floor on at least one outcome (the baseline then picks the deeper side). After a fire on a condition at `X`, the next fire on that condition is `X+180` (~6.3 min). The cooldown starts at the signal, including FOK misses.

Window 240,000 blocks ≈ 5.83 days.

| | count | per day |
| --- | --- | --- |
| opportunities (no cooldown) | 468,802 | 80,366 |
| signals after 180-block cooldown | 155,393 | 26,639 |
| distinct conditions that fired | 30,286 | — |

Cooldown factor: 3.0× fewer tickets than ungated tape.
