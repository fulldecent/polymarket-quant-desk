# Stage C sequential (frozen Stage B + extras)

Generated 2026-09-15T23:57:08Z. Wall 354.4s. Trigger 93,625,336–93,789,879. Stage A 3-of-8. Frozen Stage B `stage_b.joblib` (67 cols).

Stage B outputs (`p_act_1`, `p_bar[±k, H60]`) are **inputs**. Extra
last-100-block features Stage B does not emit: unique-account
`top_share` / `hhi` / `n_acct` (window and at `X`), fee bit.
Selective decoder: frozen B `p_act_1` plus a **train-only 30-block**
activity/barrier head. Fire only if FOK-bar, 30-block liquidity, and
a 2/3/4-tick range all clear. Pricing: passive / pay1 / pay2 ticks
on the FOK. GTC window 30 or 60. 1/hour is a floor. Train ranks,
val
promotes only if **≥1 activation/hour** and val P&L **> 0**.
Test never selects. Always-skip is $0. Naive is imbalance-side
`+0 / +1` tick after the same cooldown.

Split train 43,924  val 18,400  test 17,540
(embargo 120). 3-of-8 dropped 1,207,197.

## Headline

**Prints on test: $4.80** at 25.13/h (556 tickets, hit 0.086, zero 0.035).
Val that promoted it is **+$0.29** — the first green val, still a sliver.
H30 activity pos rate 0.95 so liquidity did not select; **k=4 in 30
blocks + pay2 + skip-fee + GTC=30** did. 25/h is 25× the 1/hour floor.

Promoted params: t_fill=0.7, t_liq=0.55, t_range=0.6, k_need=4, price=pay2, whale=any, skip_fee=True, size_mult=1.1, exit_end=30

| split | n | /hour | P&L $ | FOK hit | leftover $0 share |
| --- | --- | --- | --- | --- | --- |
| always-skip | 0 | 0.00 | 0.00 | — | — |
| naive val | 4,692 | 227.84 | -64.80 | 0.020 | 0.352 |
| naive test | 3,752 | 169.60 | -32.59 | 0.016 | 0.277 |
| C val | 503 | 24.43 | 0.29 | 0.068 | 0.000 |
| C test | 556 | 25.13 | 4.80 | 0.086 | 0.035 |

## Train grid leaders (not used to pick if val is red)

| t_fill | t_liq | t_range | k | price | z | /hour | train P&L |
| --- | --- | --- | --- | --- | --- | --- | --- |
| 0.70 | 0.55 | 0.60 | 4 | pay2 | 60 | 25.42 | 23.99 |
| 0.70 | 0.75 | 0.60 | 4 | pay2 | 60 | 25.42 | 23.99 |
| 0.70 | 0.55 | 0.60 | 4 | pay2 | 60 | 25.40 | 23.99 |
| 0.70 | 0.75 | 0.60 | 4 | pay2 | 60 | 25.40 | 23.99 |
| 0.70 | 0.55 | 0.60 | 4 | pay1 | 60 | 25.42 | 23.34 |
| 0.70 | 0.75 | 0.60 | 4 | pay1 | 60 | 25.42 | 23.34 |
| 0.70 | 0.55 | 0.60 | 4 | pay1 | 60 | 25.40 | 23.34 |
| 0.70 | 0.75 | 0.60 | 4 | pay1 | 60 | 25.40 | 23.34 |
| 0.70 | 0.55 | 0.60 | 4 | pay2 | 30 | 25.42 | 23.07 |
| 0.70 | 0.75 | 0.60 | 4 | pay2 | 30 | 25.42 | 23.07 |
| 0.70 | 0.55 | 0.60 | 4 | pay2 | 30 | 25.40 | 23.07 |
| 0.70 | 0.75 | 0.60 | 4 | pay2 | 30 | 25.40 | 23.07 |

## Val scores of those train survivors

| t_fill | t_liq | t_range | k | price | z | /hour | val P&L |
| --- | --- | --- | --- | --- | --- | --- | --- |
| 0.70 | 0.55 | 0.60 | 4 | pay2 | 30 | 24.43 | 0.29 |
| 0.70 | 0.75 | 0.60 | 4 | pay2 | 30 | 24.43 | 0.29 |
| 0.70 | 0.55 | 0.60 | 4 | pay2 | 30 | 24.38 | 0.29 |
| 0.70 | 0.75 | 0.60 | 4 | pay2 | 30 | 24.38 | 0.29 |
| 0.70 | 0.55 | 0.60 | 4 | pay1 | 60 | 24.43 | -2.45 |
| 0.70 | 0.75 | 0.60 | 4 | pay1 | 60 | 24.43 | -2.45 |
| 0.70 | 0.55 | 0.60 | 4 | pay1 | 60 | 24.38 | -2.45 |
| 0.70 | 0.75 | 0.60 | 4 | pay1 | 60 | 24.38 | -2.45 |
| 0.70 | 0.55 | 0.60 | 4 | pay2 | 60 | 24.43 | -2.83 |
| 0.70 | 0.75 | 0.60 | 4 | pay2 | 60 | 24.43 | -2.83 |
| 0.70 | 0.55 | 0.60 | 4 | pay2 | 60 | 24.38 | -2.83 |
| 0.70 | 0.75 | 0.60 | 4 | pay2 | 60 | 24.38 | -2.83 |

Search log: `/Volumes/polymarket-quant-desk/scratch/directional-fok-v1/stage_c_search.csv`.

Stage B stays frozen. This card is Stage C.
