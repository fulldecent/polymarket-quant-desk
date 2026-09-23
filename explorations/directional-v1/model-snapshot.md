# Directional FOK — model snapshot

Generated 2026-09-15T15:08:06Z by `run.py`. Features: last **100 blocks** only. No cooldown recency. Buckets are views, not family-killers.

- trigger X: 93,666,472–93,789,879 (~3.00 d)
- frontier: 93,789,999
- cooldown: 180 blocks
- elapsed: 11.5s
- parquet: `/Volumes/polymarket-quant-desk/scratch/directional-v1/attempts.parquet`

Attempts: **80,710** (26,908/day). Entry hit 2,743 (3.4%).

| state | n |
| --- | --- |
| miss | 77,967 |
| exit_full | 1,524 |
| liq_full | 166 |
| mixed | 116 |
| zero | 937 |

Headline P&L (all naive tickets): **-2,466.52** USDC (-0.0306/attempt)

| entered shares | share of q |
| --- | --- |
| GTC exit X+2..60 | 50.1% |
| dump X+61..120 | 5.9% |
| worthless | 44.0% |

## Test-window slices (not family obituaries)

A weak 15-block cut is not proof the feature is useless. Compare nearby buckets.

| slice | n | P&L | entry hit | mean q_zero/q |
| --- | --- | --- | --- | --- |
| naive all test | 23,095 | -675.87 | 3.0% | 38.7% |
| persist_floors_5 >= 3 | 60 | 2.34 | 48.3% | 6.9% |
| imbalance_15 > 0 | 18,523 | -570.72 | 3.4% | 36.6% |
| imbalance_100 > 0 | 18,626 | -588.60 | 3.4% | 37.0% |
| range_15 >= 3 ticks | 2,556 | -14.16 | 6.6% | 7.2% |
| range_100 >= 3 ticks | 4,725 | -49.13 | 5.6% | 9.4% |
| tbuy_15 > 0 | 18,745 | -578.87 | 3.4% | 36.3% |
| fee-free (last 100) | 20,177 | -644.00 | 2.6% | 48.6% |
| intersection persist+imb15+tbuy15 | 59 | 2.34 | 47.5% | 7.1% |
| HGB pnl_hat > 0 | 0 | 0.00 | 0.0% | 0.0% |
| mt_abs_med_15 <= 0.0000 | 12,705 | -426.23 | 3.7% | 37.4% |
| mt_abs_med_15 > 0.0000 | 5,872 | -152.72 | 2.9% | 34.0% |
| mt_abs_med_100 <= 0.0000 | 12,999 | -429.02 | 3.6% | 37.0% |

## HGB (absolute error, predict pnl, fire if ŷ > 0, flat m=1)

Train n=57,500  test n=23,095. Naive test P&L -675.87. Model fire n=0 P&L 0.00.

| |corr| vs pnl (test) | signed r | feature |
| --- | --- | --- |
| 0.026 | +0.026 | `n_print_blocks` |
| 0.023 | +0.023 | `tbuy_blocks_60` |
| 0.022 | +0.022 | `fee_bit` |
| 0.022 | +0.022 | `tbuy_blocks_30` |
| 0.022 | +0.022 | `tbuy_blocks_100` |
| 0.019 | +0.019 | `ret_proxy_30` |
| 0.019 | +0.019 | `ret_proxy_100` |
| 0.019 | +0.019 | `range_5` |
| 0.019 | -0.019 | `imbalance_100` |
| 0.018 | +0.018 | `range_100` |
| 0.018 | +0.018 | `ret_proxy_5` |
| 0.017 | +0.017 | `range_30` |

Kelly is not applied (flat m=1). Cap grid not run this pass.
