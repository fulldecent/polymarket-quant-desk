# Naive baseline snapshot

**DEAD as a research line.** `yes_only` (28.5%) + `no_only` (32.3%) =
**60.8% one-sided**. That mass is why complementary MM is retired.
Successor: [`../directional-fok-v1/`](../directional-fok-v1/).

Generated 2026-09-15T01:20:55Z by `explorations/complementary-mm-v1/sim.py`.

Interpretation **C** (maker-sell asks). m=1. Fire if block X floor-walk pair cost < 0.99 on both outcomes. Caps = close_yes ± tick. Aggressive = more liquid ask at X. One-sided exit: complement buy at signed cap until 428 blocks (~15 min, after 30s grace), then market-sell, **$0 at 3 days**.

- trigger X: 93,378,610–93,666,561
- data frontier: 93,789,999
- kappa: 1
- elapsed: 7.2s
- parquet: `/Volumes/polymarket-quant-desk/scratch/complementary-mm-v1/naive_attempts.parquet`

Attempts: **9,058**

| state | n | P&L |
| --- | --- | --- |
| both | 2,524 (27.9%) | 636.80 |
| yes_only | 2,584 (28.5%) | -309.16 |
| no_only | 2,924 (32.3%) | -349.34 |
| miss | 1,026 (11.3%) | 0.00 |

| inventory kind (when leftover) | n |
| --- | --- |
| complement rest | 3,362 |
| resolve | 2,723 |
| dump | 78 |
| zero (3d miss) | 4 |

| P&L | USDC |
| --- | --- |
| merge | 630.90 |
| inventory | -652.60 |
| fees (included in the marks) | 221.80 |
| **headline** | **-21.70** |

Fee-market attempts: 9,058 (P&L -21.70). Fee-free: 0 (P&L 0.00).

Headline $/attempt: -0.0024

Filled-both pair cost p50: 0.9700  share < 1.00: 88.7%

This is the naive dual-floor baseline, not a trained model. Go/no-go still requires interpretation C to stay green after inventory, including dump-miss = $0.
