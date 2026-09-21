# Fee-free complementary-MM snapshot

Fee-charging conditions dropped (5m crypto factories). Pair-cost fire `< 0.99` is Stage A.
Generated 2026-09-20T18:58:01Z by `explorations/complementary-mm-v1/sim.py`.

Interpretation **C** (maker-sell asks). m=1. Fire if block X floor-walk pair cost < 0.99 on both outcomes. Caps = close_yes ± tick. Aggressive = more liquid ask at X. One-sided exit: complement buy at signed cap until 428 blocks (~15 min, after 30s grace), then market-sell, **$0 at 3 days**.

- trigger X: 93,502,018–93,666,561
- data frontier: 93,789,999
- kappa: 1
- elapsed: 1.3s
- parquet: (none)

Attempts: **0**

| state | n | P&L |
| --- | --- | --- |
| both | 0 | 0.00 |
| yes_only | 0 | 0.00 |
| no_only | 0 | 0.00 |
| miss | 0 | 0.00 |

| inventory kind (when leftover) | n |
| --- | --- |
| complement rest | 0 |
| resolve | 0 |
| dump | 0 |
| zero (3d miss) | 0 |

| P&L | USDC |
| --- | --- |
| merge | 0.00 |
| inventory | 0.00 |
| fees (included in the marks) | 0.00 |
| **headline** | **0.00** |

Fee-market attempts: 0 (P&L 0.00). Fee-free: 0 (P&L 0.00).

This is the naive dual-floor baseline, not a trained model. Go/no-go still requires interpretation C to stay green after inventory, including dump-miss = $0.
