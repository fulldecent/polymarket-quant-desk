# Train snapshot (Stage A tape-touch ±k)

Generated 2026-09-15T18:47:32Z. Wall 160.6s. Trigger 93,707,608–93,789,879.

Stage A: inferred legal tick, last YES-equivalent, heads `k ∈ [-4,+4]`, `Z ∈ (1, 60)`. Pooled held-out AUC **0.933**.

Val: **< 1 activation/hour ⇒ hard fail, P&L not considered.**

| id | t0 | t_up | k_min | val /h | val fail | val P&L | test /h | test fail | test P&L |
| --- | --- | --- | --- | --- | --- | --- | --- | --- | --- |
| b0 | 0.20 | 0.25 | 1.0 | 360.33 | False | -4.56 | 354.23 | False | -1.88 |
| b1 | 0.23 | 0.32 | 1.1 | 288.26 | False | -4.61 | 278.69 | False | -3.83 |
| b2 | 0.10 | 0.32 | 1.2 | 524.91 | False | -4.83 | 512.46 | False | -1.93 |
| b3 | 0.16 | 0.30 | 1.1 | 385.65 | False | -4.51 | 372.61 | False | -1.86 |
| b4 | 0.18 | 0.30 | 1.4 | 347.67 | False | -4.15 | 337.90 | False | -1.86 |
| b5 | 0.12 | 0.29 | 1.2 | 463.56 | False | -4.67 | 467.55 | False | -1.66 |
| b6 | 0.23 | 0.30 | 1.2 | 296.05 | False | -4.61 | 282.77 | False | -1.53 |
| b7 | 0.12 | 0.28 | 1.4 | 477.19 | False | -4.67 | 485.92 | False | -1.66 |
| b8 | 0.16 | 0.40 | 1.8 | 324.30 | False | -4.61 | 321.57 | False | -4.16 |

**best_model** `b4` t0=0.18 t_up=0.30 k_min=1.4  val P&L -4.15  test P&L -1.86251935697151.

Log: `/Volumes/polymarket-quant-desk/scratch/directional-fok-v1/train_log.csv`
