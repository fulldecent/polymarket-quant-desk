# Walk-forward results

Generated 2026-09-17T05:27:39Z. Wall 3180.2s. Protocol [`WALKFORWARD.md`](WALKFORWARD.md).
Weekend sliver is **not** a pass of this protocol.

**Expected one-day next ~30d** (recent-scale fills × OOS $/fill, 14.0 recent test days): 0.0 trades, $0.00 total buys, +0.00 PnL

Dummy one-day: 8.0 trades, $29.93 total buys, -2.80 PnL

Folds with a test slice: **8**. Concatenated test P&L **$1.65** vs dummy **$-302.39**. OOS fills 12 (floor 500).

**Prints money under this protocol: no.**

| fold | test P&L | dummy | fills | buys $ | ticket | Brier H1 |
| --- | --- | --- | --- | --- | --- | --- |
| 1 | 1.65 | -73.94 | 12 | 30.40 | k=5 pay2 z=60 | 0.1250 |
| 5 | 0.00 | -45.69 | 0 | 0.00 | skip | 0.1513 |
| 6 | 0.00 | -23.64 | 0 | 0.00 | k=5 pay2 z=30 | 0.1443 |
| 7 | 0.00 | -28.57 | 0 | 0.00 | skip | 0.1373 |
| 10 | 0.00 | -62.09 | 0 | 0.00 | skip | 0.1572 |
| 11 | 0.00 | -37.95 | 0 | 0.00 | skip | 0.1628 |
| 12 | 0.00 | -16.34 | 0 | 0.00 | skip | 0.1533 |
| 16 | 0.00 | -14.16 | 0 | 0.00 | skip | 0.1526 |

Go-live checklist (pre-registered): 3+ slices, ≥500 OOS fills, concat P&L>0 and beats dummy, neighbors, latest-slice Brier, dump not most of P&L, shadow week. This run is research OOS, not live.
