# Walk-forward results

Generated 2026-09-18T12:53:01Z. Wall 1836.2s. Protocol [`WALKFORWARD.md`](WALKFORWARD.md).
Weekend sliver is **not** a pass of this protocol.

**Expected one-day next ~30d** (recent-scale fills × OOS $/fill, 28.0 recent test days): 9.5 trades, $20.68 total buys, +0.17 PnL

Dummy one-day: 8.2 trades, $31.04 total buys, -3.27 PnL

Folds with a test slice: **5**. Concatenated test P&L **$12.13** vs dummy **$-241.06**. OOS fills 681 (floor 500).

**Prints money under this protocol: yes.**

| fold | test P&L | dummy | fills | buys $ | entry | exit | double if | leftover $0 | liq | GTC exit |
| --- | --- | --- | --- | ---: | --- | --- | --- | ---: | ---: | ---: |
| 1 | +11.74 | −65.16 | 107 | 209 | last + 0×H1 | last + 0.5×H30 | always | 24% | 28% | 48% |
| 10 | +15.89 | −49.57 | 248 | 581 | last + 1.0×H1 | last + 0.5×H30 | edge ≥ 4 ticks | 21% | 39% | 40% |
| 11 | −0.07 | −18.94 | 59 | 118 | last + 0.5×H1 | last + 0.5×H30 | edge ≥ 2 ticks | 15% | 43% | 42% |
| 12 | −12.78 | −60.81 | 143 | 297 | last + 0.5×H1 | last + 0.5×H30 | always | 33% | 38% | 29% |
| 13 | −2.65 | −46.59 | 124 | 272 | last + 1.0×H1 | last + 0.5×H30 | edge ≥ 2 ticks | 30% | 36% | 35% |

`H1` / `H30` are Stage B’s predicted YES high (or low, if the bet is NO)
vs `last(X)`. Leftover $0 is **not** most of inventory (15–33%). Weighted
MSE of B on test is ~0.005–0.006. Latest slice is red (−$2.65). Shadow
week not run. This is research OOS, not live.
