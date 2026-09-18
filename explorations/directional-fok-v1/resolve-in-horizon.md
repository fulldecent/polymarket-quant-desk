# Stage A events vs resolution in the P&L span

Generated 2026-09-16T01:19:57Z. Wall 2.1s.
Trigger `93,625,336`–`93,789,879` (~4 days). Stage A gate **3 of last 8**.
Resolution from `condition_by_10k_v1.resolved_block` (on-chain
`ConditionResolution`). A resolve inside `X+1..X+H` is infinite
liquidity at 1.0 / 0.0 for P&L over that horizon. Nothing after `X+H`
is used.

Resolutions loaded in nearby 10Ks: 44,177.
Candidate fill-blocks: 1,996,298. After 3-of-8: **789,101**.
Of those, condition has a known resolve (any future in loaded files): 595,656 (75.485%).

## Stage A events with resolve in the next H blocks

| horizon | n | of Stage A events |
| --- | --- | --- |
| same block (`d=0`) | 7 | 0.001% |
| `X+1` | 4 | 0.001% |
| `X+1..X+30` | 623 | 0.079% |
| `X+1..X+60` | 13,967 | 1.770% |
| `X+1..X+120` | 112,068 | 14.202% |

Gap `resolved_block − X` among Stage A events whose condition resolves
at or after X (loaded files): n=595,656  min=0  p50=244  p90=7432  p99=38970  max=153657.

## Does the CLOB go quiet before resolve?

For every resolution in the loaded 10Ks that has at least one fill
in the lookback/trigger/horizon files: gap from **that condition's
last fill** to `resolved_block`. `fills_v1` already drops fills after
resolution, so this is last print → oracle.

Conditions with last-fill and resolve: 40,019.
Last fill → resolve in 1..60 blocks: 4,052 (10.125%).
Last fill → resolve in 1..120 blocks: 8,259 (20.638%).
Gap last-fill → resolve: n=40,019  min=0  p50=4020  p90=15607  p99=63254  max=155603.

If those percentages are tiny, redeem-at-1.0 inside a 60-block GTC
is rare on this tape. If they are not, Stage B/C must treat resolve
in-horizon as a touch at 1.0 / 0.0.
