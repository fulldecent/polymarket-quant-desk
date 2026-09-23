# Wide-spread maker pair (YES@last + NO@(1-last-3¢) ≤ 97¢)

Generated 2026-09-21T02:42:09Z.
Trigger X 93,554,599–93,784,999. Fee-free, cid span ≥ 2000 blocks (~50 min). Skip resolve within 200 blocks.
Maker: quotes strictly below both asks (do not take). Fill = maker-**sell** print at or below our bid on that outcome.

BBO rows (last in [0.15, 0.85]): **54,877**
Imminent-resolve skipped: 0
Can rest 1 tick inside both asks with sum ≤ 97¢: **17**
Of those, both quoted spreads ≥ 5¢: **5**

| window | both hit | YES hit | NO hit | both | wide-book both |
|---|---:|---:|---:|---:|---:|
| X+1..X+1 | 0 (0.00%) | 0 (0.00%) | 0 (0.00%) | 0 | 0 / 5 |
| X+1..X+5 | 0 (0.00%) | 0 (0.00%) | 0 (0.00%) | 0 | 0 / 5 |
| X+1..X+30 | 0 (0.00%) | 3 (17.65%) | 0 (0.00%) | 0 | 0 / 5 |
| X+1..X+120 | 0 (0.00%) | 3 (17.65%) | 0 (0.00%) | 0 | 0 / 5 |

Both-hit in **one block (X+1)** is the user's ask. If that rate is tiny, GTC pair is a longer rest, not a next-block lock.
