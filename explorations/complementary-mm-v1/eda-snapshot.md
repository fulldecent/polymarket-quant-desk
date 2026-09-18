# Complementary MM — EDA snapshot

Generated 2026-09-14T16:02:37Z by `explorations/complementary-mm-v1/eda.py`.

Numbers are consumed on-chain volume, not L2 books. An opportunity is a floor cross: **≥ 5 shares and ≥ $1.20 worst-price notional** on each side being lifted. Counts are an **upper bound** on how often that floor order could have filled; they ignore competition, the strict-better-than-cap rule, and mint/merge tagging. Read with [`fill-model.md`](fill-model.md).

## Data inventory

From partition `metadata.json` files. No parquet scan.

| dataset | partitions | non-empty | rows | parquet | first block | last block | span |
| --- | --- | --- | --- | --- | --- | --- | --- |
| fills_v1 | 6,019 | 5,724 | 1,943,094,901 | 38.81 GB | 33,600,000 | 93,789,999 | 1463 d |
| condition_by_block_v1 | 6,019 | 5,724 | 335,537,257 | 7.65 GB | 33,600,000 | 93,789,999 | 1463 d |
| condition_by_10k_v1 | 6,019 | 5,728 | 15,213,934 | 0.87 GB | 33,600,000 | 93,789,999 | 1463 d |

Polygon ~2.1s/block. Last fills partition 93,780,000–93,789,999 (598,999 legs).

## Universe (`condition_by_10k_v1`, all partitions)

Scan time 4.0s.

| metric | value |
| --- | --- |
| distinct conditions | 2,255,814 |
| ever traded | 2,253,256 |
| resolved (in-sample) | 2,215,606 |
| traded and resolved | 2,213,048 |
| YES wins `['1','0']` (slot 0) | 866,405 |
| NO wins `['0','1']` (slot 1) | 1,336,167 |
| void/draw `['1','1']` | 13,030 |
| lifetime matched USDC | $44,958,805,484 |
| lifetime fee_usdc (sell-side net) | $23,897,030 |
| conditions with any fee | 1,045,484 |
| matched USDC on fee conditions | $21,477,734,359 |
| per-condition matched USDC p50 / p90 / p99 | $377 / $21,720 / $271,757 |

Silence between last 10K-with-fills and the resolution 10K (1 partition ≈ 5.8 hours). This is the hold-to-resolution tail.

| metric | value |
| --- | --- |
| resolved conditions with at least one fill | 2,213,048 |
| last-trade → resolve gap p50 (partitions) | 0.00 |
| last-trade → resolve gap p90 (partitions) | 1.00 |
| silent > 1 partition (~6h) | 185,520 |
| silent > 10 partitions (~2.4d) | 26,772 |

Fee-condition volume share: 47.8% of matched USDC.

Fee share of volume is the fraction of matched USDC on conditions that ever printed a USDC fee. Buy-side token fees are not in `fee_usdc`.

## Typical fill rows

First 12 legs of `/Volumes/polymarket-quant-desk/derived_data/fills_v1/1M=93000000/10K=93780000/data.parquet`.

YES price formula matches `condition_by_block_v1`: same-sign `gross_usdc / net_yes_tokens`, opposite-sign `1 + ratio`.

| block | idx | taker | yes tokens | USDC | fee | yes px | neg-risk |
| --- | --- | --- | --- | --- | --- | --- | --- |
| 93,780,000 | 0 | Y | 1.8868 | 1.0000 | 0.0000 | 0.5300 | N |
| 93,780,000 | 1 | N | -1.8868 | 0.8868 | 0.0000 | 0.5300 | N |
| 93,780,000 | 2 | Y | -6.8750 | 3.3000 | 0.0000 | 0.5200 | N |
| 93,780,000 | 3 | N | 6.8750 | 3.5750 | 0.0000 | 0.5200 | N |
| 93,780,000 | 4 | Y | 71.4286 | 30.0000 | 0.0000 | 0.4200 | N |
| 93,780,000 | 5 | N | -5.0000 | 2.9000 | 0.0000 | 0.4200 | N |
| 93,780,000 | 6 | N | -66.4286 | 38.5286 | 0.0000 | 0.4200 | N |
| 93,780,000 | 7 | Y | -6.5500 | -3.4060 | 0.1144 | 0.5200 | N |
| 93,780,000 | 8 | N | 6.5500 | 3.4060 | 0.0000 | 0.5200 | N |
| 93,780,000 | 9 | Y | -100.0000 | -0.4000 | 0.0159 | 0.0040 | Y |
| 93,780,000 | 10 | N | 100.0000 | 0.4000 | 0.0000 | 0.0040 | Y |
| 93,780,000 | 11 | Y | 2.5000 | 1.5000 | 0.0000 | 0.6000 | N |

## Microstructure — last 48 non-empty partitions

48 fills partitions, scan 7.0s, blocks 93,310,000–93,789,999 (~280.0 h).

| metric | value |
| --- | --- |
| fill legs | 31,671,062 |
| distinct conditions | 131,573 |
| sum |gross_usdc| (both legs, double-counts matches) | $1,241,053,548 |
| taker |gross_usdc| | $630,802,041 |
| fee_usdc | $878,842.61 |
| legs with fee_usdc > 0 | 2,006,731 |
| taker legs | 11,846,347 |
| NegRisk legs | 6,567,615 |
| |gross_usdc| p50 / p90 / p99 / mean | $3.35 / $36.12 / $528.66 / $39.19 |

Leg sides (`gross_usdc` sign × `net_yes_tokens` sign):

| side | legs | |USDC| | taker legs | taker |USDC| |
| --- | --- | --- | --- | --- |
| buy_no | 12,738,666 | $545,764,943 | 4,614,661 | $255,811,734 |
| buy_yes | 13,539,474 | $509,026,109 | 5,223,274 | $260,119,855 |
| sell_no | 2,248,959 | $88,422,680 | 948,643 | $50,104,122 |
| sell_yes | 3,143,963 | $97,839,816 | 1,059,769 | $64,766,331 |

Opportunity screen per `(block, condition)`: a side is a floor cross if consumed depth is **≥ 5 shares and ≥ $1.20 USDC** (proxy for `N × P_worst`). Taker-buy is interpretation A in fill-model.md; maker-sell is interpretation C.

| metric | count |
| --- | --- |
| (block, condition) bars with any fill | 6,032,759 |
| YES taker-buy floor | 2,068,022 |
| NO taker-buy floor | 2,131,402 |
| both taker-buy floors same block | 407,839 |
| YES maker-sell (ask) floor | 501,499 |
| NO maker-sell (ask) floor | 511,944 |
| both ask floors same block | 64,427 |
| both taker floors and VWAP pair < $1.00 | 84,162 |
| both taker floors and VWAP pair < $0.99 | 41,242 |
| both taker floors and VWAP pair < $0.98 | 25,208 |
| mean pair cost (all-print VWAP, both floors) | 1.0094 |
| pair cost p10 / p50 / p90 | 0.9900 / 1.0100 / 1.0300 |

Depth multiples of the floor (5 shares and $1.20, then 2× and 4×). 4× is the Kelly size cap: the largest positive stake is at most four times the smallest. `both / YES` is same-block completion at that multiple.

| size | YES bars | NO bars | both same block | both / YES |
| --- | --- | --- | --- | --- |
| 1× floor | 2,068,022 | 2,131,402 | 407,839 | 19.7% |
| 2× floor | 1,591,193 | 1,623,496 | 298,064 | 18.7% |
| 4× floor | 1,177,722 | 1,186,146 | 202,218 | 17.2% |

Sided exposure. Each YES floor bar is a synthetic aggressive fill. Complements are NO floor bars in `[0, 30]` blocks.

| metric | count | share of YES floor bars |
| --- | --- | --- |
| YES floor bars | 2,068,022 | 100% |
| NO floor same block | 407,839 | 19.7% |
| NO floor in +1..+15 (grace) | 560,364 | 27.1% |
| NO floor in +1..+30 (model window) | 689,894 | 33.4% |
| NO floor anywhere in [0, 30] | 1,097,733 | 53.1% |
| NO miss through +30 | 970,289 | 46.9% |

Reverse (aggressive = NO):

| metric | count | share of NO floor bars |
| --- | --- | --- |
| NO floor bars | 2,131,402 | 100% |
| YES floor same block | 407,839 | 19.1% |
| YES floor in +1..+30 | 665,969 | 31.2% |
| YES miss through +30 | 1,057,594 | 49.6% |

Floor-clip VWAP walk on bars that already clear the floor on **both** outcomes in the same block. Walk cheapest shares until `N ≥ 5` and `N × p_marginal ≥ $1.20`. Pair cost = YES VWAP + NO VWAP of that prefix. Interpretation A, no signed cap, no competition κ. This is the merge edge at the **minimum** order, not at a model-chosen size.

| metric | value |
| --- | --- |
| dual bars with a floor walk on both books | 407,838 |
| mean pair cost | 0.9916 |
| p10 / p50 / p90 pair cost | 0.9500 / 1.0000 / 1.0166 |
| pair < $1.00 | 182,225 (44.7%) |
| pair < $0.99 | 116,313 (28.5%) |
| pair < $0.98 | 84,237 (20.7%) |
| pair < $0.97 | 64,432 (15.8%) |
| mean edge when pair < $1 (per complete share) | $0.0366 |

## Microstructure — older slice (12 partitions near block 85,720,000)

12 fills partitions, scan 4.3s, blocks 85,720,000–85,839,999 (~70.0 h).

| metric | value |
| --- | --- |
| fill legs | 22,028,517 |
| distinct conditions | 37,324 |
| sum |gross_usdc| (both legs, double-counts matches) | $709,311,135 |
| taker |gross_usdc| | $373,776,963 |
| fee_usdc | $285,735.07 |
| legs with fee_usdc > 0 | 1,154,148 |
| taker legs | 8,077,966 |
| NegRisk legs | 3,682,699 |
| |gross_usdc| p50 / p90 / p99 / mean | $3.14 / $29.70 / $429.00 / $32.20 |

Leg sides (`gross_usdc` sign × `net_yes_tokens` sign):

| side | legs | |USDC| | taker legs | taker |USDC| |
| --- | --- | --- | --- | --- |
| buy_no | 9,142,280 | $302,556,991 | 3,389,072 | $167,577,871 |
| buy_yes | 9,350,272 | $243,353,167 | 3,100,840 | $109,039,476 |
| sell_no | 1,619,838 | $104,934,795 | 715,914 | $62,607,570 |
| sell_yes | 1,916,127 | $58,466,182 | 872,140 | $34,552,046 |

Opportunity screen per `(block, condition)`: a side is a floor cross if consumed depth is **≥ 5 shares and ≥ $1.20 USDC** (proxy for `N × P_worst`). Taker-buy is interpretation A in fill-model.md; maker-sell is interpretation C.

| metric | count |
| --- | --- |
| (block, condition) bars with any fill | 3,073,221 |
| YES taker-buy floor | 965,192 |
| NO taker-buy floor | 1,186,551 |
| both taker-buy floors same block | 256,723 |
| YES maker-sell (ask) floor | 330,404 |
| NO maker-sell (ask) floor | 318,221 |
| both ask floors same block | 42,585 |
| both taker floors and VWAP pair < $1.00 | 30,199 |
| both taker floors and VWAP pair < $0.99 | 14,256 |
| both taker floors and VWAP pair < $0.98 | 8,239 |
| mean pair cost (all-print VWAP, both floors) | 1.0128 |
| pair cost p10 / p50 / p90 | 0.9988 / 1.0100 / 1.0291 |

Depth multiples of the floor (5 shares and $1.20, then 2× and 4×). 4× is the Kelly size cap: the largest positive stake is at most four times the smallest. `both / YES` is same-block completion at that multiple.

| size | YES bars | NO bars | both same block | both / YES |
| --- | --- | --- | --- | --- |
| 1× floor | 965,192 | 1,186,551 | 256,723 | 26.6% |
| 2× floor | 739,925 | 926,090 | 190,290 | 25.7% |
| 4× floor | 549,898 | 701,071 | 136,038 | 24.7% |

Sided exposure. Each YES floor bar is a synthetic aggressive fill. Complements are NO floor bars in `[0, 30]` blocks.

| metric | count | share of YES floor bars |
| --- | --- | --- |
| YES floor bars | 965,192 | 100% |
| NO floor same block | 256,723 | 26.6% |
| NO floor in +1..+15 (grace) | 285,923 | 29.6% |
| NO floor in +1..+30 (model window) | 345,234 | 35.8% |
| NO floor anywhere in [0, 30] | 601,957 | 62.4% |
| NO miss through +30 | 363,235 | 37.6% |

Reverse (aggressive = NO):

| metric | count | share of NO floor bars |
| --- | --- | --- |
| NO floor bars | 1,186,551 | 100% |
| YES floor same block | 256,723 | 21.6% |
| YES floor in +1..+30 | 332,682 | 28.0% |
| YES miss through +30 | 597,146 | 50.3% |

Floor-clip VWAP walk on bars that already clear the floor on **both** outcomes in the same block. Walk cheapest shares until `N ≥ 5` and `N × p_marginal ≥ $1.20`. Pair cost = YES VWAP + NO VWAP of that prefix. Interpretation A, no signed cap, no competition κ. This is the merge edge at the **minimum** order, not at a model-chosen size.

| metric | value |
| --- | --- |
| dual bars with a floor walk on both books | 256,723 |
| mean pair cost | 0.9963 |
| p10 / p50 / p90 pair cost | 0.9620 / 1.0078 / 1.0153 |
| pair < $1.00 | 96,186 (37.5%) |
| pair < $0.99 | 61,041 (23.8%) |
| pair < $0.98 | 42,900 (16.7%) |
| pair < $0.97 | 31,774 (12.4%) |
| mean edge when pair < $1 (per complete share) | $0.0330 |

## What this does **not** measure

- Trigger patterns (the model). These counts are unconditional floor depth, not 'after a signal at block X'.
- Signed size above the floor (Kelly 1×–4×). The walk is the minimum viable order only.
- Strict-better-than-cap fills (needs a signed `P` from block X).
- Competition multiplier κ.
- Buy-side token fees.
- Inventory mark to resolution on one-sided clips (needs the simulator + payout join).
- Live grace-period cancel denials (needs CLOB `oas` / timing logs).
