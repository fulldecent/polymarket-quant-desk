# Stage B freeze (tape-touch)

Generated 2026-09-16T16:43:48Z. Wall 135.9s. Trigger 80,282,490–93,789,879. Stage A gate **3 of last 8**.

## Modeling

Two binary **HistGradientBoosting** classifiers (sklearn), log-loss,
early stopping on a 10% train holdout. **activity** (`k=0`) and
**barrier** (`k≠0`). `H1` = block `X+1`. `H60` = `X+2..X+60`
(GTC window, not the FOK bar). Tick inferred from last 100 blocks.
Both directions via `±k` on one last YES price. `k` and horizon `Z`
are inputs. Not a neural net. Not a joint ordinal. Size / FOK stay
in Stage C. On-chain resolve **inside** H1 / H60 is infinite
liquidity at settlement (1.0 / 0.0 / 0.5). Resolve after the
horizon is unused. CLOB-only card:
[`stage-b-snapshot-clob-only.md`](stage-b-snapshot-clob-only.md).

## Features: all, then select

All **78** last-100-block condition-level columns in
**9** families, built from `fills_v1` only. No 10K
account tables, no cooldown recency, no `block > X`. Fit all on
train. Select families on val by permuting each family and dropping
it if Δ activity-H1 AUC **and** Δ hard-H60-±1 AUC are both `< 0.002`.
`level` (last, tick, min_p, hour) is never dropped. Frozen set is
**selected** if its val primary is within 0.003 of all-features,
else **all**. Test is confirmation, not selection.

Primary val metric: mean hard-subset H60 AUC at `k=±1` (lookback range has not already cleared a 1-tick barrier).

Split: train 111,516  val 46,933  test 45,297 (event-level, embargo 120 blocks). Events 203,815; 3-of-8 skipped in collect 0.

Inferred ticks: 0.0001×138793, 0.01×62419, 0.001×2463, 0.005×125, 0.0025×12, 0.1×3

Val stability (first vs second half): activity H1 AUC 0.811 / 0.812; barrier +1 H60 0.840 / 0.794; primary (hard ±1 H60) 0.727 / 0.718.

**Frozen:** `selected` (54 cols, families ['level', 'persist', 'depth', 'counts', 'range', 'return', 'adverse', 'venue']). Val primary 0.720 (all 0.720, selected 0.720).

Dropped families: flow

## Capacity check (val, not used to freeze)

Simple rule: activity ranked by `n_15`; barrier ranked by lookback
ticks already traveled toward the barrier (`dist_high` / `dist_low`).
Linear: `StandardScaler + LogisticRegression` on all columns, fit on
a train subsample. If HGB is no better than the range rule on the
**hard** subset, Stage B is not finding breakouts.

| model | activity H1 AUC | +1 H60 AUC | hard ±1 H60 (primary) |
| --- | --- | --- | --- |
| simple rule | 0.791 | 0.660 | 0.497 |
| linear | 0.801 | 0.768 | 0.657 |
| HGB all | 0.811 | 0.820 | 0.720 |
| HGB selected | 0.812 | 0.820 | 0.720 |

## Family permutation (val ΔAUC; higher = family mattered)

| family | Δ act H1 | Δ act H60 | Δ +1 H60 | Δ hard ±1 | keep |
| --- | --- | --- | --- | --- | --- |
| level | 0.001 | 0.008 | 0.029 | 0.159 | yes |
| persist | 0.025 | 0.024 | 0.001 | -0.004 | yes |
| depth | 0.003 | 0.001 | -0.000 | -0.001 | yes |
| counts | 0.196 | 0.009 | 0.005 | 0.004 | yes |
| range | 0.004 | 0.007 | 0.135 | 0.135 | yes |
| flow | 0.001 | 0.006 | 0.011 | 0.000 | no |
| return | 0.001 | 0.001 | 0.007 | 0.006 | yes |
| adverse | 0.002 | 0.057 | 0.005 | 0.003 | yes |
| venue | 0.000 | 0.001 | 0.002 | 0.004 | yes |

## Activity (`k=0`) — frozen

| split/head | n | pos | AUC | Brier |
| --- | --- | --- | --- | --- |
| val H1 | 46,933 | 0.597 | 0.812 | 0.1728 |
| val H30 | 46,933 | 0.967 | 0.931 | 0.0257 |
| val H60 | 46,933 | 0.981 | 0.955 | 0.0152 |
| test H1 | 45,297 | 0.623 | 0.831 | 0.1618 |
| test H30 | 45,297 | 0.961 | 0.926 | 0.0300 |
| test H60 | 45,297 | 0.976 | 0.939 | 0.0196 |

## Barrier (all val) — frozen

| k | H1 pos | H1 AUC | H1 Brier | H60 pos | H60 AUC | H60 Brier |
| --- | --- | --- | --- | --- | --- | --- |
| -4 | 0.215 | 0.860 | 0.1163 | 0.617 | 0.876 | 0.1263 |
| -3 | 0.217 | 0.855 | 0.1188 | 0.630 | 0.867 | 0.1302 |
| -2 | 0.220 | 0.847 | 0.1220 | 0.651 | 0.852 | 0.1352 |
| -1 | 0.234 | 0.822 | 0.1329 | 0.702 | 0.805 | 0.1476 |
| +1 | 0.232 | 0.824 | 0.1328 | 0.684 | 0.820 | 0.1443 |
| +2 | 0.219 | 0.846 | 0.1226 | 0.644 | 0.852 | 0.1365 |
| +3 | 0.216 | 0.854 | 0.1196 | 0.626 | 0.865 | 0.1323 |
| +4 | 0.214 | 0.858 | 0.1177 | 0.613 | 0.876 | 0.1278 |

## Barrier hard subset (val): lookback range < |k| ticks

If the last 100 blocks already swung more than `k` ticks, `+k` is easy.
This slice is the breakout case. **This is the Stage B question.**

| k | H1 n | H1 pos | H1 AUC | H60 n | H60 pos | H60 AUC |
| --- | --- | --- | --- | --- | --- | --- |
| -4 | 8,144 | 0.002 | 0.782 | 8,144 | 0.047 | 0.820 |
| -3 | 7,308 | 0.002 | 0.663 | 7,308 | 0.048 | 0.800 |
| -2 | 6,086 | 0.003 | 0.632 | 6,086 | 0.051 | 0.760 |
| -1 | 3,339 | 0.007 | 0.693 | 3,339 | 0.091 | 0.757 |
| +1 | 3,339 | 0.004 | 0.724 | 3,339 | 0.063 | 0.684 |
| +2 | 6,086 | 0.002 | 0.582 | 6,086 | 0.045 | 0.728 |
| +3 | 7,308 | 0.001 | 0.727 | 7,308 | 0.047 | 0.780 |
| +4 | 8,144 | 0.001 | 0.818 | 8,144 | 0.049 | 0.802 |

## Tick strata (frozen val)

| tick | n H1 | act H1 AUC | n +1 H60 | +1 H60 AUC |
| --- | --- | --- | --- | --- |
| 0.0001 | 32,557 | 0.820 | 32,557 | 0.742 |
| 0.01 | 13,835 | 0.747 | 13,835 | 0.837 |
| 0.001 | 503 | 0.703 | 503 | 0.825 |
| 0.005 | 29 | 0.721 | 29 | 0.857 |
| 0.0025 | 7 | — | 7 | — |
| 0.1 | 2 | — | 2 | — |

## Calibration (frozen val, quantile bins)

Activity H1:

| mean p | mean y | n |
| --- | --- | --- |
| 0.189 | 0.206 | 4,694 |
| 0.298 | 0.292 | 4,693 |
| 0.383 | 0.370 | 4,693 |
| 0.462 | 0.455 | 4,693 |
| 0.543 | 0.539 | 4,693 |
| 0.634 | 0.626 | 4,694 |
| 0.733 | 0.720 | 4,693 |
| 0.838 | 0.818 | 4,693 |
| 0.954 | 0.947 | 4,693 |
| 0.998 | 0.998 | 4,694 |

Barrier +1 H60:

| mean p | mean y | n |
| --- | --- | --- |
| 0.092 | 0.075 | 4,694 |
| 0.384 | 0.320 | 4,693 |
| 0.611 | 0.605 | 4,693 |
| 0.716 | 0.712 | 4,693 |
| 0.785 | 0.759 | 4,693 |
| 0.827 | 0.804 | 4,694 |
| 0.857 | 0.843 | 4,693 |
| 0.881 | 0.878 | 4,693 |
| 0.902 | 0.903 | 4,693 |
| 0.927 | 0.941 | 4,694 |

## Test confirmation (not used to pick the freeze)

| k | H1 AUC | H60 AUC | H60 hard AUC |
| --- | --- | --- | --- |
| -4 | 0.852 | 0.882 | 0.900 |
| -3 | 0.848 | 0.876 | 0.904 |
| -2 | 0.841 | 0.866 | 0.899 |
| -1 | 0.825 | 0.849 | 0.921 |
| +1 | 0.825 | 0.855 | 0.925 |
| +2 | 0.840 | 0.870 | 0.904 |
| +3 | 0.847 | 0.877 | 0.910 |
| +4 | 0.851 | 0.885 | 0.901 |

Models: `/Volumes/polymarket-quant-desk/scratch/directional-fok-v1/stage_b.joblib`.

Stage B freeze lives on this card. Stage C is the sequential decoder.
