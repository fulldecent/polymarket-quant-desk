# Stage B freeze (tape-touch)

Generated 2026-09-16T02:01:23Z. Wall 872.7s. Trigger 93,625,336–93,789,879. Stage A gate **3 of last 8**.

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

Split: train 137,204  val 60,456  test 51,966 (event-level, embargo 120 blocks). Candidates 1,996,298; 3-of-8 dropped 1,207,197. If the alive set is larger than `--max-events`, events are **strided across the trigger span** (not a time prefix).

Inferred ticks: 0.0001×127624, 0.01×114090, 0.001×8114, 0.005×126, 0.0025×43, 0.1×3

Val stability (first vs second half): activity H1 AUC 0.864 / 0.852; barrier +1 H60 0.890 / 0.893; primary (hard ±1 H60) 0.955 / 0.914.

**Frozen:** `selected` (67 cols, families ['level', 'persist', 'depth', 'counts', 'range', 'flow', 'adverse']). Val primary 0.934 (all 0.933, selected 0.934).

Dropped families: return, venue

## Capacity check (val, not used to freeze)

Simple rule: activity ranked by `n_15`; barrier ranked by lookback
ticks already traveled toward the barrier (`dist_high` / `dist_low`).
Linear: `StandardScaler + LogisticRegression` on all columns, fit on
a train subsample. If HGB is no better than the range rule on the
**hard** subset, Stage B is not finding breakouts.

| model | activity H1 AUC | +1 H60 AUC | hard ±1 H60 (primary) |
| --- | --- | --- | --- |
| simple rule | 0.787 | 0.757 | 0.696 |
| linear | 0.847 | 0.841 | 0.885 |
| HGB all | 0.859 | 0.893 | 0.933 |
| HGB selected | 0.859 | 0.892 | 0.934 |

## Family permutation (val ΔAUC; higher = family mattered)

| family | Δ act H1 | Δ act H60 | Δ +1 H60 | Δ hard ±1 | keep |
| --- | --- | --- | --- | --- | --- |
| level | 0.003 | 0.016 | 0.073 | 0.125 | yes |
| persist | 0.135 | 0.088 | 0.002 | 0.003 | yes |
| depth | 0.007 | 0.003 | 0.000 | 0.002 | yes |
| counts | 0.090 | 0.002 | 0.003 | 0.001 | yes |
| range | 0.003 | 0.004 | 0.093 | 0.011 | yes |
| flow | 0.005 | 0.008 | 0.012 | 0.007 | yes |
| return | 0.001 | 0.001 | 0.004 | 0.001 | no |
| adverse | 0.002 | 0.028 | 0.006 | 0.000 | yes |
| venue | 0.000 | 0.001 | 0.001 | 0.001 | no |

## Activity (`k=0`) — frozen

| split/head | n | pos | AUC | Brier |
| --- | --- | --- | --- | --- |
| val H1 | 60,456 | 0.627 | 0.859 | 0.1466 |
| val H60 | 60,456 | 0.970 | 0.951 | 0.0230 |
| test H1 | 51,966 | 0.682 | 0.874 | 0.1332 |
| test H60 | 51,966 | 0.977 | 0.962 | 0.0179 |

## Barrier (all val) — frozen

| k | H1 pos | H1 AUC | H1 Brier | H60 pos | H60 AUC | H60 Brier |
| --- | --- | --- | --- | --- | --- | --- |
| -4 | 0.138 | 0.893 | 0.0840 | 0.448 | 0.916 | 0.1161 |
| -3 | 0.140 | 0.887 | 0.0865 | 0.465 | 0.908 | 0.1206 |
| -2 | 0.146 | 0.876 | 0.0910 | 0.489 | 0.900 | 0.1245 |
| -1 | 0.161 | 0.850 | 0.1028 | 0.538 | 0.882 | 0.1316 |
| +1 | 0.166 | 0.851 | 0.1047 | 0.533 | 0.892 | 0.1246 |
| +2 | 0.151 | 0.877 | 0.0929 | 0.492 | 0.903 | 0.1211 |
| +3 | 0.146 | 0.886 | 0.0889 | 0.470 | 0.910 | 0.1181 |
| +4 | 0.143 | 0.893 | 0.0861 | 0.453 | 0.917 | 0.1141 |

## Barrier hard subset (val): lookback range < |k| ticks

If the last 100 blocks already swung more than `k` ticks, `+k` is easy.
This slice is the breakout case. **This is the Stage B question.**

| k | H1 n | H1 pos | H1 AUC | H60 n | H60 pos | H60 AUC |
| --- | --- | --- | --- | --- | --- | --- |
| -4 | 20,134 | 0.001 | 0.862 | 20,134 | 0.032 | 0.929 |
| -3 | 18,867 | 0.001 | 0.866 | 18,867 | 0.029 | 0.925 |
| -2 | 17,292 | 0.001 | 0.901 | 17,292 | 0.025 | 0.922 |
| -1 | 14,594 | 0.001 | 0.896 | 14,594 | 0.025 | 0.930 |
| +1 | 14,594 | 0.001 | 0.958 | 14,594 | 0.018 | 0.939 |
| +2 | 17,292 | 0.001 | 0.933 | 17,292 | 0.024 | 0.922 |
| +3 | 18,867 | 0.001 | 0.912 | 18,867 | 0.028 | 0.923 |
| +4 | 20,134 | 0.001 | 0.929 | 20,134 | 0.034 | 0.928 |

## Tick strata (frozen val)

| tick | n H1 | act H1 AUC | n +1 H60 | +1 H60 AUC |
| --- | --- | --- | --- | --- |
| 0.0001 | 29,253 | 0.828 | 29,253 | 0.788 |
| 0.01 | 29,136 | 0.873 | 29,136 | 0.913 |
| 0.001 | 2,037 | 0.942 | 2,037 | 0.990 |
| 0.005 | 26 | 0.621 | 26 | 0.709 |
| 0.0025 | 2 | — | 2 | — |
| 0.1 | 2 | — | 2 | — |

## Calibration (frozen val, quantile bins)

Activity H1:

| mean p | mean y | n |
| --- | --- | --- |
| 0.192 | 0.174 | 6,046 |
| 0.285 | 0.287 | 6,045 |
| 0.362 | 0.369 | 6,046 |
| 0.438 | 0.448 | 6,045 |
| 0.534 | 0.524 | 6,046 |
| 0.662 | 0.663 | 6,046 |
| 0.854 | 0.854 | 6,045 |
| 0.965 | 0.966 | 6,045 |
| 0.991 | 0.990 | 5,978 |
| 0.997 | 0.992 | 6,114 |

Barrier +1 H60:

| mean p | mean y | n |
| --- | --- | --- |
| 0.000 | 0.000 | 6,042 |
| 0.001 | 0.000 | 6,049 |
| 0.144 | 0.140 | 6,046 |
| 0.405 | 0.391 | 6,045 |
| 0.625 | 0.617 | 6,046 |
| 0.736 | 0.725 | 6,045 |
| 0.795 | 0.795 | 6,046 |
| 0.834 | 0.849 | 6,045 |
| 0.869 | 0.883 | 6,046 |
| 0.904 | 0.926 | 6,046 |

## Test confirmation (not used to pick the freeze)

| k | H1 AUC | H60 AUC | H60 hard AUC |
| --- | --- | --- | --- |
| -4 | 0.887 | 0.919 | 0.967 |
| -3 | 0.882 | 0.915 | 0.964 |
| -2 | 0.874 | 0.911 | 0.959 |
| -1 | 0.856 | 0.903 | 0.949 |
| +1 | 0.855 | 0.906 | 0.953 |
| +2 | 0.873 | 0.913 | 0.956 |
| +3 | 0.880 | 0.916 | 0.967 |
| +4 | 0.887 | 0.920 | 0.969 |

Models: `/Volumes/polymarket-quant-desk/scratch/directional-v1/stage_b.joblib`.

Stage B freeze lives on this card. Stage C is the sequential decoder.
