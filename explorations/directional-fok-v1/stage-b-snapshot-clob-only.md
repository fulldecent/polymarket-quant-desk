# Stage B freeze (tape-touch)

Generated 2026-09-15T20:11:21Z. Wall 890.3s. Trigger 93,625,336–93,789,879. Stage A gate **3 of last 8**.

## Modeling

Two binary **HistGradientBoosting** classifiers (sklearn), log-loss,
early stopping on a 10% train holdout. **activity** (`k=0`) and
**barrier** (`k≠0`). `H1` = block `X+1`. `H60` = `X+2..X+60`
(GTC window, not the FOK bar). Tick inferred from last 100 blocks.
Both directions via `±k` on one last YES price. `k` and horizon `Z`
are inputs. Not a neural net. Not a joint ordinal. Size / FOK stay
in Stage C.

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

Inferred ticks: 0.0001×128264, 0.01×112620, 0.001×8934, 0.005×130, 0.0025×49, 0.1×3

Val stability (first vs second half): activity H1 AUC 0.864 / 0.852; barrier +1 H60 0.892 / 0.894; primary (hard ±1 H60) 0.955 / 0.913.

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
| simple rule | 0.787 | 0.763 | 0.701 |
| linear | 0.847 | 0.841 | 0.887 |
| HGB all | 0.859 | 0.895 | 0.933 |
| HGB selected | 0.859 | 0.894 | 0.934 |

## Family permutation (val ΔAUC; higher = family mattered)

| family | Δ act H1 | Δ act H60 | Δ +1 H60 | Δ hard ±1 | keep |
| --- | --- | --- | --- | --- | --- |
| level | 0.004 | 0.020 | 0.097 | 0.148 | yes |
| persist | 0.150 | 0.076 | 0.002 | 0.002 | yes |
| depth | 0.007 | 0.004 | 0.000 | 0.001 | yes |
| counts | 0.087 | 0.002 | 0.004 | 0.001 | yes |
| range | 0.003 | 0.006 | 0.098 | 0.009 | yes |
| flow | 0.004 | 0.011 | 0.012 | 0.006 | yes |
| return | 0.001 | 0.001 | 0.005 | -0.001 | no |
| adverse | 0.002 | 0.021 | 0.004 | 0.002 | yes |
| venue | 0.000 | 0.000 | 0.001 | 0.001 | no |

## Activity (`k=0`) — frozen

| split/head | n | pos | AUC | Brier |
| --- | --- | --- | --- | --- |
| val H1 | 60,456 | 0.627 | 0.859 | 0.1466 |
| val H60 | 60,456 | 0.968 | 0.950 | 0.0239 |
| test H1 | 51,966 | 0.682 | 0.874 | 0.1332 |
| test H60 | 51,966 | 0.975 | 0.962 | 0.0188 |

## Barrier (all val) — frozen

| k | H1 pos | H1 AUC | H1 Brier | H60 pos | H60 AUC | H60 Brier |
| --- | --- | --- | --- | --- | --- | --- |
| -4 | 0.139 | 0.893 | 0.0844 | 0.445 | 0.917 | 0.1146 |
| -3 | 0.141 | 0.887 | 0.0869 | 0.461 | 0.910 | 0.1185 |
| -2 | 0.147 | 0.877 | 0.0912 | 0.486 | 0.903 | 0.1222 |
| -1 | 0.161 | 0.852 | 0.1023 | 0.533 | 0.885 | 0.1293 |
| +1 | 0.166 | 0.851 | 0.1048 | 0.528 | 0.894 | 0.1235 |
| +2 | 0.151 | 0.877 | 0.0928 | 0.486 | 0.906 | 0.1199 |
| +3 | 0.146 | 0.887 | 0.0887 | 0.463 | 0.912 | 0.1167 |
| +4 | 0.143 | 0.893 | 0.0859 | 0.447 | 0.919 | 0.1128 |

## Barrier hard subset (val): lookback range < |k| ticks

If the last 100 blocks already swung more than `k` ticks, `+k` is easy.
This slice is the breakout case. **This is the Stage B question.**

| k | H1 n | H1 pos | H1 AUC | H60 n | H60 pos | H60 AUC |
| --- | --- | --- | --- | --- | --- | --- |
| -4 | 20,059 | 0.001 | 0.817 | 20,059 | 0.031 | 0.929 |
| -3 | 18,858 | 0.001 | 0.861 | 18,858 | 0.030 | 0.927 |
| -2 | 17,289 | 0.001 | 0.884 | 17,289 | 0.024 | 0.924 |
| -1 | 14,634 | 0.002 | 0.904 | 14,634 | 0.025 | 0.930 |
| +1 | 14,634 | 0.001 | 0.951 | 14,634 | 0.018 | 0.939 |
| +2 | 17,289 | 0.001 | 0.920 | 17,289 | 0.025 | 0.919 |
| +3 | 18,858 | 0.001 | 0.888 | 18,858 | 0.028 | 0.919 |
| +4 | 20,059 | 0.001 | 0.911 | 20,059 | 0.033 | 0.924 |

## Tick strata (frozen val)

| tick | n H1 | act H1 AUC | n +1 H60 | +1 H60 AUC |
| --- | --- | --- | --- | --- |
| 0.0001 | 29,287 | 0.828 | 29,287 | 0.801 |
| 0.01 | 29,025 | 0.872 | 29,025 | 0.909 |
| 0.001 | 2,095 | 0.947 | 2,095 | 0.985 |
| 0.005 | 37 | 0.707 | 37 | 0.848 |
| 0.0025 | 10 | — | 10 | — |
| 0.1 | 2 | — | 2 | — |

## Calibration (frozen val, quantile bins)

Activity H1:

| mean p | mean y | n |
| --- | --- | --- |
| 0.190 | 0.180 | 6,046 |
| 0.285 | 0.281 | 6,045 |
| 0.361 | 0.373 | 6,046 |
| 0.438 | 0.440 | 6,045 |
| 0.534 | 0.524 | 6,046 |
| 0.665 | 0.666 | 6,046 |
| 0.859 | 0.854 | 6,045 |
| 0.965 | 0.967 | 6,045 |
| 0.990 | 0.989 | 5,946 |
| 0.997 | 0.992 | 6,146 |

Barrier +1 H60:

| mean p | mean y | n |
| --- | --- | --- |
| 0.000 | 0.000 | 5,299 |
| 0.001 | 0.001 | 6,792 |
| 0.124 | 0.126 | 6,046 |
| 0.382 | 0.370 | 6,045 |
| 0.628 | 0.613 | 6,046 |
| 0.740 | 0.726 | 6,046 |
| 0.796 | 0.794 | 6,045 |
| 0.835 | 0.842 | 6,045 |
| 0.869 | 0.881 | 6,046 |
| 0.902 | 0.927 | 6,046 |

## Test confirmation (not used to pick the freeze)

| k | H1 AUC | H60 AUC | H60 hard AUC |
| --- | --- | --- | --- |
| -4 | 0.889 | 0.920 | 0.961 |
| -3 | 0.883 | 0.917 | 0.960 |
| -2 | 0.875 | 0.914 | 0.955 |
| -1 | 0.857 | 0.906 | 0.950 |
| +1 | 0.856 | 0.908 | 0.947 |
| +2 | 0.875 | 0.914 | 0.951 |
| +3 | 0.882 | 0.917 | 0.965 |
| +4 | 0.889 | 0.922 | 0.966 |

Models: `/Volumes/polymarket-quant-desk/scratch/directional-fok-v1/stage_b.joblib`.

Stage B freeze lives on this card. Stage C is the sequential decoder.
