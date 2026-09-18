# Stage B freeze (tape-touch)

Filename is historical. Stage A is persist — see `STAGE_A.md`.

Generated 2026-09-15T19:10:07Z. Wall 169.7s. Trigger 93,707,608–93,789,879.

Two HGBs. **activity** (`k=0`) and **barrier** (`k≠0`). `H1` = block `X+1`. `H60` = `X+2..X+60` (GTC window, not the FOK bar). Tick inferred from last 100 blocks. Both directions via `±k` on one last YES price.

Split: train 45,600  val 15,957  test 16,083 (event-level, embargo 120 blocks).

Inferred ticks: 0.0001×45770, 0.01×19438, 0.001×14146, 0.005×610, 0.0025×26, 0.1×10

Val stability (first vs second half): activity H1 AUC 0.907 / 0.911; barrier +1 H60 AUC 0.897 / 0.925.

## Activity (`k=0`)

| split/head | n | pos | AUC | Brier |
| --- | --- | --- | --- | --- |
| val H1 | 15,957 | 0.350 | 0.909 | 0.1015 |
| val H60 | 15,957 | 0.701 | 0.914 | 0.1084 |
| test H1 | 16,083 | 0.375 | 0.905 | 0.1117 |
| test H60 | 16,083 | 0.723 | 0.921 | 0.1023 |

## Barrier (all val)

| k | H1 pos | H1 AUC | H1 Brier | H60 pos | H60 AUC | H60 Brier |
| --- | --- | --- | --- | --- | --- | --- |
| -4 | 0.086 | 0.915 | 0.0544 | 0.314 | 0.914 | 0.1098 |
| -3 | 0.086 | 0.912 | 0.0549 | 0.318 | 0.910 | 0.1121 |
| -2 | 0.087 | 0.907 | 0.0557 | 0.323 | 0.904 | 0.1163 |
| -1 | 0.089 | 0.896 | 0.0575 | 0.344 | 0.875 | 0.1326 |
| +1 | 0.096 | 0.897 | 0.0610 | 0.363 | 0.912 | 0.1141 |
| +2 | 0.094 | 0.908 | 0.0590 | 0.349 | 0.926 | 0.1041 |
| +3 | 0.093 | 0.912 | 0.0583 | 0.343 | 0.931 | 0.0999 |
| +4 | 0.093 | 0.914 | 0.0579 | 0.340 | 0.934 | 0.0978 |

## Barrier hard subset (val): lookback range < |k| ticks

If the last 100 blocks already swung more than `k` ticks, `+k` is easy. This slice is the breakout case.

| k | H1 n | H1 pos | H1 AUC | H60 n | H60 pos | H60 AUC |
| --- | --- | --- | --- | --- | --- | --- |
| -4 | 7,330 | 0.002 | 0.829 | 7,330 | 0.024 | 0.855 |
| -3 | 7,242 | 0.002 | 0.754 | 7,242 | 0.027 | 0.835 |
| -2 | 7,078 | 0.003 | 0.683 | 7,078 | 0.032 | 0.808 |
| -1 | 6,448 | 0.006 | 0.662 | 6,448 | 0.062 | 0.665 |
| +1 | 6,448 | 0.005 | 0.701 | 6,448 | 0.045 | 0.732 |
| +2 | 7,078 | 0.003 | 0.747 | 7,078 | 0.032 | 0.776 |
| +3 | 7,242 | 0.002 | 0.769 | 7,242 | 0.028 | 0.799 |
| +4 | 7,330 | 0.001 | 0.847 | 7,330 | 0.024 | 0.812 |

## Test confirmation (not used to pick the freeze)

| k | H1 AUC | H60 AUC | H60 hard AUC |
| --- | --- | --- | --- |
| -4 | 0.890 | 0.896 | 0.815 |
| -3 | 0.889 | 0.894 | 0.806 |
| -2 | 0.888 | 0.888 | 0.765 |
| -1 | 0.876 | 0.861 | 0.631 |
| +1 | 0.885 | 0.922 | 0.701 |
| +2 | 0.894 | 0.938 | 0.768 |
| +3 | 0.896 | 0.940 | 0.797 |
| +4 | 0.897 | 0.942 | 0.824 |

Models: `/Volumes/polymarket-quant-desk/scratch/directional-fok-v1/stage_a.joblib`.

Stage A is **frozen** on this card. Stage B is not run here.
