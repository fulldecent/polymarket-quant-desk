# Stage B — is it fadable?

Generated 2026-09-21T14:17:59Z.
On Stage A freeze: **fee-free long | walk≥5¢ | same1 A=300**. Features at T only. Label = last fill in (T, T+30] faded ≥ 2¢ from last(T) in the anti-whale direction. Train = first 70% of events by T, holdout = last 30%.

No 67-column HGB. Slices plus one small `HistGradientBoostingClassifier` if n allows. Promote a slice only if holdout 5.5sh $ beats always-fade on the same holdout and n_holdout ≥ 40.

Always-fade envelope: train n=632 T+30 $-0.0077 (23.8% fade>0); holdout n=272 T+30 $+0.0056 (34.7% fade>0).

| slice | train n | tr $ | tr fade>0 | ho n | ho $ | ho fade>0 | ho fade≥2¢ |
|---|---:|---:|---:|---:|---:|---:|---:|
| walk≥5¢ | 632 | -0.0077 | 23.8% | 272 | +0.0056 | 34.7% | 9.9% |
| walk≥10¢ | 587 | +0.0017 | 24.0% | 244 | +0.0158 | 34.1% | 8.8% |
| walk 2–10¢ | 45 | -0.1216 | 22.2% | 28 | -0.0880 | 40.0% | 20.0% |
| delay≥5 | 252 | +0.0279 | 22.8% | 109 | +0.0829 | 44.8% | 13.8% |
| delay≥30 | 165 | +0.0256 | 21.3% | 64 | +0.1683 | 60.0% | 20.0% |
| retrace<0.2 (still extended) | 613 | +0.0180 | 24.5% | 258 | -0.0030 | 33.3% | 8.3% |
| retrace 0.1–0.5 (started) | 10 | -0.0088 | 20.0% | 9 | +0.8085 | 100.0% | 50.0% |
| remaining≥5¢ | 61 | +0.2905 | 36.4% | 24 | -0.0300 | 27.3% | 18.2% |
| remaining≥10¢ | 32 | +0.5373 | 53.8% | 9 | -0.0917 | 33.3% | 0.0% |
| taker≥$25 | 413 | -0.0089 | 23.7% | 178 | -0.0082 | 35.3% | 7.4% |
| taker≥$100 | 212 | -0.0079 | 29.0% | 80 | -0.0038 | 44.7% | 7.9% |
| n_other≥3 | 231 | -0.0278 | 20.2% | 109 | +0.0285 | 41.5% | 17.0% |
| same_n≥1 | 632 | -0.0077 | 23.8% | 272 | +0.0056 | 34.7% | 9.9% |
| opp_n=0 | 592 | -0.0112 | 23.1% | 249 | -0.0204 | 29.9% | 4.6% |
| extended + remaining≥5¢ | 59 | +0.2905 | 36.4% | 24 | -0.0300 | 27.3% | 18.2% |
| walk≥5¢ + remaining≥5¢ + retrace<0.3 | 59 | +0.2905 | 36.4% | 24 | -0.0300 | 27.3% | 18.2% |

HGB max_depth=3, label fade30≥2¢. train AUC 0.999  holdout AUC 0.573. Train-chosen p≥0.50: holdout n=1  $-0.1100  fade>0 0.0%.

**No slice promoted.** `delay≥30` won holdout ($+0.1683, fade>0 60%, n=64) but train was fade>0 **21%** / $+0.03 — a 4-day time split, not a rule. `remaining≥5¢` did the opposite (train $+0.29, holdout −$0.03). HGB train AUC 0.999 / holdout 0.573 with n=1 at the chosen threshold is overfit.

Stage C therefore uses the full Stage A cell (n=904), not a B subset. The 229-row `delay≥30` keep below is diagnostic only.

Stage B keep (delay≥30, not frozen): n=229  T+30 n=62  mean fade +1.09¢  $+0.0601.

