# Stage B — tape-touch (frozen)

Applied after Stage A persist (6 of last 8 **and** Kris Kross 10+).
Four outputs, all YES-price **deltas vs last(X)**:

- X+1 high
- X+1 low
- X+1..X+30 high (includes the FOK bar)
- X+1..X+30 low

One `HistGradientBoostingRegressor`, **squared error**. Rows for the
first two heads get `sample_weight=2`; the 30-block heads get 1. That
is the loss. Not a ticket and not P&L.

**Labels:** CLOB prints in the horizon, plus on-chain resolve **inside
that horizon** (redeem = infinite liquidity at YES=1.0 / NO=0.0 /
draw=0.5). Resolve after the horizon is unused. This is not future
leakage. CLOB-only freeze card kept as
[`stage-b-snapshot-clob-only.md`](stage-b-snapshot-clob-only.md).
Counts: [`resolve-in-horizon.md`](resolve-in-horizon.md).

## CLOB-only vs resolve-in-horizon (same 250k stride)

On the 250k strided events, resolve added **activity** to **0** H1
labels and **525** H60 labels (0.21%). Barrier pos rates ticked up
~0.5pp on H60. Selected families **unchanged** (drop `return`,
`venue`). Primary **unchanged at 0.934**.

| val metric | CLOB-only | + resolve in H |
|---|---|---|
| activity H1 pos / AUC | 0.627 / 0.859 | 0.627 / 0.859 |
| activity H60 pos / AUC | 0.968 / 0.950 | 0.970 / 0.951 |
| +1 H60 pos / AUC | 0.528 / 0.894 | 0.533 / 0.892 |
| hard ±1 H60 (primary) | **0.934** | **0.934** |
| simple-rule hard ±1 | 0.701 | 0.696 |
| linear hard ±1 | 0.887 | 0.885 |
| hard +1 H60 pos / AUC | 0.018 / 0.939 | 0.018 / 0.939 |

The head did not get easier or harder in a way that moves the freeze.
The card is now **correct** for redeem-inside-GTC. Ready to freeze
this selected 67-col two-HGB (`scratch/directional-v1/stage_b.joblib`).
CLOB-only trees remain at `stage_b_clob_only.joblib`.

## Weekend vs 9-month random triggers

Four days can miss who was online. The 9-month pool is random 3-of-8
events from **14 Dec 2025 – 14 Sep 2026** (9 monthly bins, 20k each)
plus extra draws from overlapping special eras (SB 2026, Iran war,
WC 2026, freeze weekend). n=203,815. Time-ordered split. Test is the
most recent slice, not the weekend that trained the old card.

| val | weekend (11–14 Sep 2026) | 9-month random |
|---|---|---|
| n train / val / test | 137k / 60k / 52k | 112k / 47k / 45k |
| activity H1 AUC | 0.859 | 0.812 |
| +1 H60 AUC | 0.894 | 0.820 |
| **hard ±1 H60 (primary)** | **0.934** | **0.737** |
| simple-rule hard ±1 | 0.701 | 0.489 |
| linear hard ±1 | 0.887 | 0.660 |
| hard +1 H60 pos | 0.018 | 0.059 |
| families kept | 67 (drop return, venue) | **all 78** |
| val-half primary | 0.955 / 0.914 | 0.741 / 0.732 |

The weekend 0.93 was a **regime number**, not the skill we should
expect. On 9 months of random tape, HGB still beats linear and the
range rule on breakouts, but primary is **0.74**, not 0.93. Val
halves agree. That is the more honest freeze if the worry is
“someone was offline that weekend.”

Cards: weekend [`stage-b-snapshot-weekend.md`](stage-b-snapshot-weekend.md);
9-month [`stage-b-snapshot-9mo.md`](stage-b-snapshot-9mo.md) /
[`stage-b-snapshot.md`](stage-b-snapshot.md).
Joblib: `stage_b_weekend.joblib` vs `stage_b_9mo.joblib` /
`stage_b.joblib`.

Full tables: [`stage-b-snapshot.md`](stage-b-snapshot.md).
Model: `scratch/directional-v1/stage_b.joblib`.

## What kind of modeling

Two binary **HistGradientBoosting** classifiers (sklearn), **log-loss**
(`loss="log_loss"`, early stopping `scoring="loss"`). Features are
covariates, not separate objectives. Both heads are Bernoulli; log-loss
is the proper scoring rule for the probabilities C consumes.

| Model | Inputs | Target |
|---|---|---|
| **activity** | last-100-block features + horizon `Z∈{1,30,60}` | `P(any fill in that horizon)` |

`H30` (`X+2..X+30`) is part of this freeze, trained on B’s train
slice only. Stage C does not refit it.
| **barrier** | features + signed `k` + `Z` | `P(YES tape touches last ± k ticks)` |

`H1` = `X+1` (FOK bar). `H60` = `X+2..X+60` (GTC window, **not** the
FOK bar). `k` and `Z` are inputs, not eight separate models. Both
directions from one last YES-equivalent price (`+k` buy YES, `−k` buy
NO). Tick inferred from the last 100 blocks.

Not a neural net. Not a joint ordinal. Size / FOK / sequential P&L
stay in Stage C.

Diagnostics, not freeze candidates: a simple ranker (activity = `n_15`;
barrier = lookback ticks already traveled toward the barrier) and a
linear model (`StandardScaler` + logistic).

## All features, then select

Fit **all 78** last-100-block columns in **9** families from `fills_v1`.
No 10K account tables, no cooldown recency, no `block > X`. On val,
permute one family at a time. Drop if both Δ activity-H1 AUC **and**
Δ hard-H60-±1 AUC are `< 0.002`. `level` is never dropped.

**Frozen: selected, 67 columns.** Dropped `return` and `venue`. Val
primary (mean hard-subset H60 AUC at `k=±1`) 0.934 selected vs 0.933
all — smaller set wins the slack rule. Test never selected.

| Family | Δ act H1 | Δ hard ±1 | Keep |
|---|---|---|---|
| level (last, tick, min_p, hour) | +0.004 | **+0.148** | yes |
| persist | **+0.150** | +0.002 | yes |
| depth | +0.007 | +0.001 | yes |
| counts | +0.087 | +0.001 | yes |
| range | +0.003 | +0.009 | yes |
| flow | +0.004 | +0.006 | yes |
| adverse | +0.002 | +0.002 | yes |
| return | +0.001 | −0.001 | **no** |
| venue (fee, NegRisk) | +0.000 | +0.001 | **no** |

Persist still dominates the FOK bar after the 3-of-8 gate (remaining
variation is 3…8 of 8). Level (especially tick / last) dominates the
hard barrier. Range helps the *easy* +1 H60 (+0.098) much more than
the breakout slice (+0.009) — that is why the hard subset exists.
Fee bit and NegRisk do not move tape-touch probability; they may
still matter for Stage C dollars. Close-to-close returns were noise
on this sample.

## Sample

Last fills through the frontier. Trigger 93,625,336–93,789,879
(~4 days). Candidates 1,996,298. Stage A dropped 1,207,197 (keep
39.5%, matches the frozen 40.5% on the Stage A card). 789,101 alive;
**strided to 250,000 across the span** (not a prefix). Train 137,204 /
val 60,456 / test 51,966, embargo 120 blocks.

Inferred ticks: 0.0001×128,264, 0.01×112,620, 0.001×8,934, others rare
(0.1×3).

## Capacity check (val)

The question is whether HGB is just rediscovering lookback range.

| model | activity H1 AUC | +1 H60 AUC | **hard ±1 H60** |
|---|---|---|---|
| simple rule (`n_15` / dist already traveled) | 0.787 | 0.763 | **0.701** |
| linear (all 78) | 0.847 | 0.841 | 0.887 |
| HGB all | 0.859 | 0.895 | 0.933 |
| **HGB selected (frozen)** | 0.859 | 0.894 | **0.934** |

HGB beats the range rule on the **hard** slice by a lot, and beats
linear by ~0.05. Trees are doing something additive last/tick/range
does not. Activity H60 is 96.8% positive after Stage A — that head is
nearly a constant; **H1** is the activity that still has a job
(val pos 0.627, AUC 0.859, well calibrated).

Hard-subset H60 at `k=±1` is rare (pos 1.8% up / 2.5% down, n=14,634).
AUC 0.93 is ranking skill on a 2% event, not a 50% precision claim.
Test hard H60 AUC 0.95 / 0.95 at `±1` (confirmation only).

Val halves: activity H1 0.864 / 0.852; primary 0.955 / 0.913. Sign
holds; magnitude drifts.

## Required test eras (not the freeze weekend)

The Sep 2026 fit is a **busy recent weekend**. It is not the universe.
Polymarket got more popular every month in this span, and more bots
joined. We still need the same tape-touch **principles** to hold on
other regimes: election tape, Super Bowl weeks, FIFA tournaments, and
US military escalations (before and after).

Families stay frozen (selected 67 cols). We **re-fit** the two HGBs
inside each window and transfer across windows. A held-out era is never
used to pick features. Catalog: [`eras.py`](eras.py). Block bounds from
Polygon RPC: [`era-blocks.json`](era-blocks.json). Results:
[`stage-b-eras.md`](stage-b-eras.md).

| id | category | UTC | blocks |
|---|---|---|---|
| `fifa_wc_2022` | fifa | 2022-11-22 → 2022-12-19 | 35,904,127–36,998,297 |
| `euro_copa_2024` | fifa | 2024-06-14 → 2024-07-15 | 58,129,823–59,368,916 |
| `cwc_2025` | fifa | 2025-06-14 → 2025-07-14 | 72,736,754–73,929,895 |
| `fifa_wc_2026` | fifa | 2026-06-11 → 2026-07-20 | 88,287,343–90,533,734 |
| `sb_2023` | super_bowl | 2023-02-05 → 2023-02-14 | 38,919,195–39,262,905 |
| `sb_2024` | super_bowl | 2024-02-04 → 2024-02-13 | 53,094,448–53,451,228 |
| `sb_2025` | super_bowl | 2025-02-02 → 2025-02-11 | 67,427,202–67,777,034 |
| `sb_2026` | super_bowl | 2026-02-01 → 2026-02-10 | 82,391,997–82,780,788 |
| `ticket_shock_2024` | election | 2024-07-13 → 2024-07-29 | 59,289,062–59,928,369 |
| `us_election_2024` | election | 2024-10-15 → 2024-11-13 | 63,046,879–64,213,893 |
| `inauguration_2025` | election | 2025-01-13 → 2025-01-28 | 66,627,241–67,226,717 |
| `oct7_before` | us_military | 2023-09-23 → 2023-10-07 | 47,868,324–48,414,595 |
| `oct7_after` | us_military | 2023-10-07 → 2023-10-22 | 48,414,596–48,999,673 |
| `yemen_before` | us_military | 2023-12-29 → 2024-01-12 | 51,677,130–52,216,178 |
| `yemen_after` | us_military | 2024-01-12 → 2024-01-27 | 52,216,179–52,789,781 |
| `rough_rider_before` | us_military | 2025-03-01 → 2025-03-15 | 68,495,222–69,057,996 |
| `rough_rider_after` | us_military | 2025-03-15 → 2025-03-30 | 69,057,997–69,662,950 |
| `iran_before` | us_military | 2026-02-14 → 2026-02-28 | 82,953,589–83,558,384 |
| `iran_after` | us_military | 2026-02-28 → 2026-03-15 | 83,558,385–84,206,383 |
| `quiet_aug_2023` | quiet | 2023-08-14 → 2023-08-29 | 46,275,047–46,875,452 |
| `quiet_aug_2025` | quiet | 2025-08-11 → 2025-08-26 | 75,051,808–75,656,788 |
| `recent_sep2026` | recent | 2026-09-11 → 2026-09-15 | 93,586,511–93,816,910 |

Military windows are paired **before / after** the first strike day
(7 Oct 2023; 12 Jan 2024 US–UK Yemen; 15 Mar 2025 Rough Rider; 28 Feb
2026 Iran war). Super Bowl windows are conference-championship week
through the Monday after the game. FIFA windows are the tournament
inclusive. `euro_copa_2024` also covers the UK general election (4 Jul
2024). `fifa_wc_2026` overlaps the July 2026 Hormuz strike cycle — we
do not drop it; that is a real mixed tape. Quiet Augusts are controls
with no mega-event, early vs late venue.

Long windows (World Cups, CWC, election month) are **chunked** across
the span so we do not only score the first days.

### What transferred (and what did not)

Full tables: [`stage-b-eras.md`](stage-b-eras.md). Frozen 67-col two-HGB,
re-fit per era, 3-of-8 still on. Primary = hard-subset H60 AUC at `k=±1`.

**Too thin after Stage A (cannot test the principle there):** World Cup
2022 (8 events), Super Bowl 2023/2024 (4 / 72), 7 Oct 2023 before/after
(26 / 17), Yemen Jan 2024 (429 / 211), quiet Aug 2023 (307). Early
Polymarket does not pass 3-of-8. That is a gate fact, not a model fact.

**Usable (14 eras), in-era primary:**

| window | primary | act H1 |
|---|---|---|
| freeze weekend Sep 2026 | **0.939** | 0.863 |
| Super Bowl 2025 | 0.907 | 0.742 |
| inauguration 2025 | 0.905 | 0.749 |
| Rough Rider before / after | 0.889 / 0.858 | 0.764 / 0.725 |
| US election 2024 | 0.882 | 0.789 |
| Butler + Biden-out | 0.889 | **0.542** |
| quiet Aug 2025 | 0.880 | 0.738 |
| Club World Cup 2025 | 0.854 | 0.755 |
| World Cup 2026 | 0.811 | 0.804 |
| Iran war after / before | 0.792 / 0.762 | 0.848 / 0.850 |
| Super Bowl 2026 | 0.771 | 0.850 |
| Euro + Copa 2024 | **0.630** | 0.660 |

Leave-one-era-out (train on the other 13, score the held-out) tracks
in-era within ~0.05 on most 2025–2026 windows. Election drops
0.882 → 0.776. Euro stays weak (0.636). Freeze weekend stays easy
even when not in the train set (0.931).

Walk-forward (only **earlier** eras): from inauguration 2025 onward,
primary stays 0.77–0.93, including World Cup 2026 (0.803) and the
freeze weekend (0.931). Election, trained only on Euro + ticket-shock,
is 0.702.

The Sep 2026 **weights** are not a universal model. Train on that
weekend and test election: primary **0.501**. Test Euro: **0.527**.
Busy recent tape does not walk backward onto 2024. Training on the
mixed 2024–2026 eras and testing the weekend **does** work. That is
the popularity/bot point: the mapping drifted; the last-100-block
families still have a job if you re-fit.

Activity H1 is the weaker head once we leave the freeze weekend
(often 0.72–0.85). Ticket-shock H1 is a coin flip in-era (0.542)
while the barrier head stays 0.889 — FOK-bar persistence broke during
the campaign shock; 1-tick touch did not.

## What this is not

- Not a trader. No cooldown, no size, no dollar score.
- Not “0.90 AUC so we fade every tape.” Easy barriers are still
  mostly “this book already swings.” The frozen number that matters
  for Stage C is **hard ±1 H60**, and even there base rate is ~2%.
- Venue/returns were dropped from the **probability** model. Stage C
  may still want fee bit as a P&L multiplier.

Selected families are frozen. Do not re-grid families or `k` on test.
Do not treat the Sep 2026 weekend as the only era that counts.
