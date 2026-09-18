# Experimental rigor

This is a **sequential policy**, scored in dollars, with a finite wall-clock
budget. It is not a classifier on shuffled `(condition, block)` rows.

The tape already said a min-tick sided book on **all** floors loses.
This protocol exists so that if a sparse policy is real, we can see it,
and if it is not, we stop without fooling ourselves.

## Decision at (condition, block X)

Universe: every `(condition_id, block)` that has at least one fill,
**after Stage A** (3 of the last 8 blocks have a fill;
[`STAGE_A.md`](STAGE_A.md)). Features: **last 100 blocks** only,
`block ≤ X`. Stage B is tape-touch. Stage C is the sequential policy.

If a prior participate=yes on this condition was at `X'` and
`X < X' + 180`, the model is not queried (cooldown). Output is skip.

Stage B freeze includes horizons `Z ∈ {1, 30, 60}`. The 30-block head
is fit on B’s train slice and **frozen with B**. Stage C consumes
those probabilities. C does **not** refit H30 on the same events it
scores. C’s decoder search starts after B’s `train_cut_block` plus
embargo.

Otherwise the model outputs:

| Field | Domain |
|---|---|
| `participate` | yes / no. If no, all other fields are null. |
| `side` | buy YES or buy NO |
| `P_entry` | buy cap, on the market tick grid |
| `P_exit` | sell floor, on the **inferred** tick grid, `P_exit > P_entry` (long) |
| `N` | shares. Must lie in `[1.1 × N_min(P_entry), 4.4 × N_min(P_entry)]` |

`N_min(P) = max(5, ceil(1.20 / P))`. The 1.1 floor is a buffer above the
legal clip. The 4.0 ratio is the desk’s 4:1 size cap (1.1× to 4.4×).

A participate=yes starts the 180-block dark period **even if the FOK
misses**.

## Throughput floor (hard fail)

On **recent** partitions (the latest val fold, not test), count
`participate=yes` after cooldown.

```
activations_per_hour = n_participate / (span_blocks / 1714)
```

If `activations_per_hour < 1` (fewer than one ticket per hour **across
all markets**), the candidate is a **hard fail**. Do not compute P&L.
Do not log a dollar number. It is not interesting enough to ever try
in production.

Always-skip ($0) is a valid baseline, not a candidate for promotion.
A mutant that almost never fires is discarded the same way.

## Evaluator

The historical fill model in [`fill-model.md`](fill-model.md), nothing
else:

1. FOK take at `X+1` (`px ≤ P_entry`, ≥ `N` shares). Miss → $0.
2. GTC sell `X+2..X+60` (strict better on `X+2`, at-or-better after).
3. Dump `X+61..X+120` at any bid.
4. Rest **$0**.

Headline number is **sum of attempt P&L** on the split, after cooldown
has been applied **in time order**. Always-skip is $0. Naive
`close+1 / close−1` is the dumb baseline.

Do not score accuracy, F1, or “did price go up.”

## Splits

Walk-forward in **block time**. Never shuffle.

Stage B family select still uses a time-ordered train/val/test on the
B freeze pool.

**Stage C validation is day → day+7 (same weekday).** Train the
decoder on calendar day `D`. Freeze those params. Score sequential
P&L on `D+7`. No information from `D+7` in selection. A 4-day window
cannot do this. C’s event span must cover multiple weeks **after**
B’s train cut.

```
for each weekday D in C’s span:
    fit decoder on D          # train
    evaluate on D+7           # test (same day of week)
```

Headline: do those `D+7` days print (≥1/hour **and** P&L > 0) often
enough to trust the book next week. One lucky weekend is not that.

Cooldown state does **not** carry across splits (each day is its own
replay). Features must not use fills after `X`.

A later era (older slice / election week) is a **frozen** extra test,
not a place to pick mutations.

Timing, embargo, heat features, C shortlist, promotion, go-live, and
production vs research: [`WALKFORWARD.md`](WALKFORWARD.md).

## How the model is updated

Train proposes. Val **selects**. Test **reports**. Test never selects.

Allowed proposers (mix is fine):

- Regression / HGB / linear: predict components
  `P(entry fill)`, `E[pnl | fill]`, then decode a ticket
  (side, ticks, size) with a greedy rule. Fit on train only.
- Search (including genetic mutations): mutate the decoder, the
  thresholds, or a small weight vector. Each mutant is a full
  sequential replay on **train**, then scored on **val**.

Not allowed:

- Mutate, look at test, keep if test went up.
- Fit on train+val, then quote test.
- Independent iid batches that ignore cooldown.

Promotion rule: a candidate becomes `best_model` only if **val P&L**
beats the current best by a pre-declared margin (e.g. > 0 and >
always-skip, and not worse than naive by accident of one week). Test
P&L is logged at promotion time and **must not** be the reason for
the promotion.

## Feature stability

A feature stays in the model only if its association with simulator
P&L does **not** flip sign across train-era slices (e.g. three
contiguous thirds of train, or adjacent 10K-block groups).

Buckets (1/5/15/30/60/100) are **views**. Killing the 15-block column
does not retire the family. A family is retired only if **no** view
and no cross-bucket summary is stable and useful on val.

No cooldown-recency feature. No lookback past 100 blocks.

## Tracking and time

The training process writes a log (append-only):

```
timestamp, candidate_id, parent_id, val_pnl, test_pnl, n_participate,
hit_rate, pct_zero, wall_s_cum, note
```

`best_model` snapshots copy the weights/thresholds and the timestamp.
Wall-clock budget is declared before the run (e.g. 4 hours). When the
budget hits, stop. The last promoted `best_model` is the result, even
if the next mutant looks tasty.

## What would count as improvement

In order, on **test**, versus always-skip and versus naive `+1/−1`:

1. P&L > 0 after dump/zero.
2. Participate rate low enough that this is a filter, not 27k tickets/day.
3. Entry hit rate on participated tickets materially above naive’s 3%.
4. Worthless share of entered tokens down from ~44%.
5. The same signs on a second time period.

If val is green and test is red, that is overfitting. Do not ship.
If every split is red after a serious search, this book is dead — same
conclusion as the cap grid, with a fairer search.

## How to program it

Do **not** start with a genetic search over the full ticket, and do
**not** start with one linear vector dotted onto raw features for
`P_entry` / `P_exit`. Those spend the wall-clock budget on a geometry
the cap grid already killed.

### Stage B — tape-touch heads (train only, no cooldown)

See [`FEATURES.md`](FEATURES.md). One last YES-equivalent price; both
directions are `+k` / `−k`. Tick inferred from the last 100 blocks
among the documented legal steps. Horizons: `H1` = `X+1` only, `H60` =
`X+2..X+60` (exit window, not the FOK bar). Two HGBs: **activity**
(`k=0`) and **barrier** (`k≠0`), so the easy print-head cannot dominate
loss. `k` and horizon are inputs. Not a neural net.

Features: **all** last-100-block condition-level families from
`fills_v1`, then **select families on val** (permutation; drop if both
Δ activity-H1 and Δ hard-H60-±1 AUC `< 0.002`). `level` is never
dropped. Freeze selected if it is within 0.003 of all-features on the
primary (mean hard-subset H60 AUC at `k=±1`). Test is confirmation,
not selection. A range-rule and a linear model are diagnostics so we
can see whether HGB is just rediscovering lookback range.

If the condition **resolves inside** the label horizon, that is infinite
liquidity at settlement (YES=1.0, NO=0.0, draw=0.5). It counts as
activity and as a barrier touch if settlement crosses `last ± k` ticks.
Resolve **after** the horizon is unused. This is a label change, not a
feature. Counts: [`resolve-in-horizon.md`](resolve-in-horizon.md).

These heads are features for Stage C. They are not the trader.

Required generalization eras (election, Super Bowl, FIFA, US military
before/after) are listed in [`eras.py`](eras.py). The Sep 2026 freeze
weekend is not the universe. Re-fit the two HGBs in each era; do not
re-select families on those eras.

### Stage C — tiny policy, sequential P&L

A decoder with **few** parameters, e.g.:

- `participate` if `P(fill|c) > t_fill` and `P(exit|f) > t_exit` and
  `c − f` is at least `k` ticks
- `side` = argmax expected fill×exit edge
- `(c, f)` = argmax of
  `P(fill) P(exit) (f − vwap_in) + P(fill)(1−P(exit)) E[dump or 0]`
  on the same small grid
- `N` = clip(Kelly(edge, var), `1.1 N_min`, `4.4 N_min`)

`t_fill`, `t_exit`, `k`, maybe a couple of head weights: that is the
search vector. Random walk / coordinate descent / a short genetic
loop on **train sequential P&L**, promote on **val**. Hard-fail
throughput first, then P&L.

### Why not the other two, as the whole system

**Static vector · features → each output, then random walk.** Fast and
honest as Stage C **if** the inputs are Stage B probabilities and
tick offsets. As the whole system, linear maps from raw tape onto
`P_entry` in `[0,1]` waste steps on illegal prices and rediscover
“fire often, miss FOK.”

**Band-touch net as the trader.** The intermediary problem is the
right Stage B. Using it *as* participate/side/price without a
sequential decoder ignores cooldown, size, FOK all-or-nothing, and
the 1/hour floor.

**Something else (not yet):** a net over 100-block fill sequences.
Only if Stage B HGB is clearly under-capacity (stable AUC but still
blind to order of prints). Not first.

## Search-space honesty

Participate × side × tick pair × size is large. Most of it is naive
min-tick noise. The only economically plausible region is **rare
participate**, **entry cap wide enough that X+1 still has size**,
**exit not greedier than the tape**. The cap grid already showed that
widening 0–3 ticks on *all* floors still loses. A model that mostly
says yes will rediscover that loss. Rigor is forcing participate=no
as the default, not adding more genes.
