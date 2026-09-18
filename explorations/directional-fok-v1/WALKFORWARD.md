# Walk-forward B/C and production

This is the timing protocol. The weekend sliver (+$0.29 val / +$4.80
test) and a one-shot 9-month B freeze are **not** a go-live. They are
history.

A is frozen (5-of-8) for the whole study. Test never selects: not the
ticket, not the B window, not the feature pack, not the promotion rule.

## Picture

One cutoff `t`. Left of the embargo may fit. Right of val may only score.

```
|------ B-fit trees (60–90d) ------|-- B-calib --| E |-- C-val --| E |-- C-test --|
                                   isotonic only     select $         score only
```

- `E` = embargo ≥ max(100-block features, 100-block B labels, GTC 30/60,
  cooldown 180). Floor **180** blocks; **280** if horizon-100 labels
  still overlap the next slice. We use **280**.
- Slide `t` forward ~**14 days** and repeat.
- Concatenated **test** path is the only “is there an edge” number.

Need **three** honest test slices before a go-live sample. From ~9
months of tape, after burning the first B-fit, expect about 4–8 slices.

## Heat features (in B, not C)

Not calendar dummies. Live = had a **fill** in `[X-99, X]` (tape-touched,
not “open on the catalog”). Notional = `sum(|gross_usdc|)`.

| Feature | Definition |
|---|---|
| `notional_pctile` | rank / `N_live` of this cid’s last-100 notional among names that printed in `[X-99,X]` |
| `log_venue_vs_7d` | `log((V_t+ε)/(median_7+ε))` where `V_t` is venue last-100 notional at `X`, and `median_7` is the median of last-100 venue notional at the same `hour_slot` on each of the prior 7 **completed** protocol-days (`BLOCKS_PER_DAY`) |
| `log_venue_vs_t7` | `log((V_t+ε)/(V_{X-7d}+ε))` — this clock vs exactly 7 protocol-days ago |

Floor `ε`. `N_live=1` → pctile 1.0. Do not include our own simulated
fills. `hour_slot` stays as a weak clock prior. No DOW one-hots.

Title / sport / TTR do **not** enter `X`. They **do** enter evaluation:
slice Brier and C dollars by weekend, category if known, and
time-to-resolution.

These inputs are nonstationary. Recalibrate B probabilities on a short
window even if trees stay frozen.

## Train B

Trees: rolling last **60–90** days of completed tape, ending at `t-E`.
Targets: `P(any liquidity)` and `P(touch last ± k)` at horizons 1, 30,
100-as-60 (`H60` = `X+2..X+60`). No dollars in B. No refit of B inside
C-val because dollars looked bad.

**Calibration** (not a new model): isotonic on the last **7–21** days of
that same train tail, still before C-val. Recalibrate heads C spends
(`P(touch ± k)`, FOK-bar fill-proxy), not only `P(any print)`.

**9-month B** is a robustness card, not the production freeze. Horse-race
60–90d vs 9mo on Brier and concatenated C dollars. Production uses the
winner plus frequent recalibration.

H30 is fit on B’s train slice and frozen with B. C does not refit it.

## Train C (select a ticket)

C does not train like B. It **selects a ticket** on dollars.

C-val = next **14–21** days after embargo (do not stretch until a cell
looks good). Pre-registered activation floor: **≥ 1/hour** on that
slice (`n_part / (span_blocks/1714) ≥ 1`), and **≥ 100** participates
on the slice.

**Pre-declared shortlist** (not an open grid):

- pay ∈ {0, 1, 2} ticks on the FOK
- k ∈ {1, 3, 4, 5}
- GTC 30 vs 60
- skip-fee on/off
- always-skip = $0 (floor)
- naive imbalance +0/+1 with the **same** search rights (dummy)

Inputs: frozen + calibrated B probs; extras B does not emit (HHI /
top_share / n_acct, fee bit). Optional coarse bin of this-market share
(low/mid/high), not a new grid axis.

Rank by sequential P&L, leftover **$0** as the **selection** objective.
Report mark-at-bid and hold-to-resolution as stress, not as the
optimizee.

**Promotion rule (written before the slice):**

1. P&L > 0 after fees
2. beats the dummy on that val slice
3. neighbor cells (`k±1` in the shortlist, pay±1, GTC 30 vs 60) not
   disasters (neighbor P&L > −max(5, 3·|best|))
4. activation count ≥ the floor above

If val fails: this fold’s ticket is **skip**. Still score test
(skip vs dummy), then move on. Do **not** peek at test and try another
cell.

C-test = next 14–21 days. No selection.

**Headline metric (how a run is read):** expected **one-day** results
at **recent scale** (what the next ~30 days should look like):

`X trades, $Y.## total buys, ±$Z.## PnL`

- **Scale** = fills/day and mix from test slices that overlap the last
  30 protocol days of tape (not the 9-month average).
- **Edge** = concatenated OOS P&L / OOS fills (and buys / fills).
- Forward day = recent fills/day × OOS $/fill (same for buys).
- trades = FOK fills; total buys = USDC spent (`usdc_in`); PnL after
  fees, leftover $0.

Dummy gets the same line at the same recent scale. Do not quote a
quiet-month average as “typical going forward.”

## Repeat

For each cutoff `t`:

1. Freeze A (already).
2. Fit B trees on 60–90d ending `t-E`. Optionally emit 9mo card in the
   horse race.
3. Recalibrate B on the train tail.
4. Select C on C-val, or skip if promotion fails.
5. Lock. Score C-test.
6. Snapshot B + ticket + rule.
7. Advance `t` by ~14 days.

Architecture, feature spec, shortlist, and promotion rule stay frozen
across folds. Changing any of those after seeing a test slice burns
that slice and restarts the OOS clock.

Report: each slice’s test P&L (including losers); concatenated path in
time order; Brier/reliability of B on the latest slice, also sliced by
weekend / TTR even if those never entered `X`.

## Go-live (pre-registered)

1. Concatenated **test** P&L > 0 after fees, and **beats the dummy**.
2. At least 3 test slices. Floor on total OOS activations set before
   seeing dollars: **≥ 500** participates on concatenated test (not a
   weekend cluster).
3. Neighbor heatmap is not a single spike. Winner rank survives dropping
   20% of trades or shifting the window by one cooldown.
4. B still calibrated on the **latest** test slice for heads C spends.
5. Dump is not most of the P&L. Stress leftover at bid / hold-to-resolution
   not a blow-up.
6. Trial budget: the old weekend cell does **not** count as a pass.
7. One locked ticket (or skip-when-dead that itself survived walk-forward).
8. Kill-switch specs written before size-on.
9. **Shadow** the locked ticket plus current B snapshot for at least one
   full weekly cycle. Research OOS is not live.

If any fail: paper trade, run another fold. Do not widen the grid until
it passes.

## Research vs production

| Piece | Research | Production |
|---|---|---|
| A | Frozen spec | Same |
| B trees | Fit each fold on 60–90d | Frozen between scheduled refits (weekly/monthly or monitor fire) |
| B probabilities | Isotonic on train tail | Recalibrate on a trailing purged window every day or few days |
| C | Shortlist search on val | Locked. No morning grid |
| Size | 1.1 N_min as a cell | Start smaller until fill journal matches; cap vs live depth |
| Fill model | Tape-touch | Own FOK fill/miss/partial, GTC time-to-fill |
| Leftover | $0 for selection | Mark at live book |
| Features | Tape + 3 heat stats | Same; **do not put own live orders into venue-liquidity features** without a flag |
| Book | Often absent historically | Live L2 as a hard cap |
| Fees | Training fee bit | Live schedule |
| Latency | Replay assumes X+1 | If that print is gone, skip |

**Kill-switch — any one, flatten:** rolling ~50-fill markout below
threshold; B calibration break for N days; activation-rate collapse;
dump-gap / leftover tail; fee or venue-mix shock; live fill rate much
worse than the simulator. Do not use “val would still promote” as a
live kill.

**Journal every decision**, including skips. That journal is the next
B-calib set and the next walk-forward train set.

## One paragraph

Fit B trees on the last 60–90 days, recalibrate probabilities on the
last couple of weeks, embargo, pick C only on the next 2–3 weeks from a
frozen shortlist, score the following 2–3 weeks without touching
anything, slide two weeks, repeat until you have several test slices.
Go live only if the concatenated test path beats the dummy, neighbors
are not a spike, B is still calibrated last slice, and a shadow week
shows fills that resemble the simulator. In production, freeze the trees
and the ticket, recalibrate the probabilities often, cap size on the
live book, journal your own fills, and kill on markout / calibration /
dump — not on a new grid search.
