# Stage C — always-on decoder

C **bets every** Stage A event after the 180-block cooldown. No skip
head. Frozen Stage B’s four high/low deltas are the only inputs.

**Outputs (YES-space deltas to `last(X)`):**

| Output | How |
|---|---|
| Direction | YES if `h1_hi+h30_hi ≥ −h1_lo−h30_lo`, else NO |
| Entry delta | `in_frac ×` predicted X+1 extreme |
| Exit delta | `out_frac ×` predicted X+1..X+30 extreme |
| `should_bet_double` | 1 iff `P_exit−P_entry ≥ double_k` ticks |

`in=0.0 out=0.5 dbl=0` means: FOK at **last**, GTC at **half** of B’s
30-block predicted extreme, and **every** round is 2× on the score.
Size stays `1.1 × n_min`. Double is eval weight, not size.

If exit would sit at or below entry, exit is **entry + 1 tick**. GTC
window is `X+2..X+30`. Leftover after liquidation is **$0**.

Val picks `(in_frac, out_frac, double_k)` by **weighted** PnL
(`2×` when double). Test never selects. Headline dollars are **raw**
PnL. Walk-forward: [`walkforward-results.md`](walkforward-results.md).

## Wider margins (2–3 ticks only)

Same window, decoder locked to `P_exit − P_entry` of **exactly 2**, **2 or 3**,
or **exactly 3** ticks. 1-tick tickets are gone.

| edge | train best | tickets | $/ticket | val of that cell |
|---|---|---|---|---|
| 1 tick (prior grid) | +$16.48 | 2,965 | $0.006 | not promoted |
| **exactly 2 ticks** | **+$22.72** | 2,832 | $0.008 | **−$5.07** at 53/h |
| 2 or 3 ticks | +$19.61 | 2,852 | $0.007 | (not in val top) |
| **exactly 3 ticks** | +$18.83 | 2,781 | $0.007 | (not in val top) |

Val of the 2-tick train leaders: **−$4.92 to −$11.83**, all still ≥1/hour.
Nothing promoted. Wider margin improves **train** by a few dollars and
still dies on the next 20 hours of tape.

## Selective 30-block range + aggressive FOK

1/hour is a floor. This pass waits for a **loud 4-tick move in 30
blocks**, pays up to 2 ticks on the FOK (`pay2`), skips fee books, and
rests the GTC only 30 blocks (dump starts at 31). Frozen Stage B still
scores the FOK bar. A **train-only** 30-block activity/barrier head
scores the range. Families are not re-selected.

H30 **activity** val AUC 0.935 but pos rate **0.953** — after Stage A
almost every book still prints in 30 blocks, so the liquidity
threshold did not select. The 4-tick **barrier** did.

| split | n | /hour | P&L | FOK hit | $0 leftover |
|---|---|---|---|---|---|
| naive val | 4,692 | 228 | −$64.80 | 1.9% | 35% |
| naive test | 3,752 | 170 | −$32.59 | 1.6% | 28% |
| **C val** (promoted) | 503 | 24.4 | **+$0.29** | 6.8% | 0% |
| **C test** | 556 | 25.1 | **+$4.80** | 8.6% | 3.5% |

Promoted: `t_fill=0.70`, `t_range=0.60`, `k=4`, `pay2`, `exit_end=30`,
`skip_fee`. Same cell with GTC=60 is **val −$2.45**. Same cell `pay1`
is val −$2.45. 2-tick and 3-tick needs did not win.

## Does it print money?

A sliver: test **+$4.80** on 556 tickets in ~22 hours (~0.9¢/ticket),
val **+$0.29** (noise that happens to be positive). It beats naive and
always-skip on this weekend, and it is the first decoder that val
would promote. It is **not** a desk. 25/hour is still 25× the floor —
we can tighten `t_range` further. H30 activity is not a filter; the
4-tick-in-30-blocks head plus paying for the FOK is.

Same 4-day window as the Stage B freeze (11–14 Sep 2026), Stage A
was 3-of-8 on that card (now 5-of-8), 80k strided events. Naive
imbalance-side `+0/+1` is red
(val **−$65**, test **−$33**, FOK hit ~2%).

The best **train** cells are `t_fill=0.50`, `t_exit=0.50`, `k=2`,
account mode almost irrelevant:

| | |
|---|---|
| Train P&L | **+$16.70** |
| Tickets | 2,861 at 54/hour |
| FOK hit | 4.1% |
| $ / ticket | **$0.006** |

That is not an edge. One ghost (−$2.50 in the Stage A envelope) wipes
~400 of those tickets. Account-dominance filters (`crowd50` /
`diverse5` / `dominate50`) barely moved the dollar number — on this
weekend they are not the switch.

Those 12 train-green cells were scored on **val**. None cleared
≥1/hour **and** val P&L > 0, so **test is not a result**. Do not
quote a test dollar as a win.

## What this means

Stage A is still a usable gate. Stage B still ranks tape-touch.
Stage C, with those probs plus who is on the tape, still cannot turn
a 1-tick FOK/GTC clip into dollars after cooldown, fees, misses, and
leftover $0. The book is not printed. Always-skip is the honest
policy until a later decoder actually beats $0 on **val**, then
**test**.
