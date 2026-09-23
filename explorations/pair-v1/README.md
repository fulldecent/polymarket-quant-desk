# Pair (v1) — two-sided YES+NO

Formerly `complementary-mm-v1`. **DEAD.** Tickets =
[`two_sided_take_pair`](../FRAME.md) and
[`two_sided_make_pair`](../FRAME.md). Map: [`../README.md`](../README.md).
Do not extend this line.

Naive interpretation-C baseline
(`sim.py`, 7 days of triggers, 9,058 attempts): **yes_only 28.5% +
no_only 32.3% = 60.8% one-sided**. Both-leg completes were only 27.9%.
Merge P&L (+$631) was more than wiped by inventory (−$653). Headline
**−$21.70**. The simultaneous two-buy ticket cannot be the book: the
one-sided mass is too large to price away with a 15-minute complement
rest, a dump, or a 3-day $0 cap.

Successor: [`../directional-v1/`](../directional-v1/) (one
direction, FOK take at `X+1`, GTC exit `X+2..X+60`, liquidate
`X+61..X+120`).

This folder is an archive: philosophy, fill rules, naive snapshot.

---

A market-making idea for Polymarket binary conditions: buy **YES** and **NO**
of the same condition at the same time, complete a dollar, merge, and keep the
residual. This folder is the research desk for that idea — philosophy here,
fill rules in [`fill-model.md`](fill-model.md), the work plan in
[`PROPOSAL.md`](PROPOSAL.md), and a first look at the data via
[`eda.py`](eda.py).

This is not a live trader. Nothing here posts orders.

## What we are making

A complete set is 1 YES + 1 NO = $1 of collateral. If both legs are acquired
for less than $1 after fees, they merge into locked cash. If they cost more
than $1, the round trip lost money and should not have been sent.

That is market making in this microstructure. This is not a classic two-sided
quote on one book. It is a pair of buys on both outcome tokens of one
condition: **take** the liquid leg in the next block, **rest** the complement
through the CLOB grace period.

```
block X     a pattern is computable from landed data
            both orders are signed on the hot path
            (share count + worst price; no book re-lookup)
block X+1   the aggressive leg takes, or it misses
block X+1.. X+30
            the complement may take immediately or rest
            the grace period forbids a fast cancel (~30s / 15 blocks)
then        both filled  → merge complete pairs; leftover is inventory
            one filled   → sided exposure until an exit or resolution
            neither      → flat; only the attempt was spent
```

YES/NO labels are a convention in the research code. In production the
aggressive 1-block leg is whichever book is expected to fill now; the
resting 30-block leg is the other outcome.

## Why send both buys at once

Two implementation shapes can lock a complementary dollar:

| | **Simultaneous (this study)** | **Sequential** |
|---|---|---|
| Orders | Buy YES and buy NO signed together at block `X` | Buy one outcome; only after that fill is observed, buy the complement or sell the position at the other book |
| Latency | Aggressive leg modeled in `X+1`; complement in `[X+1, X+30]` | Second order cannot POST until the first fill has landed. Earliest complement is `X+2`, often later |
| Missed take | Complement may still rest and fill → **complement-only** inventory | If the first buy misses, the second is never sent. That path is flat |
| Confirmed take, failed hedge | Still one-sided (grace forbids a fast cancel of a live complement) | Same, but the second order is optional and can be skipped if the book has moved |
| Book at second send | Both caps from block `X` (no re-lookup) | Second cap could use post-fill state — richer, and a second stochastic layer |

Sequential is the cleaner risk profile: the desk never holds a resting
complement unless the first purchase is known to have gone through.
That is also why it is **not** the v1 design. Confirming a fill costs at
least one extra block. The cheap other side is often the print that
*was* available in `X+1`. A model of “wait, then lift” has to replay a
book that the first take already walked, with a lag this desk has not
measured. That is a different simulator.

This study sends both buys on the hot path so the complement is already
on the book during the same window as the trigger. One-sided risk is
accepted and **priced** (see [`fill-model.md`](fill-model.md)), not
assumed away. A later sequential variant can reuse the same floor,
fees, and resolution marks once Polynode inclusion lag is an input
rather than a guess.

One-sided inventory is neutralized in three phases: rest a complement
**buy** at the signed cap until **15 minutes** (30s minimum on the book),
then market-sell the held token, then **$0 at 3 days**. That is not the
entry ticket. Entry is two buys that hope to merge.

The recommended action at block `X` is always **buy YES and buy NO**.
It is never a purchase and a sale. Lifting both asks can print through
the last trade on each book (pay more than last on YES, pay more than
last on NO). That is still two buys. A sell is not on that ticket.

## Out of scope: momentum (buy, then sell the same token)

A rising YES book can look like “buy now, sell later higher.” That is a
directional momentum trade. This project does not do it.

- The fire decision is “can we complete a cheap YES+NO set,” not “will
  this token continue.”
- The two orders at block `X` are both **buys** (YES and NO). The model
  does not get to rewrite the second leg into a sell of the first.
- A sell exists only if the pair is already one-sided, and only as the
  day-7 **dump** (or a resolution payout before that). The dump is a
  liquidation at whatever bid depth is there. It does not wait for a
  trend, does not scan 7 days for the high, and a miss is **$0**.

If a dump happens to sell above entry because the tape went up, that
shows up in `inventory_pnl`. That is an accident of the mark, not a
strategy the model is trained or allowed to select. A momentum book
would need a different entry, a different exit, and a different label.

## Order constraints

Every signed buy must satisfy **both**:

1. **N ≥ 5 shares** (CLOB `min_order_size`, observed on all watched markets).
2. **N × P_worst ≥ $1.20**

The exchange minimum notional is $1.00. Orders are signed as share count
plus a **worst** price, not a limit at the intended fill. Realized spend is
`N × fill_price`, and `fill_price < P_worst` under the fill model (cap-touch
does not count). Signing at exactly $1.00 of worst-price notional can
realize below $1.00 and fail to execute. The working floor is therefore
**$1.20 of worst-price notional**.

Which constraint binds depends on `P_worst`:

| P_worst | N_min = max(5, ceil(1.20 / P)) | signed notional |
|---|---|---|
| 0.10 | 12 | $1.20 |
| 0.24 | 5 | $1.20 |
| 0.50 | 5 | $2.50 |
| 0.90 | 5 | $4.50 |

An **opportunity** is a moment when both books can be crossed at this
floor: each leg has depth of at least 5 shares with at least $1.20 of
worst-price notional at prices strictly better than the signed cap. Bars
that print $1 of USDC in 3 shares, or 20 shares at a penny without $1.20
of signed notional, are not opportunities.

Size above the floor is not a fixed clip. The model may output a
confidence that maps to a Kelly stake, capped so the largest positive
stake is at most **4×** the smallest (see [`PROPOSAL.md`](PROPOSAL.md)).
The floor itself still has to clear 5 shares and $1.20.

## Why this is hard

1. **Grace period.** A marketable order has a minimum time on the book of
   ~30 seconds (CLOB `oas`, ~15 Polygon blocks at ~2.1s). The complement
   cannot be pulled the moment the first leg fills. Sided exposure is a
   structural cost.
2. **Hot-path signing.** Share count and worst price come from block `X`.
   The book is not read again. Fill prices can be better than the cap;
   they are never worse. Realized USDC is ≤ signed notional (usually
   strictly less — [How CLOB works](../../docs/How%20CLOB%20works.md)).
3. **Strict fill rule.** A leg clears only if the window shows enough
   depth at prices **strictly better** than the signed cap to cover that
   leg’s `N` and `N × P_worst`. Liquidity at the cap exactly is a miss.
4. **Illiquid tails.** After a one-sided fill: complement buy at the
   signed cap for 15 minutes, then a market sale, then **$0 at 3 days**.
   Long-dated politics that cannot be completed or dumped is a full loss.
5. **Fees.** Taker fees on crypto up/down and some sports (~1.35% of the
   lopsidedness, net of refunds) can erase a 1–2¢ pair edge. Most politics /
   geopolitics / culture conditions are fee-free. `fills_v1.fee_usdc` is
   USDC-denominated only; buy-side token fees need a conversion.
6. **Mint/merge matching.** The exchange already matches BUY-vs-BUY (mint)
   and SELL-vs-SELL (merge). This strategy competes with that flow.

## What “good” looks like

An attempt is good if **economic EV** is positive after the 15-minute
complement rest, the later dump, and the 3-day $0 cap. Completing both
legs at `p_yes + p_no = 0.97` is not enough if a large share of attempts
die one-sided into a token that is marked to zero. The recipe is in
[`fill-model.md`](fill-model.md).

## Which markets, which features

**Naive baseline (what actually fires today).** A condition is in the
universe at block `X` only if all of these hold:

- Maker-sell depth on **both** outcomes meets the floor (5 shares and
  $1.20 worst-price notional) in that block.
- Floor-walk pair cost (cheapest legal clip on each ask) **< 0.99**.
- `condition_by_block_v1.close_yes_price` exists so caps can be signed
  (`close ± 1 tick`).
- Aggressive side = the outcome with more floor-walk shares at `X`.

That is the whole participation rule. The naive baseline does **not**
filter on fee vs fee-free, NegRisk, clock hour, account competition,
title, sports start, or time-to-resolution.

**Trained model (not fit yet).** Features at `X` only (`block ≤ X`):

| Family | Features |
|---|---|
| Completeness | floor-walk pair cost, dual-floor flag last 1/15/30 blocks, mint-match share |
| Depth | 1× / 2× / 4× floor shares and USDC on each ask |
| Microstructure | YES OHLC / range / matched USDC last 1/15/30/100 blocks, taker imbalance, seconds since last print each side |
| Venue | fee bit (`fee_usdc>0` on the condition), NegRisk `market_id`, inferred tick |
| Clock | hour residue (`block % 41136`) |
| Competition | lagged maker-fill share of top accounts on this condition |
| Horizon proxy | age since first trade, historical lifetime of **similar** conditions — never the eventual resolve block |

Gamma titles and game-start times are not in the parquet; live trading
may add them later. They are not in the backtest unless a historical
join is built.

The research question is not whether complementary asks ever sum to less
than $1. They do. The question is whether a trigger computable at block
`X`, signed without a second book lookup, at a size between the 5-share /
$1.20 floor and 4× that floor, still has edge after grace-period inventory
and fees.

## Data already on the desk

On-chain fills and bars only. No historical L2 book. Liquidity in a block
is **consumed** volume, a lower bound on what was resting. See
[`fill-model.md`](fill-model.md) for the simulator.

| Dataset | Role |
|---|---|
| `fills_v1` | Every beneficial fill leg; YES-equivalent size, USDC, fees, taker flag |
| `condition_by_block_v1` | Per-block OHLC YES price, matched volume |
| `condition_by_10k_v1` | Partition bars plus resolution (`payout_numerators`) |
| `token_id_map_v1` | Token → condition, YES/NO index set, NegRisk `market_id` |
| `account_*_by_10k_v1` | Who is already making these markets (competition) |

## How to run the first look

```sh
source .venv/bin/activate
python explorations/pair-v1/eda.py
python explorations/pair-v1/eda.py --last-partitions 20
```

Writes `eda-snapshot.md` next to this README. Early exploration; a dirty
git tree is fine.

Naive fill simulator (interpretation C, 15m complement rest, $0 at 3d):

```sh
source .venv/bin/activate
python -m pytest explorations/pair-v1/tests -v
python explorations/pair-v1/sim.py --trigger-days 1
```

Writes `baseline-snapshot.md` here and attempts parquet under `$SCRATCH_DIR/pair-v1/`.
