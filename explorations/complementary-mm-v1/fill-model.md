# Fill model

**DEAD research line.** See [`README.md`](README.md). Kept as the
archive of the two-buy simulator. `yes_only + no_only` was too large.

Rules the simulator uses. These are research assumptions, not CLOB
guarantees. When live behavior disagrees, change this file and rerun; do
not quietly “fix” a backtest.

Polygon block time in this repo is ~2.1s (`BLOCKS_PER_HOUR = 1714` in
`account_by_10k_v1`). 15 blocks ≈ 31.5s.

## Minimum viable order

A buy is legal on the CLOB only if it has **at least 5 shares** and at
least **$1.00** of order value. Orders are signed as `(N, P_worst)`, not
as a dollar amount at the intended fill. Realized spend is `N × fill_price`
with `fill_price < P_worst` under the strict fill rule below, so a $1.00
worst-price notional can realize under $1.00 and fail. The desk floor is:

```
N        ≥ 5
N × P    ≥ 1.20
```

`N_min(P) = max(5, ceil(1.20 / P))`. Signed notional at the floor is
`N_min(P) × P`, which is $1.20 on cheap outcomes and `5P` on expensive
ones.

An **opportunity** on one outcome is depth that can fill some `N ≥ N_min(P)`
at prices strictly better than `P`. A **pair opportunity** is that
condition on both YES and NO (aggressive leg in block `X+1`, complement
in `[X+1, X+30]`). Prints that fail either the 5-share test or the $1.20
worst-price-notional test are out of universe.

Size above the floor is chosen later (Kelly, 4:1 cap). The simulator’s
opportunity screen does not assume a fixed dollar clip.

## Orders signed at block X

Two buys, signed together, no book re-lookup:

| Leg | Intent | Size | Fill window | Time in force |
|---|---|---|---|---|
| Aggressive (YES in research code) | Take | `N_yes ≥ N_min(P_yes)` | **block X+1 only** | FOK (marketable) |
| Complement (NO in research code) | Take or rest | `N_no ≥ N_min(P_no)` | **blocks X+1 .. X+30** | marketable, then rest through grace |

`P_yes` and `P_no` are outcome-token prices in `[tick, 1-tick]`, chosen
from block `X`. Equivalent YES price of a NO buy at `P_no` is `1 - P_no`.

`N` is an integer share count. Realized spend is `N × fill_price ≤ N × P`,
usually strictly less ([How CLOB works](../../docs/How%20CLOB%20works.md)).

The complement may use a slightly worse cap or a few extra shares (δ) to
raise fill chance. δ is a priced parameter, not a constant. Both legs
still have to clear the 5-share / $1.20 floor after δ.

## Aggressive-leg fill (block X+1)

Walk consumed depth on that outcome in block `X+1`, best price first
(lowest outcome price).

Fill **if and only if** the walk accumulates **strictly more than** `N`
shares at prices **strictly better** than `P`, and `N × P ≥ 1.20`.

- Liquidity at exactly `P` does **not** count. Cap-touch is a miss.
- If the shares exist but the marginal share is at `P`, miss.
- Fill price is the VWAP of the shares taken, which is `< P`.
- No partial credit: FOK for the research clip.

“Equivalent YES price” means a NO-book trade at `p_no` is stored as YES
price `1 - p_no` in `condition_by_block_v1` / the fills formula.

## Complement-leg fill (next 30 blocks)

Same walk, over complement-outcome depth in `[X+1, X+30]` in block order,
best-price-first inside each block.

Fill if the window ever accumulates `N_no` shares at prices strictly
better than `P_no` with `N_no × P_no ≥ 1.20`. The first time the
cumulative crosses, that is the fill block and VWAP.

30 blocks (~63s) is wider than the ~15-block grace period. Grace is the
cancel constraint (~30s, CLOB `oas`). The 30-block window is a modeling
buffer for operator batching, Polynode `B+1` misses, and GTD expiration
thresholds. Do not shrink it without a live timing study.

## Polynode timing

[How CLOB works](../../docs/How%20CLOB%20works.md) — observe block `X`,
POST, land in `X+1` most of the time. Backtests assume `X+1` for the
aggressive leg. A sensitivity case delays it to `X+2`.

## What is not observed

Historical L2 books. `fills_v1` is **consumed** liquidity.

| Interpretation | Fill rule | Bias |
|---|---|---|
| A. Replace a taker | `N` shares traded at better-than-cap ⇒ we would have been that taker | Optimistic if we add demand |
| B. Compete | need `N × κ` shares traded, `κ ≈ 2`, before we assume we also filled | Conservative |
| C. Maker-only depth | count maker sell volume on that outcome (resting offers hit) | Closest to “asks we would lift” |

Default research path: **C** for the walk, report **A** and **B** as
bands. Maker sells of YES are the YES asks; maker sells of NO are the NO
asks. BUY-vs-BUY mint prints as two buys (taker YES + maker NO or the
reverse) — complementary demand, not an ask that can be lifted. Tag
mint/merge matches and do not treat the maker-buy as lift-able ask depth.

## Sided exposure — economic P&L

After the entry windows close, four states:

| Aggressive | Complement | Position | Economic P&L |
|---|---|---|---|
| fill | fill | `q_yes` YES + `q_no` NO | merge `min(q)` pairs at $1, minus fees; leftover shares follow the inventory recipe below |
| fill | miss | `q_yes` YES | inventory recipe |
| miss | fill | `q_no` NO | inventory recipe |
| miss | miss | flat | $0 |

Inventory is neutralized in **three phases**. Earliest event wins.
Nothing after 3 days is read.

```
GRACE_BLOCKS    = 15           # ~30s minimum time on the book (CLOB oas)
CUTOVER_BLOCKS  = 428          # 15 minutes (BLOCKS_PER_HOUR/4)
HORIZON_DAYS    = 3
HORIZON_BLOCKS  = 123_408
```

Fifteen minutes is the cutover from a fixed-price complement **buy** to
a market sale of the held token. The allowed band was 30 seconds to 12
hours. 30 seconds is only grace (no extra rest). 12 hours is a
directional session. 15 minutes is past grace, inside a 15-minute
crypto bar, and well under 12 hours.

Worthless at **3 days**, not 7. Labels are complete only when
`fill_block + HORIZON_BLOCKS ≤ data frontier`.

### Recipe (labels, not features)

Held `q` shares of one outcome after entry. Missing side is the
complement. Fixed complement cap `P_comp` is the **signed** cap from
block `X` (no re-lookup).

1. **Complement rest (fill, fill+15 min].** Rest a buy of the missing
   side at `P_comp`. The order has a 30-second minimum on the book;
   it is not cancelled at grace. Walk complement **asks** at `px < P_comp`
   in block order, same FOK share rule as entry. If that fills, merge:
   `inventory_pnl = q × 1 − complement_usdc − entry_usdc − fees`. Stop.

2. **Market sale (15 min, 3 days].** FOK **sell** the held token at
   whatever bid exists. First legal dump in **block order** (best bid
   inside each block). Not the 3-day high. Legal sell still needs 5
   shares and $1.20 worst-price notional. If it fills:
   `inventory_pnl = proceeds − entry_usdc − fees`. Stop.

3. **Still open at 3 days → $0.** `inventory_pnl = −entry_usdc − fees`.
   No later payout, no mid, no last trade.

**Resolution** inside 3 days is cash if it is the earliest of
{complement fill, resolve, dump}. A dump or complement fill that
happens first is not overwritten by a later payout.

Dust below 5 shares cannot rest a complement buy or dump; it resolves
if the market pays inside 3 days, else $0.

Leftover share counts below 5 cannot be sold. At the dump they are
worth **$0** (same as a dump miss). Dust from rounding is $0.

### What this is not

| Temptation | Why not |
|---|---|
| Hold to resolution with no time cap | The model’s EV wants infinity. The book is a 3-day MM book. |
| Mark to mid, last trade, or a later payout after a dump miss | A print is not cash. After a failed dump the label is $0. |
| Best bid anywhere in the 3 days | Lookahead of our own exit. Dump is first legal fill after cutover. |
| Funding rate as the whole one-sided cost | `r_block × capital_blocks` may sit in utility `V`. It is not `inventory_pnl`. |
| `λ × P(one-sided)` as the EV | `λ` is a fire-rule haircut. Headline EV uses the recipe above with `λ = 0`. |

### Adverse selection

Short-dated one-sided fills that resolve inside 3 days show the **actual
payout**, so a systematically expensive stuck side is negative `μ_inv`.
Long-dated or quiet books show up as dump-or-zero, which is usually a
full loss of `entry_usdc`. That is the tax for simultaneous entry on
markets that cannot be exited.

### Leakage line

```
features(block ≤ X)     no fills after X, no payout, no resolve block
labels                  fills after X, only through fill+7d:
                        resolution-if-in-horizon, else dump window, else $0
utility V               labels’ EV minus λ, μ  (val-only knobs)
headline P&L            labels’ EV only; attempts with fill+7d
                        past the data frontier are excluded
```

## Fees

- Fee-free vs fee-on is a **per-condition** bit. Once `fee_usdc > 0` on
  any fill, the whole condition charges.
- Taker ~1.35% of `min(p, 1-p)` on fee markets. Makers typically 0 after
  refund.
- `fills_v1.fee_usdc` is net USDC fee on sell legs. Buy-side token fees
  convert as `token_fee × mark_price`.
- The aggressive take pays the taker rate. A complement that actually
  makes pays 0 on fee markets; if it takes, it pays.

## Simulator outputs per attempt

```
N_yes, N_no, P_yes, P_no       signed size and caps
filled_yes, filled_no          bool
usdc_yes, usdc_no              realized spend
q_yes, q_no                    shares received
pair_cost                      (usdc_yes/q_yes) + (usdc_no/q_no) if both
merge_pnl                      min(q) * (1 - pair_cost) - fees_on_pairs
inventory_q, inventory_side
inventory_pnl                  complement rest 15m, else dump, else $0 at 3d
fees_usdc
capital_blocks                 blocks until flat
confidence                     model output, if sized
size_multiple                  in {0} ∪ [1, 4] of that attempt’s floor
state                          both / yes_only / no_only / miss
```

Headline attempt P&L = `merge_pnl + inventory_pnl − fees` on leftover
shares. Loss functions in [`PROPOSAL.md`](PROPOSAL.md) consume this
vector, not a fill/no-fill label. Size is an input to the simulator, not
a fixed clip the EDA invents.
