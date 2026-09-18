# Proposal: complementary market-making research

**Status: DEAD.** Stop. Naive C baseline showed **yes_only + no_only =
60.8%** of attempts. Completes 27.9%. Inventory ate the merge. The
simultaneous two-buy design is not viable on this tape. Next line of
research: [`../directional-fok-v1/`](../directional-fok-v1/).

**Former status:** research proposal, not a trading go-ahead.
**Companion:** philosophy in [`README.md`](README.md), execution rules in
[`fill-model.md`](fill-model.md), first numbers in [`eda-snapshot.md`](eda-snapshot.md).

This is a machine-learning project with a **dollar P&L** objective. The
model decides whether a dual YES+NO clip at block `X` is +EV after
grace-period inventory, fees, and a **3-day** inventory mark
(complement rest 15 min, then dump, else $0). Size is not
a fixed dollar clip. The universe is the CLOB floor (5 shares and $1.20
of worst-price notional). Size above that floor comes from a
**4:1-capped Kelly** map of a model confidence, justified on nested
walk-forward so the map is not a free knob on the loss.

---

## 1. Decision this work is for

After this study the desk should be able to say one of:

1. **Trade a narrow subset** (fee-free, short-dated, dual-depth, confidence
   above a frozen threshold) at a Kelly size in `[1×, 4×]` the floor, with
   a stated expected $/day.
2. **Do not trade.** Edge in the prints is real, but after the 1-block /
   30-block fill model, fees, and one-sided inventory it does not survive.
3. **Collect L2 books** before deciding — fill-from-prints is too biased
   to bet.

The first-look EDA is enough to justify the study. It is not enough to
size a live book.

---

## 2. Opportunity definition

A signed buy is `(N, P_worst)`. The CLOB will not accept the order unless
**both** hold:

1. `N ≥ 5` shares
2. order value ≥ $1.00

Because the signed price is a **worst** price, realized spend is
`N × fill_price` with `fill_price < P_worst` (cap-touch does not fill).
A $1.00 worst-price notional can realize under $1.00 and fail. The desk
floor is therefore:

```
N        ≥ 5
N × P    ≥ 1.20
N_min(P) = max(5, ceil(1.20 / P))
```

EDA and the simulator **only** count a side as crossable when consumed
depth on that outcome is at least 5 shares **and** at least $1.20 of
USDC (proxy for `N × P_worst`) at prices that would fill under the
strict-better-than-cap rule. A pair opportunity is that condition on
both outcomes (aggressive leg in block `X+1`, complement in
`[X+1, X+30]`).

The model does **not** pick a research clip of $2 or any other fixed
dollar amount. It may refuse the attempt (`size = 0`) or take a stake
between the floor and four times the floor for that attempt’s `P`.

---

## 3. Why simultaneous buys, not buy-then-sell

The economic object is a cheap complete set: YES + NO < $1 after fees.
There are two ways to get there.

**This study — both buys at block `X`.** The aggressive take and the
resting complement are signed together. The complement is already on
the book in the same window as the trigger. Polynode `B+1` applies once.
The cost is structural one-sided risk, including the ugly path where
the *resting* leg fills and the take misses. That path is priced with
the inventory recipe in [`fill-model.md`](fill-model.md), not assumed
to be rare.

**Not this study — sequential.** Buy one outcome, wait until that fill
is observed, then buy the complement or sell the position at the other
book. If the first purchase misses, nothing else is sent, so
complement-only inventory cannot happen. That is a real reduction in
one-sided risk. It also adds at least one block of latency (observe
`X+1`, POST, land `X+2` at best). The other price that made the pair
cheap is often gone by then. Modeling it means a second fill simulator
conditional on the first take having already walked the book — a
different implementation, a different lag study.

Selling a one-sided position back out is in scope only as the
**day-7 forced dump** in §4. It is not the entry design.

The block-`X` ticket is always **two buys**. The model never emits a
purchase and a sale as the recommended action, including the case where
both prices would go through the last print on their books (lift YES
ask and hit a bid). That would be a different strategy.

**Momentum is out of scope.** The model cannot choose “buy the trending
leg, skip the complement, sell later.” Both entry orders are buys.
`p_yes_only` / `p_no_only` are failure states to be priced, not a
momentum signal to lean into. A dump that happens to sell above entry
is an accident of the inventory mark, not a selectable strategy.

---

## 4. One-sided exposure in expected value

One-sided inventory is the main way this strategy dies. The mark is
capped at **3 days**. The simulator uses a complement rest, a market
sell**, in that order, and never looks further.

### Three-phase exit, 3-day death

| Phase | Window from one-sided fill | Action |
|---|---|---|
| Complement rest | (0, **15 min**] | Buy the missing side at the **signed** cap from block `X`. Min time on book = 30s (grace). Cutover is 15 min (allowed band was 30s–12h). |
| Market sale | (15 min, **3 days**] | FOK sell the held token at whatever bid exists, first legal dump in block order. |
| Worthless | after **3 days** | `$0`. No later payout. |

Resolution inside 3 days is cash only if it is the earliest of
complement fill, resolve, and dump.

Fifteen minutes is past grace and inside a 15-minute crypto bar. Three
days is the hard cap. Attempts with `fill+3d` past the frontier are
excluded from headline P&L.

### Economic EV (headline, go/no-go)

```
PnL = merge_pnl + inventory_pnl − extra_fees
```

- `merge_pnl` is locked cash from complete pairs. No future beyond the
  two fills.
- `inventory_pnl` is the 3-phase recipe above, as a **label**. Using a
  payout or dump that occurs after `X` is not leakage. Using the
  payout, resolve block, or dump price as a **feature** at `X` is
  leakage and is forbidden.
- No lookahead of the best bid inside the 3 days. The dump is one
  window at the deadline. A miss is zero even if a print exists the
  next day or the market later resolves YES.

Short-dated one-sided EV can be near zero (paid ~p, paid out ~p) or
negative under adverse selection. Long-dated or quiet one-sided EV is
usually **−entry**, because the dump fails. That is intentional.

Time value of money is **not** the mark. `r_block × capital_blocks`
may appear in utility `V`. It never replaces `inventory_pnl`.

### Decision utility (fire rule only)

```
V = E[PnL | π, μ_merge, μ_inv]
  − λ × (p_yes_only + p_no_only) × q_hat
  − μ × E[capital_blocks] × r_block
```

`E[PnL]` already contains complement rest, dump, resolve, or zero. `λ` is an
extra **risk** haircut. `λ` and `μ` are chosen on **validation**, frozen
on test.

Headline go/no-go uses `sum(PnL)` with **`λ = 0` and `μ = 0`**.

### What the heads see

Stage-2 `μ_inv` is trained on `inventory_pnl` from the 3-phase recipe,
per leftover share, Huber loss, at `m = 1`. It will be near `−entry`
on long-dated misses and near `payout − entry` on short-dated resolves.
It is not a constant and not an unbounded resolution expectation.

---

## 5. What the first look already shows

Window: last 48 non-empty `fills_v1` partitions, blocks 93,310,000–93,789,999
(~11.7 days ending 2026-09-14). Older slice near block 85.7M (~3 days)
agrees on the shape.

### Data the study would train on

| Dataset | On disk | Grain |
|---|---|---|
| `fills_v1` | **1.94B legs, 38.8 GB**, 6,019 partitions, blocks 33.6M–93.8M (~4 years) | one beneficial fill leg |
| `condition_by_block_v1` | 336M bars, 7.7 GB | `(block, condition)` OHLC |
| `condition_by_10k_v1` | 15.2M rows, 0.87 GB | partition bar + resolution |
| `token_id_map_v1` | token → condition, YES/NO, NegRisk | join |
| `account_*_by_10k_v1` | who already makes these books | competition features |

**2.26M** distinct conditions, **2.21M** resolved in-sample. Payouts
(on-disk JSON includes spaces, `["1", "0"]`): YES wins **866,405**, NO
wins **1,336,167**, voids **13,030**. Lifetime matched USDC **~$45B**.
Conditions that ever printed a USDC fee: **1.05M**, carrying **47.8%** of
matched USDC.

Typical print: `|gross_usdc|` p50 **$3.35**, p90 **$36**, p99 **$529**.
The 5-share / $1.20 floor sits in the liquid center of the book.

### Sided exposure at the floor

Treat every `(block, condition)` with a YES **taker-buy floor** as a
synthetic aggressive fill (interpretation A — optimistic). Ask whether a
NO floor appears in the complement window.

| Window | YES floor bars that find a NO floor | Miss |
|---|---|---|
| same block | 19.7% | 80.3% |
| +1..+15 (grace, ~30s) | +27.1% vs nothing | — |
| +1..+30 (model window) | +33.4% vs same-block-only | — |
| anywhere in [0, 30] | **53.1%** | **46.9%** |

Reverse (aggressive NO): **49.6%** miss through +30.

Same-block **maker-sell** (resting asks actually hit) dual floor is
thinner: **64,427** vs **407,839** taker-buy duals (~6× less). If the
strategy only lifts asks, capacity is the 64k figure.

Firing whenever the aggressive book prints a floor clip is a one-sided
inventory machine. The model’s job is to refuse most of those bars.

### Merge edge at the floor clip

On the 407,838 same-block dual floor bars, walk the cheapest shares
until `N ≥ 5` and `N × p_marginal ≥ $1.20`:

| | |
|---|---|
| mean pair cost (YES VWAP + NO VWAP) | **0.9916** |
| p10 / p50 / p90 | 0.9500 / 1.0000 / 1.0166 |
| pair < $1.00 | 44.7% |
| pair < $0.98 | 20.7% |
| mean edge **when** pair < $1, per complete share | **3.66¢** |

Five complete shares at that edge is ~**18¢** gross merge **before** fees
and before the 47% miss path. That is the minimum order, not a
model-chosen size.

Unconditional all-print VWAP (not the floor walk) has mean pair cost
**1.0094**. Average flow is **above** $1. The edge is in the **best**
shares. The simulator must walk price.

### Size cap vs depth

Kelly may scale a positive stake up to 4× the floor (20 shares and $4.80
as a coarse global proxy; the live cap is 4× `N_min(P)` for that
attempt).

| Size | Dual same-block taker-buy bars | both / YES |
|---|---|---|
| 1× floor | 407,839 | 19.7% |
| 2× floor | 298,064 | 18.7% |
| 4× floor | 202,218 | 17.2% |

About half of 1× dual bars still have 4× depth. Completion rate does not
collapse. There is room to size the best attempts without leaving the
universe.

Hold-to-resolution: of 2.21M resolved-and-traded conditions, last-fill
10K to resolve 10K is **p50 = 0 partitions**, **p90 = 1** (~6h). **185k**
(8.4%) go quiet more than ~6h; **27k** (1.2%) more than ~2.4d.
Short-dated crypto / sports die fast. The left tail is long-dated
politics sitting illiquid.

### What this look does not measure

Trigger-conditional fills, strict-better-than-cap, competition κ,
buy-side token fees, one-sided mark-to-resolution, live `oas` grace,
and P&L at Kelly size rather than at the floor. Those are the study.

---

## 6. Modeling plan

### 6.1 Unit of prediction

One **attempt** = `(trigger_block X, condition_id, side_map, P_yes, P_no)`.

At `X` both orders are signed. The simulator in [`fill-model.md`](fill-model.md)
replays `X+1` for the aggressive leg and `[X+1, X+30]` for the complement,
then marks leftover inventory.

The model outputs:

1. A **state distribution** and **payoff heads** (below).
2. A **confidence** `c` used only to size accepted attempts.

It does not output a dollar clip. `N_yes = round(m × N_min(P_yes))` with
the same for NO, `m ∈ {0} ∪ [1, 4]`.

Labels are the simulator’s dollar vector at the **chosen** `m`, not a
fill/no-fill bit.

### 6.2 Simulator (must exist before any model)

New code, not a derived dataset v1 until the rules stabilize:

1. Build per-block, per-condition, per-outcome **consumed depth by price**
   from `fills_v1` (taker-buy vs maker-sell, mint/merge tagged).
2. Screen the floor: skip a side that cannot fill `N_min(P)` shares at
   prices strictly better than `P` with `N × P ≥ 1.20`.
3. Walk the booked size `m × N_min(P)` under the same strict cap.
4. If one-sided or leftover: 15-min complement rest at signed cap,
   then market dump, else **$0 at 3 days** (§4). Attempts with
   `fill+3d` past the frontier are excluded.
5. Apply the fee curve: condition-level fee bit from historical
   `fee_usdc > 0`; taker fee ≈ 1.35% × `min(p, 1-p)` on fee markets;
   maker 0.
6. Emit the attempt vector in fill-model.md.

Three fill interpretations, reported as bands: **A** replace-a-taker,
**B** compete (`κ=2`), **C** maker-sell asks only. Headline path **C**.

Candidate generation:

- **Unconditional grid** (calibration): every bar that clears the floor
  on the aggressive book. Too many; downsample.
- **Naive baseline:** fire at `m = 1` if last-block floor-walk pair cost
  < 0.99 and both books printed a floor.
- **Model candidates:** fire iff the decision value is positive; size
  from capped Kelly.

Caps `P_yes` / `P_no` from block `X` close plus a tick buffer, **not**
from `X+1`.

### 6.3 Features at block X (no leakage)

**Naive baseline (live participation rule today):** dual maker-sell
floor at `X`, floor-walk pair cost < 0.99, `close_yes` for caps,
relative depth to pick the aggressive side. No fee filter, no clock,
no competition, no title.

**Trained model** (not fit yet). All features must be computable from
data with `block_number ≤ X`.

| Family | Source | Examples |
|---|---|---|
| Microstructure | `fills_v1`, `condition_by_block_v1` | last 1/15/30/100-block YES OHLC, range, matched USDC, taker-buy YES vs NO imbalance, floor-walk VWAP each side if the floor exists, seconds-since-last-print each side |
| Completeness | same | last observed pair cost, fraction of last 30 blocks with dual floor, mint-match share |
| Fees / venue | `fills_v1.fee_usdc`, `market_id` | fee bit, NegRisk bit, tick inferred from price grid |
| Clock | block number | hour residue (`block % 41136` as in `account_by_10k_v1`) |
| Competition | `account_by_10k_v1` lagged | maker-fill ratio of top accounts in this condition’s last 10K |
| Horizon | `condition_by_10k_v1` | age since first trade, historical lifetime of similar conditions. **Do not** use the eventual resolve block. |
| Size room | fills | 1× / 2× / 4× floor depth last block (consumed shares and USDC) |

Gamma titles, sports start times, CLOB `oas` are not in the parquet.
Phase 2 can scrape Gamma for live features; backtests run without them
unless a cheap historical join appears.

### 6.4 Model heads

Tabular, local CPU. `scikit-learn` is already in `pyproject.toml`
(`HistGradientBoostingRegressor` / classifier). No PyTorch for v1.

**Stage 1 — outcome head** (multinomial or four binaries):

```
π = (p_both, p_yes_only, p_no_only, p_miss)
```

Trained on simulator states at **m = 1**. Log-loss. No notional
re-weighting that smuggles size into this head.

**Stage 2 — payoff heads** (Huber regressions), also at m = 1:

```
μ_merge  | both          # merge P&L per complete share, after fees
μ_inv    | one-sided     # inventory_pnl per leftover share (7-day §4 recipe)
σ²_pnl   | attempt       # variance of simulator P&L, for Kelly
```

`μ_inv` is fit on the 3-phase inventory mark in §4 (complement rest,
dump, resolve, or $0). It is not a mid and not an unbounded payout.

**Decision value** at the floor (`m = 1`) is the utility `V` in §4:
predicted economic EV, minus val-only `λ` / `μ` haircuts. Fire iff
`V ≥ V_min` and `p_both ≥ p_min` and predicted pair cost
`< 1 − fee_buffer`. Headline P&L ignores `λ` and `μ`.

**Stage 3 — confidence and Kelly size**, only on attempts that pass the
fire rule. See §6.5.

A single-head alternative (Huber on simulator P&L at m = 1) is a
baseline. It is not the Kelly input.

**Not the loss:** accuracy, F1, or “predict pair cost < 1.” Those will
buy one-sided inventory.

### 6.5 Confidence and 4:1 Kelly

The model may emit a confidence `c` for accepted attempts. `c` is the
only channel into size. Constraints:

- On any train, val, or test fold, among **positive** bets,
  `max(c) / min(c) ≤ 4`. Implement as `c = clip(c_raw, c0, 4 c0)` with
  `c0` frozen from the train fold (e.g. the 5th percentile of `c_raw` on
  fired train attempts). The model is not free to span an arbitrary
  dynamic range.
- Size multiple `m = 1 + 3 × (c − c0) / (4 c0 − c0) ∈ [1, 4]`, or `m = 0`
  when the fire rule fails. Equivalently, `m` is a monotone map of `c`
  onto `[1, 4]`.
- Kelly supplies the **shape** of that map, not an uncapped fraction of
  bankroll. Full Kelly `f* = μ / σ²` (or `p − q/b` on a binarized merge)
  is estimated on the **train** fold from simulator P&L of fired
  attempts at m = 1, then scaled so the implied `m` lands in `[1, 4]`.
  Half-Kelly is an allowed variant, chosen on val, frozen on test.

This is a legitimate use of Kelly (size proportional to estimated edge
over variance, bounded). It is not legitimate if `c` is a learned
scalar whose only job is to down-weight losses in-sample.

**Guards so Kelly is not a free loss-control variable:**

1. **Nested walk-forward.** Inner loop (train→val) may pick among
   {flat m=1, half-Kelly 4:1, full-Kelly 4:1}. Outer loop (test) sees
   one frozen choice. No peeking.
2. **Ablations reported on every test fold:** flat floor; 4:1 Kelly;
   uncapped Kelly (diagnostic only, not a candidate). If 4:1 Kelly beats
   flat on train and val but not on test, the map overfit and is
   discarded.
3. **Monotonicity.** Higher `c` must not be allowed to reverse `V`’s
   sign. `c` sizes; it does not veto. Veto is the fire rule in §6.4.
4. **No extra loss term on `c`.** Do not add a penalty that uses `c` to
   shrink reported loss. `c` is applied after the heads are fit.
5. **Bankroll is not the account.** Kelly is relative to a **per-attempt
   floor**, not to total equity. A 4× floor on a 50¢ outcome is 20
   shares / $10 signed, not 4× the desk’s cash.

### 6.6 Evaluation

Walk-forward on **time**, never random rows.

```
train:  partitions [T0, T1)
val:    [T1, T2)     embargo 3 days so every inventory label is complete
test:   [T2, T3)
then slide
```

Primary folds on the last ~90 days. One frozen fold on 2024-11 (election)
and one on the older 85.7M slice.

**Report in dollars, not points:**

| Metric | Why |
|---|---|
| Sum P&L, P&L / day, P&L / $ signed notional | headline |
| P&L split: merge vs resolve-in-7d vs dump vs dump-miss ($0) vs fees | where the tax is |
| State mix: both / yes_only / no_only / miss | sided-exposure tax |
| Share of one-sided marked $0 (dump miss) | illiquid tail |
| P&L fee vs fee-free, NegRisk vs binary | venue |
| Calibration of `p_both` | stage 1 |
| Flat m=1 vs 4:1 Kelly vs uncapped Kelly | §6.5 |
| vs naive baseline (pair<0.99 and dual floor) | is the model doing work |
| vs always-miss (0) | sanity |
| Capacity: attempted notional at 1× / 2× / 4× | can we size |
| Turnover vs available dual-ask floor bars | are we picking scraps |

**Go criterion (draft, revisited after sim v0):**

- Test P&L > 0 after interpretation **C** and the `κ=2` band, **and**
  after replacing 4:1 Kelly with flat m=1 (Kelly may add, it may not
  rescue a dead edge).
- Inventory P&L is not the sole source of profit.
- `p_both` Brier better than the naive dual-floor rate.
- Worst test week not worse than `−2 ×` median week.

Fail any of those → recommendation 2 or 3 in §1.

### 6.7 Leakage and cheats to forbid

- Features from `X+1` or the fill window.
- Using resolution block as a feature.
- Training on the same `(condition, day)` that is tested (purge).
- Counting mint maker-buys as lift-able asks.
- Filling at exactly the signed cap.
- Marking inventory with a payout or print after day 3, or with a last trade.
- Scoring attempts whose `fill+3d` is past the data frontier.
- Fitting Kelly, `λ`, `μ`, or `c0` on the test fold.
- Letting `c` span more than 4:1 on positive bets.
- Scoring the model on P&L that used a size the fire rule would not have
  permitted.
- Treating one-sided inventory as a momentum trade (buy the trending
  token, sell later). Entry is two buys; a sell is only the day-7 dump.

---

## 7. Sources and resources

### Already on the desk

- Parquet pipeline in the [data catalog](../../docs/Data%20catalog.md).
- Fill semantics and fees: [How CLOB works](../../docs/How%20CLOB%20works.md),
  [How transactions work](../../docs/How%20Polymarket%20transactions%20work.md).
- Execution primitives: `exchange_client/lib/trading_lib.py` (FOK buy,
  GTD with a short TTL), `traders/buy_token`.
- Hardware as in the repo README: MacBook Pro M5 Pro, 24 GB, 2 TB SSD.
  Full `fills_v1` is 39 GB; DuckDB scans of 48 partitions were ~7s in the
  EDA. A 90-day simulator pass stays on this machine with a 10 GB DuckDB
  cap and `SCRATCH_DIR` spill.
- Python: DuckDB, pandas, sklearn, scipy — already pinned.

### Not required for v1

- GPU / cloud training.
- New raw scrape of the chain.
- A live trader (out of scope until §1 says “trade”).

### Optional, only if v1 is inconclusive

| Extra | Why | Cost |
|---|---|---|
| CLOB L2 snapshots (best 5 levels / 2s on a watchlist) | kill interpretation A vs C | new producer, disk, live process |
| Gamma market metadata (end date, `feesEnabled`, sport vs crypto) | horizon + fee bit without waiting for the first fee print | API + join table |
| Polynode inclusion log from `explorations/polynode_inclusion_test*.py` | replace `X+1` with an empirical lag mix | already in repo, small |
| Live `GET /clob-markets/{id}` `oas` (min order age) | confirm 30s grace vs per-market | one-shot script |

### Compute budget (this machine)

| Job | Data | Wall clock (est.) |
|---|---|---|
| EDA (done) | metadata + 48 + 12 partitions + full 10K | ~20s |
| Depth cubes last 90d | ~360 partitions × ~0.6M legs | 30–90 min |
| Simulator v0 on 90d candidates | tens of millions of attempts, downsampled | 1–3 h |
| Feature materialization | join to `condition_by_block` | 1–2 h |
| HGB walk-forward, 4 folds, heads + nested Kelly | sklearn, CPU | 2–4 h |
| Sensitivity (κ, δ, m, λ, C vs A) | rerun sim, not refit | 2–4 h |
| Full-history robustness (optional) | 39 GB fills | overnight |

Peak RAM: DuckDB at 10 GB; spill to `SCRATCH_DIR`. If a 90-day cube does
not fit, cut iteration to 30 days and keep 90 days for the final test
fold.

---

## 8. Work sequence and calendar

One person on this desk. Parallelizable where noted.

| Phase | Work | Time | Output |
|---|---|---|---|
| **0. Done** | philosophy, fill rules, floor-screen EDA | — | this folder |
| **1. Simulator v0** | price-walk depth, mint tag, strict cap, 5-share / $1.20 floor, 1-block / 30-block, 15m complement rest then dump, $0 at 3d, fees | **3–4 days** | `sim.py` + labeled attempt parquet in scratch |
| **2. Baseline P&L** | naive “dual floor and pair<0.99” at m=1 under A/B/C | **1 day** | go/no-go *hint*: if C+naive is deep red after inventory, shrink scope |
| **3. Features** | block-X feature table, leak checks | **2–3 days** | feature parquet + dictionary |
| **4. Model v0** | stage-1 state probs + stage-2 payoffs, walk-forward, **flat m=1 only** | **2–3 days** | metrics vs naive |
| **5. Capped Kelly** | nested val choice of flat vs half vs full 4:1; test frozen; uncapped diagnostic | **2 days** | size ablation table |
| **6. Price the miss** | λ/μ grid, fee vs free, short-dated vs long, δ | **2–3 days** | decision table |
| **7. Robustness** | older slice, election fold, κ=2, X+2 lag, leftover < 5 shares, 3-day / 14-day horizon | **2 days** | fail cases |
| **8. Write-up** | this proposal §1 circled | **1 day** | decision memo |

**Elapsed: ~15–19 working days** to a decision memo.
**Calendar: ~3–4 weeks** if this is the only project; **5 weeks** if it
shares the machine with scrapes.

Checkpoint after phase 2 (end of week 1): if naive dual-floor under
interpretation C loses money *even ignoring inventory* (pair cost ≥ 1
after fees on maker-sell asks), stop and switch to L2 collection. Do not
spend weeks fitting a model to noise. Do not introduce Kelly before
flat m=1 is green on val.

---

## 9. Profitability range

Capacity envelopes from the 11.7-day window, then haircuts. They assume
attempts can be chosen; they are not a forecast.

**Gross merge at the floor (do not bank):**
same-block dual taker-buy floor bars with pair < $1: 182k in 11.7d.
~18¢ per completed 5-share clip → **~$2.8k/day** if every such bar were
taken, no fees, no competition, no cap misses, no one-sided path.

**Ask-side (interpretation C):**
64.4k dual maker-sell floor bars in 11.7d ≈ 5.5k/day. If the 44.7% /
3.66¢ shape carried over: **~$400/day** at m=1, still no fees/inventory.

**4× Kelly ceiling on the same fantasy:** about half of dual bars have
4× depth. If the model put 4× on the best half and 1× on the rest, signed
notional might rise ~2–2.5×, not 4×. Kelly does not mint a 4× P&L line.

**Haircuts that will apply:**

| Haircut | Effect |
|---|---|
| Fees on 47.8% of volume | ~0.5–1.4¢ per share on 50¢ fee books; can wipe a 1–2¢ edge; 3.7¢ edge still breathes |
| Competition κ=2 | maybe half the dual bars |
| Strict cap (no fill at the touch) | unknown; EDA did not sign a `P` |
| 47% one-sided if unfiltered | **the killer.** Short-dated leftover marks at payout; long-dated or quiet leftover is a dump, and a dump miss is **−entry**. One failed 5-share clip wipes many 18¢ merges |
| Leftover < 5 shares | cannot sell; at day 3 it is **$0** unless it resolved first |
| Not every bar is takeable | capital, rate limits, one position per condition |

**Ranges this strategy could support if the model works:**

| Outcome | What it would look like | $/day (order of mag.) | Capital |
|---|---|---|---|
| **Dead** | C+fees+inventory ≤ 0 on walk-forward at flat m=1 | $0; do not trade | — |
| **Hobby** | fee-free, short-dated, m=1, few dozen completes/day after gating | **$20–150** | low hundreds (inventory) |
| **Small desk** | 4:1 Kelly on the gated subset, refuse fee books | **$200–800** | low thousands |
| **Stretched** | same, using 4× depth on the fat tail | **$1k–3k** | mid thousands, real inventory tails |
| **Not this design** | needing >$5k/day | would require L2, larger than 4×, or quoting instead of lifting | — |

The prior after the EDA: **alive as a small desk module only if the model
can cut one-sided fills from ~47% into the low teens without giving up
the 3–4¢ merge**, with Kelly allowed to scale the remaining attempts
inside 4:1. If flat m=1 is dead, Kelly will not save it.

Annualized, the “small desk” band is **~$50k–200k/year** before
operational misses. It is a desk module that can pay for itself and
teach the fill model needed for larger MM later.

---

## 10. Risks that can zero the result

1. **Prints ≠ book.** Consumed volume is someone else’s fill.
   Interpretation A overstates the ability to lift. If C is already thin,
   L2 is required.
2. **Adverse selection on the filled leg.** The book that fills in one
   block is the one people are hitting. Completing the other side 30
   blocks later may be buying a knife. Inventory μ_inv can be largely
   negative; the model must be allowed to refuse.
3. **Fee markets.** Half of volume. Crypto up/down is also the
   short-dated inventory-friendly set. Attractive horizon and the fee
   bit may anti-correlate.
4. **Grace period.** The complement cannot be cancelled for ~15 blocks.
   A model that assumes optional cancel is cheating.
5. **Capacity illusion.** Dual-floor tape is everyone else’s. A FOK is
   additional demand. κ is not a decoration.
6. **Kelly overfitting.** A 4:1 confidence range can still memorize
   which train days were lucky. Nested CV and the flat-m=1 go criterion
   exist to catch that.
7. **Regime.** 11.7 days is one tape. Election week and quiet summer
   will not look like 2026-09.

---

## 11. Deliverables (if approved)

Inside `explorations/complementary-mm-v1/` (still exploration, not
`derived_data/` until rules freeze):

- `sim.py` — fill simulator, documented against `fill-model.md`
- `features.py` — block-X features
- `train.py` — walk-forward HGB, nested 4:1 Kelly, metrics JSON
- `eval-snapshot.md` — dollar report vs naive, A/B/C, flat vs Kelly
- Updated `PROPOSAL.md` §1 with a circled recommendation

No live `traders/` job in this phase.

---

## 12. Ask

Approve **phases 1–2** (simulator + naive baseline at m=1, ~one week).
That is enough to see whether interpretation C still has a merge edge
after fees at the 5-share / $1.20 floor. If yes, continue 3–8. If no,
stop or start L2 snapshots instead of fitting a model or a Kelly map.
