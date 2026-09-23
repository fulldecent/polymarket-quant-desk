# Signals and features (brainstorm)

The naive gate (~27k tickets/day after the 180-block cooldown) is not a
strategy. It only says “this condition had a legal maker-sell floor at
`X`.” The model’s job is to **refuse most of those tickets** and keep
the ones whose simulator P&L is positive.

This note is a menu. Nothing here is fitted. Every feature is
`block ≤ X` only, and **only from the last 100 blocks** (~3.5 minutes).
No cooldown recency, no lagged 10K account tables, no lifetime-since-
genesis. Evaluation is **dollars**, not accuracy.

**Buckets are views, not verdicts.** Windows of 1 / 5 / 15 / 30 / 60 /
100 blocks are tried together. A dead 15-block column is not proof that
imbalance (or distance, or return) is useless — the 5-block or 60-block
view, or a cross-bucket max/mean, may still pay. Do not retire a family
because one bucket failed.

## Stage A — persist (before any model)

See [`STAGE_A.md`](STAGE_A.md) (frozen). **6 of the last 8** blocks
have a fill **and** Kris Kross count ≥ **10**.

## Stage B — tape-touch (the heads)

Every fill has a buyer and a seller. YES at `p` is NO at `1−p`. Last
price is **one** YES-equivalent number. Both entry directions are
`+k` (price up → buy YES) and `−k` (price down → buy NO). A side is
never skipped because that token did not print at `X`.

**Tick (an input).** From prices in the last 100 blocks, take the
coarsest documented step that still fits every print:

`0.1`, `0.01`, `0.005`, `0.0025`, `0.001`, `0.0001`
([market details](https://docs.polymarket.com/market-data/market-details)).

If that would be `0.1` and there are fewer than 3 distinct prices, use
`0.01` (sparse `0.50`/`0.60` is not a 10¢ book). Tick is a feature.

**Labels** (binary). Two horizons, not one mushy `Z`:

| Horizon | Blocks | Role |
|---|---|---|
| `H1` | `X+1` only | FOK bar |
| `H30` | `X+2..X+30` | short GTC / “soon” range. Frozen **with** Stage B |
| `H60` | `X+2..X+60` | long GTC window (does **not** include `X+1`) |

`H30` is not fit inside Stage C. It is trained on Stage B’s **train
slice only** (strictly earlier than C’s decoder search) and frozen in
the same joblib. If C refit it on the labels C is scored on, C is
just relearning B.

| Head | Yes iff |
|---|---|
| `0` | any fill on this condition in that horizon, **or** on-chain resolve in that horizon (redeem is liquidity) |
| `+k` | some fill with YES-price `≥ last + k·tick` (`k = 1..4`), **or** resolve in-horizon that settles YES at a price that clears the barrier (YES win → 1.0) |
| `−k` | some fill with YES-price `≤ last − k·tick` (`k = 1..4`), **or** resolve in-horizon that settles YES at a price that clears it (NO win → 0.0) |

Resolve **after** the horizon is unused (not a feature, not a label).
Draw/void `["1","1"]` settles at 0.5. Same-block and `X+1` resolves are
rare; dump `X+61..X+120` is where this bites (see
[`resolve-in-horizon.md`](resolve-in-horizon.md)).

**Modeling (not a net).** Two sklearn `HistGradientBoostingClassifier`s
(`loss="log_loss"`, early stopping on that loss) so the easy “will it
print?” head cannot dominate, and C gets proper probabilities:

- **activity:** features + horizon `Z` → `P(any fill)` (`k = 0`)
- **barrier:** features + signed `k` + `Z` → `P(touch last±k)` (`k ≠ 0`)

`k` and `Z` are inputs, not eight separate models. Early stopping on a
10% **train** holdout. Val selects. Test never selects.

**All families, then select — not a 20-column starter.** Every
last-100-block condition-level family we can build from `fills_v1`
goes in (level, persist including `kris_count_8`, depth, counts, range,
flow, return, adverse, venue). No 10K account tables, no cooldown
recency, no `block > X`.
Fit all on train. On val, permute one family at a time. Drop it if
both Δ activity-H1 AUC and Δ hard-subset H60 `±1` AUC are `< 0.002`.
`level` (last, tick, min_p, hour) is never dropped. Freeze
**selected** if its val primary is within 0.003 of all-features, else
**all**.

Primary val metric: mean hard-subset H60 AUC at `k = ±1` (lookback
range has not already cleared a 1-tick barrier). Headline ~0.90 AUC
on the easy slice is not the question. A simple-rule ranker (lookback
ticks already traveled) and a linear model are diagnostics, not freeze
candidates.

Freeze metrics are **per head**, plus that **hard subset**, plus
family permutation, tick strata, and calibration. Size / FOK stay in
Stage C. Code: [`stage_b.py`](stage_b.py). Stage B is **frozen** as an
input to Stage C, not the trader.

## Stage C — extras on top of Stage B

Stage B does not see unique-account concentration (it summed per-block
counts). Stage C adds, still last-100-block only:

| Feature | Why |
|---|---|
| `top_share` / `hhi` / `n_acct` | one account dominating the window vs a crowd |
| `top_share_x` / `n_acct_x` | same at `X` only |
| fee bit | Stage B dropped venue; a 1-tick edge dies on fees |

Decoder (few parameters): fire if `p_act_1` and `p_bar[±k,H60]` clear
thresholds, account-mode filter, optional skip-fee, long caps with
`P_exit > P_entry`, size `1.1 N_min`. Sequential P&L after 180-block
cooldown. Val ≥1/hour and P&L>0 to promote. Test never selects.
Code: [`stage_c.py`](stage_c.py).

## What the model must get right

P&L of an entered clip, per share, is roughly:

```
pnl ≈  (share_exit)  × (vwap_exit − vwap_entry)
     + (share_liq)   × (vwap_liq  − vwap_entry)
     + (share_zero)  × (0         − vwap_entry)
     − fees
```

plus `pnl = 0` when the FOK misses. So four questions, four heads (or
one joint utility):

| Head | Question | If we get it wrong |
|---|---|---|
| Fire | Is E[pnl] > 0 after fees and zero-mass? | 27k/day of noise |
| Direction | YES or NO | we buy the dumped side |
| Caps | `P_entry`, `P_exit` in ticks | FOK miss, or an exit that never prints |
| Kelly | how many floors to take in `[1, 4]` | size the losers |

A 1-tick round trip at 50¢ is 2¢/share before fees. Fee markets take
~0.7¢/share at 50¢. The model is hunting a **thin** edge on a **2-minute**
exit (`X+2..X+60` ≈ 2 min, dump another ~2 min). Features that matter
are short-horizon tape, not “will this resolve YES next month.”

Universe: naive legal opportunities (maker-sell floor, legal clip,
cooldown). The model does not re-discover the floor; it **ranks and
filters** it.

---

## A. Will the FOK fill at X+1?

EDA: only **17.6%** of floor bars also have a floor at `X+1`. Entry
miss is free in P&L but burns the 180-block cooldown. Prefer tickets
where depth **persists**.

| Feature | How | Why |
|---|---|---|
| Floor persistence | legal maker-sell floor on the chosen side in `X-1`, `X-2`, … `X-5` (count and shares) | one-block prints are ghosts |
| Ask depth at/inside `P_entry` | maker-sell shares with `px ≤ P_entry` at `X` (and `X-1`) | the FOK walks this |
| Depth / `N_min` | that share count divided by the legal clip | room for Kelly 4× |
| κ-gap | depth vs `2N` | sensitivity to competition |
| Last take size | largest taker-buy in `X` on this side | someone already walked the ask |
| Multi-maker vs one print | distinct maker accounts in `X` on this side | one whale vs a real book |
| Blocks since last floor | `X − last floor block` on this side | stale vs live |

**Hypothesis:** persist ≥ 3 of last 5 blocks + depth ≥ 2`N_min` raises
`X+1` hit rate a lot. Test that cut **alone** before any ML.

---

## B. Will the resting sell print in X+2..X+60?

This is the money head. `X+2` is hostile (no time priority: need
`px > P_exit`). `X+3..X+60` matches `px ≥ P_exit`.

| Feature | How | Why |
|---|---|---|
| Taker-buy USDC last 15 / 30 / 60 blocks on the **chosen** side | `is_taker ∧ buy that outcome` | the tape that would hit our sell |
| Taker-buy vs taker-sell imbalance | `(buy − sell) / (buy + sell)` last 15 and 60 | flow with us or against us |
| Last 1 / 5 / 15 / 30 / 100-block return | close-to-close on the chosen side (`condition_by_block` YES, or 1−YES for NO) | momentum vs already-done |
| Close vs VWAP last 15 | close − dollar VWAP | chasing the print vs buying a dip |
| Close location in the bar | `(close − low) / (high − low)` last block and last 15 | close-on-high exhaustion |
| Range | `high − low` last 15 and 60, in ticks | if range < 2 ticks, `P_exit = close − 1 tick` may never print |
| Bid prints last 15 / 30 | maker-buy shares/USDC on this side | there is a bid we might become |
| Seconds/blocks since last **taker-buy** on this side | | dead tape → exit miss → dump/zero |
| Other-side flow | taker-buy on the opposite outcome last 15 | mint/dump of the pair; we might be catching a knife |
| `P_exit` distance | `(P_exit − last_bid_proxy)` in ticks | greedy exits don’t fill |

**Hypotheses to try as hard filters (no model):**

1. Require taker-buy on this side in at least 5 of the last 15 blocks
   (live two-way tape).
2. Require last-15-block range ≥ 3 ticks (or don’t ask for a 1-tick
   exit).
3. Require imbalance **with** the direction we buy (we are not fading
   a dump).
4. Forbid `P_exit` more than 1 tick above the last taker-buy VWAP
   (don’t rest above the tape).

`X+2` strict-better is a reason **not** to need the first exit block
to make the trade. If the ticket is only +EV assuming an `X+2` fill,
kill it.

---

## C. Dump vs worthless (X+61..X+120)

If the GTC does not finish, leftover is dumped at **any** bid, then $0.

| Feature | How | Why |
|---|---|---|
| Maker-buy depth last 30 / 60 / 100 blocks | bids that were hit | dump needs bids |
| Same, share of volume vs asks | two-sided vs one-way melt | |
| Fill-count last 100 blocks | busy vs quiet | quiet → zero |
| Age since first trade in a long lookback | `condition_by_10k` first-seen 10K | brand-new books vanish |
| Matched USDC last 10K vs last 100 blocks | burst vs franchise | burst dies in two minutes |

**Hypothesis:** no maker-buy in the last 30 blocks → dump will fail →
do not enter unless E[full GTC exit] is enough on its own.

---

## D. Fees, venue, clock

A 1-tick edge dies on a 50¢ fee market.

| Feature | How | Why |
|---|---|---|
| Fee bit | any `fee_usdc > 0` on this condition in the last 100 blocks | skip or demand more ticks |
| `min(p, 1−p)` | from close | fee amount and lopsidedness |
| NegRisk | `market_id IS NOT NULL` | different flow, convert/arb |
| Inferred tick | 0.1 / 0.01 / 0.001 from recent prices | 1 tick is not 1¢ |
| Hour residue | `block % 41136` | clock at `X`, not a lookback |
| `notional_pctile` | this cid last-100 USDC rank / N_live among names that **printed** in `[X-99,X]` | which book (tape-touched, not catalog-open) |
| `log_venue_vs_7d` | log((V_t+ε)/(median same-clock last 7 protocol-days + ε)) | which session vs recent life |
| `log_venue_vs_t7` | log((V_t+ε)/(V at X−7 protocol-days + ε)) | this clock vs last week |

Heat features are in **B**, frozen on B’s train slice. No DOW one-hots.
Category / TTR do not enter `X`; they enter evaluation slices. See
[`WALKFORWARD.md`](WALKFORWARD.md).

**Hypothesis:** naive tickets are almost all fee markets (true on the
dead MM line). Either skip fee books, or require `P_exit − P_entry` ≥
2 ticks there.

---

## E. Adverse selection / who is on the tape

| Feature | How | Why |
|---|---|---|
| Taker share of volume last 15 / 60 / 100 | `is_taker` rate | we are about to be the next taker |
| Unique accounts last 100 blocks | from fills in the window | crowd vs one flow |
| Maker–taker \|px\| median | per same-book match, `\|p_taker − p_maker\|`; median in each of 1/5/15/30/60/100 | wide walk = stale/thin book vs tight recent liquidity |

No lagged 10K account tables (outside the 100-block window). No
“blocks since last fire” (cooldown recency).

---

## F. Direction, not just fire

Naive picks the **deeper ask**. That may be the side being dumped.

| Feature | How | Why |
|---|---|---|
| Sign of 15-block return | | buy the up-tape, or fade it — **test both**, don’t assume |
| Imbalance sign vs depth sign | deeper ask but negative imbalance | depth is a dump |
| YES vs NO floor-walk size | | naive rule; model can override |
| Close vs 0.5 | | cheap side has more ticks to the floor |

A separate direction head (YES vs NO) trained on simulator P&L of
“force YES” vs “force NO” on the same `X` is cleaner than baking
direction into the fire score.

---

## G. Caps (`P_entry`, `P_exit`) as outputs

Naive is close+1 tick / close−1 tick. That is a 2-tick round trip
that has to exist in ~2 minutes.

Candidates:

| Recipe | `P_entry` | `P_exit` |
|---|---|---|
| Naive | close + 1 tick | close − 1 tick |
| Touch | floor-walk VWAP of asks at `X` | last taker-buy VWAP at `X` |
| Tight | ask VWAP | ask VWAP + 1 tick (only if we believe momentum) |
| Wide | ask VWAP + 1 tick | last bid proxy − 1 tick | fee markets |

The model can pick a **grid** of tick offsets (entry ∈ {0,+1,+2},
exit ∈ {−2,−1,0,+1}) and the simulator labels each. That is a small
discrete head, not a free price.

Wider `P_entry` raises FOK hit rate and worsens entry VWAP. Tighter
`P_exit` raises GTC hit rate and shrinks edge. The loss has to see
**both**.

---

## H. What we will not use

| Feature | Why |
|---|---|
| Resolve **after** the label horizon | leakage. Resolve **inside** H1 / H60 / dump is a label (infinite liq at 1.0/0.0), not a feature |
| Any fill with `block > X` | leakage |
| Best print in `X+2..X+120` as a feature | lookahead of our own exit |
| Gamma title, sport, game start | not in parquet; live-only until a join exists |
| “This account is smart” from future fills | leakage |
| Complementary pair cost | different strategy; that line is dead |

---

## I. How to try them (order)

Not a kitchen sink on day one. Each step is a P&L table on the same
walk-forward window (7 days of triggers, 180-block cooldown, fill
model as written).

1. **Naive** (already specified): deeper floor, close±1 tick.
2. **Hard filters, no ML:** fee-free only; persist 3/5; imbalance with
   us; range ≥ 3 ticks; taker-buy in last 15. Report P&L and ticket
   count for each filter and the intersection.
3. **Direction swap:** naive but buy the *shallower* / *up-tape* /
   *down-tape* side. Three rows.
4. **Cap grid:** naive fire set, vary tick offsets. Heatmap of P&L.
5. **Small model:** HistGradientBoosting on the survivors of (2),
   features from A–D only (~20 columns). Train to predict simulator
   `pnl` (Huber). Fire if predicted pnl > 0. Flat m=1.
6. **Kelly** only if (5) is green on val at m=1.

Kill a family if removing it does not change test P&L.

---

## J. Tiny starter set (~20 columns)

Stage B does **not** use this as the feature set. It fits all
last-100-block families, then selects on val. This list is only the
“if we had to fit tomorrow” sketch.

```
persist_floors_5
ask_shares_at_cap_X / Xm1 / depth_over_nmin
taker_buy_usdc_{5,15,30,60,100}
taker_imbalance_{5,15,30,60,100}
ret_{1,5,15,30,60,100}
close_minus_vwap_15
close_loc_last
range_ticks_{15,60,100}
blocks_since_taker_buy   # clipped to 100
maker_buy_usdc_{15,30,60,100}
mt_abs_med_{1,5,15,30,60,100}
fee_bit                  # last 100 blocks
min_p_1mp
tick_size
hour_slot
neg_risk
n_accounts_100
```

Direction = sign of `ret_15` **or** naive deeper-ask, as an A/B.
Caps = naive close±1 tick until the grid in I.4 is run.
