# Shared research frame

Every line in this folder is the same object with different knobs.
Code: [`frame/`](frame/). This file is the contract. Line-specific
fill-models specialize it; they do not invent a second language.

## Pipeline

```
Stage A   hard-fast gate at signal block S   (no model)
Stage B   tape-touch / score from block ≤ S  (optional)
Stage C   decoder → Ticket                   (how we trade)
```

Features use **`block ≤ S` only**. Labels are fills after S, inside the
ticket windows, leftover **$0**. In-horizon resolve is a label
(infinite liquidity at 1.0 / 0.0 / 0.5), not a feature.

`S` is usually called `X`. A delayed gate (PRIME, then wait) is the
same S: the first block where A is true.

## Ticket

One `Ticket` at S. **One-sided** = buy one outcome, sell that same
token. **Two-sided** = buy YES and buy NO; merge if both fill.

Each `Leg`:

| Field | Values |
|---|---|
| outcome | YES / NO |
| action | BUY / SELL |
| tif | FOK / FAK / GTC / GTD |
| intent | **TAKE** (walk the tape as taker) / **MAKE** (rest; fill when the other side takes) |
| cap | signed worst price |
| n | shares |
| window | `[S+start_lag, S+end_lag]` |
| cap_touch | **INCLUSIVE** (signed FOK worst) / **STRICT** (no cap-touch, no time priority) |
| share_need | at-least-n / strictly-more-than-n |

Factories: `one_sided_take_then_make`, `two_sided_take_pair`,
`two_sided_make_pair` in [`frame/ticket.py`](frame/ticket.py).

## How the tape is walked (interpretation C)

`fills_v1` is **consumed** liquidity. We do not have L2 books.

| Intent + action | Depth |
|---|---|
| TAKE buy | maker-sell of that outcome (asks that were hit) |
| TAKE sell | maker-buy of that outcome (bids that were hit) |
| MAKE sell | taker-buy of that outcome (tape that would hit our rest) |
| MAKE buy | maker-sell at or below our bid (tape that would hit our bid) |
| DUMP | maker-buy, any price, legal prefix |

Best-first inside a block. Multi-block windows keep **block order**
(`walk_take_buy(..., order="given")` after concatenating per-block
best-first lists — do not globally sort). κ=1 headline; κ=2 is a
competition band.

**Cap-touch is the only FOK vs “inside” split.** Directional entry
is INCLUSIVE (the cap is a signed worst). Pair take is STRICT
(research “strictly better than P”). GTC first rest block is STRICT
(we do not have time priority); later rest blocks are INCLUSIVE.

## Legal clip

`N ≥ 5` and `N × P_worst ≥ $1.20`. `n_min(P) = max(5, ceil(1.20 / P))`.
Kelly size is a multiple in `{0} ∪ [1, 4]` of that floor. Leftover
after the liquidation window is **$0** (no mark to last, no hold-to-
resolve unless resolve is inside the window).

## Recipes we actually ran

| Line | Ticket | Entry | Exit / other leg |
|---|---|---|---|
| `directional-v1` | `one_sided_take_then_make` | FOK TAKE buy, INCLUSIVE, S+1 | GTC MAKE sell S+2..S+60; dump S+61..S+120 |
| `pair-v1` take | `two_sided_take_pair` | FOK TAKE YES, STRICT, S+1 | TAKE-window NO S+1..S+30 (ask walk, not a true rest bid); then inventory recipe |
| `pair-v1` make | `two_sided_make_pair` | GTC MAKE buy both, STRICT | fill = other side hits our bid |
| `directional-fade-v1` | one-sided TAKE fade | FOK TAKE the anti-whale side at T+1 | dump / GTC take-profit |

Cooldown is per **condition**, default 180 blocks. An entry miss still
consumes it.

## Leakage

```
features     block ≤ S
labels       fills in the ticket windows; resolve-if-in-horizon
headline $   leftover = 0; attempts whose window past the fills frontier are out
```

Fee-on vs fee-free is a per-condition bit (`fee_usdc > 0` once → fee
market). Taker ~1.35% of `min(p,1-p)`. Crypto 5m factories are fee-on
and short-lived; sports/politics research used fee-free + cid span
≥ ~2000 blocks unless the line was explicitly crypto.

## Reopening a line

1. Read [`README.md`](README.md) for the verdict.
2. Keep this frame. Add a knob (new Stage A, new `Leg`), do not fork a
   third fill language.
3. Put snapshots next to the line. Test never selects. Train/val/holdout
   are time-ordered.
