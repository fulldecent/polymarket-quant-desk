# Directional FOK entry / GTC exit (v1)

One outcome, one direction. At block `X` a model names **YES or NO**, an
**entry cap**, an **exit floor**, and a **Kelly size**. The entry is a
fill-or-kill take in `X+1`. If that fills, a resting sell works `X+2`
through `X+60`. Whatever is still open is liquidated `X+61` through
`X+120`. Dust after that is **$0**.

This replaces complementary two-buy MM
([`../complementary-mm-v1/`](../complementary-mm-v1/)), which died
because **yes_only + no_only was 60.8%** of naive attempts. This book
never tries to complete a set. It is a single-token round trip.

Not a live trader. Nothing here posts orders.

## Ticket at block X

**Stage A** persist (5 of last 8 blocks): [`STAGE_A.md`](STAGE_A.md).
Kris Kross count (`kris_count_8`) is a Stage B persist feature.
**Stage B** tape-touch (`±k`, inferred tick): [`STAGE_B.md`](STAGE_B.md),
[`FEATURES.md`](FEATURES.md).
**Stage C** sequential decoder: frozen Stage B probs **plus** last-100-block
extras Stage B does not emit (account dominance, unique accounts, fee).
[`stage_c.py`](stage_c.py).

The model, using only data with `block ≤ X`, outputs:

| Field | Meaning |
|---|---|
| Direction | Buy YES or buy NO |
| `P_entry` | Worst price on the FOK take (buy cap) |
| `P_exit` | Worst price on the resting sell (sell floor, above `P_entry`) |
| Kelly `f` | Size multiple of the legal floor clip |

The ticket is **one buy**, then **one sell of the same token**. It is
never a YES buy plus a NO buy.

## Timeline

```
block X        signal. sign FOK (N, P_entry) and the exit (P_exit). no re-lookup.
block X+1      FOK take. all-or-nothing. miss → outcome 1, flat, $0.
               fill → q shares, spend usdc_in
block X+2      first exit block. we do **not** have time priority.
               fill only on prints **strictly better** than P_exit.
block X+3..X+60
               we have time priority. fill on prints at **P_exit or better**.
               partials allowed. remainder stays on the exit.
block X+61..X+120
               unfilled remainder is liquidated at any bid, best first.
               leftover after X+120 is worthless.
```

Polygon ~2.1s/block. `X+2..X+60` is ~2 minutes. Liquidation is another
~2 minutes. This is a short book.

## Outcomes

**Per signal**

1. **Entry failed** at `X+1` — not enough liquidity at `P_entry`. P&L = 0.
2. **Entry succeeded** — some dollar amount `usdc_in` for `q` shares.

**Per share entered** (after a successful entry), in order:

1. **Exit** in `X+2..X+60` at or better than `P_exit` (strictly better
   on `X+2`).
2. **Liquidate** in `X+61..X+120` at any available market price.
3. **Worthless** — still held after `X+120`.

Headline attempt P&L is the sum of those tranches minus entry spend and
fees. See [`fill-model.md`](fill-model.md).

## Legal clip

Entry FOK still has to be a legal CLOB buy: **N ≥ 5** shares and
**N × P_entry ≥ $1.20**. Size above that floor is Kelly, with a **4:1**
cap on positive bets (same desk rule as the dead MM line).

## Cooldown

If a signal fires on a **condition** at block `X`, that market is dark
until **`X+180`**. No second ticket on the same condition, even if the
FOK missed, even if both YES and NO look good. 180 blocks ≈ 6.3 minutes
at 2.1s. The clock starts at the **signal**, not at the fill. A trained
model uses the same gate.

## Why this approach

Complementary MM needed both books to fill. They did not. A directional
take plus a short, hard exit window puts the whole P&L on one tape we
can actually measure: did `X+1` lift, and did the same token print our
exit (or a dump) in the next two minutes.

## Data

On-chain fills only. No historical L2. Liquidity is consumed volume.
Interpretation C (maker-sell asks) for the **entry take**. Exit and
liquidation use the prints on that outcome as specified in the fill
model.

| Dataset | Role |
|---|---|
| `fills_v1` | Legs, YES-equivalent size, USDC, taker flag |
| `condition_by_block_v1` | Close YES for a default cap; OHLC features |
| `condition_by_10k_v1` | Fee bit, NegRisk, age — not a 3-day hold-to-resolve |

Signal/feature menu (brainstorm, not fitted): [`FEATURES.md`](FEATURES.md).

```sh
source .venv/bin/activate
python explorations/directional-fok-v1/eda.py
python explorations/directional-fok-v1/run.py --trigger-days 3
python explorations/directional-fok-v1/grid.py --trigger-days 3
# Stage A persist is frozen in STAGE_A.md (3 of last 8).
python explorations/directional-fok-v1/stage_b.py --trigger-days 4  # weekend freeze (not representative)
python explorations/directional-fok-v1/stage_b_span.py  # random 5-of-8 triggers, last 9 months + overlapping eras
python explorations/directional-fok-v1/stage_b.py --from-events "$SCRATCH_DIR/directional-fok-v1/stage_b_span_events.joblib"
python explorations/directional-fok-v1/resolve_era_blocks.py
python explorations/directional-fok-v1/stage_b_cross.py  # election / SB / FIFA / military eras
python explorations/directional-fok-v1/walkforward.py  # B 60–90d, C-val/test 14d, slide 14d
# Protocol: explorations/directional-fok-v1/WALKFORWARD.md
```
