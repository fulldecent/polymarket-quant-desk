# Proposal: directional FOK / GTC research

**Status: archive.** No sports alpha on the live 3-of-8 cell. Shared
ticket: [`../FRAME.md`](../FRAME.md). Map: [`../README.md`](../README.md).

**Former status:** research proposal, not a trading go-ahead.
**Companion:** [`README.md`](README.md), [`fill-model.md`](fill-model.md).

Predecessor complementary MM is **dead**
([`../complementary-mm-v1/`](../complementary-mm-v1/)): naive C had
**yes_only + no_only = 60.8%**. This line is a single-token take at
`X+1` and a two-minute exit.

---

## 1. Decision this work is for

After a simulator + naive baseline:

1. Trade a gated subset (direction + caps + Kelly) with a stated $/day, or
2. Do not trade — entry miss rate or dump/zero mass kills the round trip, or
3. Need L2 books before deciding.

---

## 2. Unit of prediction

One **attempt** = `(X, condition_id, direction, P_entry, P_exit, N)`.

At `X` the FOK and the exit are signed. The simulator in
[`fill-model.md`](fill-model.md) replays `X+1` (FOK), `X+2..X+60`
(GTC partials, priority as specified), `X+61..X+120` (dump), then $0.

The model outputs direction, the two prices, and a Kelly score. It does
not output a complementary second buy.

Attempts with `X+120` past the fills frontier are excluded from
headline P&L.

---

## 3. Naive baseline (before a trained model)

Fire at `m = 1` when, at block `X`:

- Chosen direction = the outcome whose **maker-sell** floor walk at `X`
  is larger (more shares).
- `P_entry` = that side’s close-equivalent + 1 tick (YES close from
  `condition_by_block_v1`; NO = 1 − close).
- `P_exit` = close-equivalent − 1 tick (must stay above tick).
- Skip if `P_exit ≥ P_entry` or the clip is illegal.
- **Cooldown:** if this condition already fired at `X'`, skip unless
  `X ≥ X'+180`. Missed FOKs still consume the 180-block dark period.

That is a “take through last, sell a tick better than last” naive
momentum/spread ticket. It is a baseline, not the model.

---

## 4. Features at block X (no leakage)

**Naive participation (what fires in §3):** maker-sell floor on the
chosen side, close for caps, legal `N_min`, **180-block per-condition
cooldown**. No fee filter.

Brainstorm of heads, filters, and columns: [`FEATURES.md`](FEATURES.md).
The list below is the short form.

**Trained model** — `block ≤ X` only:

| Family | Features |
|---|---|
| Direction | last 1/15/30/100-block YES return, taker-buy imbalance, last print vs prior, high−low |
| Entry cap | floor-walk VWAP of the take side, close, tick, 1×/2×/4× ask depth at X |
| Exit floor | same-side bid depth last 1/15/30 blocks (maker-buy), last sell VWAP |
| Venue | fee bit, NegRisk, inferred tick |
| Clock | hour residue (`block % 41136`) |
| Competition | lagged maker-fill share |
| Horizon | age since first trade; **not** resolve block, **not** payout |

Gamma titles / game start are out of the backtest until a historical
join exists.

Kelly `f` is computed from train-fold μ/σ² of simulator P&L, mapped
into `[1, 4]` of the floor, nested walk-forward. `f` does not veto;
the fire rule does. Uncapped Kelly is diagnostic only.

---

## 5. Loss and evaluation

Labels are the simulator dollar vector, not fill/no-fill.

Walk-forward on time. Embargo 120 blocks so every label is complete.

Headline: `sum(pnl)` with no λ haircut. Report:

- Entry hit rate
- Share of entered notional in exit / liq / zero
- P&L of each tranche
- Fee vs fee-free
- vs always-miss
- vs naive §3

Go: test P&L > 0 at κ=1 and κ=2, zero mass not the whole story, worst
week ≥ −2× median week.

Leakage: no features from `X+1` or later; no resolve block; no picking
the best print in `X+2..X+120` out of order; no filling `X+2` at
`P_exit` exactly.

---

## 6. Work sequence

| Phase | Work | Time |
|---|---|---|
| 0 | Spec (this folder) | done |
| 1 | Simulator + unit tests (FOK, X+2 strict, X+3..60 match, dump, $0) | 2–3 days |
| 2 | Naive §3 on a 7-day complete window | 1 day |
| 3 | Features + walk-forward model, flat m=1 | 3–4 days |
| 4 | Nested 4:1 Kelly | 2 days |
| 5 | κ=2, fee split, older slice | 2 days |

Checkpoint after phase 2: if naive entry-hit × (exit VWAP − entry VWAP)
is red after dump/zero, stop or collect L2.

Machine: same MacBook / DuckDB / sklearn. `X+120` is ~4 minutes of
chain; a 7-day trigger window plus 120-block lookforward is smaller
than the dead MM’s 3-day hold.

---

## 7. Ask

Approve **phases 1–2** (simulator + naive). That answers whether a
one-tick-better GTC after a FOK take is even a round trip on this tape.
