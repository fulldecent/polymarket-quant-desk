# Stage A — persist liveness (frozen)

Hardcoded gate, not a model. Applied before tape-touch (Stage B) and
before the sequential policy (Stage C).

## Rule

At block `X` on a condition, keep the trigger only if **at least 6 of
the last 8 blocks** (including `X`) have a fill **and** Kris Kross
count in that window is **≥ 10**.

```
PREFILTER_WINDOW   = 8
PREFILTER_NEED     = 6
PREFILTER_KRIS_MIN = 10
```

`prefilter_alive()` in `sim_lib.py`.

## Sample

Last 24 non-empty `fills_v1` partitions, blocks 93,550,000–93,789,999.
Universe: every `(condition, block)` with at least one fill.
Censor if `X+60` is past the partition frontier (691 rows). **n = 2,919,747**.

Four **exclusive** outcomes after `X`. Distinct prices are YES-equivalent
in `[X+1, X+60]`. **Ghost** is `X+1` prints and `[X+2, X+60]` does not
(`[X+1, X+60]` always contains `X+1`, so that interval cannot be empty
if `X+1` filled).

| Outcome | Meaning | Raw rate | Count |
|---|---|---|---|
| **no fill** | `X+1` has no fill | 66.1% | 1,930,019 |
| **motion** | `X+1` prints, `[X+2, X+60]` prints, **>1** distinct price in `[X+1, X+60]` | 23.0% | 672,525 |
| **flat** | `X+1` prints, `[X+2, X+60]` prints, **exactly 1** distinct price in `[X+1, X+60]` | 9.4% | 273,639 |
| **ghost** | `X+1` prints, then silence in `[X+2, X+60]` | 1.49% | 43,564 |

## Money table (class mix among kept)

Rates are **P(class | keep)**. `1+ of 8` equals original: every trigger
already has a fill at `X`. Universe kept: 100, 55.4, 40.5, 32.1, 26.2,
21.7, 18.2, 14.6%.

| Class | Meaning | Orig | 1+ | 2+ | **3+** | 4+ | 5+ | 6+ | 7+ | 8+ |
|---|---|---|---|---|---|---|---|---|---|---|
| no fill | `X+1` empty | 66.1% | 66.1% | 46.4% | **34.3%** | 25.5% | 18.2% | 12.1% | 7.2% | 3.6% |
| motion | `X+1` + `[X+2,X+60]` + **>1** price | 23.0% | 23.0% | 36.6% | **43.7%** | 47.6% | 49.5% | 49.8% | 48.1% | 44.0% |
| flat | same prints, **1** price | 9.4% | 9.4% | 16.2% | **21.5%** | 26.6% | 32.1% | 38.0% | 44.6% | 52.3% |
| ghost | `X+1` then silence | 1.5% | 1.5% | 0.8% | **0.5%** | 0.3% | 0.2% | 0.2% | 0.1% | 0.1% |

| | 1+ | 2+ | **3+** | 4+ | 5+ | 6+ | 7+ | 8+ |
|---|---|---|---|---|---|---|---|---|
| Keep motion | 100% | 88.0% | **76.8%** | 66.3% | 56.3% | 46.9% | 38.0% | 27.8% |
| Drop flat | 0% | 4.5% | **7.0%** | 8.8% | 10.3% | 11.9% | 13.2% | 18.7% |
| Drop ghost | 0% | 68.5% | **85.9%** | 92.7% | 95.7% | 97.5% | 98.8% | 99.5% |
| Motion / ghost (count) | 15 | 43 | **84** | 140 | 202 | 292 | 478 | 858 |

Tighter gates raise motion *share* but also raise **flat** share (one-price
tapes print every block). Persist cannot see a round-trip.

## Envelope P&L (why not 2-of-8)

Not a knee in the mix. A ghost that we actually FOK is inventory into
silence: dump then $0. A motion is at best a few ticks on a tiny clip.

Assume we **trade every** motion and ghost that passes the gate (no-fill
and flat = $0), 5-share clip at 50¢:

| | |
|---|---|
| Motion, +1 tick the right way | **+$0.05** (`5 × $0.01`) |
| Ghost, full clip worthless | **−$2.50** |
| Break-even motion/ghost | **50 : 1** |

Envelope on this sample (`n_motion × $0.05 + n_ghost × −$2.50`):

| Gate | Motion | Ghost | Envelope |
|---|---|---|---|
| 1+ (raw) | 672,525 | 43,564 | **−$75k** |
| 2+ of 8 | 592,034 | 13,716 | **−$4.7k** |
| **3+ of 8** | 516,805 | 6,152 | **+$10k** |
| 4+ of 8 | 446,110 | 3,195 | +$14k |
| 8+ of 8 | 186,968 | 218 | +$8.8k |

2-of-8 is still **red** on this back of the envelope. 3-of-8 is the first
gate that clears +1 tick vs full ghost loss. 4+ pays more per remaining
ghost but dumps motion (and fattens flat). If motion is only +1 tick
*half* the time (wrong side), the hurdle doubles and even 3-of-8 is
tight.

This is not simulator P&L. It is why Stage A is **3 of 8**, not 2 of 8.

The envelope is why 3-of-8 beat 2-of-8. The freeze is now **5 of 8**.
Do not re-grid `W`. Kris Kross count is a **Stage B feature**, not a
second Stage A cut.

## Kris Kross (Stage B feature; 8-block maker zigzag)

Persist only asks whether a block printed. A book that prints every
block at one tick (Shakhtar Donetsk 2026–27 UCL, 0.1–0.2¢ YES, two
wallets ping-ponging) is 5-of-8 on almost every block. Kris Kross
counts **maker-price direction changes** in the same 8-block window.
That count is `kris_count_8` in Stage B’s persist family. The 5-of-8
gate does not require 8+ crosses; trees see the raw count.

### Rule

On condition `cid` at block `X`, take maker legs (`is_taker = false`)
with `block ∈ {X−7 … X}`, ordered `(block_number, logical_fill_index)`.
Price is YES-equivalent, rounded to 6 decimals (`run._yes_px_sql`).

```
kris_count(px):
  if empty or every print equals px[0]: return 0
  skip leading equals of px[0]
  first ≠ px[0] sets direction (up or down) and count = 1
  stay while next is equal-or-with direction vs the previous fill
  first move against: flip direction, count += 1
  repeat
```

`sim_lib.kris_count`. Equals never start a run and never increment.
Count is legs, not fills. `c+` means count ≥ c.

### Universe (reproducible)

| | |
|---|---|
| Source | `fills_v1` parquet, `FILLS_V1_DIR` |
| Clock | recent Polygon **1.5 s/block** (not `sim_lib.BLOCK_SEC = 2.1`) |
| Window | last **7 calendar days** = `int(7 * 86400 / 1.5)` = 403,200 blocks |
| `x_hi` | `frontier − LIQ_END` (`LIQ_END = 120`) |
| `x_lo` | `x_hi − 403,199` |
| Signal | every `(X, cid)` with a fill at `X` in `[x_lo, x_hi]` |
| Persist | unique fill blocks in `{X−7 … X}` (any taker or maker) |
| This snapshot | **93,386,680–93,789,879**, `n = 5,052,875` |

Scripts: [`kris_kross_matrix.py`](kris_kross_matrix.py) (full grid),
[`plot_4of8_ohlc.py`](plot_4of8_ohlc.py) (OHLC overlay).

```
python explorations/directional-fok-v1/kris_kross_matrix.py
python explorations/directional-fok-v1/plot_4of8_ohlc.py --need 5 --kk-min 8 --n-plot 5 --seed 0
```

`--exclude-crypto` is optional and uses **cold** `ConditionalTokens/condition_preparation.oracle`, not Gamma. Short-horizon crypto factories:

```
58e1745bedda7312c4cddb72618923da1b90efde   # 5m/15m (~2.8k cid/day)
e4ff29ae397712a8bd250be92f06e8208134d580   # 5m/15m
29ae8827af7fec3660aa76c1ee4f6bc84e46c590   # ~1h
1a78eda82e34b446f296f6ed9e0847e5a7c2acd0   # 4h
```

UMA / NegRisk (politics, sports, crypto-policy) stay in.

### Cumulative matrix (this snapshot)

Cell = persist **≥ k of 8** and Kris Kross **≥ c**. Nested down and right.

| count | 1+ of 8 | 2+ of 8 | 3+ of 8 | 4+ of 8 | 5+ of 8 | 6+ of 8 | 7+ of 8 | 8+ of 8 |
|---|---:|---:|---:|---:|---:|---:|---:|---:|
| 0+ | 5,052,875 | 2,871,088 | 2,113,654 | 1,671,085 | 1,352,122 | 1,108,563 | 920,521 | 730,406 |
| 1+ | 2,322,552 | 2,021,437 | 1,552,459 | 1,195,647 | 914,478 | 692,906 | 516,181 | 356,872 |
| 2+ | 1,599,104 | 1,519,692 | 1,342,504 | 1,111,755 | 882,678 | 681,666 | 512,590 | 355,798 |
| 3+ | 1,202,468 | 1,186,475 | 1,119,102 | 995,273 | 830,350 | 660,876 | 504,951 | 353,272 |
| 4+ | 965,432 | 959,285 | 932,127 | 868,597 | 763,294 | 631,990 | 495,264 | 350,766 |
| 5+ | 812,284 | 809,525 | 796,734 | 762,247 | 695,480 | 597,890 | 482,096 | 347,143 |
| 6+ | 701,856 | 700,406 | 693,497 | 674,068 | 631,959 | 561,435 | 466,621 | 342,893 |
| 7+ | 620,312 | 619,493 | 615,394 | 603,652 | 576,415 | 525,687 | 449,176 | 337,259 |
| 8+ | 557,676 | 557,205 | 554,669 | 547,177 | 528,901 | 492,243 | 431,047 | 330,941 |
| 9+ | 507,703 | 507,428 | 505,789 | 500,671 | 487,940 | 461,037 | 412,204 | 323,318 |
| 10+ | 467,124 | 466,947 | 465,797 | 462,192 | 453,052 | 432,955 | 393,941 | 315,430 |

Interest cell **5+ of 8 ∩ 8+ crosses = 528,901** = 10.5% of all signals
(528,901 / 5,052,875). Stage A freeze is the `0+` / `5+ of 8` cell
(**1,352,122**). Seed-0 sample of five 5+/8 × 8+ paths:
`Desktop/stage-a-5of8-kk8-ohlc-5.svg`.

### OHLC overlay

Last 100 blocks + 60 forward. YES minus `last(X)` so X sits on y=0.
Classic bar: **O tick left of the HL stem, HL vertical, C tick right**,
O and C share the stem x. Connector at **50% opacity** from the right
tip of C to the left tip of the next O. Seed **0**. Sample of 5 from
the 5+/8 × 8+ cell: `Desktop/stage-a-5of8-kk8-ohlc-5.svg`.
