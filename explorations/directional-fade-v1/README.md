# Directional fade (v1) — one-sided fade of a sweep

Formerly `large-taker-illiquid-v1`. Same family as
[`../directional-v1/`](../directional-v1/); different Stage A (PRIME,
quiet whale, then other tape).

**CLOSED. No fade.** Ticket would have been one-sided TAKE against
the whale at T+1 ([`../FRAME.md`](../FRAME.md)). Map:
[`../README.md`](../README.md). FOK hit 0%. Do not freeze Stage A.

Not X+1. The sweep is **PRIME**; we wait.

## Stage A (hard-fast)

At trigger block `T`:

1. **PRIME** in the last `A` blocks: one taker fills at **two or more
   prices** (no cap on maker accounts).
2. That taker **does not bet again** on this condition after PRIME
   (through `T`).
3. **Other activity** after PRIME, calibrated in [`STAGE_A.md`](STAGE_A.md).

Calibrated result: waiting for activity does **not** make a fade. Mean
move from `last(T)` is slightly with the whale. No A cell is frozen as
a trade. `other1` / `A=60` / walk≥5¢ / fee-free long is only a liveness
definition.

## Stage B

Is this fadable from `last(T)`? Slices + a small HGB. See
[`STAGE_B.md`](STAGE_B.md).

## Stage C

Fade the whale at `T+1` (FOK, optional GTC take-profit). Does the FOK
print? See [`STAGE_C.md`](STAGE_C.md).

Overview: [`snapshot.md`](snapshot.md). Runner: [`analyze.py`](analyze.py).
