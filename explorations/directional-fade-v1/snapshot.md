# Large taker fade after activity (not X+1)

Generated 2026-09-21T14:18:02Z.
Stage A waits until **after** the sweep: PRIME in the last A blocks, whale quiet on this cid, then someone else trades. Stage B asks if that is fadable. Stage C tries to actually fade at T+1.

- Primes (1 taker, 2+ prices, walk≥1¢): **619,037** (7,318 fee-free long)
- Stage A: **do not freeze**. Every fee-free cell fades ≤ 0 from last(T). Least-bad diagnostic cell `same1 A=300 walk≥5¢`: n=904, T+30 **−0.07¢** / **−$0.0037**, fade>0 27%.
- Stage B: no promoted slice (holdout winners disagree with train).
- Stage C: **FOK hit 0%** at last / 1 tick / 2 ticks. The fade does not print.

Details: [STAGE_A.md](STAGE_A.md), [STAGE_B.md](STAGE_B.md), [STAGE_C.md](STAGE_C.md).

