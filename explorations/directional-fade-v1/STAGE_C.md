# Stage C — how to fade, and does it print?

Generated 2026-09-21T14:18:02Z.
Entry at **T+1** (not T). Fade = opposite the whale in YES-space. FOK depth = maker legs in T+1 on the side we take (same interpretation C as directional-v1). Miss → $0. Hit → 5.5 sh at VWAP, then either dump at the last print in (T, T+h] or a GTC take-profit at last(T) ± 50% of remaining walk (partials; leftover dumped at horizon).

Universe: Stage B keep (n=229) on freeze `fee-free long | walk≥5¢ | same1 A=300`.

| recipe | FOK hit | n | mean $ (all) | mean $ |hit| | GTC fill p50 sh |
|---|---:|---:|---:|---:|---:|
| FOK at last(T), dump T+30 | 0.0% | 229 | +0.0000 | +0.0000 | nan |
| FOK 1 tick through, dump T+30 | 0.0% | 229 | +0.0000 | +0.0000 | nan |
| FOK 2 ticks through, dump T+30 | 0.0% | 229 | +0.0000 | +0.0000 | nan |
| FOK at last, dump T+120 | 0.0% | 229 | +0.0000 | +0.0000 | nan |
| FOK at last, GTC 50% remaining, dump 30 | 0.0% | 229 | +0.0000 | +0.0000 | nan |

**It does not print.** 0/229 FOK hits at last, 1 tick through, or 2 ticks through. After a YES-up sweep the bid is not sitting at last(T); T+1 on these thin books usually has no takeable fade-side depth.

A maker rest at last would only fill if a *later* same-way taker lifts us — that is selling into continuation, i.e. becoming the next stale quote, which is the side that just lost. We did not freeze that.

