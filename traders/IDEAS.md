# Trader ideas (not live)

Archive map: [`../explorations/README.md`](../explorations/README.md).
Any new line uses [`../explorations/FRAME.md`](../explorations/FRAME.md)
(same ticket object for FOK/GTC and one-sided/two-sided). Do not fork
a third fill language.

Closed: `pair-v1` two-buy and 97¢ dual-maker, `directional-v1`
buy-then-sell on sports, `directional-fade-v1` after PRIME.

## Opposite of criss-cross — directional, not market-neutral

Criss-cross (many maker-price reversals in 8 blocks) is **flicker**:
two-sided tape, MM ping-pong. It is a timing cue for a **±** (pair)
trade, not a **+** (one-way) trade.

The **opposite** is a **quiet book, then one burst** (kickoff, goal,
print, resolution print). That is **directional**. Do not mix it into
complementary-MM or other market-neutral lines. Those lines need
**both** sides to print so a YES buy and a NO buy can complete a
dollar. A one-way shock is the wrong gate for that.

If we ever build a directional shock trader, Stage A should be:
few/no prints in the last N blocks, then a large one-sided burst at X.
That is a different folder, a different P&L, not an overlay on CMM.

## Stage A for market-neutral (open)

Need a **hard, fast** timing gate for “both books can complete a dollar
in the next few blocks,” without criss-cross.

**Tried 2026-09-20:** original CMM fire (both ask floors, pair cost
`< 0.99`) on **fee-free** conditions, 4 days. **0 candidates.**
The only times YES+NO floors sum under $0.99 are fee’d (5m crypto).
So that gate **is** the tape we banned. Criss-cross felt right because
it was pointing at the same books.

**Tried 2026-09-21:** maker pair YES+NO summing to **97¢**, inside carried
spreads, fee-free, cid alive ≥ ~50 min, not resolving in ~5 min.
4 days: **17** quoteable holes. **Both bids hit in X+1: 0.** Still 0
through X+120 (~3 min). One leg (YES) hit 3/17; NO never. So “wide
spread, rest both, expect both fills next block” does **not** show up
on non-crypto, non-expiry tape. Detail:
[`../explorations/pair-v1/wide-spread-maker.md`](../explorations/pair-v1/wide-spread-maker.md).

Still untested (still ±, not directional shock):

- **Maker-in / maker-out on both books** without requiring pair `< 0.99`
  (join last−1¢ and 1−last−1¢, no FOK). Sports may never complete a
  dollar cheaply; this is a quote, not a lock.
- Sequential hedge (buy one, *then* buy complement) — cleaner risk,
  extra block of lag (see pair-v1 README).
- Relax pair fire to `< 1.02` (pay to complete; probably worse).
