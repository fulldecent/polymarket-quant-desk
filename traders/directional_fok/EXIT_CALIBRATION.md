# Exit calibration — 3-of-8 live, 20 Sep 2026 02:00–13:22Z

Trader stopped. Hourly scheduler cancelled.

## What actually happened

20 closed round-trips (Data API) + JMU redeem + 3 still open.

**Sports/weather (18 clips, what we meant to trade)**  
Realized **−$3.02** (~−$0.17/clip). Median hold **255s** = the 4-minute dump, not the GTC.

**Dogecoin 5m (2 clips, should have been banned; RPC had no title)**  
**+$3.08**. One dump into resolve at 0.99 (+$2.89); one 23s GTC (+$0.19).

**JMU** dust FOK, held, redeemed **+$2.40** on $3.18.

**Still open:** KHL Traktor, Fresno SJSU, Maduro No.

Cash vs wrap start $84.89: about **−$6**. Sports bleed plus unmarked leftovers; Doge luck and JMU redeem offset some.

### How each sports clip exited

| Time | Market | Hold | P_in | P_out | P&L | Exit |
|---|---|---:|---:|---:|---:|---|
| 02:20 | Ole Miss | 45s | 0.59 | 0.58 | −0.06 | Dump too soon (30-block bug) |
| 03:07 | UCLA −9.5 1H | 254s | 0.43 | 0.32 | −0.61 | 4m FAK, price already −11¢ |
| 03:16 | Wuhan 25°C No | 253s | 0.72 | 0.74 | −0.00 | 4m FAK, flat |
| 05:11 | Taipei 31°C No | **21s** | 0.62 | 0.65 | **+0.10** | **GTC filled** |
| 05:21 | Querétaro Yes | 255s | 0.37 | 0.36 | −0.19 | 4m FAK −1¢ |
| 05:22 | Auxerre Yes | 255s | 0.33 | 0.32 | −0.19 | 4m FAK −1¢ |
| 06:03 | Nordsjælland No | 255s | 0.58 | 0.57 | −0.20 | 4m FAK −1¢ |
| 06:06 | London 25°C No | 253s | 0.63 | 0.60 | −0.30 | 4m FAK −3¢ |
| 06:06 | Göztepe O/U | 255s | 0.67 | 0.65 | −0.24 | 4m FAK −2¢ |
| 06:23 | København HT | 254s | 0.35 | 0.34 | −0.19 | 4m FAK −1¢ |
| 08:48 | Panthers −6.5 | 255s | 0.36 | 0.34 | −0.12 | 4m FAK −2¢ |
| 09:09 | Dender No | 256s | 0.42 | 0.41 | −0.20 | 4m FAK −1¢ |
| 09:51 | Ratchaburi Yes | 255s | 0.47 | 0.45 | −0.25 | 4m FAK −2¢ |
| 10:35 | Hannover HT | 254s | 0.41 | 0.40 | −0.19 | 4m FAK −1¢ |
| 10:43 | Vitória O/U | 255s | 0.73 | 0.72 | −0.17 | 4m FAK −1¢ |
| 11:09 | Vikings | 257s | 0.35 | 0.34 | −0.06 | 4m FAK −1¢ |
| 11:30 | Bucs / Browns | 255s | 0.78/0.23 | 0.77/0.22 | −0.15 | 4m FAK both sides |
| 12:30 | Doge 8:30 Down | **23s** | 0.67 | 0.72 | **+0.19** | **GTC filled** |

Pattern: **16/18 sports are the same trade** — rest a GTC ~2¢ above last, it does not trade, 4 minutes later we **FAK across the spread** and pay 1–3¢. Two times the market ran to us in ~20s and GTC **did** fill. So the order **does hit the book**; it just rarely gets lifted in 4 minutes.

## Counterfactuals on these same 18 sports clips

Using CLOB `prices-history` after each buy (mark, not our FAK).

| Policy | Sum P&L | Avg / clip | Notes |
|---|---:|---:|---|
| **Actual** (4m FAK / rare GTC) | **−$3.02** | −$0.17 | What we did |
| Mark at 4m (no spread) | −$1.48 | −$0.08 | FAK costs extra ~1¢ vs mid |
| Mark at 15m | −$1.42 | −$0.08 | Same as 4m |
| Mark at 1h | **+$1.20** | +$0.07 | Mixed; a few games move |
| Mark at 4h | **+$4.89** | +$0.27 | Ole Miss / Purdue 1H / Hannover resolve; Ratchaburi −$2.69 |
| **+2¢ take, first touch** | +$2.47 on 7 hits | +$0.35 **on hits** | 7/18 ever print +2¢ |
| **−3¢ stop, first touch** | −$5.88 on 8 hits | −$0.73 | Noise; does not help |

**+2¢ before we dumped (255s):** only **Wuhan** (117s). We sat through it and FAK’d flat. Ole Miss would have printed +2¢ at 218s but we had already dumped at 45s. Purdue / London print +2¢ **after** the 4m dump.

## Did the exit order rest as expected?

- **Yes, when the book moved immediately.** Taipei 21s and Doge 23s GTC fills prove post → book → fill in ~20s for 5–6 shares.
- **Yes, but unfilled, then we cancelled it with a FAK.** 4-minute GTC at `p_out` is above last; last never got there, then dump crossed.
- **No, on phantom-exit hours.** FOK *buy* was counted as a sell (maker sold *to* us). Slot closed, GTC may not have been managed. Fixed after 08:24Z.
- **Dump FAK** is a taker sell. On these books it consistently fills ~1–2¢ **through** last. That is executable. It is also why almost every sports clip is red.

Tight −3¢ stop: the path **does** trade there (8/18), so it would execute — and it would have **lost more**.

Holding hours: executable if you leave a GTC on; it is no longer a microstructure exit, it is a game bet. 4h helped the football resolves and killed Ratchaburi.

## 7-day history (not the live 18)

3-of-8, last ∈ [0.10, 0.90], **no fee** in the window, 5% random sample, 25,498 triggers, 5.5 sh:

| Policy | Avg / clip |
|---|---:|
| Dump ~4m | **−$0.015** |
| TP +2¢ or dump 15m | **−$0.11** (74% “high” hits; wick-optimistic and still worse) |
| Hold 1h | **−$0.004** |
| Stop −3¢ within 1h | **+$0.11** (78% hit; likely resolve contamination) |

So the **broad** 3-of-8 no-fee universe does **not** say “rest +2¢ for 15m.” It says 4-minute dump is a small bleed, same as live sports without Doge. The live 18 are a **selected** criss-cross subset; even there, 4m FAK is a grind, not a catastrophe.

## What to change

1. **Stop the 4-minute FAK.** Rest GTC at +1–2¢ for **15–30 min**. If unfilled, maker-exit **one tick through last**, not a panic FAK at 0.01.
2. **Do not use a −3¢ stop** on these clips. It fires and loses.
3. **Do not hold hours** as the default. That’s a different model (game outcome). Optional only if Stage B’s 30-block high actually printed — it usually didn’t in 4 min.
4. **Ban 5m crypto on token/slug, not tape title.** Doge leaked on RPC and paid this session; next time it can expire 0.
5. Execution latency is **not** the sports problem. The problem is **taking the spread after a rest that never trades.**

Live trader is **stopped**. Hourly pings **off**.
