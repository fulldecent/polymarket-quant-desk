# Burndown — before next live

From [`SESSION_2026-09-18.md`](SESSION_2026-09-18.md).

| # | Item | Why |
|---|---|---|
| 1 | Wrap USDC.e → pUSD before FOK; wrap after sells | v2 buy needs pUSD; sells pay USDC.e |
| 2 | Skip FOK if pUSD < $2 | Morning run spammed FOK at $1.07 |
| 3 | Ban crypto up/down (title/outcome/slug) | 6-of-8 + Kris 10+ *is* the 5m factory |
| 4 | Skip fee’d books | Crypto 5m taker fee ≈ predicted edge |
| 5 | `last ≥ 0.10` and `n ≤ 20` | $2 / 1¢ = 200-share lottery |
| 6 | Exit = our **sell** fill only, never FOK buy | Same-second `filled_exit` closed the slot |
| 7 | Dump once (cancel GTC, one FAK); no 20-block retry | 235 dump_fails, Polynode 429 |
| 8 | Flatten / never hold into 5m resolve | Leftovers expired at 0 (−$61) |
| 9 | Kill if irrecoverable loss ≥ $30 vs start cash | User cap |
| 10 | Report and continue if liq ≥ start + $100 | User |
| 11 | Periodic redeem + wrap | Recycle collateral |
| 12 | Prefer rpc if Polynode 429 | Night spent in reconnect |

Status: implementing 1–11 in the trader; 12 is `--listen rpc` until Polynode has a free socket.
