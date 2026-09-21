# Fill model

Specialization of [`../FRAME.md`](../FRAME.md) for the one-sided
FOK-then-GTC ticket (`cap_touch=INCLUSIVE` on entry; GTC first block
STRICT, then INCLUSIVE). Shared walks: `explorations.frame`.

Research rules for the directional FOK / GTC book. Not CLOB law. Change
this file and rerun; do not quietly patch a backtest.

Polygon ~2.1s/block (`BLOCKS_PER_HOUR = 1714`).

## Signal at block X

Using only `block ≤ X`:

```
direction ∈ {yes, no}     buy that outcome token
P_entry                   buy cap in (tick, 1-tick)
P_exit                    sell floor in (tick, 1-tick)
f                         Kelly size → N = round(m(f) × N_min(P_entry))
                          m(f) ∈ {0} ∪ [1, 4]
```

`N_min(P) = max(5, ceil(1.20 / P))`. Entry that is not a legal clip is
not a signal.

Hot path: `N`, `P_entry`, `P_exit` are signed at `X`. No book re-lookup.

## Cooldown

```
COOLDOWN_BLOCKS = 180   # ~6.3 minutes
```

After a signal on `condition_id` at `X`, the next signal on that
condition is not eligible until `X+180`. The gate is per **condition**,
not per outcome. An entry miss still consumes the cooldown. Overlapping
YES/NO opportunities in the same block are one ticket (naive picks the
deeper ask).

A buy is a **purchase of one token**. The exit is a **sale of that
same token**. The model never emits a buy and a sell as two entry legs.

## Entry — block X+1, FOK

Liquidity-taking, all-or-nothing, worst price `P_entry`.

Walk **maker-sell** depth of that outcome in `X+1`, best (lowest) price
first (interpretation **C**: asks that were actually hit).

Fill iff there are **at least `N` shares** at prices **≤ `P_entry`**.
Cap-touch **counts** on the entry (this is a signed worst price on a
FOK, not the complementary-MM strict-better rule).

- Miss → outcome **entry_failed**. P&L = 0. No exit is placed.
- Fill → `q = N` shares (FOK, no partial), `usdc_in` = VWAP × `q`,
  VWAP ≤ `P_entry`. Taker fee if the condition is a fee market
  (`0.0135 × min(p,1-p) × q`).

Competition band: require `κ × N` shares of ask depth before counting a
fill (`κ = 1` headline, `κ = 2` sensitivity).

## Exit — blocks X+2 .. X+60, GTC, partials allowed

Resting **sell** of the `q` shares, worst price `P_exit` (will not sell
below). Good-until-the-window-closes. Partials keep the remainder on
the order.

Available size in a block is consumed **taker-buy** volume of that
outcome (people lifting / hitting that token — the tape that would
trade with a resting sell). Price of a YES taker-buy is the YES price;
of a NO taker-buy is the NO price.

| Block | Time priority | A share fills if the print’s price is |
|---|---|---|
| **X+2** | Others ahead of us | **strictly better** than `P_exit` (`px > P_exit` for a sell) |
| **X+3 .. X+60** | We are ahead | **at or better** (`px ≥ P_exit`) |

Walk each block in order. Inside a block, best (highest) qualifying
prints first. Take `min(remaining, qualifying_size)`. Remainder carries
to the next block.

Tranches that fill here are **exit** tranches: `usdc_out = q_i × vwap_i`
with `vwap_i ≥ P_exit` (strictly `>` on `X+2`).

## Liquidation — blocks X+61 .. X+120

Unfilled remainder `q_left` is dumped at **any** bid. Walk **maker-buy**
depth of that outcome (bids that were hit), best (highest) first, block
order `X+61` then `X+62` … `X+120`. No `P_exit` floor. No 5-share /
$1.20 constraint on the crumb (the desk is flattening, not placing a
new legal clip).

Tranches that fill here are **liquidate** tranches at whatever VWAP the
walk paid.

## Worthless

Shares still unfilled after `X+120` are **$0**. No resolution mark, no
last trade, no later print.

## P&L

```
entry_failed:  pnl = 0

else:
  pnl = usdc_out_exit + usdc_out_liq − usdc_in − fees
      + 0 × q_worthless
```

Fees: taker fee on the entry. Exit that makes (we are the resting
sell) is 0 on fee markets; if a tranche is economically a take, apply
the taker rate. Liquidation is a take: taker fee on dump USDC.

## What this is not

| Temptation | Why not |
|---|---|
| Hold to resolution | This book is ~4 minutes. Unfilled after `X+120` is $0. |
| Fill the exit at `P_exit` on `X+2` | We do not have time priority. Cap-touch on `X+2` is a miss. |
| Best bid anywhere in `X+2..X+120` | Lookahead of our own queue. Windows and priority are as above. |
| Two-buy complementary MM | Dead. `yes_only + no_only` was 60.8%. |

## Simulator vector

```
direction, P_entry, P_exit, N, f
entry_ok                    bool
usdc_in, q
q_exit, usdc_exit           X+2..X+60
q_liq, usdc_liq             X+61..X+120
q_zero
fees_usdc
pnl
state                       miss | exit_full | mixed | liq_full | zero
```

`state = mixed` if shares split across exit / liq / zero.
