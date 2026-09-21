# Explorations archive

One nugget. Shared contract: [`FRAME.md`](FRAME.md) /
[`frame/`](frame/). Each folder is a **line** (a ticket recipe + a
Stage A/B/C) with snapshots frozen as of the last run. Nothing here
posts live orders except via `traders/`, which imports the same frame.

Start here, then open one line. Do not read HISTORY.md first.

## Verdicts

| Line | Kind | Ticket | Verdict |
|---|---|---|---|
| [`complementary-mm-v1/`](complementary-mm-v1/) | ± two-sided | FOK TAKE + TAKE-window complement | **DEAD.** yes_only+no_only = 60.8%. Fee-free pair&lt;99¢: **0** candidates. Wide 97¢ maker pair: 17 holes / 4d, **both-hit 0** through X+120. |
| [`directional-fok-v1/`](directional-fok-v1/) | + one-sided | FOK buy S+1, GTC sell, dump | **No sports alpha** on buy-then-sell-right-away. Walk-forward printed a sliver then died OOS / live. Criss-cross is flicker, not direction. |
| [`large-taker-illiquid-v1/`](large-taker-illiquid-v1/) | + one-sided fade | PRIME, wait, FOK fade at T+1 | **CLOSED.** Sweep sticks. After quiet-whale + other activity, fade from last(T) ≤ 0. FOK hit **0%**. |

Live attempt of directional: [`../traders/directional_fok/`](../traders/directional_fok/)
(`formula.json` is 3-of-8 / criss 5 / skip crypto+fee). Session notes
there. Do not restart it without a new Stage A.

Untested (still inside this frame): [`../traders/IDEAS.md`](../traders/IDEAS.md).

## How the lines share a ticket

```
                    TAKE                          MAKE
                 (walk asks/bids)            (rest, get hit)
one-sided     directional FOK entry         directional GTC exit
              large-taker fade FOK
two-sided     complementary aggressive      wide-spread dual bid
              + complement take-window
```

Same clip (`n_min`, $1.20), same interpretation C, same leftover=$0.
The only FOK/GTC split that is not “tif on the leg” is **cap-touch**:
inclusive on a signed FOK worst; strict on the first GTC block and on
CMM takes. Tests: [`frame/tests/test_frame.py`](frame/tests/test_frame.py).

## Stage A/B/C (same names everywhere)

| Stage | Job | Frozen examples |
|---|---|---|
| A | Hard-fast, no model | persist 3-of-8 (live) vs 6-of-8 (card); PRIME+quiet+other fill |
| B | Score from `block ≤ S` | four YES-delta heads; fade slices (none promoted) |
| C | Ticket decoder | in/out fractions; fade FOK at last(T) |

A delayed trigger (PRIME at P, fire at T) is still S=T. Do not call
the first fill X+1 if A has not armed.

## Lineage (directional Stage A)

Do not mix these up. They all used the same ticket.

| Artifact | Persist | Kris / criss |
|---|---|---|
| [`directional-fok-v1/STAGE_A.md`](directional-fok-v1/STAGE_A.md) freeze card | 6 of last 8 | ≥ 10 |
| [`directional-fok-v1/sim_lib.py`](directional-fok-v1/sim_lib.py) | 3 of last 8 | ≥ 5 |
| [`../traders/directional_fok/formula.json`](../traders/directional_fok/formula.json) live | 3 of last 8 | criss_min 5 |

HISTORY.md traces 2→3→5→6-of-8. Live ran the 3-of-8 cell.

## Other folders

| Path | What |
|---|---|
| [`token-search/`](token-search/) | lookup helper, not a strategy |
| `polynode_inclusion_test*.py` | Polynode vs RPC lag, not a strategy |

## Reopen

New idea = new folder `explorations/<name>-v1/`, import `explorations.frame`,
write STAGE_A/B/C against [`FRAME.md`](FRAME.md), link it from the table
above. Do not copy a third `walk_fok_buy`.
