# Explorations archive

One nugget. Shared contract: [`FRAME.md`](FRAME.md) /
[`frame/`](frame/). Each folder is a **line** (a ticket recipe + a
Stage A/B/C) with snapshots frozen as of the last run. Nothing here
posts live orders except via `traders/`, which imports the same frame.

Names are **families**, not order types. FOK/GTC are leg TIFs inside
the ticket. Live trader folder = exploration family (`traders/directional`
↔ `explorations/directional-v1`).

| Folder | Family | Formerly |
|---|---|---|
| [`pair-v1/`](pair-v1/) | ± two-sided (YES+NO) | `complementary-mm-v1` |
| [`directional-v1/`](directional-v1/) | + one-sided roundtrip | `directional-fok-v1` |
| [`directional-fade-v1/`](directional-fade-v1/) | + one-sided fade | `large-taker-illiquid-v1` |
| [`../traders/directional/`](../traders/directional/) | live of directional-v1 | `traders/directional_fok` |

Scratch blobs from the original runs still sit under the old names
(`$SCRATCH_DIR/directional-fok-v1`, `complementary-mm-v1`). New runs
write to the new names.

Start here, then open one line. Do not read HISTORY.md first.

## Verdicts

| Line | Kind | Ticket | Verdict |
|---|---|---|---|
| [`pair-v1/`](pair-v1/) | ± two-sided | TAKE pair + TAKE-window complement | **DEAD.** yes_only+no_only = 60.8%. Fee-free pair&lt;99¢: **0** candidates. Wide 97¢ maker pair: 17 holes / 4d, **both-hit 0** through X+120. |
| [`directional-v1/`](directional-v1/) | + one-sided | TAKE S+1, MAKE sell, dump | **No sports alpha** on buy-then-sell-right-away. Walk-forward printed a sliver then died OOS / live. Criss-cross is flicker, not direction. |
| [`directional-fade-v1/`](directional-fade-v1/) | + one-sided fade | PRIME, wait, TAKE fade at T+1 | **CLOSED.** Sweep sticks. After quiet-whale + other activity, fade from last(T) ≤ 0. FOK hit **0%**. |

Live attempt of directional: [`../traders/directional/`](../traders/directional/)
(`formula.json` is 3-of-8 / criss 5 / skip crypto+fee). Session notes
there. Do not restart it without a new Stage A.

Untested (still inside this frame): [`../traders/IDEAS.md`](../traders/IDEAS.md).

## How the lines share a ticket

```
                    TAKE                          MAKE
                 (walk asks/bids)            (rest, get hit)
one-sided     directional entry             directional exit
              directional-fade entry
two-sided     pair aggressive               pair wide-spread dual bid
              + complement take-window
```

Same clip (`n_min`, $1.20), same interpretation C, same leftover=$0.
The only FOK/GTC split that is not “tif on the leg” is **cap-touch**:
inclusive on a signed FOK worst; strict on the first GTC block and on
pair takes. Tests: [`frame/tests/test_frame.py`](frame/tests/test_frame.py).

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
| [`directional-v1/STAGE_A.md`](directional-v1/STAGE_A.md) freeze card | 6 of last 8 | ≥ 10 |
| [`directional-v1/sim_lib.py`](directional-v1/sim_lib.py) | 3 of last 8 | ≥ 5 |
| [`../traders/directional/formula.json`](../traders/directional/formula.json) live | 3 of last 8 | criss_min 5 |

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
