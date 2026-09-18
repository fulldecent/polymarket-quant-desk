# Historical notes

- Complementary two-buy MM: dead; `yes_only + no_only` 60.8%.
- Stage A freeze moved **3-of-8 → 5-of-8** after Kris Kross (healthy
  5-of-8 sample; envelope still shows 2-of-8 red). `kris_count_8` is a
  Stage B persist feature (`sim_lib.kris_count`), not a second A cut.
- Naive FOK `close+1 / close−1` and the 16-cell dollar grid: all red.
- Stage A persist was briefly 2-of-8 (class-mix knee). Envelope P&L
  is red at 2-of-8 and first green at 3-of-8; freeze is now **5-of-8**.
- Walk-forward after 5-of-8 + `kris_count_8` (203,329 events): concat
  test **+$1.65** vs dummy **−$302.39**, 12 OOS fills (floor 500).
  Protocol **prints=no**. Recent-scale one-day 0 trades (recent folds
  skipped). Fold 1 ticket k=5 pay2 GTC60 test +$1.65 on 12 fills.
- Naming: persist liveness is **Stage A**. Tape-touch HGB is **Stage B**
  (`stage_b.py` / [`STAGE_B.md`](STAGE_B.md)). Sequential policy is
  **Stage C** (`train.py`). An early FOK-size / $0.01-tick classifier
  was a prototype of Stage B and was replaced by ±k tape-touch with
  inferred ticks. Stage B fit all 78 last-100-block columns, then
  dropped `return` and `venue` on val; frozen selected 67-col two-HGB.
  Hard ±1 H60 AUC 0.934 vs simple-rule 0.701 vs linear 0.887.
  Stage B required test eras (election, Super Bowl, FIFA, US military
  before/after, quiet controls) are in `eras.py` / `era-blocks.json`;
  cross-fit is `stage_b_cross.py`. The Sep 2026 weekend is not the universe.
- Stage C (`stage_c.py`) keeps frozen Stage B heads as inputs and adds
  account-dominance / fee extras; sequential P&L is the score.
- In-horizon on-chain resolve is a **label** (infinite liq at 1.0/0.0),
  not a feature and not leakage after H. Stage A 3-of-8: 1.77% resolve
  in X+1..X+60, 14.2% in X+1..X+120 (`resolve-in-horizon.md`). Stage B
  refit with those labels vs CLOB-only card.
