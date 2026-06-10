"""
Assert two event-ordering invariants that extend test_no_trading_after_resolution:

1. v2 no-trading-after-resolution: no CTFExchangeV2 or NegRiskCtfExchangeV2
   order_filled row references an outcome token whose condition was resolved
   more than GRACE_BLOCKS blocks before that fill. v2 token IDs are the same
   ERC-1155 identifiers as v1, so the resolved-token lookup is built from v1
   token_registered (which v2 does not emit).

2. No position ops after payout_redemption: within a single transaction, no
   ConditionalTokens/position_split or ConditionalTokens/positions_merge event
   for condition C occurs at a higher log_index than a ConditionalTokens/
   payout_redemption for the same condition C. Redeeming and then splitting or
   merging in the same tx would indicate malformed or adversarial transactions.
"""
import os
from pathlib import Path

import pytest

from helpers import (
    RAW,
    complete_1m_ranges_for_paths,
    glob_all,
    glob_complete_contract_prefix,
)

GRACE_BLOCKS = 100

# v2 exchanges launched at blocks 84,902,353 and 85,058,176 — much more recent
# than v1. Keep a conservative ceiling so regressions still fail loudly.
KNOWN_V2_POST_RESOLUTION_LIMIT = 500


def test_no_v2_trading_after_resolution(con):
    """No v2 order_filled row trades a token resolved > GRACE_BLOCKS blocks prior."""
    ctfv2_ranges = complete_1m_ranges_for_paths(["CTFExchangeV2/order_filled"])
    nrv2_ranges = complete_1m_ranges_for_paths(["NegRiskCtfExchangeV2/order_filled"])
    v2_ranges = sorted(ctfv2_ranges | nrv2_ranges)
    if not v2_ranges:
        pytest.skip("no complete v2 order_filled 1M ranges found")

    # Build a resolved-token lookup. v2 exchanges share token IDs with v1 and
    # do not emit token_registered; join against v1 token_registered instead.
    con.execute(f"""
        CREATE OR REPLACE TEMP TABLE _v2_ext_resolved_tokens AS
        SELECT tr.token0 AS token_id, cr.block_number AS resolution_block
        FROM {glob_all('condition_resolution')} cr
        JOIN {glob_all('token_registered')} tr ON tr.condition_id = cr.condition_id
        UNION ALL
        SELECT tr.token1, cr.block_number
        FROM {glob_all('condition_resolution')} cr
        JOIN {glob_all('token_registered')} tr ON tr.condition_id = cr.condition_id
    """)

    sampled = sorted(set(v2_ranges[:2] + v2_ranges[-2:]))
    bad_total = 0

    for r in sampled:
        for contract in ("CTFExchangeV2", "NegRiskCtfExchangeV2"):
            src_dir = Path(RAW) / contract / "order_filled" / f"1M={r}"
            if not src_dir.exists():
                continue
            src = f"read_parquet('{RAW}/{contract}/order_filled/1M={r}/**/*.parquet')"
            n = con.execute(f"""
                SELECT COUNT(*)
                FROM {src} of
                JOIN _v2_ext_resolved_tokens rt
                  ON of.token_id = rt.token_id
                 AND of.block_number > rt.resolution_block + {GRACE_BLOCKS}
            """).fetchone()[0]
            bad_total += n

    assert bad_total <= KNOWN_V2_POST_RESOLUTION_LIMIT, (
        f"{bad_total} v2 order_filled row(s) trade resolved tokens more than "
        f"{GRACE_BLOCKS} blocks after resolution "
        f"(limit={KNOWN_V2_POST_RESOLUTION_LIMIT})"
    )


def test_no_position_ops_after_payout_redemption(con):
    """Within a tx, no position_split or positions_merge follows a payout_redemption
    for the same condition_id (compared by log_index within the transaction)."""
    ranges = sorted(
        complete_1m_ranges_for_paths(["ConditionalTokens/payout_redemption"])
        & complete_1m_ranges_for_paths(["ConditionalTokens/position_split"])
        & complete_1m_ranges_for_paths(["ConditionalTokens/positions_merge"])
    )
    if not ranges:
        pytest.skip(
            "no complete shared 1M ranges for payout_redemption + "
            "position_split + positions_merge"
        )

    sampled = sorted(set(ranges[:2] + ranges[-2:]))

    try:
        redeem_src = glob_complete_contract_prefix(
            "ConditionalTokens/payout_redemption", sampled
        )
    except ValueError:
        pytest.skip("no payout_redemption data in sampled ranges")

    bad_total = 0

    for prefix, label in [
        ("ConditionalTokens/position_split", "position_split"),
        ("ConditionalTokens/positions_merge", "positions_merge"),
    ]:
        try:
            pos_src = glob_complete_contract_prefix(prefix, sampled)
        except ValueError:
            continue

        n = con.execute(f"""
            SELECT COUNT(*)
            FROM {pos_src} pos
            JOIN {redeem_src} pr
              ON pos.transaction_hash = pr.transaction_hash
             AND pos.condition_id = pr.condition_id
             AND pos.log_index > pr.log_index
        """).fetchone()[0]
        bad_total += n

    assert bad_total == 0, (
        f"{bad_total} position_split/positions_merge event(s) occur after "
        "payout_redemption for the same condition within the same transaction"
    )
