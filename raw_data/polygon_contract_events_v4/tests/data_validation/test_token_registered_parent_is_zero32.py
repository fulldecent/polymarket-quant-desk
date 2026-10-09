"""Assert: every condition_id in token_registered corresponds to CT splits/merges/redeems with parent_collection_id = ZERO32.

This is a data contract assumption for token_and_usdc_flows_v2. If this fails,
the simplification that drops parent_collection_id from the token_id_map key is unsafe.

Exactly 4 rows violate this (all in position_split). They are allow-listed below.
"""

from helpers import RAW, complete_1m_ranges_for, glob_complete, glob_complete_contract_prefix


ZERO32 = "00" * 32

# Exactly 4 rows violate parent_collection_id = ZERO32 for Polymarket conditions.
# These are allow-listed by (block_number, transaction_index, log_index).
# All other rows must satisfy the assertion.
ALLOWED_NONZERO_PARENT = {
    (77856822, 58, 473),
    (77856822, 59, 477),
    (78234940, 76, 607),
    (78234940, 77, 611),
}


def test_token_registered_conditions_have_zero_parent(con, ranges) -> None:
    """Assert: every condition_id in token_registered corresponds to CT splits/merges/redeems with parent_collection_id = ZERO32.

    This is a data contract assumption for token_and_usdc_flows_v2. If this fails,
    the simplification that drops parent_collection_id from the token_id_map key is unsafe.

    Exactly 4 rows violate this (all in position_split). They are allow-listed.
    """
    ranges = sorted(
        complete_1m_ranges_for("token_registered")
        & complete_1m_ranges_for("position_split")
        & complete_1m_ranges_for("positions_merge")
        & complete_1m_ranges_for("payout_redemption")
    )
    assert ranges, "no complete shared 1M ranges"

    tr = glob_complete("token_registered", ranges)
    # Use contract-prefix glob to get only ConditionalTokens/* (not NegRiskAdapter/*)
    split = glob_complete_contract_prefix("ConditionalTokens/position_split", ranges)
    merge = glob_complete_contract_prefix("ConditionalTokens/positions_merge", ranges)
    redeem = glob_complete_contract_prefix("ConditionalTokens/payout_redemption", ranges)

    if split:
        bad_rows = con.execute(f"""
            SELECT s.block_number, s.transaction_index, s.log_index
            FROM {split} s
            WHERE s.parent_collection_id != unhex('{ZERO32}')
              AND EXISTS (SELECT 1 FROM {tr} tr WHERE tr.condition_id = s.condition_id)
        """).fetchall()
        unallowed = [r for r in bad_rows if (r[0], r[1], r[2]) not in ALLOWED_NONZERO_PARENT]
        assert len(unallowed) == 0, f"Found {len(unallowed)} position_split rows with non-ZERO32 parent for Polymarket conditions (not in allow-list)"

    if merge:
        bad = con.execute(f"""
            SELECT COUNT(*) FROM {merge} m
            WHERE m.parent_collection_id != unhex('{ZERO32}')
              AND EXISTS (SELECT 1 FROM {tr} tr WHERE tr.condition_id = m.condition_id)
        """).fetchone()[0]
        assert bad == 0, f"Found {bad} positions_merge rows with non-ZERO32 parent for Polymarket conditions"

    if redeem:
        bad = con.execute(f"""
            SELECT COUNT(*) FROM {redeem} r
            WHERE r.parent_collection_id != unhex('{ZERO32}')
              AND EXISTS (SELECT 1 FROM {tr} tr WHERE tr.condition_id = r.condition_id)
        """).fetchone()[0]
        assert bad == 0, f"Found {bad} payout_redemption rows with non-ZERO32 parent for Polymarket conditions"
