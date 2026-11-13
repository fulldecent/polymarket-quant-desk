"""Assert: every CT split/merge/redeem for a Polymarket-registered condition uses USDC.e collateral."""

from helpers import complete_1m_ranges_for, glob_complete, glob_complete_contract_prefix


USDC_E = "2791bca1f2de4661ed88a30c99a7a9449aa84174"


def test_token_registered_collateral_is_usdce(con, ranges) -> None:
    """Assert: every CT split/merge/redeem for a Polymarket-registered condition uses USDC.e collateral.

    This is a data contract assumption for token_and_usdc_flows_v2. If this fails,
    the simplification that hardcodes USDC_E for all output rows is unsafe.
    """
    ranges = sorted(
        complete_1m_ranges_for("token_registered")
        & complete_1m_ranges_for("position_split")
        & complete_1m_ranges_for("positions_merge")
        & complete_1m_ranges_for("payout_redemption")
    )
    assert ranges, "no complete shared 1M ranges for collateral checks"

    tr = glob_complete("token_registered", ranges)
    # Use contract-prefix glob to get only ConditionalTokens/* (not NegRiskAdapter/*)
    split = glob_complete_contract_prefix("ConditionalTokens/position_split", ranges)
    merge = glob_complete_contract_prefix("ConditionalTokens/positions_merge", ranges)
    redeem = glob_complete_contract_prefix("ConditionalTokens/payout_redemption", ranges)

    if split:
        bad = con.execute(f"""
            SELECT COUNT(*) FROM {split} s
            WHERE s.collateral_token != unhex('{USDC_E}')
              AND EXISTS (SELECT 1 FROM {tr} tr WHERE tr.condition_id = s.condition_id)
        """).fetchone()[0]
        assert bad == 0, f"Found {bad} position_split rows with non-USDC.e collateral for Polymarket conditions"

    if merge:
        bad = con.execute(f"""
            SELECT COUNT(*) FROM {merge} m
            WHERE m.collateral_token != unhex('{USDC_E}')
              AND EXISTS (SELECT 1 FROM {tr} tr WHERE tr.condition_id = m.condition_id)
        """).fetchone()[0]
        assert bad == 0, f"Found {bad} positions_merge rows with non-USDC.e collateral for Polymarket conditions"

    if redeem:
        bad = con.execute(f"""
            SELECT COUNT(*) FROM {redeem} r
            WHERE r.collateral_token != unhex('{USDC_E}')
              AND EXISTS (SELECT 1 FROM {tr} tr WHERE tr.condition_id = r.condition_id)
        """).fetchone()[0]
        assert bad == 0, f"Found {bad} payout_redemption rows with non-USDC.e collateral for Polymarket conditions"
