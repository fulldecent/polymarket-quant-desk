"""
Assert: the vast majority of collateral_token values in ConditionalTokens
position events (position_split, positions_merge, payout_redemption) are the
known Polymarket USDC.e address on Polygon.

Any non-USDC.e collateral_token address is surfaced in the failure message so
outliers can be investigated. A small tolerance is allowed for edge cases such
as test markets or conditions prepared with other collateral.

Note: test_token_registered_collateral_is_usdce.py already asserts zero
non-USDC.e rows for conditions registered on the exchange. This test broadens
that check to all ConditionalTokens events, including conditions that were
never registered on the CTFExchange, and uses a fractional limit rather than
a hard zero to tolerate the known small tail.
"""
import pytest

from helpers import (
    complete_1m_ranges_for_paths,
    glob_complete_contract_prefix,
)

# Polymarket USDC.e on Polygon (lowercase, no 0x prefix)
USDC_E = "2791bca1f2de4661ed88a30c99a7a9449aa84174"

# Maximum tolerated fraction of rows with non-USDC.e collateral (0.1 %).
# The vast majority of markets use USDC.e; anything above this threshold
# indicates an unexpected new collateral type or a data error.
OUTLIER_FRACTION_LIMIT = 0.001


def _fraction_non_usdce(con, src: str) -> tuple[int, int, list[str]]:
    """Return (outlier_row_count, total_row_count, outlier_descriptions)."""
    rows = con.execute(f"""
        SELECT lower(hex(collateral_token)) AS addr, COUNT(*) AS cnt
        FROM {src}
        GROUP BY collateral_token
        ORDER BY cnt DESC
    """).fetchall()
    total = sum(cnt for _, cnt in rows)
    outliers = [(addr, cnt) for addr, cnt in rows if addr != USDC_E]
    descriptions = [f"0x{addr} ({cnt:,} rows)" for addr, cnt in outliers]
    return sum(cnt for _, cnt in outliers), total, descriptions


def test_collateral_token_majority_usdce(con):
    """≥ 99.9 % of ConditionalTokens split/merge/redeem rows use USDC.e collateral."""
    ranges = sorted(
        complete_1m_ranges_for_paths(["ConditionalTokens/position_split"])
        & complete_1m_ranges_for_paths(["ConditionalTokens/positions_merge"])
        & complete_1m_ranges_for_paths(["ConditionalTokens/payout_redemption"])
    )
    if not ranges:
        pytest.skip("no complete shared 1M ranges for collateral normalization check")

    sampled = sorted(set(ranges[:3] + ranges[-3:]))

    outlier_total = 0
    grand_total = 0
    all_outlier_descriptions: list[str] = []

    for prefix, label in [
        ("ConditionalTokens/position_split", "position_split"),
        ("ConditionalTokens/positions_merge", "positions_merge"),
        ("ConditionalTokens/payout_redemption", "payout_redemption"),
    ]:
        try:
            src = glob_complete_contract_prefix(prefix, sampled)
        except ValueError:
            continue
        outliers, total, descriptions = _fraction_non_usdce(con, src)
        outlier_total += outliers
        grand_total += total
        all_outlier_descriptions.extend(f"{d} in {label}" for d in descriptions)

    if grand_total == 0:
        pytest.skip("no position event rows found in sampled ranges")

    frac = outlier_total / grand_total
    assert frac <= OUTLIER_FRACTION_LIMIT, (
        f"{outlier_total:,}/{grand_total:,} position event rows ({frac:.4%}) use non-USDC.e "
        f"collateral (limit {OUTLIER_FRACTION_LIMIT:.1%}). "
        f"Outliers: {'; '.join(all_outlier_descriptions) or 'none'}"
    )
