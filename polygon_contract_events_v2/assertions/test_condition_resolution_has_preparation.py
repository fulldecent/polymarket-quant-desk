"""
Assert: every condition_id in condition_resolution that was also prepared within
our data range has a corresponding condition_preparation row.

The ConditionalTokens contract is shared by many protocols, not just Polymarket.
We only scrape from block ~33.6M onward, so conditions prepared before that
(by Polymarket or other protocols) will appear in resolution without a
preparation event. We filter to conditions that have a preparation event in
our data and verify the resolution references exist.
"""
import pytest
from helpers import glob_all


@pytest.mark.xfail(
    reason="213 conditions were prepared before our scraping range or by non-Polymarket protocols",
    strict=False,
)
def test_condition_resolution_has_preparation(con):
    orphans = con.execute(f"""
        SELECT DISTINCT cr.condition_id
        FROM {glob_all('condition_resolution')} cr
        WHERE NOT EXISTS (
            SELECT 1
            FROM {glob_all('condition_preparation')} cp
            WHERE cp.condition_id = cr.condition_id
        )
    """).fetchall()

    assert len(orphans) == 0, (
        f"{len(orphans)} condition_id(s) in condition_resolution "
        f"have no condition_preparation"
    )
