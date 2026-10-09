"""
Assert: condition_resolution rows should generally reference a known preparation.

Because ConditionalTokens is shared by non-Polymarket protocols and we do not
scrape from genesis, a bounded number of historical orphan condition_ids is
expected. This test enforces a hard regression ceiling.
"""
from helpers import glob_all

# As of 2026-05-11 there are 2793 historical/non-Polymarket orphan
# condition_ids in our current raw snapshot. Keep headroom for drift.
KNOWN_ORPHAN_LIMIT = 3500


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

    assert len(orphans) <= KNOWN_ORPHAN_LIMIT, (
        f"{len(orphans)} condition_id(s) in condition_resolution "
        f"have no condition_preparation (limit={KNOWN_ORPHAN_LIMIT})"
    )
