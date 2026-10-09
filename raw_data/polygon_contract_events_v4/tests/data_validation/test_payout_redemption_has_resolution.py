"""
Assert: every condition_id in payout_redemption (within our scrape window)
has a condition_resolution event.
"""
from helpers import glob_all, glob_complete


def test_payout_redemption_has_resolution(con, ranges):
    sampled = sorted(set(ranges[:2] + ranges[-2:]))
    orphans = con.execute(f"""
        SELECT DISTINCT pr.condition_id
        FROM {glob_complete('payout_redemption', sampled)} pr
        WHERE EXISTS (
            SELECT 1
            FROM {glob_all('condition_preparation')} cp
            WHERE cp.condition_id = pr.condition_id
        )
        AND NOT EXISTS (
            SELECT 1
            FROM {glob_all('condition_resolution')} cr
            WHERE cr.condition_id = pr.condition_id
        )
    """).fetchall()

    assert len(orphans) == 0, (
        f"{len(orphans)} condition_id(s) in payout_redemption "
        f"have no condition_resolution"
    )
