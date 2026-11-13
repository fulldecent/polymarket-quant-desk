"""
Assert: every condition_id in token_registered exists in condition_preparation.
Tolerates pre-scrape orphans — conditions prepared before our start block.
The ConditionalTokens contract is shared by many protocols, so some conditions
were prepared before we began scraping at block ~33.6M.
"""
from helpers import glob_all
from helpers import complete_1m_ranges_for, glob_complete

# As of 2026-05-11 there are 105 historical pre-scrape orphans in completed ranges.
# Keep headroom for small drift while still failing loudly on regressions.
KNOWN_ORPHAN_LIMIT = 150


def test_token_registered_conditions_have_preparation(con):
    ranges = sorted(
        complete_1m_ranges_for("token_registered")
        & complete_1m_ranges_for("condition_preparation")
    )
    assert ranges, "no complete shared 1M ranges for token-registration checks"

    tr = glob_complete("token_registered", ranges)
    cp = glob_complete("condition_preparation", ranges)

    orphans = con.execute(f"""
        SELECT DISTINCT tr.condition_id
        FROM {tr} tr
        WHERE NOT EXISTS (
            SELECT 1
            FROM {cp} cp
            WHERE cp.condition_id = tr.condition_id
        )
    """).fetchall()

    assert len(orphans) <= KNOWN_ORPHAN_LIMIT, (
        f"{len(orphans)} condition_id(s) in token_registered have no "
        f"condition_preparation (limit={KNOWN_ORPHAN_LIMIT})"
    )
