"""
Assert: every condition_id in token_registered exists in condition_preparation.
Tolerates pre-scrape orphans — conditions prepared before our start block.
The ConditionalTokens contract is shared by many protocols, so some conditions
were prepared before we began scraping at block ~33.6M.
"""
from helpers import glob_all

# As of 2026-03-03 there are 104 orphans. Allow headroom for new data.
KNOWN_ORPHAN_LIMIT = 250


def test_token_registered_conditions_have_preparation(con):
    orphans = con.execute(f"""
        SELECT DISTINCT tr.condition_id
        FROM {glob_all('token_registered')} tr
        WHERE NOT EXISTS (
            SELECT 1
            FROM {glob_all('condition_preparation')} cp
            WHERE cp.condition_id = tr.condition_id
        )
    """).fetchall()

    assert len(orphans) <= KNOWN_ORPHAN_LIMIT, (
        f"{len(orphans)} condition_id(s) in token_registered have no "
        f"condition_preparation (limit={KNOWN_ORPHAN_LIMIT})"
    )
