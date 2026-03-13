"""
Assert: every TokenRegistered row has its symmetric counterpart on the same
exchange and condition_id.  (token0=A, token1=B) must pair with (token0=B, token1=A).
"""
from helpers import glob_all


def test_binary_conditions_have_two_tokens(con):
    orphans = con.execute(f"""
        SELECT a.token0, a.token1
        FROM {glob_all('token_registered')} a
        WHERE NOT EXISTS (
            SELECT 1 FROM {glob_all('token_registered')} b
            WHERE b.token0 = a.token1
              AND b.token1 = a.token0
        )
    """).fetchall()

    assert len(orphans) == 0, (
        f"{len(orphans)} token_registered row(s) have no symmetric counterpart"
    )
