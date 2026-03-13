"""
Assert: every outcome token ID belongs to exactly one condition_id.
"""
from helpers import glob_all


def test_token_belongs_to_one_condition(con):
    duplicates = con.execute(f"""
        SELECT token, COUNT(DISTINCT condition_id) AS n_conditions
        FROM (
            SELECT token0 AS token, condition_id FROM {glob_all('token_registered')}
            UNION ALL
            SELECT token1, condition_id FROM {glob_all('token_registered')}
        )
        GROUP BY token
        HAVING n_conditions > 1
    """).fetchall()

    assert len(duplicates) == 0, (
        f"{len(duplicates)} token(s) belong to more than one condition_id"
    )
