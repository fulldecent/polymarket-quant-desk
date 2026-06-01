"""
Assert: outcome_reported.outcome is 0 or 1.
"""
from helpers import glob_all


def test_outcome_reported_value_is_bool(con):
    bad = con.execute(f"""
        SELECT question_id, outcome
        FROM {glob_all('outcome_reported')}
        WHERE outcome NOT IN (0, 1)
        LIMIT 10
    """).fetchall()

    assert len(bad) == 0, (
        f"{len(bad)} outcome_reported row(s) have outcome not in {{0, 1}}"
    )
