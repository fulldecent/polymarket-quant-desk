"""
Assert: every question_id in outcome_reported exists in question_prepared.
"""
from helpers import glob_all


def test_outcome_reported_has_question(con):
    orphans = con.execute(f"""
        SELECT DISTINCT ore.question_id
        FROM {glob_all('outcome_reported')} ore
        WHERE NOT EXISTS (
            SELECT 1
            FROM {glob_all('question_prepared')} qp
            WHERE qp.question_id = ore.question_id
        )
    """).fetchall()

    assert len(orphans) == 0, (
        f"{len(orphans)} question_id(s) in outcome_reported "
        f"have no question_prepared"
    )
