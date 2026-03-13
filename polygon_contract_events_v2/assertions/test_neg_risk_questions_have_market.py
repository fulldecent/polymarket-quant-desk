"""
Assert: every market_id in question_prepared exists in market_prepared.
"""
from helpers import glob_all


def test_neg_risk_questions_have_market(con):
    orphans = con.execute(f"""
        SELECT DISTINCT qp.market_id
        FROM {glob_all('question_prepared')} qp
        WHERE NOT EXISTS (
            SELECT 1
            FROM {glob_all('market_prepared')} mp
            WHERE mp.market_id = qp.market_id
        )
    """).fetchall()

    assert len(orphans) == 0, (
        f"{len(orphans)} market_id(s) in question_prepared "
        f"have no market_prepared"
    )
