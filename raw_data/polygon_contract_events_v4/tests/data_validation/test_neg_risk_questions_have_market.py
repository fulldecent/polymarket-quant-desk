"""
Assert: every market_id in question_prepared exists in market_prepared.
"""
from helpers import glob_all
from helpers import complete_1m_ranges_for, glob_complete


def test_neg_risk_questions_have_market(con):
    ranges = sorted(
        complete_1m_ranges_for("question_prepared")
        & complete_1m_ranges_for("market_prepared")
    )
    assert ranges, "no complete shared 1M ranges for neg-risk market checks"

    qp = glob_complete("question_prepared", ranges)
    mp = glob_complete("market_prepared", ranges)

    orphans = con.execute(f"""
        SELECT DISTINCT qp.market_id
        FROM {qp} qp
        WHERE NOT EXISTS (
            SELECT 1
            FROM {mp} mp
            WHERE mp.market_id = qp.market_id
        )
    """).fetchall()

    assert len(orphans) == 0, (
        f"{len(orphans)} market_id(s) in question_prepared "
        f"have no market_prepared"
    )
