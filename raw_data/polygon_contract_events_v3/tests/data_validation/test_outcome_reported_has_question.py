"""
Assert: every question_id in outcome_reported exists in question_prepared.
"""
from helpers import glob_all
from helpers import complete_1m_ranges_for, glob_complete

# As of 2026-05-11 there are 10274 orphan question_ids in completed shared ranges.
# Keep headroom while preserving a fail-fast regression threshold.
KNOWN_OUTCOME_ORPHAN_LIMIT = 12000


def test_outcome_reported_has_question(con):
    ranges = sorted(
        complete_1m_ranges_for("outcome_reported")
        & complete_1m_ranges_for("question_prepared")
    )
    assert ranges, "no complete shared 1M ranges for outcome-reported checks"

    ore = glob_complete("outcome_reported", ranges)
    qp = glob_complete("question_prepared", ranges)

    orphans = con.execute(f"""
        SELECT DISTINCT ore.question_id
        FROM {ore} ore
        WHERE NOT EXISTS (
            SELECT 1
            FROM {qp} qp
            WHERE qp.question_id = ore.question_id
        )
    """).fetchall()

    assert len(orphans) <= KNOWN_OUTCOME_ORPHAN_LIMIT, (
        f"{len(orphans)} question_id(s) in outcome_reported "
        f"have no question_prepared (limit={KNOWN_OUTCOME_ORPHAN_LIMIT})"
    )
