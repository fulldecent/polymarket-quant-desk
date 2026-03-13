"""
Assert: every market_id in positions_converted exists in question_prepared, and
every question_id from those question_prepared rows exists in
condition_preparation.

This validates the two-hop lookup chain that the token_and_usdc_flows_v1
materialization uses to resolve convert events:
  positions_converted.market_id
    → question_prepared.question_id (+ index)
    → condition_preparation.condition_id (+ outcome_slot_count)
"""
from helpers import glob_all


def test_converted_market_has_questions_and_conditions(con):
    # Step 1: every market_id in positions_converted has question_prepared rows
    orphan_markets = con.execute(f"""
        SELECT DISTINCT pc.market_id
        FROM {glob_all('positions_converted')} pc
        WHERE NOT EXISTS (
            SELECT 1
            FROM {glob_all('question_prepared')} qp
            WHERE qp.market_id = pc.market_id
        )
    """).fetchall()

    assert len(orphan_markets) == 0, (
        f"{len(orphan_markets)} market_id(s) in positions_converted have no "
        f"question_prepared rows"
    )

    # Step 2: every question_id reachable from those markets has a
    # condition_preparation row (which provides condition_id and
    # outcome_slot_count)
    orphan_questions = con.execute(f"""
        SELECT DISTINCT qp.question_id
        FROM {glob_all('question_prepared')} qp
        WHERE EXISTS (
            SELECT 1
            FROM {glob_all('positions_converted')} pc
            WHERE pc.market_id = qp.market_id
        )
        AND NOT EXISTS (
            SELECT 1
            FROM {glob_all('condition_preparation')} cp
            WHERE cp.question_id = qp.question_id
        )
    """).fetchall()

    assert len(orphan_questions) == 0, (
        f"{len(orphan_questions)} question_id(s) reachable from "
        f"positions_converted have no condition_preparation"
    )
