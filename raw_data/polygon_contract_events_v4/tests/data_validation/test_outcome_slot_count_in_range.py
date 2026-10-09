"""
Assert: every ``outcome_slot_count`` value lies in ``[1, 256]``.

The Gnosis ConditionalTokens contract caps the number of outcome slots
per condition at 256 (the upper bound of the bitmask used for index
sets). It also rejects ``outcome_slot_count == 0``. So every row in
``ConditionalTokens/condition_preparation`` and
``ConditionalTokens/condition_resolution`` must have
``outcome_slot_count`` in ``[1, 256]``.

Scope: every directory that emits ``outcome_slot_count`` — currently
``ConditionalTokens/condition_preparation`` and
``ConditionalTokens/condition_resolution``.
"""
from helpers import glob_all


def test_outcome_slot_count_in_range_condition_preparation(con):
    src = glob_all("condition_preparation")
    bad = con.execute(f"""
        SELECT outcome_slot_count, COUNT(*) AS n
        FROM {src}
        WHERE outcome_slot_count < 1 OR outcome_slot_count > 256
        GROUP BY outcome_slot_count
        ORDER BY n DESC
        LIMIT 5
    """).fetchall()
    assert not bad, (
        f"{len(bad)} distinct outcome_slot_count value(s) in "
        f"condition_preparation outside [1, 256]; first offenders "
        f"(outcome_slot_count, occurrences): {bad[:5]}"
    )


def test_outcome_slot_count_in_range_condition_resolution(con):
    src = glob_all("condition_resolution")
    bad = con.execute(f"""
        SELECT outcome_slot_count, COUNT(*) AS n
        FROM {src}
        WHERE outcome_slot_count < 1 OR outcome_slot_count > 256
        GROUP BY outcome_slot_count
        ORDER BY n DESC
        LIMIT 5
    """).fetchall()
    assert not bad, (
        f"{len(bad)} distinct outcome_slot_count value(s) in "
        f"condition_resolution outside [1, 256]; first offenders "
        f"(outcome_slot_count, occurrences): {bad[:5]}"
    )
