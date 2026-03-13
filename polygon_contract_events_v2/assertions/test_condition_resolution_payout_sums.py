"""
Assert: for binary conditions (outcome_slot_count = 2), the payout_numerators
have sum > 0 and each element >= 0.
"""
from helpers import glob_all


def test_condition_resolution_payout_sums(con):
    bad = con.execute(f"""
        SELECT condition_id, payout_numerators
        FROM {glob_all('condition_resolution')}
        WHERE json_array_length(payout_numerators) = 2
          AND (
              CAST(json_extract_string(payout_numerators, '$[0]') AS HUGEINT)
              + CAST(json_extract_string(payout_numerators, '$[1]') AS HUGEINT)
              <= 0
              OR CAST(json_extract_string(payout_numerators, '$[0]') AS HUGEINT) < 0
              OR CAST(json_extract_string(payout_numerators, '$[1]') AS HUGEINT) < 0
          )
        LIMIT 10
    """).fetchall()

    assert len(bad) == 0, (
        f"{len(bad)} binary condition_resolution row(s) have invalid "
        f"payout_numerators (sum <= 0 or negative element)"
    )
