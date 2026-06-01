"""
Assert: every token_registered row has a non-null, non-empty condition_id.

This guarantees that the token_registered → condition_id lookup used by the
token_and_usdc_flows_v1 materialization always produces a value.
"""
from helpers import glob_all


def test_token_registered_has_condition_id(con):
    bad = con.execute(f"""
        SELECT token0, token1, condition_id
        FROM {glob_all('token_registered')}
        WHERE condition_id IS NULL
           OR octet_length(condition_id) != 32
           OR condition_id = unhex(repeat('00', 32))
        LIMIT 50
    """).fetchall()

    assert len(bad) == 0, (
        f"{len(bad)} token_registered row(s) have missing or invalid condition_id"
    )
