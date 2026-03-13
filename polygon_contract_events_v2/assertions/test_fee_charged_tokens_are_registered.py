"""
Assert: every non-zero token_id in fee_charged exists in token_registered.
"""
from helpers import glob_all, glob_complete, complete_1m_ranges, ZERO_ASSET_ID_SQL


def test_fee_charged_tokens_are_registered(con, ranges):
    con.execute(f"""
        CREATE OR REPLACE TEMP TABLE _fee_tokens AS
        SELECT DISTINCT token_id
        FROM {glob_complete('fee_charged', ranges)}
        WHERE token_id != {ZERO_ASSET_ID_SQL}
    """)

    con.execute(f"""
        CREATE OR REPLACE TEMP TABLE _reg_tokens AS
        SELECT DISTINCT token0 AS token_id FROM {glob_all('token_registered')}
        UNION
        SELECT DISTINCT token1 FROM {glob_all('token_registered')}
    """)

    unregistered = con.execute("""
        SELECT f.token_id
        FROM _fee_tokens f
        LEFT JOIN _reg_tokens r ON f.token_id = r.token_id
        WHERE r.token_id IS NULL
    """).fetchall()

    assert len(unregistered) == 0, (
        f"{len(unregistered)} unregistered token_id(s) in fee_charged"
    )
