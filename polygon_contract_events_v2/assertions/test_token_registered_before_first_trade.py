"""
Assert: for every (contract, token) in order_filled, a token_registered
row exists at a strictly earlier block.
"""
from helpers import glob_all, glob_complete, ZERO_ASSET_ID_SQL


def test_token_registered_before_first_trade(con, ranges):
    con.execute(f"""
        CREATE OR REPLACE TEMP TABLE _first_reg AS
        SELECT token0 AS token_id, MIN(block_number) AS first_reg_block
        FROM {glob_all('token_registered')}
        GROUP BY token0
        UNION ALL
        SELECT token1, MIN(block_number)
        FROM {glob_all('token_registered')}
        GROUP BY token1
    """)

    con.execute(f"""
        CREATE OR REPLACE TEMP TABLE _first_fill AS
        SELECT maker_asset_id AS token_id, MIN(block_number) AS first_fill_block
        FROM {glob_complete('order_filled', ranges)}
        WHERE maker_asset_id != {ZERO_ASSET_ID_SQL}
        GROUP BY maker_asset_id
        UNION ALL
        SELECT taker_asset_id, MIN(block_number)
        FROM {glob_complete('order_filled', ranges)}
        WHERE taker_asset_id != {ZERO_ASSET_ID_SQL}
        GROUP BY taker_asset_id
    """)

    bad = con.execute("""
        SELECT ff.token_id, ff.first_fill_block,
               COALESCE(fr.first_reg_block, -1) AS first_reg_block
        FROM _first_fill ff
        LEFT JOIN _first_reg fr ON ff.token_id = fr.token_id
        WHERE fr.token_id IS NULL
           OR ff.first_fill_block <= fr.first_reg_block
        LIMIT 50
    """).fetchall()

    assert len(bad) == 0, (
        f"{len(bad)} token(s) have fills at or before their first "
        f"token_registered block"
    )
