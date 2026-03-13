"""
Assert: every non-collateral token ID in order_filled exists in token_registered.
"""
from helpers import glob_all, glob_complete, ZERO_ASSET_ID_SQL


def test_order_filled_tokens_are_registered(con, ranges):
    con.execute(f"""
        CREATE OR REPLACE TEMP TABLE _traded_assets AS
        SELECT DISTINCT maker_asset_id AS asset_id
        FROM {glob_complete('order_filled', ranges)}
        WHERE maker_asset_id != {ZERO_ASSET_ID_SQL}
        UNION
        SELECT DISTINCT taker_asset_id
        FROM {glob_complete('order_filled', ranges)}
        WHERE taker_asset_id != {ZERO_ASSET_ID_SQL}
    """)

    con.execute(f"""
        CREATE OR REPLACE TEMP TABLE _reg_tokens AS
        SELECT DISTINCT token0 AS asset_id FROM {glob_all('token_registered')}
        UNION
        SELECT DISTINCT token1 FROM {glob_all('token_registered')}
    """)

    unregistered = con.execute("""
        SELECT t.asset_id
        FROM _traded_assets t
        LEFT JOIN _reg_tokens r ON t.asset_id = r.asset_id
        WHERE r.asset_id IS NULL
    """).fetchall()

    assert len(unregistered) == 0, (
        f"{len(unregistered)} unregistered token ID(s) in order_filled"
    )
