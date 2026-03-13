"""
Assert: every non-collateral token in orders_matched exists in token_registered.
"""
from helpers import glob_all, glob_complete, ZERO_ASSET_ID_SQL


def test_orders_matched_tokens_are_registered(con, ranges):
    con.execute(f"""
        CREATE OR REPLACE TEMP TABLE _om_assets AS
        SELECT DISTINCT maker_asset_id AS asset_id
        FROM {glob_complete('orders_matched', ranges)}
        WHERE maker_asset_id != {ZERO_ASSET_ID_SQL}
        UNION
        SELECT DISTINCT taker_asset_id
        FROM {glob_complete('orders_matched', ranges)}
        WHERE taker_asset_id != {ZERO_ASSET_ID_SQL}
    """)

    con.execute(f"""
        CREATE OR REPLACE TEMP TABLE _om_reg AS
        SELECT DISTINCT token0 AS asset_id FROM {glob_all('token_registered')}
        UNION
        SELECT DISTINCT token1 FROM {glob_all('token_registered')}
    """)

    unregistered = con.execute("""
        SELECT t.asset_id
        FROM _om_assets t
        LEFT JOIN _om_reg r ON t.asset_id = r.asset_id
        WHERE r.asset_id IS NULL
    """).fetchall()

    assert len(unregistered) == 0, (
        f"{len(unregistered)} unregistered token ID(s) in orders_matched"
    )
