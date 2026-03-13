"""
Assert: exactly one of maker_asset_id or taker_asset_id equals "0" (USDC).
"""
from helpers import glob_complete, ZERO_ASSET_ID_SQL


def test_order_filled_one_side_is_collateral(con, ranges):
    bad = con.execute(f"""
        SELECT transaction_hash, log_index, maker_asset_id, taker_asset_id
        FROM {glob_complete('order_filled', ranges)}
        WHERE NOT (
            (maker_asset_id = {ZERO_ASSET_ID_SQL})
            != (taker_asset_id = {ZERO_ASSET_ID_SQL})
        )
        LIMIT 50
    """).fetchall()

    assert len(bad) == 0, (
        f"{len(bad)} order_filled row(s) violate the one-collateral-side rule"
    )
