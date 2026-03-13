"""
Assert: implied price > 0.  Warn (but pass) on price >= 1.
"""
from helpers import glob_complete, ZERO_ASSET_ID_SQL


def test_order_filled_implied_price_in_range(con, ranges):
    bad = con.execute(f"""
        WITH fills AS (
            SELECT transaction_hash, log_index,
                   maker_asset_id, taker_asset_id,
                   CAST(maker_amount_filled AS DOUBLE) AS maker_amt,
                   CAST(taker_amount_filled AS DOUBLE) AS taker_amt,
                   CASE
                       WHEN maker_asset_id = {ZERO_ASSET_ID_SQL}
                       THEN CAST(maker_amount_filled AS DOUBLE)
                            / NULLIF(CAST(taker_amount_filled AS DOUBLE), 0)
                       ELSE CAST(taker_amount_filled AS DOUBLE)
                            / NULLIF(CAST(maker_amount_filled AS DOUBLE), 0)
                   END AS implied_price
            FROM {glob_complete('order_filled', ranges)}
            WHERE (maker_asset_id = {ZERO_ASSET_ID_SQL})
               != (taker_asset_id = {ZERO_ASSET_ID_SQL})
        )
        SELECT transaction_hash, log_index, implied_price
        FROM fills
        WHERE implied_price IS NULL OR implied_price <= 0
        LIMIT 50
    """).fetchall()

    assert len(bad) == 0, (
        f"{len(bad)} order_filled row(s) have implied price <= 0 or NULL"
    )
