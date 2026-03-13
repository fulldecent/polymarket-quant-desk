"""
Assert: maker_amount_filled > 0 and taker_amount_filled > 0 for every
order_filled row.
"""
from helpers import glob_complete


def test_order_filled_amounts_positive(con, ranges):
    bad = con.execute(f"""
        SELECT transaction_hash, log_index,
               maker_amount_filled, taker_amount_filled
        FROM {glob_complete('order_filled', ranges)}
        WHERE CAST(maker_amount_filled AS HUGEINT) = 0
           OR CAST(taker_amount_filled AS HUGEINT) = 0
        LIMIT 50
    """).fetchall()

    assert len(bad) == 0, (
        f"{len(bad)} order_filled row(s) have zero amount(s)"
    )
