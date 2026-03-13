"""
Assert: for every orders_matched row, the matching order_filled taker row
has identical maker_amount_filled and taker_amount_filled.
"""
import os
from helpers import RAW, glob_complete


def test_order_filled_taker_matches_orders_matched(con, ranges):
    mismatches = []
    for r in ranges:
        # Check each exchange that has both tables in this 1M range
        for contract in ("CTFExchange", "NegRiskCtfExchange"):
            om_dir = os.path.join(RAW, contract, "orders_matched", f"1M={r}")
            of_dir = os.path.join(RAW, contract, "order_filled", f"1M={r}")
            if not os.path.isdir(om_dir) or not os.path.isdir(of_dir):
                continue

            om = f"read_parquet('{om_dir}/**/*.parquet')"
            of = f"read_parquet('{of_dir}/**/*.parquet')"

            batch = con.execute(f"""
                SELECT om.transaction_hash, om.taker_order_hash
                FROM {om} om
                JOIN {of} of
                  ON of.transaction_hash = om.transaction_hash
                 AND of.order_hash = om.taker_order_hash
                WHERE om.maker_amount_filled != of.maker_amount_filled
                   OR om.taker_amount_filled != of.taker_amount_filled
                LIMIT 10
            """).fetchall()
            mismatches.extend(batch)

        if len(mismatches) >= 50:
            break

    assert len(mismatches) == 0, (
        f"{len(mismatches)} taker order_filled rows have amounts "
        f"that differ from orders_matched"
    )
