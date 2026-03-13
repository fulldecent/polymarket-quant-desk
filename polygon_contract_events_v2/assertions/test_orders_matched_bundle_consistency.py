"""
Assert: within every transaction, no maker order_hash is filled more than once.
"""
import os
from helpers import RAW


def test_orders_matched_bundle_consistency(con, ranges):
    bad_total = 0
    for r in ranges:
        for contract in ("CTFExchange", "NegRiskCtfExchange"):
            om_dir = os.path.join(RAW, contract, "orders_matched", f"1M={r}")
            of_dir = os.path.join(RAW, contract, "order_filled", f"1M={r}")
            if not os.path.isdir(om_dir) or not os.path.isdir(of_dir):
                continue

            om = f"read_parquet('{om_dir}/**/*.parquet')"
            of = f"read_parquet('{of_dir}/**/*.parquet')"

            dupes = con.execute(f"""
                SELECT COUNT(*) FROM (
                    WITH maker_fills AS (
                        SELECT of.transaction_hash, of.order_hash
                        FROM {of} of
                        JOIN {om} om ON om.transaction_hash = of.transaction_hash
                        WHERE of.order_hash != om.taker_order_hash
                    )
                    SELECT transaction_hash, order_hash
                    FROM maker_fills
                    GROUP BY transaction_hash, order_hash
                    HAVING COUNT(*) > 1
                )
            """).fetchone()[0]
            bad_total += dupes

    assert bad_total == 0, (
        f"{bad_total} duplicate maker order_hash(es) within a bundle"
    )
