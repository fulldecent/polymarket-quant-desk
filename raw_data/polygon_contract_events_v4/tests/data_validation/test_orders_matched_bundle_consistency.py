"""
Assert: within every transaction, no maker order_hash is filled more than once.

Checks a bounded sample: first + last 2 UTC days.
"""
from helpers import event_day_parquet


def test_orders_matched_bundle_consistency(con, ranges):
    sampled = sorted(set(ranges[:2] + ranges[-2:]))
    bad_total = 0
    for day_key in sampled:
        for contract in ("CTFExchange", "NegRiskCtfExchange"):
            om = event_day_parquet(contract, "orders_matched", day_key)
            of = event_day_parquet(contract, "order_filled", day_key)
            if not om.exists() or not of.exists():
                continue
            dupes = con.execute(f"""
                SELECT COUNT(*) FROM (
                    WITH maker_fills AS (
                        SELECT of.block_number, of.transaction_index, of.order_hash
                        FROM read_parquet('{of}') of
                        JOIN read_parquet('{om}') om
                          ON om.block_number = of.block_number
                         AND om.transaction_index = of.transaction_index
                        WHERE of.order_hash != om.taker_order_hash
                    )
                    SELECT block_number, transaction_index, order_hash
                    FROM maker_fills
                    GROUP BY block_number, transaction_index, order_hash
                    HAVING COUNT(*) > 1
                )
            """).fetchone()[0]
            bad_total += dupes

    assert bad_total == 0, (
        f"{bad_total} duplicate maker order_hash(es) within a bundle"
    )
