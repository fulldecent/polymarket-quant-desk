"""
Assert: for every orders_matched row, the matching order_filled taker row
has identical maker_amount_filled and taker_amount_filled.

Checks a bounded sample: first + last 2 UTC days.
"""
from helpers import event_day_parquet


def test_order_filled_taker_matches_orders_matched(con, ranges):
    sampled = sorted(set(ranges[:2] + ranges[-2:]))
    mismatches = []
    for day_key in sampled:
        for contract in ("CTFExchange", "NegRiskCtfExchange"):
            om = event_day_parquet(contract, "orders_matched", day_key)
            of = event_day_parquet(contract, "order_filled", day_key)
            if not om.exists() or not of.exists():
                continue

            batch = con.execute(f"""
                SELECT om.block_number, om.transaction_index, om.taker_order_hash
                FROM read_parquet('{om}') om
                JOIN read_parquet('{of}') of
                  ON of.block_number = om.block_number
                 AND of.transaction_index = om.transaction_index
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
