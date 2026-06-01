"""
Assert: no order_filled row has a block_number after the order_cancelled
block for the same order_hash.
"""
from helpers import glob_all, glob_complete_v1


def test_no_fill_after_cancel(con, ranges):
    # order_cancelled only exists in V1; sample first + last 2 ranges.
    sampled = sorted(set(ranges[:2] + ranges[-2:]))
    bad = con.execute(f"""
        SELECT
            of.order_hash,
            oc.block_number AS cancel_block,
            of.block_number AS fill_block
        FROM {glob_all('order_cancelled')} oc
        JOIN {glob_complete_v1('order_filled', sampled)} of
          ON of.order_hash = oc.order_hash
         AND of.block_number > oc.block_number
        LIMIT 50
    """).fetchall()

    assert len(bad) == 0, (
        f"{len(bad)} order_filled row(s) appear after their order was cancelled"
    )
