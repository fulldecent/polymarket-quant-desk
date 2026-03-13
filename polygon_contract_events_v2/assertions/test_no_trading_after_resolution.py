"""
Assert: no order_filled row uses an outcome token whose condition was resolved
more than GRACE_BLOCKS blocks before that fill.

Known failure: ~54 fills on CTFExchange (binary markets) trade after resolution.
This is an off-chain invariant only — the contracts do not enforce it.
"""
import os
import pytest
from helpers import RAW, glob_all, glob_complete, complete_1m_ranges

GRACE_BLOCKS = 100


@pytest.mark.xfail(reason="~54 known post-resolution fills on CTFExchange")
def test_no_trading_after_resolution(con, ranges):
    con.execute(f"""
        CREATE OR REPLACE TEMP TABLE _resolved_tokens AS
        SELECT tr.token0 AS token_id, cr.block_number AS resolution_block
        FROM {glob_all('condition_resolution')} cr
        JOIN {glob_all('token_registered')} tr
          ON tr.condition_id = cr.condition_id
        UNION ALL
        SELECT tr.token1, cr.block_number
        FROM {glob_all('condition_resolution')} cr
        JOIN {glob_all('token_registered')} tr
          ON tr.condition_id = cr.condition_id
    """)

    bad_rows = []
    for r in ranges:
        of = glob_complete('order_filled', [r])
        batch = con.execute(f"""
            WITH violations AS (
                SELECT of.transaction_hash, of.block_number AS fill_block,
                       rt.resolution_block, rt.token_id
                FROM {of} of
                JOIN _resolved_tokens rt ON of.maker_asset_id = rt.token_id
                 AND of.block_number > rt.resolution_block + {GRACE_BLOCKS}
                UNION
                SELECT of.transaction_hash, of.block_number,
                       rt.resolution_block, rt.token_id
                FROM {of} of
                JOIN _resolved_tokens rt ON of.taker_asset_id = rt.token_id
                 AND of.block_number > rt.resolution_block + {GRACE_BLOCKS}
            )
            SELECT transaction_hash, fill_block, resolution_block, token_id
            FROM violations
            LIMIT 50
        """).fetchall()
        bad_rows.extend(batch)
        if len(bad_rows) >= 50:
            break

    assert len(bad_rows) == 0, (
        f"{len(bad_rows)} order_filled row(s) trade a resolved token "
        f"more than {GRACE_BLOCKS} blocks after resolution"
    )
