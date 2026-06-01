"""
Assert: no order_filled row uses an outcome token whose condition was resolved
more than GRACE_BLOCKS blocks before that fill.

Known failures exist in historical data (off-chain invariant only; contracts do
not enforce it). This test keeps a hard regression ceiling.
"""
from helpers import glob_all, glob_complete_v1

GRACE_BLOCKS = 100
# As of 2026-05-11 sampled windows show 48250 post-resolution fills.
# Keep bounded guardrail with headroom so regressions still fail loudly.
KNOWN_POST_RESOLUTION_LIMIT = 60000


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

    sampled = sorted(set(ranges[:2] + ranges[-2:]))

    bad_total = 0
    for r in sampled:
        of = glob_complete_v1('order_filled', [r])
        n = con.execute(f"""
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
            SELECT COUNT(*) FROM violations
        """).fetchone()[0]
        bad_total += n

    assert bad_total <= KNOWN_POST_RESOLUTION_LIMIT, (
        f"{bad_total} order_filled row(s) trade resolved tokens "
        f"more than {GRACE_BLOCKS} blocks after resolution "
        f"(limit={KNOWN_POST_RESOLUTION_LIMIT})"
    )
