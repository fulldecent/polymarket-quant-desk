"""
Assert: every TokenRegistered row has its symmetric counterpart on the same
exchange and condition_id.  (token0=A, token1=B) must pair with (token0=B, token1=A).
"""
from helpers import glob_all


def test_binary_conditions_have_two_tokens(con):
    tr = glob_all('token_registered')
    orphans = con.execute(f"""
        SELECT
            hex(a.token0)                  AS token0_hex,
            hex(a.token1)                  AS token1_hex,
            octet_length(a.token0)         AS token0_len,
            octet_length(a.token1)         AS token1_len,
            a.block_number
        FROM {tr} a
        WHERE NOT EXISTS (
            SELECT 1 FROM {tr} b
            WHERE b.token0 = a.token1
              AND b.token1 = a.token0
        )
        ORDER BY a.block_number
    """).fetchall()

    assert len(orphans) == 0, (
        f"{len(orphans)} token_registered row(s) have no symmetric counterpart:\n"
        + "\n".join(
            f"  block={r[4]} token0({r[2]}B)={r[0][:64]}{'...' if len(r[0]) > 64 else ''}"
            f"  token1({r[3]}B)={r[1][:64]}{'...' if len(r[1]) > 64 else ''}"
            for r in orphans[:10]
        )
    )
