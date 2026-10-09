"""
Assert: every non-zero token_id in fee_charged exists in token_registered.

V1 fee_charged (CTFExchange/fee_charged, NegRiskCtfExchange/fee_charged) has a
token_id column.  V2 fee_charged (CTFExchangeV2/fee_charged, …) drops it.
This test scopes exclusively to V1 paths to avoid schema conflicts.
"""
import os
import pytest
from helpers import RAW, glob_all, ZERO_ASSET_ID_SQL


def test_fee_charged_tokens_are_registered(con, ranges):
    # Sample first + last 2 complete ranges to keep runtime bounded.
    sampled = sorted(set(ranges[:2] + ranges[-2:]))

    # Build explicit V1-only fee_charged glob (no V2 dirs — different schema).
    v1_paths = []
    for contract in ("CTFExchange", "NegRiskCtfExchange"):
        for r in sampled:
            d = os.path.join(RAW, contract, "fee_charged", f"1M={r}")
            if os.path.isdir(d):
                v1_paths.append(f"'{d}/**/*.parquet'")

    if not v1_paths:
        pytest.skip("no V1 fee_charged partitions found")

    v1_fc_src = (
        f"read_parquet([{', '.join(v1_paths)}])"
        if len(v1_paths) > 1
        else f"read_parquet({v1_paths[0]})"
    )

    con.execute(f"""
        CREATE OR REPLACE TEMP TABLE _fee_tokens AS
        SELECT DISTINCT token_id
        FROM {v1_fc_src}
        WHERE token_id != {ZERO_ASSET_ID_SQL}
    """)

    con.execute(f"""
        CREATE OR REPLACE TEMP TABLE _reg_tokens AS
        SELECT DISTINCT token0 AS token_id FROM {glob_all('token_registered')}
        UNION
        SELECT DISTINCT token1 FROM {glob_all('token_registered')}
    """)

    unregistered = con.execute("""
        SELECT f.token_id
        FROM _fee_tokens f
        LEFT JOIN _reg_tokens r ON f.token_id = r.token_id
        WHERE r.token_id IS NULL
    """).fetchall()

    assert len(unregistered) == 0, (
        f"{len(unregistered)} unregistered token_id(s) in fee_charged"
    )
