"""
Assert: the set of non-collateral token_ids traded on CTFExchange is disjoint
from the set traded on NegRiskCtfExchange.

Binary markets use CTFExchange; NegRisk markets use NegRiskCtfExchange.  A
token_id appearing on both exchanges would indicate a data or contract anomaly.
"""
from helpers import RAW, ZERO_ASSET_ID_SQL


def test_ctf_negrisk_token_ids_disjoint(con, ranges):
    range_globs = []
    for contract in ("CTFExchange", "NegRiskCtfExchange"):
        for v in ranges:
            path = f"{RAW}/{contract}/order_filled/1M={v}/**/*.parquet"
            range_globs.append((contract, path))

    ctf_paths = [p for c, p in range_globs if c == "CTFExchange"]
    nr_paths = [p for c, p in range_globs if c == "NegRiskCtfExchange"]

    if not ctf_paths or not nr_paths:
        return  # nothing to compare

    ctf_src = f"read_parquet([{', '.join(repr(p) for p in ctf_paths)}])"
    nr_src = f"read_parquet([{', '.join(repr(p) for p in nr_paths)}])"

    con.execute(f"""
        CREATE OR REPLACE TEMP TABLE _ctf_tokens AS
        SELECT DISTINCT maker_asset_id AS token_id
        FROM {ctf_src}
        WHERE maker_asset_id != {ZERO_ASSET_ID_SQL}
        UNION
        SELECT DISTINCT taker_asset_id
        FROM {ctf_src}
        WHERE taker_asset_id != {ZERO_ASSET_ID_SQL}
    """)

    con.execute(f"""
        CREATE OR REPLACE TEMP TABLE _nr_tokens AS
        SELECT DISTINCT maker_asset_id AS token_id
        FROM {nr_src}
        WHERE maker_asset_id != {ZERO_ASSET_ID_SQL}
        UNION
        SELECT DISTINCT taker_asset_id
        FROM {nr_src}
        WHERE taker_asset_id != {ZERO_ASSET_ID_SQL}
    """)

    overlap = con.execute("""
        SELECT hex(c.token_id) AS token_hex
        FROM _ctf_tokens c
        JOIN _nr_tokens n ON c.token_id = n.token_id
        LIMIT 50
    """).fetchall()

    assert len(overlap) == 0, (
        f"{len(overlap)} token_id(s) appear on both CTFExchange and "
        f"NegRiskCtfExchange: {[r[0] for r in overlap[:5]]}"
    )
