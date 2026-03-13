"""
Assert: all address columns are exactly 20 bytes, all hash columns are exactly
32 bytes. (v2 stores these as fixed-length BLOBs via pa.binary(N).)

Since PyArrow enforces exact lengths at write time, this test samples the first
and last complete 1M ranges to verify the schema is correct without scanning
every partition.
"""
from helpers import glob_complete, glob_all


def _check_blob_len(con, label, source, col, expected_len):
    bad = con.execute(f"""
        SELECT COUNT(*) AS n
        FROM {source}
        WHERE octet_length({col}) != {expected_len}
    """).fetchone()[0]
    if bad > 0:
        return [(label, col, expected_len, bad)]
    return []


def test_address_hash_format_valid(con, ranges):
    # Sample first and last ranges to avoid full-dataset scan
    sampled = sorted(set(ranges[:2] + ranges[-2:]))

    failures = []

    of = glob_complete("order_filled", sampled)
    om = glob_complete("orders_matched", sampled)

    for col in ("maker", "taker"):
        failures += _check_blob_len(con, "order_filled", of, col, 20)
    for col in ("order_hash", "transaction_hash"):
        failures += _check_blob_len(con, "order_filled", of, col, 32)

    for col in ("taker_order_maker",):
        failures += _check_blob_len(con, "orders_matched", om, col, 20)
    for col in ("taker_order_hash", "transaction_hash"):
        failures += _check_blob_len(con, "orders_matched", om, col, 32)

    fc = glob_complete("fee_charged", sampled)
    failures += _check_blob_len(con, "fee_charged", fc, "receiver", 20)
    failures += _check_blob_len(con, "fee_charged", fc, "transaction_hash", 32)

    tr = glob_all("token_registered")
    failures += _check_blob_len(con, "token_registered", tr, "transaction_hash", 32)

    assert len(failures) == 0, (
        f"{len(failures)} column(s) have BLOB length violations: "
        + "; ".join(
            f"{t}.{c} expected {elen}B, {n} bad rows"
            for t, c, elen, n in failures[:5]
        )
    )
