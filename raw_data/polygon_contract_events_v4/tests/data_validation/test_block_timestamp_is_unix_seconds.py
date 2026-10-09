"""
Assert: ``block_timestamp`` is present, non-null, constant per
``block_number`` within a partition, and in a plausible Unix-seconds range.

Header times come from ``eth_getBlockByNumber``. They are data, not a
partition key. Polygon header times can go backwards; this test does not
require timestamps to be monotonic.
"""
from helpers import iter_partitions_with_files

_LOWER = 1_577_836_800  # 2020-01-01T00:00:00Z
_UPPER = 1_893_456_000  # 2030-01-01T00:00:00Z


def test_block_timestamp_is_unix_seconds(con):
    partitions = iter_partitions_with_files()
    assert partitions, "no parquet partitions found"

    sampled = list(partitions[:2]) + list(partitions[-2:])
    range_offenders: list[tuple[int, int, int]] = []
    variance_offenders: list[tuple[int, int, int]] = []
    missing_offenders: list[str] = []

    for block_start, files in sampled:
        union_sql = "\nUNION ALL\n".join(
            f"SELECT block_number, block_timestamp FROM read_parquet('{f}')"
            for f in files
        )
        missing = con.execute(f"""
            WITH all_events AS ({union_sql})
            SELECT COUNT(*) FROM all_events
            WHERE block_timestamp IS NULL
        """).fetchone()[0]
        if missing:
            missing_offenders.append(f"10K={block_start} nulls={missing}")

        out_of_range = con.execute(f"""
            WITH all_events AS ({union_sql})
            SELECT MIN(block_timestamp), MAX(block_timestamp), COUNT(*)
            FROM all_events
            WHERE block_timestamp < {_LOWER} OR block_timestamp > {_UPPER}
        """).fetchone()
        if out_of_range and out_of_range[2]:
            range_offenders.append(
                (block_start, int(out_of_range[0]), int(out_of_range[1]))
            )

        varying = con.execute(f"""
            WITH all_events AS ({union_sql})
            SELECT block_number, COUNT(DISTINCT block_timestamp) AS n
            FROM all_events
            GROUP BY block_number
            HAVING COUNT(DISTINCT block_timestamp) > 1
            LIMIT 5
        """).fetchall()
        for bn, n in varying:
            variance_offenders.append((block_start, int(bn), int(n)))

    assert not missing_offenders, (
        f"block_timestamp is NULL: {missing_offenders[:5]}"
    )
    assert not range_offenders, (
        f"block_timestamp outside [{_LOWER}, {_UPPER}]: {range_offenders[:5]}"
    )
    assert not variance_offenders, (
        f"block_timestamp not constant per block_number: {variance_offenders[:5]}"
    )
