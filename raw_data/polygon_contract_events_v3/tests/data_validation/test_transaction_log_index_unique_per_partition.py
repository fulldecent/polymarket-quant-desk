"""
Assert: within every 10K parquet partition, the tuple
``(transaction_hash, log_index)`` is unique across every event row from
every contract.

This is the partition-scoped half of the globally-unique invariant
described in `test_transaction_log_index_globally_unique.py`. Combined
with the partition-bounds check below, the two together imply the
global invariant — but each individual check is bounded in memory and
runtime to one 10K range across at most ~32 event tables, so the suite
finishes in minutes instead of hours.

Specifically this test enforces:

  (a) every row's ``block_number`` lies inside the 10K bounds encoded in
      the file's path, and
  (b) for each 10K partition, ``GROUP BY transaction_hash, log_index``
      over every event-table file in that partition contains no group
      with ``COUNT(*) > 1``.
"""
from helpers import iter_partitions_with_files

_PARTITION_SIZE = 10_000


def test_transaction_log_index_unique_per_partition(con):
    con.execute("SET preserve_insertion_order = false")

    partitions = iter_partitions_with_files()
    assert partitions, "no parquet partitions found"

    bad_partition_bounds: list[tuple[int, str, int, int]] = []
    bad_duplicates: list[tuple[int, str, int, int]] = []

    for block_start, files in partitions:
        block_end = block_start + _PARTITION_SIZE - 1

        union_sql = "\nUNION ALL\n".join(
            f"SELECT transaction_hash, log_index, block_number, '{f}' AS src "
            f"FROM read_parquet('{f}')"
            for f in files
        )

        # (a) every row's block_number is inside the partition bounds.
        out_of_bounds = con.execute(f"""
            WITH all_events AS ({union_sql})
            SELECT src, MIN(block_number), MAX(block_number)
            FROM all_events
            WHERE block_number < {block_start} OR block_number > {block_end}
            GROUP BY src
            LIMIT 5
        """).fetchall()
        for src, lo, hi in out_of_bounds:
            bad_partition_bounds.append((block_start, src, int(lo), int(hi)))

        # (b) within this partition, (transaction_hash, log_index) is unique.
        dupes = con.execute(f"""
            WITH all_events AS ({union_sql}),
            dupes AS (
                SELECT transaction_hash, log_index, COUNT(*) AS n
                FROM all_events
                GROUP BY transaction_hash, log_index
                HAVING COUNT(*) > 1
            )
            SELECT hex(transaction_hash), log_index, n
            FROM dupes
            ORDER BY n DESC, transaction_hash, log_index
            LIMIT 5
        """).fetchall()
        for tx_hex, log_index, n in dupes:
            bad_duplicates.append((block_start, tx_hex, int(log_index), int(n)))

        if len(bad_partition_bounds) >= 20 or len(bad_duplicates) >= 20:
            break

    assert not bad_partition_bounds, (
        f"{len(bad_partition_bounds)} parquet file(s) contain rows whose "
        f"block_number falls outside the 10K partition bounds encoded in "
        f"the path; first offenders (partition_start, file, min_block, "
        f"max_block): {bad_partition_bounds[:5]}"
    )
    assert not bad_duplicates, (
        f"{len(bad_duplicates)} (transaction_hash, log_index) duplicate(s) "
        f"found within a 10K partition; first offenders (partition_start, "
        f"tx_hash, log_index, occurrences): {bad_duplicates[:5]}"
    )
