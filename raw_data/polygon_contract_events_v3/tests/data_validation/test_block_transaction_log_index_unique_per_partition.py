"""
Assert: within every 10K parquet partition, the tuple
``(block_number, transaction_index, log_index)`` is unique across every
event row from every contract.

A log line on the Polygon blockchain is uniquely identified by either
``(transaction_hash, log_index)`` or by ``(block_number,
transaction_index, log_index)`` — both are valid global keys, and every
row in this dataset must satisfy both. This test enforces the
all-integer form within each 10K partition; its sibling
``test_transaction_log_index_unique_per_partition`` enforces the
``transaction_hash``-based form.

Why two tests for the same conceptual invariant: grouping by three small
unsigned integers is ~20% faster than grouping by a 32-byte BLOB plus
an integer (measured on the 5 busiest partitions: 150 ms vs 190 ms per
partition with 4 threads on a warm DuckDB connection). Keeping both
tests means each is a tight, single-purpose check, and an integrity
violation that shows up in one but not the other points directly at
which column family is at fault.

This test enforces:

  (a) every row's ``block_number`` lies inside the 10K bounds encoded in
      the file's path, and
  (b) for each 10K partition, ``GROUP BY block_number, transaction_index,
      log_index`` over every event-table file in that partition contains
      no group with ``COUNT(*) > 1``.
"""
from helpers import iter_partitions_with_files

_PARTITION_SIZE = 10_000


def test_block_transaction_log_index_unique_per_partition(con):
    con.execute("SET preserve_insertion_order = false")

    partitions = iter_partitions_with_files()
    assert partitions, "no parquet partitions found"

    bad_partition_bounds: list[tuple[int, str, int, int]] = []
    bad_duplicates: list[tuple[int, int, int, int, int]] = []

    for block_start, files in partitions:
        block_end = block_start + _PARTITION_SIZE - 1

        union_sql = "\nUNION ALL\n".join(
            f"SELECT block_number, transaction_index, log_index, "
            f"'{f}' AS src FROM read_parquet('{f}')"
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

        # (b) within this partition, (block_number, transaction_index,
        # log_index) is unique.
        dupes = con.execute(f"""
            WITH all_events AS ({union_sql}),
            dupes AS (
                SELECT block_number, transaction_index, log_index, COUNT(*) AS n
                FROM all_events
                GROUP BY block_number, transaction_index, log_index
                HAVING COUNT(*) > 1
            )
            SELECT block_number, transaction_index, log_index, n
            FROM dupes
            ORDER BY n DESC, block_number, transaction_index, log_index
            LIMIT 5
        """).fetchall()
        for bn, ti, li, n in dupes:
            bad_duplicates.append(
                (block_start, int(bn), int(ti), int(li), int(n))
            )

        if len(bad_partition_bounds) >= 20 or len(bad_duplicates) >= 20:
            break

    assert not bad_partition_bounds, (
        f"{len(bad_partition_bounds)} parquet file(s) contain rows whose "
        f"block_number falls outside the 10K partition bounds encoded in "
        f"the path; first offenders (partition_start, file, min_block, "
        f"max_block): {bad_partition_bounds[:5]}"
    )
    assert not bad_duplicates, (
        f"{len(bad_duplicates)} (block_number, transaction_index, log_index) "
        f"duplicate(s) found within a 10K partition; first offenders "
        f"(partition_start, block_number, transaction_index, log_index, "
        f"occurrences): {bad_duplicates[:5]}"
    )
