"""
Assert: within every 10K parquet partition, every ``transaction_hash``
maps to exactly one ``(block_number, transaction_index)`` pair across
every event row from every contract.

This is the partition-scoped half of the globally-unique mapping
invariant described in
`test_transaction_hash_maps_to_block_number_index.py`. Combined with the
partition-bounds check below, the two together imply the global
invariant — but each individual check is bounded in memory and runtime
to one 10K range across at most ~32 event tables.

This test enforces:

  (a) every row's ``block_number`` lies inside the 10K bounds encoded in
      the file's path, and
  (b) for each 10K partition, over every event-table file in that
      partition, no ``transaction_hash`` is associated with more than
      one ``(block_number, transaction_index)`` pair.
"""
from helpers import iter_partitions_with_files

_PARTITION_SIZE = 10_000


def test_transaction_hash_maps_to_block_number_index_per_partition(con):
    con.execute("SET preserve_insertion_order = false")

    partitions = iter_partitions_with_files()
    assert partitions, "no parquet partitions found"

    bad_partition_bounds: list[tuple[int, str, int, int]] = []
    bad_mappings: list[tuple[int, str, int]] = []

    for block_start, files in partitions:
        block_end = block_start + _PARTITION_SIZE - 1

        union_sql = "\nUNION ALL\n".join(
            f"SELECT transaction_hash, block_number, transaction_index, "
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

        # (b) within this partition, every transaction_hash maps to a single
        # (block_number, transaction_index).
        bad = con.execute(f"""
            WITH all_events AS ({union_sql}),
            bad AS (
                SELECT
                    transaction_hash,
                    COUNT(DISTINCT (block_number, transaction_index)) AS n
                FROM all_events
                GROUP BY transaction_hash
                HAVING COUNT(DISTINCT (block_number, transaction_index)) > 1
            )
            SELECT hex(transaction_hash), n
            FROM bad
            ORDER BY n DESC, transaction_hash
            LIMIT 5
        """).fetchall()
        for tx_hex, n in bad:
            bad_mappings.append((block_start, tx_hex, int(n)))

        if len(bad_partition_bounds) >= 20 or len(bad_mappings) >= 20:
            break

    assert not bad_partition_bounds, (
        f"{len(bad_partition_bounds)} parquet file(s) contain rows whose "
        f"block_number falls outside the 10K partition bounds encoded in "
        f"the path; first offenders (partition_start, file, min_block, "
        f"max_block): {bad_partition_bounds[:5]}"
    )
    assert not bad_mappings, (
        f"{len(bad_mappings)} transaction_hash(es) map to multiple "
        f"(block_number, transaction_index) pairs within a 10K partition; "
        f"first offenders (partition_start, tx_hash, distinct_locations): "
        f"{bad_mappings[:5]}"
    )
