"""
Assert: the tuple ``(block_number, transaction_index, log_index)`` is
unique globally across the entire data set — every event table, every
contract, every partition.

**This test is skipped.** A single dataset-wide ``GROUP BY`` over
several billion rows needs tens of gigabytes of hash-table memory and
takes hours on a workstation. The same invariant is enforced cheaply by
``test_block_transaction_log_index_unique_per_partition``: within every
10K partition the grain is unique, and every parquet file's
``block_number`` is strictly inside the 10K bounds encoded in the path,
so a duplicate pair cannot land in two partitions.

The truly-global query is still defined below so the invariant remains
self-documenting and can be run by hand on a beefy machine when desired.
"""
import pytest

from helpers import iter_partitions_with_files


@pytest.mark.skip(
    reason=(
        "Dataset-wide GROUP BY over several billion rows takes hours and "
        "tens of GB of memory. Implied by "
        "test_block_transaction_log_index_unique_per_partition combined "
        "with its partition-bounds check."
    )
)
def test_block_transaction_log_index_globally_unique(con):
    con.execute("SET preserve_insertion_order = false")

    partitions = iter_partitions_with_files()
    assert partitions, "no parquet partitions found"

    all_files = [pq for _, files in partitions for pq in files]
    union_sql = "\nUNION ALL\n".join(
        f"SELECT block_number, transaction_index, log_index "
        f"FROM read_parquet('{f}')"
        for f in all_files
    )

    dupes = con.execute(f"""
        WITH all_events AS ({union_sql})
        SELECT block_number, transaction_index, log_index, COUNT(*) AS n
        FROM all_events
        GROUP BY block_number, transaction_index, log_index
        HAVING COUNT(*) > 1
        ORDER BY n DESC, block_number, transaction_index, log_index
        LIMIT 5
    """).fetchall()

    assert not dupes, (
        f"{len(dupes)} (block_number, transaction_index, log_index) "
        f"duplicate(s) found globally; first offenders "
        f"(block_number, transaction_index, log_index, occurrences): "
        f"{dupes[:5]}"
    )
