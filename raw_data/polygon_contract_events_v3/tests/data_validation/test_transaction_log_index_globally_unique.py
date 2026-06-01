"""
Assert: the tuple ``(transaction_hash, log_index)`` is unique globally
across the entire data set — every event table, every contract, every
partition.

A single ``(transaction_hash, log_index)`` pair identifies one specific
log line on the Polygon blockchain. It must never appear twice anywhere
in our scraped output.

**This test is skipped.** A single dataset-wide ``GROUP BY`` over
several billion rows needs tens of gigabytes of hash-table memory and
takes hours on a workstation. The same invariant is enforced cheaply by
the partition-scoped tests:

* ``test_transaction_log_index_unique_per_partition`` — within every 10K
  partition, ``(transaction_hash, log_index)`` is unique across every
  event row from every contract.
* ``test_block_transaction_log_index_unique_per_partition`` — within
  every 10K partition, ``(block_number, transaction_index, log_index)``
  is unique across every event row from every contract.

Two rows sharing a ``(transaction_hash, log_index)`` pair must come from
the same Polygon log, which lives in exactly one block. Every parquet
file in this dataset has its ``block_number`` strictly inside the 10K
bounds encoded in the path (validated by both partition-scoped tests
above), so any such pair of rows would land in the same partition —
which the partition-scoped uniqueness check forbids.

The truly-global query is still defined below so the invariant remains
self-documenting and can be run by hand on a beefy machine when desired.
"""
import pytest

from helpers import iter_partitions_with_files


@pytest.mark.skip(
    reason=(
        "Dataset-wide GROUP BY over several billion rows takes hours and "
        "tens of GB of memory. Implied by "
        "test_transaction_log_index_unique_per_partition combined with the "
        "partition-bounds check that lives inside it."
    )
)
def test_transaction_log_index_globally_unique(con):
    con.execute("SET preserve_insertion_order = false")

    partitions = iter_partitions_with_files()
    assert partitions, "no parquet partitions found"

    all_files = [pq for _, files in partitions for pq in files]
    union_sql = "\nUNION ALL\n".join(
        f"SELECT transaction_hash, log_index FROM read_parquet('{f}')"
        for f in all_files
    )

    dupes = con.execute(f"""
        WITH all_events AS ({union_sql})
        SELECT hex(transaction_hash), log_index, COUNT(*) AS n
        FROM all_events
        GROUP BY transaction_hash, log_index
        HAVING COUNT(*) > 1
        ORDER BY n DESC, transaction_hash, log_index
        LIMIT 5
    """).fetchall()

    assert not dupes, (
        f"{len(dupes)} (transaction_hash, log_index) duplicate(s) found "
        f"globally; first offenders (tx_hash, log_index, occurrences): "
        f"{dupes[:5]}"
    )
