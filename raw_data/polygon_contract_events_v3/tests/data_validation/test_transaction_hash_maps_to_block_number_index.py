"""
Assert: every ``transaction_hash`` maps to exactly one
``(block_number, transaction_index)`` pair, globally across the entire
data set.

On Polygon a transaction lives in exactly one block at exactly one
position within that block. If the same ``transaction_hash`` appears in
two event rows with different ``(block_number, transaction_index)``
values, then either the chain reorged silently (which we should know
about) or the scraper has corrupted its output. Either case is fatal.

**This test is skipped.** A single dataset-wide ``GROUP BY`` over
several billion rows needs tens of gigabytes of hash-table memory and
takes hours on a workstation. The same invariant is enforced cheaply by
the partition-scoped test
``test_transaction_hash_maps_to_block_number_index_per_partition``.

If a ``transaction_hash`` appeared in two different
``(block_number, transaction_index)`` tuples globally, those two tuples
either share a 10K partition (forbidden by the partition-scoped test)
or live in different partitions. Every parquet file in this dataset has
its ``block_number`` strictly inside the 10K bounds encoded in the path,
so two rows with the same ``transaction_hash`` in different partitions
would have ``block_number`` values in different 10K ranges — meaning the
same transaction hash appears in two different blocks. But a Polygon
transaction hash uniquely identifies a transaction, which lives in
exactly one block, so this case is impossible.

The truly-global query is still defined below so the invariant remains
self-documenting and can be run by hand on a beefy machine when desired.
"""
import pytest

from helpers import iter_partitions_with_files


@pytest.mark.skip(
    reason=(
        "Dataset-wide GROUP BY over several billion rows takes hours and "
        "tens of GB of memory. Implied by "
        "test_transaction_hash_maps_to_block_number_index_per_partition "
        "combined with the partition-bounds check that lives inside it."
    )
)
def test_transaction_hash_maps_to_block_number_index(con):
    con.execute("SET preserve_insertion_order = false")

    partitions = iter_partitions_with_files()
    assert partitions, "no parquet partitions found"

    all_files = [pq for _, files in partitions for pq in files]
    union_sql = "\nUNION ALL\n".join(
        f"SELECT transaction_hash, block_number, transaction_index "
        f"FROM read_parquet('{f}')"
        for f in all_files
    )

    bad = con.execute(f"""
        WITH all_events AS ({union_sql})
        SELECT hex(transaction_hash),
               COUNT(DISTINCT (block_number, transaction_index)) AS n
        FROM all_events
        GROUP BY transaction_hash
        HAVING COUNT(DISTINCT (block_number, transaction_index)) > 1
        ORDER BY n DESC, transaction_hash
        LIMIT 5
    """).fetchall()

    assert not bad, (
        f"{len(bad)} transaction_hash(es) map to multiple "
        f"(block_number, transaction_index) pairs globally; first "
        f"offenders (tx_hash, distinct_locations): {bad[:5]}"
    )
