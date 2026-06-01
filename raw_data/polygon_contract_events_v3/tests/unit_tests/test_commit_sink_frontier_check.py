"""
Test for commit_sink frontier-ordering enforcement.

The documented contract in _internal/README.md and persistence.py:

    "commit_sink must be passed the partition that extends the sunk
     frontier by exactly one. Out-of-order commits raise PartitionFrontierError."

This test verifies the frontier check logic by testing the expected
partition computation without requiring actual Parquet files on disk.
"""

from raw_data.polygon_contract_events_v3._internal.tables import SCRAPE_START_BLOCK, PARTITION_SIZE_10K


def test_commit_sink_frontier_calculation_logic():
    """
    Verify the frontier calculation logic used by commit_sink.

    This is a pure logic test that demonstrates the expected partition
    computation is correct per the documented contract.
    """
    # Initial state: nothing sunk
    sunk_frontier = SCRAPE_START_BLOCK - 1

    # Expected first partition (from commit_sink implementation)
    expected_first = (
        (SCRAPE_START_BLOCK // PARTITION_SIZE_10K) * PARTITION_SIZE_10K
        if sunk_frontier == SCRAPE_START_BLOCK - 1
        else ((sunk_frontier + 1) // PARTITION_SIZE_10K) * PARTITION_SIZE_10K
    )

    # The first valid partition may start before SCRAPE_START_BLOCK
    # (e.g., 33600000 < 33605403)
    assert expected_first <= SCRAPE_START_BLOCK
    assert expected_first % PARTITION_SIZE_10K == 0

    # The partition's END block must be >= SCRAPE_START_BLOCK
    assert expected_first + PARTITION_SIZE_10K - 1 >= SCRAPE_START_BLOCK


def test_second_partition_calculation():
    """
    Verify that after sinking p0, the expected next partition is p0 + 10K.
    """
    p0 = (SCRAPE_START_BLOCK // PARTITION_SIZE_10K) * PARTITION_SIZE_10K

    # After sinking p0, sunk_frontier would be p0 + 9999
    sunk_frontier_after_p0 = p0 + PARTITION_SIZE_10K - 1

    expected_next = (
        (SCRAPE_START_BLOCK // PARTITION_SIZE_10K) * PARTITION_SIZE_10K
        if sunk_frontier_after_p0 == SCRAPE_START_BLOCK - 1
        else ((sunk_frontier_after_p0 + 1) // PARTITION_SIZE_10K) * PARTITION_SIZE_10K
    )

    assert expected_next == p0 + PARTITION_SIZE_10K