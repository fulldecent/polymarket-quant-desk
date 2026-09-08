"""
Reconciling against the cold tier must never claim blocks that were not written.

The sunk frontier means "[SCRAPE_START_BLOCK, N] is entirely on the cold tier". Recovering it by
looking at files on disk is only safe while those files are contiguous from where the frontier
already sits: a partition sitting above a gap says nothing about the gap, and claiming it would
silently drop the missing blocks from the work queue forever.
"""

import sys
from pathlib import Path

import pytest

sys.path.insert(0, str(Path(__file__).resolve().parents[2]))

from _internal.persistence import HotStore, HotStoreConfig
from _internal.tables import PARTITION_SIZE_10K, SCRAPE_START_BLOCK

SCHEMA = str(Path(__file__).resolve().parents[2] / "schema.sql")
FIRST = (SCRAPE_START_BLOCK // PARTITION_SIZE_10K) * PARTITION_SIZE_10K


@pytest.fixture
def store(tmp_path):
    hot = HotStore(
        str(tmp_path / "test.db"),
        SCHEMA,
        config=HotStoreConfig(duckdb_memory_limit="256MB", duckdb_temp_dir=str(tmp_path)),
    )
    yield hot
    hot.close()


@pytest.fixture
def cold_root(tmp_path):
    root = tmp_path / "cold"
    root.mkdir()
    return root


def write_partition(cold_root: Path, partition_start: int) -> None:
    one_million = (partition_start // 1_000_000) * 1_000_000
    directory = (
        cold_root
        / "ConditionalTokens"
        / "condition_preparation"
        / f"1M={one_million}"
        / f"10K={partition_start}"
    )
    directory.mkdir(parents=True, exist_ok=True)
    (directory / "data.parquet").write_bytes(b"parquet")


def test_a_partition_above_a_gap_is_not_claimed(store, cold_root):
    """The earlier partition has no files, so it still has to be scraped."""
    write_partition(cold_root, FIRST + PARTITION_SIZE_10K)

    claimed = store.reconcile_with_cold_tier(str(cold_root))

    assert claimed == 0
    assert store.get_sunk_frontier() == SCRAPE_START_BLOCK - 1


def test_contiguous_partitions_are_claimed(store, cold_root):
    write_partition(cold_root, FIRST)
    write_partition(cold_root, FIRST + PARTITION_SIZE_10K)

    claimed = store.reconcile_with_cold_tier(str(cold_root))

    assert claimed == 2
    assert store.get_sunk_frontier() == FIRST + 2 * PARTITION_SIZE_10K - 1


def test_claiming_stops_at_the_first_gap(store, cold_root):
    write_partition(cold_root, FIRST)
    write_partition(cold_root, FIRST + 2 * PARTITION_SIZE_10K)  # a hole in between

    claimed = store.reconcile_with_cold_tier(str(cold_root))

    assert claimed == 1
    assert store.get_sunk_frontier() == FIRST + PARTITION_SIZE_10K - 1


def test_nothing_on_disk_leaves_the_frontier_alone(store, cold_root):
    claimed = store.reconcile_with_cold_tier(str(cold_root))

    assert claimed == 0
    assert store.get_sunk_frontier() == SCRAPE_START_BLOCK - 1
