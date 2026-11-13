"""
Reconciling against the cold tier follows manifests, not loose Parquet.

A day folder with no ``_SUCCESS`` is unpublished. A hole in the manifest
run is an error. Claiming either one would drop blocks from the work queue.
"""

import sys
from pathlib import Path

import pytest

sys.path.insert(0, str(Path(__file__).resolve().parents[2]))
sys.path.insert(0, str(Path(__file__).resolve().parent))

from _internal.errors import V4Error
from _internal.persistence import HotStore, HotStoreConfig
from _internal.tables import SCRAPE_START_BLOCK
from day_layout import D0, D1, D2, bounds, plant_day

SCHEMA = str(Path(__file__).resolve().parents[2] / "schema.sql")


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


def test_parquet_without_a_manifest_is_not_claimed(store, cold_root):
    plant_day(cold_root, D0, *bounds(0))
    plant_day(cold_root, D1, *bounds(1))

    claimed = store.reconcile_with_cold_tier(str(cold_root))

    assert claimed == 0
    assert store.get_sunk_frontier() == SCRAPE_START_BLOCK - 1


def test_contiguous_manifests_are_claimed(store, cold_root):
    plant_day(cold_root, D0, *bounds(0), manifest=True)
    _first, last = bounds(1)
    plant_day(cold_root, D1, *bounds(1), manifest=True)

    claimed = store.reconcile_with_cold_tier(str(cold_root))

    assert claimed == 2
    assert store.get_sunk_frontier() == last


def test_a_manifest_hole_is_not_a_prefix(store, cold_root):
    plant_day(cold_root, D0, *bounds(0), manifest=True)
    plant_day(cold_root, D2, *bounds(2), manifest=True)

    with pytest.raises(V4Error, match="skip"):
        store.reconcile_with_cold_tier(str(cold_root))

    assert store.get_sunk_frontier() == SCRAPE_START_BLOCK - 1


def test_nothing_on_disk_leaves_the_frontier_alone(store, cold_root):
    claimed = store.reconcile_with_cold_tier(str(cold_root))

    assert claimed == 0
    assert store.get_sunk_frontier() == SCRAPE_START_BLOCK - 1
