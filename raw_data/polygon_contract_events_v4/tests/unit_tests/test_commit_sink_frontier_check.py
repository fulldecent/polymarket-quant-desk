"""commit_sink accepts only the closed day that starts on the next unsunk block."""

import sys
from pathlib import Path

import pytest

sys.path.insert(0, str(Path(__file__).resolve().parents[2]))
sys.path.insert(0, str(Path(__file__).resolve().parent))

from _internal.errors import PartitionFrontierError
from _internal.persistence import HotStore, HotStoreConfig
from day_layout import D0, D1, bounds

SCHEMA = str(Path(__file__).resolve().parents[2] / "schema.sql")


@pytest.fixture
def store(tmp_path):
    hot = HotStore(
        str(tmp_path / "test.db"),
        SCHEMA,
        config=HotStoreConfig(duckdb_memory_limit="256MB", duckdb_temp_dir=str(tmp_path)),
    )
    hot.open_first_day(D0)
    first, last = bounds(0)
    hot.persist(first, last + 1, {}, day_cuts=[(D1, last + 1)])
    yield hot
    hot.close()


def test_commit_sink_rejects_a_later_day(store):
    with pytest.raises(PartitionFrontierError):
        store.commit_sink(D1)

    _first, last = bounds(0)
    store.commit_sink(D0)
    assert store.get_sunk_frontier() == last


def test_an_open_day_cannot_be_committed(store):
    _first, last = bounds(0)
    store.commit_sink(D0)

    with pytest.raises(PartitionFrontierError, match="not a closed span"):
        store.commit_sink(D1)
    assert store.get_sunk_frontier() == last
