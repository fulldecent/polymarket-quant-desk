"""UTC day math, and the rule that a closed day waits for the safe head."""

import sys
from pathlib import Path

import pytest

sys.path.insert(0, str(Path(__file__).resolve().parents[2]))
sys.path.insert(0, str(Path(__file__).resolve().parents[4]))
sys.path.insert(0, str(Path(__file__).resolve().parent))

from _internal.persistence import HotStore, HotStoreConfig
from _internal.tables import REORG_BUFFER_BLOCKS
from day_layout import D0, D1, bounds
from lib.ymd_partition import (
    day_dir,
    day_key_from_timestamp,
    day_label,
    first_block_on_or_after,
    midnights_crossed,
    next_day_key,
    utc_midnight,
)

SCHEMA = str(Path(__file__).resolve().parents[2] / "schema.sql")


def test_a_utc_day_is_a_function_of_the_timestamp():
    midnight = utc_midnight(20261012)
    assert day_key_from_timestamp(midnight) == 20261012
    assert day_key_from_timestamp(midnight - 1) == 20261011
    assert day_label(20261012) == "2026-10-12"
    assert day_dir(20261012) == "year=2026/month=10/day=12"
    assert next_day_key(20261231) == 20270101
    assert midnights_crossed(midnight - 10, midnight) == [midnight]
    assert midnights_crossed(midnight, midnight + 10) == []


def test_binary_search_finds_the_first_block_of_the_new_day():
    # Block 10 is the last of the old day. Block 11 is the first of the new one.
    stamps = {10: 100, 11: 200, 12: 201}
    midnight = 200
    assert first_block_on_or_after(stamps.get, 10, 12, midnight) == 11


@pytest.fixture
def store(tmp_path):
    hot = HotStore(
        str(tmp_path / "test.db"),
        SCHEMA,
        config=HotStoreConfig(duckdb_memory_limit="256MB", duckdb_temp_dir=str(tmp_path)),
    )
    yield hot
    hot.close()


def test_a_closed_day_is_not_ready_until_it_is_behind_the_safe_head(store):
    store.open_first_day(D0)
    first, last = bounds(0)
    store.persist(first, last + 1, {}, day_cuts=[(D1, last + 1)])

    assert store.list_days_ready_to_sink() == []
    store.set_safe_head(last - 1)
    assert store.list_days_ready_to_sink() == []
    store.set_safe_head(last)
    assert store.list_days_ready_to_sink() == [D0]
    # The next day is still open, so a higher head does not sink it.
    store.set_safe_head(last + REORG_BUFFER_BLOCKS)
    assert store.list_days_ready_to_sink() == [D0]


def test_max_loaded_block_is_what_a_rewound_head_is_compared_to(store):
    store.open_first_day(D0)
    first, last = bounds(0)
    store.persist(first, last, {})

    assert store.max_loaded_block() == last
    assert store.max_loaded_block() > last - 1
