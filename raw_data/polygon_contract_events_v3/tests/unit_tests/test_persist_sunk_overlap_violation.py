"""
Re-loading data that has already been sunk is a caller bug and must be refused.

Once a partition is sunk its Parquet is immutable, so its rows are deleted from the hot DB. If
``persist`` accepted a range at or below the sunk frontier, those rows would reappear in the hot
DB with nowhere to go: the cold partition can never be rewritten to include them.
"""

import sys
from pathlib import Path

import pytest

sys.path.insert(0, str(Path(__file__).resolve().parents[2]))

from _internal.persistence import HotStore, HotStoreConfig
from _internal.tables import PARTITION_SIZE_10K, SCRAPE_START_BLOCK, table_name

SCHEMA = str(Path(__file__).resolve().parents[2] / "schema.sql")
TARGET = ("ConditionalTokens", "condition_preparation")


@pytest.fixture
def store(tmp_path):
    hot = HotStore(
        str(tmp_path / "test.db"),
        SCHEMA,
        config=HotStoreConfig(duckdb_memory_limit="256MB", duckdb_temp_dir=str(tmp_path)),
    )
    yield hot
    hot.close()


def one_row(block: int) -> dict:
    return {
        "block_number": block,
        "transaction_index": 0,
        "transaction_hash": b"\x00" * 32,
        "log_index": 0,
        "condition_id": b"\x01" * 32,
        "oracle": b"\x02" * 20,
        "question_id": b"\x03" * 32,
        "outcome_slot_count": 2,
    }


def test_a_range_below_the_sunk_frontier_is_refused(store):
    sunk_through = SCRAPE_START_BLOCK + PARTITION_SIZE_10K - 1
    store.sunk_frontier = sunk_through

    with pytest.raises(ValueError, match="sunk frontier"):
        store.persist(
            SCRAPE_START_BLOCK + 50,
            SCRAPE_START_BLOCK + 150,
            {TARGET: [one_row(SCRAPE_START_BLOCK + 50)]},
        )


def test_a_range_straddling_the_sunk_frontier_is_refused(store):
    sunk_through = SCRAPE_START_BLOCK + 999
    store.sunk_frontier = sunk_through

    with pytest.raises(ValueError, match="sunk frontier"):
        store.persist(
            sunk_through - 10,
            sunk_through + 10,
            {TARGET: [one_row(sunk_through)]},
        )


def test_a_range_starting_just_above_the_sunk_frontier_is_accepted(store):
    sunk_through = SCRAPE_START_BLOCK + 999
    store.sunk_frontier = sunk_through

    result = store.persist(
        sunk_through + 1,
        sunk_through + 100,
        {TARGET: [one_row(sunk_through + 1)]},
    )

    assert result.rows_inserted == 1


def test_a_refused_range_leaves_no_trace(store):
    store.sunk_frontier = SCRAPE_START_BLOCK + 999

    with pytest.raises(ValueError):
        store.persist(
            SCRAPE_START_BLOCK,
            SCRAPE_START_BLOCK + 100,
            {TARGET: [one_row(SCRAPE_START_BLOCK)]},
        )

    assert store.list_loaded_ranges() == []
    rows = store.connection.execute(
        f"SELECT count(*) FROM {table_name(*TARGET)}"
    ).fetchone()[0]
    assert rows == 0
