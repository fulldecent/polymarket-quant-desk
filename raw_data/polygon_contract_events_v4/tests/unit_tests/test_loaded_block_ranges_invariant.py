"""
The ``loaded_block_ranges`` invariant: rows are strictly disjoint and non-adjacent.

Sunk state is no longer a column. Every row in the table is unsunk; how far the cold tier has been
written is tracked separately as ``HotStore.sunk_frontier``. So the invariant reduces to: no two
rows overlap or touch, because anything touching should have been coalesced into one row.
"""

import sys
from pathlib import Path

import pytest

sys.path.insert(0, str(Path(__file__).resolve().parents[2]))

from _internal.persistence import HotStore, HotStoreConfig
from _internal.tables import SCRAPE_START_BLOCK

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


def violations(store: HotStore):
    """Neighbouring rows that overlap or touch, which coalescing should have merged."""
    rows = store.connection.execute(
        "SELECT from_block, to_block FROM loaded_block_ranges ORDER BY from_block"
    ).fetchall()
    return [
        ((a_from, a_to), (b_from, b_to))
        for (a_from, a_to), (b_from, b_to) in zip(rows, rows[1:])
        if a_to >= b_from - 1
    ]


def test_touching_ranges_are_coalesced(store):
    store.persist(SCRAPE_START_BLOCK, SCRAPE_START_BLOCK + 100, {TARGET: []})
    store.persist(SCRAPE_START_BLOCK + 101, SCRAPE_START_BLOCK + 200, {TARGET: []})

    assert violations(store) == []
    assert store.list_loaded_ranges(include_sunk=False) == [
        (SCRAPE_START_BLOCK, SCRAPE_START_BLOCK + 200, False)
    ]


def test_ranges_arriving_out_of_order_are_coalesced(store):
    """Chunks complete in whatever order the provider answers, not in block order."""
    store.persist(SCRAPE_START_BLOCK + 101, SCRAPE_START_BLOCK + 200, {TARGET: []})
    store.persist(SCRAPE_START_BLOCK, SCRAPE_START_BLOCK + 100, {TARGET: []})

    assert violations(store) == []
    assert store.list_loaded_ranges(include_sunk=False) == [
        (SCRAPE_START_BLOCK, SCRAPE_START_BLOCK + 200, False)
    ]


def test_a_range_that_closes_a_hole_joins_both_sides(store):
    store.persist(SCRAPE_START_BLOCK, SCRAPE_START_BLOCK + 99, {TARGET: []})
    store.persist(SCRAPE_START_BLOCK + 200, SCRAPE_START_BLOCK + 299, {TARGET: []})

    store.persist(SCRAPE_START_BLOCK + 100, SCRAPE_START_BLOCK + 199, {TARGET: []})

    assert violations(store) == []
    assert store.list_loaded_ranges(include_sunk=False) == [
        (SCRAPE_START_BLOCK, SCRAPE_START_BLOCK + 299, False)
    ]


def test_separated_ranges_stay_separate(store):
    store.persist(SCRAPE_START_BLOCK, SCRAPE_START_BLOCK + 99, {TARGET: []})
    store.persist(SCRAPE_START_BLOCK + 200, SCRAPE_START_BLOCK + 299, {TARGET: []})

    assert violations(store) == []
    assert store.list_loaded_ranges(include_sunk=False) == [
        (SCRAPE_START_BLOCK, SCRAPE_START_BLOCK + 99, False),
        (SCRAPE_START_BLOCK + 200, SCRAPE_START_BLOCK + 299, False),
    ]


def test_the_invariant_survives_many_scattered_ranges(store):
    for offset in range(0, 2_000, 200):
        store.persist(
            SCRAPE_START_BLOCK + offset, SCRAPE_START_BLOCK + offset + 99, {TARGET: []}
        )
    # Fill every hole, in an order unrelated to block order.
    for offset in reversed(range(100, 2_000, 200)):
        store.persist(
            SCRAPE_START_BLOCK + offset, SCRAPE_START_BLOCK + offset + 99, {TARGET: []}
        )

    assert violations(store) == []
    assert store.list_loaded_ranges(include_sunk=False) == [
        (SCRAPE_START_BLOCK, SCRAPE_START_BLOCK + 1_999, False)
    ]
