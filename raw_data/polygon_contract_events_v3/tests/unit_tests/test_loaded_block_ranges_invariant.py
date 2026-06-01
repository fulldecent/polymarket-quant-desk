"""
Test for loaded_block_ranges invariant violations.

The schema invariant documented in schema.sql and _internal/README.md:

    "Within rows of the same `sunk_to_parquet` value, rows are strictly
     disjoint AND non-adjacent."

This test verifies that after various persist() and commit_sink() operations,
the invariant holds. A violation would be two rows with the same sunk_to_parquet
value where a.to_block + 1 == b.from_block (touching) or overlapping.
"""

import tempfile
import os
from pathlib import Path

import pytest

from raw_data.polygon_contract_events_v3._internal.persistence import HotStore, HotStoreConfig
from raw_data.polygon_contract_events_v3._internal.tables import SCRAPE_START_BLOCK, PARTITION_SIZE_10K


@pytest.fixture
def temp_db_and_schema():
    with tempfile.TemporaryDirectory() as tmpdir:
        db_path = os.path.join(tmpdir, "test.db")
        schema_path = Path(__file__).resolve().parents[2] / "schema.sql"
        yield db_path, str(schema_path)


def _check_invariant(conn):
    """Return list of violating row pairs, or empty list if invariant holds."""
    rows = conn.execute("""
        SELECT from_block, to_block, sunk_to_parquet
        FROM loaded_block_ranges
        ORDER BY sunk_to_parquet, from_block
    """).fetchall()

    violations = []
    # Group by sunk_to_parquet
    for sunk in [False, True]:
        sunk_rows = [(f, t) for f, t, s in rows if s == sunk]
        for i in range(len(sunk_rows)):
            for j in range(i + 1, len(sunk_rows)):
                a_f, a_t = sunk_rows[i]
                b_f, b_t = sunk_rows[j]
                # Check for overlap or adjacency
                if a_t >= b_f - 1 and a_f <= b_t + 1:
                    violations.append((a_f, a_t, b_f, b_t, sunk))
    return violations


def test_persist_coalesces_touching_ranges(temp_db_and_schema):
    """
    Verify that persist() coalesces touching unsunk ranges.

    If we persist [100, 200] then [201, 300], the result should be
    a single row [100, 300], not two adjacent rows.
    """
    db_path, schema_path = temp_db_and_schema
    store = HotStore(db_path, schema_path, config=HotStoreConfig(duckdb_memory_limit="256MB"))

    try:
        # First persist
        store.persist(
            SCRAPE_START_BLOCK,
            SCRAPE_START_BLOCK + 100,
            {("ConditionalTokens", "condition_preparation"): []},
        )

        # Second persist that touches the first (adjacent, not overlapping)
        store.persist(
            SCRAPE_START_BLOCK + 101,
            SCRAPE_START_BLOCK + 200,
            {("ConditionalTokens", "condition_preparation"): []},
        )

        # Check invariant
        violations = _check_invariant(store.connection)
        assert len(violations) == 0, f"Invariant violated: {violations}"

        # Should have coalesced into one row
        rows = store.list_loaded_ranges(include_sunk=False)
        assert len(rows) == 1, f"Expected 1 coalesced row, got {len(rows)}: {rows}"
        assert rows[0][0] == SCRAPE_START_BLOCK
        assert rows[0][1] == SCRAPE_START_BLOCK + 200
    finally:
        store.close()


def test_persist_coalesces_adjacent_ranges(temp_db_and_schema):
    """
    Verify that persist() coalesces adjacent unsunk ranges.
    """
    db_path, schema_path = temp_db_and_schema
    store = HotStore(db_path, schema_path, config=HotStoreConfig(duckdb_memory_limit="256MB"))

    try:
        # Persist two adjacent ranges - they should coalesce
        store.persist(
            SCRAPE_START_BLOCK,
            SCRAPE_START_BLOCK + 100,
            {("ConditionalTokens", "condition_preparation"): []},
        )
        store.persist(
            SCRAPE_START_BLOCK + 101,
            SCRAPE_START_BLOCK + 200,
            {("ConditionalTokens", "condition_preparation"): []},
        )

        # Check invariant holds
        violations = _check_invariant(store.connection)
        assert len(violations) == 0, f"Invariant violated: {violations}"

        # Should have coalesced into one row
        rows = store.list_loaded_ranges(include_sunk=False)
        assert len(rows) == 1, f"Expected 1 coalesced row, got {len(rows)}: {rows}"
    finally:
        store.close()