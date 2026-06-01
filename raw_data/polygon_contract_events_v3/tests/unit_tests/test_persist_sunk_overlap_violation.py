"""
Test demonstrating a postcondition violation in HotStore.persist().

The documented precondition states:
    "Overlap with an existing sunk_to_parquet=TRUE row is a caller bug
     (re-loading already-sunk data) and is rejected with ValueError."

However, the current implementation of persist() does NOT perform this check.
It validates that from_block >= SCRAPE_START_BLOCK and that row columns match,
but it never queries loaded_block_ranges to detect overlap with sunk rows.

This test demonstrates that calling persist() with a range that overlaps
an existing sunk range does NOT raise ValueError as documented — instead
it silently inserts rows and corrupts the loaded_block_ranges invariant.

This is a violation of the documented precondition/postcondition contract.
"""

import tempfile
import os
from pathlib import Path

import duckdb
import pytest

from raw_data.polygon_contract_events_v3._internal.persistence import HotStore, HotStoreConfig
from raw_data.polygon_contract_events_v3._internal.tables import SCRAPE_START_BLOCK


@pytest.fixture
def temp_db_and_schema():
    """Create a temporary hot DB with schema applied."""
    with tempfile.TemporaryDirectory() as tmpdir:
        db_path = os.path.join(tmpdir, "test.db")
        schema_path = Path(__file__).resolve().parents[2] / "schema.sql"
        yield db_path, str(schema_path)


def test_persist_overlapping_sunk_range_does_not_raise_valueerror(temp_db_and_schema):
    """
    Demonstrate that persist() accepts a range overlapping a sunk row
    without raising ValueError, violating the documented precondition.

    Expected behavior per docs: ValueError should be raised.
    Actual behavior: No error is raised; rows are inserted and ranges coalesce.
    """
    db_path, schema_path = temp_db_and_schema
    store = HotStore(db_path, schema_path, config=HotStoreConfig(duckdb_memory_limit="256MB"))

    try:
        # First, persist a range and then simulate sinking it by directly
        # manipulating loaded_block_ranges (as reconcile_with_cold_tier would).
        # We use a small range for the test.
        first_range_start = SCRAPE_START_BLOCK
        first_range_end = SCRAPE_START_BLOCK + 100

        # Persist some rows in the first range
        rows_by_target = {
            ("ConditionalTokens", "condition_preparation"): [
                {
                    "block_number": SCRAPE_START_BLOCK,
                    "transaction_index": 0,
                    "transaction_hash": b"\x00" * 32,
                    "log_index": 0,
                    "condition_id": b"\x01" * 32,
                    "oracle": b"\x02" * 20,
                    "question_id": b"\x03" * 32,
                    "outcome_slot_count": 2,
                }
            ]
        }
        result = store.persist(first_range_start, first_range_end, rows_by_target)
        assert result.rows_inserted == 1

        # Now simulate the range being sunk (as if commit_sink had run).
        # We directly update the row to sunk_to_parquet=TRUE.
        conn = store.connection
        conn.execute(
            """
            UPDATE loaded_block_ranges
            SET sunk_to_parquet = TRUE
            WHERE from_block = ? AND to_block = ?
            """,
            (first_range_start, first_range_end),
        )

        # Verify the range is now marked sunk
        sunk_rows = conn.execute(
            "SELECT from_block, to_block, sunk_to_parquet FROM loaded_block_ranges"
        ).fetchall()
        assert any(r[2] is True for r in sunk_rows), "Expected a sunk row"

        # Now attempt to persist a range that OVERLAPS the sunk range.
        # Per the documented precondition, this should raise ValueError.
        overlapping_start = SCRAPE_START_BLOCK + 50  # Overlaps [first_range_start, first_range_end]
        overlapping_end = SCRAPE_START_BLOCK + 150

        overlapping_rows = {
            ("ConditionalTokens", "condition_preparation"): [
                {
                    "block_number": overlapping_start,
                    "transaction_index": 1,
                    "transaction_hash": b"\x04" * 32,
                    "log_index": 0,
                    "condition_id": b"\x05" * 32,
                    "oracle": b"\x06" * 20,
                    "question_id": b"\x07" * 32,
                    "outcome_slot_count": 2,
                }
            ]
        }

        # After the fix, persist() MUST reject overlap with a sunk range.
        with pytest.raises(ValueError, match="sunk|overlap|forbidden"):
            store.persist(overlapping_start, overlapping_end, overlapping_rows)
    finally:
        store.close()