"""
Test demonstrating a potential invariant violation in reconcile_with_cold_tier().

The documented postcondition for reconcile_with_cold_tier states:
    "The invariants on loaded_block_ranges documented in schema.sql hold
     (in particular, at most one row has sunk_to_parquet=TRUE and it covers
     a prefix of loaded coverage starting at SCRAPE_START_BLOCK)."

However, reconcile_with_cold_tier can be called with arbitrary Parquet files
on disk. If the cold tier has files for partition P but NOT for partition P-10K,
reconcile will:

1. Find partition P on disk
2. Delete any overlapping coverage (none in this case)
3. Insert [P, P+9999] as sunk
4. Coalesce — but since there's no sunk row starting at SCRAPE_START_BLOCK,
   the coalesce logic only ensures the first merged range starts at or before
   SCRAPE_START_BLOCK by extending it DOWNWARD if needed.

Wait, looking at the code more carefully:

```python
if merged and merged[0][0] > SCRAPE_START_BLOCK:
    merged[0] = (SCRAPE_START_BLOCK, merged[0][1])
```

This EXTENDS the first sunk range DOWN to SCRAPE_START_BLOCK, which would
create a sunk range [SCRAPE_START_BLOCK, P+9999] even though blocks
[SCRAPE_START_BLOCK, P-1] have no Parquet files on disk!

This is a violation: reconcile marks blocks as "sunk" (meaning their Parquet
files exist) when in fact they don't.

This test demonstrates that reconcile_with_cold_tier can create a sunk range
that claims coverage for blocks whose Parquet files are NOT on disk.
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
        yield db_path, str(schema_path), tmpdir


def test_reconcile_creates_sunk_range_without_files_for_earlier_blocks(temp_db_and_schema):
    """
    Demonstrate that reconcile_with_cold_tier marks blocks as sunk even when
    their Parquet files do not exist on disk.

    Scenario:
    - Cold tier has files only for partition P1 (second 10K partition)
    - No files for partition P0 (first 10K partition)
    - reconcile is called
    - Result: [SCRAPE_START_BLOCK, P1+9999] is marked sunk

    This violates the postcondition that sunk rows represent actual files on disk.
    """
    db_path, schema_path, tmpdir = temp_db_and_schema
    store = HotStore(db_path, schema_path, config=HotStoreConfig(duckdb_memory_limit="256MB"))

    try:
        cold_root = os.path.join(tmpdir, "cold")
        os.makedirs(cold_root)

        # Create ONLY the second partition's directory structure with a data.parquet
        p0 = (SCRAPE_START_BLOCK // PARTITION_SIZE_10K) * PARTITION_SIZE_10K
        p1 = p0 + PARTITION_SIZE_10K

        # Create the directory for p1 only
        dst_dir = Path(cold_root) / "ConditionalTokens" / "condition_preparation" / "1M=33600000" / f"10K={p1}"
        dst_dir.mkdir(parents=True, exist_ok=True)
        (dst_dir / "data.parquet").write_bytes(b"fake parquet content for p1 only")

        # Call reconcile — only p1 should be marked sunk
        newly_marked = store.reconcile_with_cold_tier(cold_root)
        assert newly_marked == 1

        # Verify: only p1 is marked sunk; p0's range is NOT claimed
        sunk_rows = store.connection.execute(
            "SELECT from_block, to_block FROM loaded_block_ranges WHERE sunk_to_parquet = TRUE ORDER BY from_block"
        ).fetchall()

        # After the fix, reconcile must NOT extend downward.
        # The only sunk row should be exactly [p1, p1+9999].
        assert len(sunk_rows) == 1, f"Expected exactly one sunk row, got {sunk_rows}"
        assert sunk_rows[0] == (p1, p1 + PARTITION_SIZE_10K - 1), (
            f"Expected sunk range for p1 only, got {sunk_rows[0]}"
        )
    finally:
        store.close()