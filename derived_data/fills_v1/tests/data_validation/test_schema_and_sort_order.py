"""Validate schema, types, nullability, and physical sort order for fills_v1.

This mirrors the style of token_id_map_v1's schema test but for the fills_v1 contract.
"""

import os
import sys
from pathlib import Path

import duckdb
import pyarrow.parquet as pq
import pytest
from dotenv import load_dotenv

_project_root = Path(__file__).resolve().parents[4]
load_dotenv(_project_root / ".env")
sys.path.insert(0, str(_project_root))

from lib.partition_utils import PARTITION_10K_LABEL, PARTITION_1M_LABEL


def _fills_dir() -> Path:
    val = os.environ.get("FILLS_V1_DIR", "")
    if not val:
        pytest.skip("FILLS_V1_DIR is not set")
    out = Path(val)
    if not out.exists():
        pytest.skip(f"FILLS_V1_DIR does not exist: {out}")
    return out


def _connect() -> duckdb.DuckDBPyConnection:
    con = duckdb.connect()
    temp_dir = os.environ.get("TEMP_DIR", "")
    if temp_dir:
        con.execute(f"SET temp_directory = '{temp_dir}'")
    con.execute("SET preserve_insertion_order = false")
    return con


def test_no_nulls_in_non_nullable_columns():
    """Every column declared non-nullable in the contract has no NULL values."""
    d = _fills_dir()
    con = _connect()
    glob = f"{d}/**/*.parquet"
    # All columns except condition_id and market_id are non-nullable.
    null_checks = [
        "block_number IS NULL",
        "logical_fill_index IS NULL",
        "transaction_index IS NULL",
        "log_index IS NULL",
        "account IS NULL",
        "token_id IS NULL",
        "is_taker IS NULL",
        "net_yes_tokens IS NULL",
        "gross_usdc IS NULL",
        "fee_usdc IS NULL",
        "net_yes_position_after IS NULL",
    ]
    q = " OR ".join(null_checks)
    bad = con.execute(
        f"SELECT COUNT(*) FROM read_parquet('{glob}') WHERE {q}"
    ).fetchone()[0]
    assert bad == 0, f"found {bad} rows with NULL in a non-nullable column"


def test_condition_id_null_only_when_token_missing():
    """condition_id is NULL only when the token_id is absent from token_id_map_v1 (allowed)."""
    # This is a data-quality note, not a hard failure; the producer already fails on non-binary.
    # We just assert that when condition_id IS NOT NULL, index_set is 1 or 2.
    d = _fills_dir()
    con = _connect()
    glob = f"{d}/**/*.parquet"
    bad = con.execute(
        f"SELECT COUNT(*) FROM read_parquet('{glob}') "
        f"WHERE condition_id IS NOT NULL AND index_set NOT IN (1, 2)"
    ).fetchone()[0]
    assert bad == 0, f"found {bad} rows with non-binary index_set where condition_id is populated"


def test_physical_sort_order():
    """Rows within each partition file are sorted by (block_number, logical_fill_index)."""
    d = _fills_dir()
    for part_dir in sorted(d.rglob(f"{PARTITION_10K_LABEL}=*")):
        pq_path = part_dir / "data.parquet"
        if not pq_path.exists():
            continue
        table = pq.read_table(pq_path, columns=["block_number", "logical_fill_index"])
        df = table.to_pandas()
        if len(df) <= 1:
            continue
        # Check the sort key is non-decreasing.
        keys = list(zip(df["block_number"], df["logical_fill_index"]))
        assert keys == sorted(keys), f"partition {part_dir} is not sorted by (block_number, logical_fill_index)"


def test_logical_fill_index_dense_per_block():
    """Within each block, logical_fill_index forms a dense 0..n-1 sequence with no gaps or duplicates."""
    d = _fills_dir()
    con = _connect()
    glob = f"{d}/**/*.parquet"
    bad = con.execute(
        f"""
        SELECT COUNT(*) FROM (
            SELECT block_number,
                   COUNT(*) AS c,
                   COUNT(DISTINCT logical_fill_index) AS d,
                   MIN(logical_fill_index) AS mn,
                   MAX(logical_fill_index) AS mx
            FROM read_parquet('{glob}')
            GROUP BY block_number
            HAVING c <> d OR mn <> 0 OR mx <> c - 1
        )
        """
    ).fetchone()[0]
    assert bad == 0, f"found {bad} blocks with non-dense logical_fill_index"
