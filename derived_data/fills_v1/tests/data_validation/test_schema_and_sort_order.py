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


def _fills_data_glob() -> str:
    d = _fills_dir()
    return f"{d}/{PARTITION_1M_LABEL}=*/{PARTITION_10K_LABEL}=*/data.parquet"


def test_no_nulls_in_non_nullable_columns():
    """Every column declared non-nullable in the contract has no NULL values."""
    con = _connect()
    glob = _fills_data_glob()
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
    """condition_id matches token_id_map when token_id exists there; otherwise NULL is allowed."""
    con = _connect()
    fills_glob = _fills_data_glob()
    token_map_dir = os.environ.get("TOKEN_ID_MAP_V1_DIR", "")
    if not token_map_dir:
        pytest.skip("TOKEN_ID_MAP_V1_DIR is not set")
    tok_glob = f"{Path(token_map_dir)}/**/*.parquet"

    bad = con.execute(
        f"""
        WITH tok AS (
            SELECT token_id, condition_id
            FROM read_parquet('{tok_glob}')
        ),
        fills AS (
            SELECT token_id, condition_id
            FROM read_parquet('{fills_glob}')
        )
        SELECT COUNT(*)
        FROM fills f
        LEFT JOIN tok t USING (token_id)
        WHERE (t.token_id IS NULL AND f.condition_id IS NOT NULL)
           OR (t.token_id IS NOT NULL AND f.condition_id IS NULL)
           OR (t.token_id IS NOT NULL AND f.condition_id != t.condition_id)
        """
    ).fetchone()[0]
    assert bad == 0, f"found {bad} rows where condition_id is inconsistent with token_id_map_v1"


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


def test_logical_fill_index_unique_per_block():
    """Within each block, logical_fill_index is unique (gaps are allowed)."""
    con = _connect()
    glob = _fills_data_glob()
    bad = con.execute(
        f"""
        SELECT COUNT(*) FROM (
            SELECT block_number,
                   COUNT(*) AS c,
                   COUNT(DISTINCT logical_fill_index) AS d
            FROM read_parquet('{glob}')
            GROUP BY block_number
            HAVING c <> d
        )
        """
    ).fetchone()[0]
    assert bad == 0, f"found {bad} blocks with duplicate logical_fill_index values"


def test_blob_column_byte_lengths():
    """BLOB columns have contract byte lengths (or NULL where allowed)."""
    con = _connect()
    glob = _fills_data_glob()
    bad = con.execute(
        f"""
        SELECT COUNT(*)
        FROM read_parquet('{glob}')
        WHERE octet_length(account) != 20
           OR octet_length(token_id) != 32
           OR (condition_id IS NOT NULL AND octet_length(condition_id) != 32)
           OR (market_id IS NOT NULL AND octet_length(market_id) != 32)
        """
    ).fetchone()[0]
    assert bad == 0, f"found {bad} rows with unexpected BLOB byte lengths"
