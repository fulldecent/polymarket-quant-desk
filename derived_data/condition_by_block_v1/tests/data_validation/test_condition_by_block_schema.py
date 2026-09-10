"""Validate schema, types, and sort order for condition_by_block_v1."""

from __future__ import annotations

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

_EXPECTED_COLUMNS = [
    "block_number",
    "condition_id",
    "open_yes_price",
    "high_yes_price",
    "low_yes_price",
    "close_yes_price",
    "matched_yes_tokens",
    "matched_usdc",
    "fee_usdc",
    "fill_count",
]
_UINT64 = {"matched_yes_tokens", "matched_usdc", "fee_usdc"}
_UINT32 = {"block_number", "fill_count"}
_DOUBLE = {"open_yes_price", "high_yes_price", "low_yes_price", "close_yes_price"}


def _output_dir() -> Path:
    val = os.environ.get("CONDITION_BY_BLOCK_V1_DIR", "")
    if not val:
        pytest.skip("CONDITION_BY_BLOCK_V1_DIR is not set")
    out = Path(val)
    if not out.exists():
        pytest.skip(f"CONDITION_BY_BLOCK_V1_DIR does not exist: {out}")
    return out


def _all_data_files(out: Path) -> list[Path]:
    files = []
    for m_dir in out.glob("1M=*"):
        if not m_dir.is_dir():
            continue
        for k_dir in m_dir.glob("10K=*"):
            if not k_dir.is_dir() or k_dir.name.endswith(".tmp"):
                continue
            data = k_dir / "data.parquet"
            if data.exists():
                files.append(data)
    return sorted(files)


def test_physical_types_and_column_order():
    files = _all_data_files(_output_dir())
    if not files:
        pytest.skip("no data.parquet files found")
    pf = pq.ParquetFile(files[0])
    names = [pf.schema.column(i).name for i in range(len(_EXPECTED_COLUMNS))]
    assert names == _EXPECTED_COLUMNS
    by_name = {pf.schema.column(i).name: pf.schema.column(i) for i in range(len(_EXPECTED_COLUMNS))}
    cond = by_name["condition_id"]
    assert cond.physical_type == "BYTE_ARRAY"
    assert cond.logical_type.type == "NONE"
    for name in _UINT64:
        col = by_name[name]
        assert col.physical_type == "INT64"
        assert "isSigned=false" in str(col.logical_type)
    for name in _UINT32:
        col = by_name[name]
        assert col.physical_type == "INT32"
        assert "isSigned=false" in str(col.logical_type)
    for name in _DOUBLE:
        assert by_name[name].physical_type == "DOUBLE"


def test_rows_sorted_by_block_and_condition():
    files = _all_data_files(_output_dir())
    if not files:
        pytest.skip("no data.parquet files found")
    offenders = []
    for f in files:
        table = pq.read_table(f, columns=["block_number", "condition_id"])
        if table.num_rows < 2:
            continue
        keys = list(zip(table.column("block_number").to_pylist(), table.column("condition_id").to_pylist()))
        if keys != sorted(keys, key=lambda x: (int(x[0]), x[1])):
            offenders.append(str(f))
            if len(offenders) >= 10:
                break
    assert not offenders, f"partitions not sorted: {offenders}"


def test_grain_unique_within_partition():
    files = _all_data_files(_output_dir())
    if not files:
        pytest.skip("no data.parquet files found")
    con = duckdb.connect()
    for f in files:
        n = con.execute(
            f"""
            SELECT COUNT(*) FROM (
                SELECT block_number, condition_id, COUNT(*) AS c
                FROM read_parquet('{f.as_posix()}')
                GROUP BY 1, 2
                HAVING c > 1
            )
            """
        ).fetchone()[0]
        assert n == 0, f"duplicate grain in {f}"
