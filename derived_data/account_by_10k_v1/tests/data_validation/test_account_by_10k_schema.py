"""Validate schema, types, and sort order for account_by_10k_v1."""

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
    "account",
    "fill_count",
    "condition_count",
    "volume_usdc",
    "fee_usdc",
    "maker_fills",
    "taker_fills",
    "maker_fill_ratio",
    "taker_hour_entropy",
    "maker_hour_entropy",
    "first_fill_block",
    "last_fill_block",
]


def _output_dir() -> Path:
    val = os.environ.get("ACCOUNT_BY_10K_V1_DIR", "")
    if not val:
        pytest.skip("ACCOUNT_BY_10K_V1_DIR is not set")
    out = Path(val)
    if not out.exists():
        pytest.skip(f"ACCOUNT_BY_10K_V1_DIR does not exist: {out}")
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
    assert by_name["account"].physical_type == "BYTE_ARRAY"
    assert by_name["account"].logical_type.type == "NONE"
    for name in ("volume_usdc", "fee_usdc"):
        assert "isSigned=false" in str(by_name[name].logical_type)
    for name in ("maker_fill_ratio", "taker_hour_entropy", "maker_hour_entropy"):
        assert by_name[name].physical_type == "DOUBLE"


def test_rows_sorted_grain_unique_and_fill_identity():
    files = _all_data_files(_output_dir())
    if not files:
        pytest.skip("no data.parquet files found")
    con = duckdb.connect()
    for f in files:
        accounts = pq.read_table(f, columns=["account"]).column("account").to_pylist()
        if len(accounts) >= 2:
            assert accounts == sorted(accounts), f"not sorted: {f}"
        path = f.as_posix().replace("'", "''")
        bad = con.execute(
            f"""
            SELECT COUNT(*) FROM (
                SELECT account, COUNT(*) c FROM read_parquet('{path}')
                GROUP BY 1 HAVING c > 1
            )
            """
        ).fetchone()[0]
        assert bad == 0, f"duplicate account in {f}"
        mismatch = con.execute(
            f"""
            SELECT COUNT(*) FROM read_parquet('{path}')
            WHERE fill_count <> maker_fills + taker_fills
            """
        ).fetchone()[0]
        assert mismatch == 0, f"fill_count != maker+taker in {f}"
