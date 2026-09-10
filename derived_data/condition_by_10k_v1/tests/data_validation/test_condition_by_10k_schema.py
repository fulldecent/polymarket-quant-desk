"""Validate schema, types, and sort order for condition_by_10k_v1."""

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
    "condition_id",
    "open_yes_price",
    "high_yes_price",
    "low_yes_price",
    "close_yes_price",
    "matched_yes_tokens",
    "matched_usdc",
    "fee_usdc",
    "fill_count",
    "resolved_block",
    "resolved_log_index",
    "outcome_slot_count",
    "payout_numerators",
]


def _output_dir() -> Path:
    val = os.environ.get("CONDITION_BY_10K_V1_DIR", "")
    if not val:
        pytest.skip("CONDITION_BY_10K_V1_DIR is not set")
    out = Path(val)
    if not out.exists():
        pytest.skip(f"CONDITION_BY_10K_V1_DIR does not exist: {out}")
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
    by_name = {c.name: c for c in (pf.schema.column(i) for i in range(len(_EXPECTED_COLUMNS)))}
    assert by_name["condition_id"].physical_type == "BYTE_ARRAY"
    assert by_name["condition_id"].logical_type.type == "NONE"
    for name in ("matched_yes_tokens", "matched_usdc", "fee_usdc"):
        assert "isSigned=false" in str(by_name[name].logical_type)
    assert by_name["payout_numerators"].logical_type.type == "STRING"


def test_rows_sorted_by_condition_id():
    files = _all_data_files(_output_dir())
    if not files:
        pytest.skip("no data.parquet files found")
    offenders = []
    for f in files:
        ids = pq.read_table(f, columns=["condition_id"]).column("condition_id").to_pylist()
        if len(ids) >= 2 and ids != sorted(ids):
            offenders.append(str(f))
    assert not offenders, offenders[:10]


def test_grain_unique_and_null_rules():
    files = _all_data_files(_output_dir())
    if not files:
        pytest.skip("no data.parquet files found")
    con = duckdb.connect()
    for f in files:
        path = f.as_posix().replace("'", "''")
        dups = con.execute(
            f"""
            SELECT COUNT(*) FROM (
                SELECT condition_id, COUNT(*) c FROM read_parquet('{path}')
                GROUP BY 1 HAVING c > 1
            )
            """
        ).fetchone()[0]
        assert dups == 0, f"duplicate condition_id in {f}"
        bad = con.execute(
            f"""
            SELECT COUNT(*) FROM read_parquet('{path}')
            WHERE (fill_count = 0 AND open_yes_price IS NOT NULL)
               OR (fill_count > 0 AND open_yes_price IS NULL)
               OR (fill_count = 0 AND (resolved_block IS NULL))
               OR ((resolved_block IS NULL) <> (payout_numerators IS NULL))
            """
        ).fetchone()[0]
        assert bad == 0, f"nullability invariant broken in {f}"
