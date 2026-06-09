"""Validate token_id_map_v1 physical schema and sort order for reproducibility.

Two contracts from DATA_DICTIONARY.md:

1. Physical Parquet types must match the raw polygon_contract_events_v3 logical
   types so joins are clean: the four ID/address columns are ``BLOB`` (Parquet
   ``BYTE_ARRAY``, no logical type — NOT ``FIXED_LEN_BYTE_ARRAY``) and
   ``index_set`` is ``UINTEGER`` (``INT(bitWidth=32, isSigned=false)``).

2. Within each partition file, rows are sorted ascending by the grain key
   ``(collateral_token, parent_collection_id, condition_id, index_set)``. This
   total order is required for byte-for-byte reproducibility.
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

_EXPECTED_COLUMNS = [
    "collateral_token",
    "parent_collection_id",
    "condition_id",
    "index_set",
    "token_id",
]
_BLOB_COLUMNS = {"collateral_token", "parent_collection_id", "condition_id", "token_id"}
_SORT_KEY = ["collateral_token", "parent_collection_id", "condition_id", "index_set"]


def _output_dir() -> Path:
    val = os.environ.get("TOKEN_ID_MAP_V1_DIR", "")
    if not val:
        pytest.skip("TOKEN_ID_MAP_V1_DIR is not set")
    out = Path(val)
    if not out.exists():
        pytest.skip(f"TOKEN_ID_MAP_V1_DIR does not exist: {out}")
    return out


def _all_data_files(out: Path) -> list[Path]:
    """Return all data.parquet files from properly-named 1M/10K partitions.

    Directory names are validated to ensure they match the format 1M=<int> and
    10K=<int>, avoiding false matches on temporary or malformed directories.
    """
    files = []
    for m_dir in out.glob("1M=*"):
        if not m_dir.is_dir():
            continue
        # Validate 1M directory name is exactly 1M=<integer>
        try:
            parts = m_dir.name.split("=")
            if len(parts) != 2 or parts[0] != "1M":
                continue
            int(parts[1])  # Validate it's an integer
        except ValueError:
            continue

        for k_dir in m_dir.glob("10K=*"):
            if not k_dir.is_dir():
                continue
            # Validate 10K directory name is exactly 10K=<integer>
            try:
                parts = k_dir.name.split("=")
                if len(parts) != 2 or parts[0] != "10K":
                    continue
                int(parts[1])  # Validate it's an integer
            except ValueError:
                continue

            data_file = k_dir / "data.parquet"
            if data_file.exists():
                files.append(data_file)

    return sorted(files)


def test_physical_types_match_raw_dataset():
    """BLOB columns are BYTE_ARRAY (no logical type); index_set is unsigned INT32."""
    out = _output_dir()
    files = _all_data_files(out)
    if not files:
        pytest.skip("no data.parquet files found")

    # Inspecting one file is sufficient; the writer enforces a single schema.
    pf = pq.ParquetFile(files[0])
    by_name = {pf.schema.column(i).name: pf.schema.column(i) for i in range(len(_EXPECTED_COLUMNS))}

    assert [pf.schema.column(i).name for i in range(len(_EXPECTED_COLUMNS))] == _EXPECTED_COLUMNS

    for name in _BLOB_COLUMNS:
        col = by_name[name]
        assert col.physical_type == "BYTE_ARRAY", (
            f"{name} is {col.physical_type}, expected BYTE_ARRAY (BLOB). "
            f"FIXED_LEN_BYTE_ARRAY breaks joins with the raw dataset."
        )
        assert col.logical_type.type == "NONE", (
            f"{name} has logical type {col.logical_type}, expected none (raw BLOB)"
        )

    idx = by_name["index_set"]
    assert idx.physical_type == "INT32"
    assert "isSigned=false" in str(idx.logical_type) or "isSigned=0" in str(idx.logical_type), (
        f"index_set logical type {idx.logical_type}, expected unsigned INT(bitWidth=32)"
    )


def test_rows_sorted_by_grain_key():
    """Every partition file's rows are ascending by the grain key."""
    out = _output_dir()
    files = _all_data_files(out)
    if not files:
        pytest.skip("no data.parquet files found")

    offenders: list[str] = []
    for f in files:
        table = pq.read_table(f, columns=_SORT_KEY)
        if table.num_rows < 2:
            continue
        rows = list(zip(*[table.column(c).to_pylist() for c in _SORT_KEY]))
        # bytes compare byte-wise; ints compare numerically; tuple order matches the contract
        if rows != sorted(rows):
            offenders.append(str(f.relative_to(out)))
            if len(offenders) >= 10:
                break
    assert not offenders, f"partitions not sorted by grain key: {offenders}"


def test_grain_key_is_globally_unique():
    """Every 4-tuple (collateral_token, parent_collection_id, condition_id, index_set)
    appears exactly once across the entire dataset.

    This test will fail if the dataset was materialized with the old _process_nr_partition
    code that did not properly deduplicate 4-tuples. To fix: delete the dataset and
    regenerate with the corrected code.
    """
    out = _output_dir()
    files = _all_data_files(out)
    if not files:
        pytest.skip("no data.parquet files found")

    # Use DuckDB to check uniqueness across all partitions efficiently.
    # Pattern matches exactly 1M=<int>/10K=<int>/data.parquet (validated by _all_data_files).
    glob_pattern = str(out / "1M=*" / "10K=*" / "data.parquet")
    result = duckdb.query(f"""
        SELECT
            COUNT(*) as total_rows,
            COUNT(DISTINCT (collateral_token, parent_collection_id, condition_id, index_set)) as distinct_tuples
        FROM read_parquet('{glob_pattern}')
    """).to_df()

    total = result["total_rows"][0]
    distinct = result["distinct_tuples"][0]

    assert total == distinct, (
        f"grain key is not globally unique across dataset: "
        f"{total} total rows but only {distinct} distinct tuples. "
        f"Likely cause: dataset was materialized with buggy _process_nr_partition code. "
        f"Delete {out} and regenerate."
    )
