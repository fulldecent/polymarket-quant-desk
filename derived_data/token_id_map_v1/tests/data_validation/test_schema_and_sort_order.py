"""Validate token_id_map_v1 physical schema and sort order for reproducibility.

Two contracts from DATA_DICTIONARY.md:

1. Physical Parquet types must match the raw polygon_contract_events_v3 logical
   types so joins are clean: the four ID/address columns are ``BLOB`` (Parquet
   ``BYTE_ARRAY``, no logical type — NOT ``FIXED_LEN_BYTE_ARRAY``) and
   ``index_set`` is ``UINTEGER`` (``INT(bitWidth=32, isSigned=false)``).

2. Within each partition file, rows are sorted ascending by ``token_id``
    (byte-wise). This total order is required for byte-for-byte reproducibility.
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
    "token_id",
    "collateral_token",
    "parent_collection_id",
    "condition_id",
    "index_set",
    "market_id",
]
_BLOB_COLUMNS = {"collateral_token", "parent_collection_id", "condition_id", "token_id", "market_id"}
_SORT_KEY = ["token_id"]
_ZERO32_HEX = "00" * 32
_NEGRISK_ADAPTER_HEX = "d91E80cF2E7be2e162c6513ceD06f1dD0dA35296".lower().removeprefix("0x")

# As of 2026-06-12 there are 20 historical/non-Polymarket orphan condition_ids
# (40 rows) in token_id_map_v1 that have no condition_preparation row. These
# arise because token discovery is via trade-linked CT split/merge events, and
# some conditions were prepared before SCRAPE_START_BLOCK or by other protocols.
# Keep headroom for drift.
KNOWN_ORPHAN_LIMIT = 100


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


def _raw_dir() -> Path:
    val = os.environ.get("POLYGON_CONTRACT_EVENTS_V3_DIR", "")
    if not val:
        pytest.skip("POLYGON_CONTRACT_EVENTS_V3_DIR is not set")
    raw = Path(val)
    if not raw.exists():
        pytest.skip(f"POLYGON_CONTRACT_EVENTS_V3_DIR does not exist: {raw}")
    return raw


def _duckdb_file_list(files: list[Path]) -> str:
    """Return a DuckDB SQL list literal for the exact landed data files."""
    quoted = [f"'{str(path).replace("'", "''")}'" for path in files]
    return f"[{', '.join(quoted)}]"


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


def test_rows_sorted_by_token_id():
    """Every partition file's rows are ascending by token_id."""
    out = _output_dir()
    files = _all_data_files(out)
    if not files:
        pytest.skip("no data.parquet files found")

    offenders: list[str] = []
    for f in files:
        table = pq.read_table(f, columns=_SORT_KEY)
        if table.num_rows < 2:
            continue
        token_ids = table.column("token_id").to_pylist()
        if token_ids != sorted(token_ids):
            offenders.append(str(f.relative_to(out)))
            if len(offenders) >= 10:
                break
    assert not offenders, f"partitions not sorted by token_id: {offenders}"


def test_grain_key_is_globally_unique():
    """Every 4-tuple (collateral_token, parent_collection_id, condition_id, index_set)
    appears exactly once across the entire dataset.
    """
    out = _output_dir()
    files = _all_data_files(out)
    if not files:
        pytest.skip("no data.parquet files found")

    # Use the exact landed files, not a glob, so temporary artifacts are never scanned.
    file_list = _duckdb_file_list(files)
    summary = duckdb.query(f"""
        WITH grouped AS (
            SELECT
                collateral_token,
                parent_collection_id,
                condition_id,
                index_set,
                COUNT(*) AS c
            FROM read_parquet({file_list})
            GROUP BY 1, 2, 3, 4
        )
        SELECT
            SUM(c) AS total_rows,
            COUNT(*) AS distinct_tuples,
            SUM(c) - COUNT(*) AS extra_rows,
            SUM(CASE WHEN c > 1 THEN 1 ELSE 0 END) AS duplicated_keys
        FROM grouped
    """).to_df()

    total = int(summary["total_rows"][0])
    distinct = int(summary["distinct_tuples"][0])
    extra_rows = int(summary["extra_rows"][0])
    duplicated_keys = int(summary["duplicated_keys"][0])

    if total != distinct:
        offenders = duckdb.query(f"""
            WITH per_file AS (
                SELECT
                    filename,
                    collateral_token,
                    parent_collection_id,
                    condition_id,
                    index_set,
                    COUNT(*) AS c
                FROM read_parquet({file_list}, filename=true)
                GROUP BY 1, 2, 3, 4, 5
            )
            SELECT
                regexp_replace(filename, '^.*/derived_data/token_id_map_v1/', '') AS partition_file,
                SUM(c - 1) AS extra_rows,
                COUNT(*) FILTER (WHERE c > 1) AS duplicated_keys
            FROM per_file
            WHERE c > 1
            GROUP BY 1
            ORDER BY 1
            LIMIT 10
        """).to_df()
        offender_rows = [
            f"{row.partition_file} (extra_rows={int(row.extra_rows)}, duplicated_keys={int(row.duplicated_keys)})"
            for row in offenders.itertuples(index=False)
        ]
        assert False, (
            f"grain key is not globally unique across dataset: "
            f"{total} total rows, {distinct} distinct tuples, "
            f"extra_rows={extra_rows}, duplicated_keys={duplicated_keys}. "
            f"first offending partition files: {offender_rows}"
        )


def test_parent_collection_id_is_zero32_everywhere():
    """Every row has parent_collection_id = ZERO32."""
    out = _output_dir()
    files = _all_data_files(out)
    if not files:
        pytest.skip("no data.parquet files found")

    file_list = _duckdb_file_list(files)
    bad = duckdb.query(f"""
        SELECT COUNT(*) AS c
        FROM read_parquet({file_list})
        WHERE parent_collection_id <> unhex('{_ZERO32_HEX}')
    """).to_df()
    assert int(bad["c"][0]) == 0, (
        f"{int(bad['c'][0])} rows have parent_collection_id != ZERO32"
    )


def test_index_set_is_strictly_positive_everywhere():
    """Every row has index_set > 0."""
    out = _output_dir()
    files = _all_data_files(out)
    if not files:
        pytest.skip("no data.parquet files found")

    file_list = _duckdb_file_list(files)
    bad = duckdb.query(f"""
        SELECT COUNT(*) AS c
        FROM read_parquet({file_list})
        WHERE index_set <= 0
    """).to_df()
    assert int(bad["c"][0]) == 0, (
        f"{int(bad['c'][0])} rows have index_set <= 0"
    )


def test_token_id_is_globally_unique():
    """Every token_id appears exactly once across the entire dataset."""
    out = _output_dir()
    files = _all_data_files(out)
    if not files:
        pytest.skip("no data.parquet files found")

    file_list = _duckdb_file_list(files)
    summary = duckdb.query(f"""
        WITH grouped AS (
            SELECT token_id, COUNT(*) AS c
            FROM read_parquet({file_list})
            GROUP BY 1
        )
        SELECT
            SUM(c) AS total_rows,
            COUNT(*) AS distinct_token_ids,
            SUM(c) - COUNT(*) AS extra_rows,
            SUM(CASE WHEN c > 1 THEN 1 ELSE 0 END) AS duplicated_token_ids
        FROM grouped
    """).to_df()

    total = int(summary["total_rows"][0])
    distinct = int(summary["distinct_token_ids"][0])
    extra_rows = int(summary["extra_rows"][0])
    duplicated = int(summary["duplicated_token_ids"][0])

    if total != distinct:
        offenders = duckdb.query(f"""
            WITH per_file AS (
                SELECT
                    filename,
                    token_id,
                    COUNT(*) AS c
                FROM read_parquet({file_list}, filename=true)
                GROUP BY 1, 2
            )
            SELECT
                regexp_replace(filename, '^.*/derived_data/token_id_map_v1/', '') AS partition_file,
                SUM(c - 1) AS extra_rows,
                COUNT(*) FILTER (WHERE c > 1) AS duplicated_token_ids
            FROM per_file
            WHERE c > 1
            GROUP BY 1
            ORDER BY 1
            LIMIT 10
        """).to_df()
        offender_rows = [
            f"{row.partition_file} (extra_rows={int(row.extra_rows)}, duplicated_token_ids={int(row.duplicated_token_ids)})"
            for row in offenders.itertuples(index=False)
        ]
        assert False, (
            f"token_id is not globally unique across dataset: "
            f"{total} total rows, {distinct} distinct token_ids, "
            f"extra_rows={extra_rows}, duplicated_token_ids={duplicated}. "
            f"first offending partition files: {offender_rows}"
        )


def test_market_id_mask_invariant():
    """Every non-null market_id is 32 bytes with its final byte cleared.

    NegRisk market ids are the question id masked with ~0xFF
    (NegRiskIdLib.getMarketId), so the low byte is always zero. NULL market_id
    (standard binary / UMA conditions) is allowed.
    """
    out = _output_dir()
    files = _all_data_files(out)
    if not files:
        pytest.skip("no data.parquet files found")

    file_list = _duckdb_file_list(files)
    bad = duckdb.query(f"""
        SELECT COUNT(*) AS c
        FROM read_parquet({file_list})
        WHERE market_id IS NOT NULL
          AND (octet_length(market_id) <> 32 OR right(lower(hex(market_id)), 2) <> '00')
    """).to_df()
    assert int(bad["c"][0]) == 0, (
        f"{int(bad['c'][0])} rows have a market_id that is not 32 bytes with a zero "
        f"final byte (NegRiskIdLib mask invariant)"
    )


def test_market_id_functionally_determined_by_condition():
    """Each condition_id maps to a single market_id value across the dataset."""
    out = _output_dir()
    files = _all_data_files(out)
    if not files:
        pytest.skip("no data.parquet files found")

    file_list = _duckdb_file_list(files)
    offenders = duckdb.query(f"""
        SELECT lower(hex(condition_id)) AS condition_id,
               COUNT(DISTINCT COALESCE(lower(hex(market_id)), 'NULL')) AS distinct_market_ids
        FROM read_parquet({file_list})
        GROUP BY 1
        HAVING COUNT(DISTINCT COALESCE(lower(hex(market_id)), 'NULL')) > 1
        ORDER BY 1
        LIMIT 10
    """).to_df()
    assert offenders.empty, (
        f"condition_id maps to multiple market_id values: "
        f"{offenders.to_dict(orient='records')}"
    )


def test_market_id_is_null_iff_condition_is_non_negrisk():
    """market_id is populated if and only if condition oracle is NegRiskAdapter."""
    out = _output_dir()
    raw = _raw_dir()
    files = _all_data_files(out)
    if not files:
        pytest.skip("no data.parquet files found")

    file_list = _duckdb_file_list(files)
    prep_glob = str(raw / "ConditionalTokens" / "condition_preparation" / "**" / "data.parquet")

    inconsistent_oracle = duckdb.query(f"""
        SELECT COUNT(*) AS c
        FROM (
            SELECT
                condition_id,
                COUNT(DISTINCT lower(hex(oracle))) AS oracle_count
            FROM read_parquet('{prep_glob}')
            GROUP BY 1
        )
        WHERE oracle_count > 1
    """).to_df()
    assert int(inconsistent_oracle["c"][0]) == 0, (
        f"{int(inconsistent_oracle['c'][0])} condition_id values map to multiple oracle addresses"
    )

    summary = duckdb.query(f"""
        WITH prep AS (
            SELECT
                condition_id,
                lower(hex(any_value(oracle))) AS oracle_hex
            FROM read_parquet('{prep_glob}')
            GROUP BY 1
        ),
        joined AS (
            SELECT m.condition_id, m.market_id, p.oracle_hex
            FROM read_parquet({file_list}) AS m
            LEFT JOIN prep AS p USING (condition_id)
        )
        SELECT
            COUNT(*) FILTER (WHERE oracle_hex IS NULL) AS missing_preparation_rows,
            COUNT(*) FILTER (
                WHERE (oracle_hex = '{_NEGRISK_ADAPTER_HEX}' AND market_id IS NULL)
                   OR (oracle_hex <> '{_NEGRISK_ADAPTER_HEX}' AND market_id IS NOT NULL)
            ) AS mismatched_market_id_rows
        FROM joined
    """).to_df()

    missing_prep = int(summary["missing_preparation_rows"][0])
    mismatched = int(summary["mismatched_market_id_rows"][0])

    assert missing_prep <= KNOWN_ORPHAN_LIMIT, (
        f"{missing_prep} map rows reference condition_id values missing from condition_preparation "
        f"(limit={KNOWN_ORPHAN_LIMIT})"
    )
    assert mismatched == 0, (
        f"{mismatched} map rows violate the market_id nullability invariant by oracle"
    )
