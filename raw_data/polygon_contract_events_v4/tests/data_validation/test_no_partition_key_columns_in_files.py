"""
Assert: no parquet file contains a column named ``1M`` or ``10K``.

The data dictionary's Partitioning section states that the partition
values are hive-style directory keys, not data, and must not appear as
columns inside the parquet file. This test enforces that.

Scope: every `*/*/1M=*/10K=*/data.parquet` file on disk.

Implementation: read column names from each file's footer via
``parquet_metadata``. No row data is scanned.
"""
from helpers import RAW

_FORBIDDEN_COLUMNS = ("1M", "10K")


def test_no_partition_key_columns_in_files(con):
    glob_pattern = f"{RAW}/*/*/1M=*/10K=*/data.parquet"
    n_files = con.execute(f"""
        SELECT COUNT(DISTINCT file_name)
        FROM parquet_metadata('{glob_pattern}')
    """).fetchone()[0]
    assert n_files and n_files > 0, "no parquet files matched"

    forbidden_list = ", ".join(f"'{c}'" for c in _FORBIDDEN_COLUMNS)
    offenders = con.execute(f"""
        SELECT file_name, path_in_schema
        FROM parquet_metadata('{glob_pattern}')
        WHERE path_in_schema IN ({forbidden_list})
        GROUP BY file_name, path_in_schema
        ORDER BY file_name, path_in_schema
        LIMIT 20
    """).fetchall()

    assert not offenders, (
        f"{len(offenders)} parquet file(s) contain a forbidden partition-key "
        f"column ('1M' or '10K') inside the file; partition values must only "
        f"appear in the directory path. First offenders: {offenders[:5]}"
    )
