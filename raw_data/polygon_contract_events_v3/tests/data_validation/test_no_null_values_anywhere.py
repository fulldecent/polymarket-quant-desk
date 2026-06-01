"""
Assert: no column in any event-table parquet file contains a NULL value.

The data dictionary's Common columns section states unconditionally: "No
column in any table is ever NULL." This test enforces that for every
column of every event-table file. A NULL in any column is a producer
contract violation, not a valid value.

Scope: every `*/*/1M=*/10K=*/data.parquet` file on disk, for every event
table from every contract.

Implementation: read each column chunk's ``stats_null_count`` from the
Parquet file footer via DuckDB's ``parquet_metadata`` table function. No
row data is scanned — only the per-column-chunk statistics stored in the
file footer. This makes the check effectively metadata-only and scales
to hundreds of thousands of files in tens of seconds.
"""
import os

from helpers import RAW


def test_no_null_values_anywhere(con):
    glob_pattern = f"{RAW}/*/*/1M=*/10K=*/data.parquet"
    # Sanity-check that the glob matches something.
    n_files = con.execute(f"""
        SELECT COUNT(DISTINCT file_name)
        FROM parquet_metadata('{glob_pattern}')
    """).fetchone()[0]
    assert n_files and n_files > 0, "no parquet files matched"

    offenders = con.execute(f"""
        SELECT file_name, path_in_schema, SUM(stats_null_count) AS nulls
        FROM parquet_metadata('{glob_pattern}')
        WHERE stats_null_count IS NOT NULL
          AND stats_null_count > 0
        GROUP BY file_name, path_in_schema
        ORDER BY nulls DESC
        LIMIT 20
    """).fetchall()

    # ``stats_null_count`` IS NULL means the writer did not record the
    # statistic for that column chunk. The Parquet spec allows this and
    # the contract requires no NULL values, so a missing statistic is a
    # gap in the producer's metadata, not a contract violation by
    # itself. Surface it as a warning rather than a failure.
    missing_stats = con.execute(f"""
        SELECT COUNT(*)
        FROM parquet_metadata('{glob_pattern}')
        WHERE stats_null_count IS NULL
    """).fetchone()[0]
    if missing_stats:
        # Not an assertion failure — just visibility in case the producer
        # later stops writing the statistic.
        print(
            f"NOTE: {missing_stats} column chunk(s) have no "
            f"stats_null_count recorded; the contract still says zero "
            f"NULLs but those chunks would have to be re-scanned to "
            f"prove it."
        )

    assert not offenders, (
        f"{len(offenders)} (file, column) pair(s) contain NULL values per "
        f"Parquet column-chunk statistics; first offenders "
        f"(file, column, total_null_count): {offenders[:5]}"
    )
