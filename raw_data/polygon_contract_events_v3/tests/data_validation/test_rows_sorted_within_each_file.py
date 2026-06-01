"""
Assert: each parquet file is sorted internally by
``(block_number, transaction_index, log_index)``.

The data dictionary's Physical sort order section states: "Each partition
is sorted internally by ``block_number, transaction_index, log_index``."
This test enforces that.

Scope: every `*/*/1M=*/10K=*/data.parquet` file on disk.

Implementation: a row is in order if the tuple at row ``i`` is
lexicographically less than or equal to the tuple at row ``i+1``. We
check this with a single DuckDB query per file that compares each row to
its successor via the window function ``LAG``. A file passes if every
adjacent pair is non-decreasing.
"""
from helpers import iter_partitions_with_files


def test_rows_sorted_within_each_file(con):
    con.execute("SET preserve_insertion_order = true")  # required for ROW_NUMBER preservation

    partitions = iter_partitions_with_files()
    assert partitions, "no parquet partitions found"

    offenders: list[tuple[str, int]] = []  # (file, count_of_out_of_order_pairs)

    for _block_start, files in partitions:
        for f in files:
            row = con.execute(f"""
                WITH numbered AS (
                    SELECT
                        ROW_NUMBER() OVER () AS rn,
                        block_number,
                        transaction_index,
                        log_index
                    FROM read_parquet('{f}')
                ),
                adjacent AS (
                    SELECT
                        rn,
                        block_number       AS bn,
                        transaction_index  AS ti,
                        log_index          AS li,
                        LAG(block_number)      OVER (ORDER BY rn) AS prev_bn,
                        LAG(transaction_index) OVER (ORDER BY rn) AS prev_ti,
                        LAG(log_index)         OVER (ORDER BY rn) AS prev_li
                    FROM numbered
                )
                SELECT COUNT(*) FROM adjacent
                WHERE prev_bn IS NOT NULL
                  AND (
                        (prev_bn, prev_ti, prev_li) > (bn, ti, li)
                      )
            """).fetchone()
            n = int(row[0]) if row else 0
            if n > 0:
                offenders.append((f, n))
                if len(offenders) >= 20:
                    break
        if len(offenders) >= 20:
            break

    assert not offenders, (
        f"{len(offenders)} parquet file(s) are not sorted by "
        f"(block_number, transaction_index, log_index); first offenders "
        f"(file, count_of_out_of_order_pairs): {offenders[:5]}"
    )
