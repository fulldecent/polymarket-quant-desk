"""
Assert: no event-table Parquet file has a ``transaction_hash`` column.

v4 dropped the hash. Grain is ``(block_number, transaction_index, log_index)``.
"""
from helpers import iter_partitions_with_files


def test_no_transaction_hash_column(con):
    partitions = iter_partitions_with_files()
    assert partitions, "no parquet partitions found"

    offenders: list[str] = []
    sampled = list(partitions[:2]) + list(partitions[-2:])
    seen: set[str] = set()
    for _block_start, files in sampled:
        for f in files:
            if f in seen:
                continue
            seen.add(f)
            cols = [
                row[0]
                for row in con.execute(
                    f"DESCRIBE SELECT * FROM read_parquet('{f}') LIMIT 0"
                ).fetchall()
            ]
            if "transaction_hash" in cols:
                offenders.append(f)
            if len(offenders) >= 10:
                break
        if len(offenders) >= 10:
            break

    assert not offenders, (
        f"{len(offenders)} parquet file(s) still have a transaction_hash "
        f"column: {offenders[:5]}"
    )
