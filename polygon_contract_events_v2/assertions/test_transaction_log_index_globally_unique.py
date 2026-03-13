"""
Assert: (transaction_hash, log_index) is unique within every event table.
"""
import os
from helpers import RAW, EVENT_LOCATIONS


def test_transaction_log_index_globally_unique(con):
    total_dupes = 0
    tables_checked = []

    for event, paths in sorted(EVENT_LOCATIONS.items()):
        for rel_path in paths:
            full = os.path.join(RAW, rel_path)
            partitions = sorted(
                e.name for e in os.scandir(full)
                if e.is_dir() and e.name.startswith("1M=")
            )
            table_dupes = 0
            for part in partitions:
                path = f"read_parquet('{RAW}/{rel_path}/{part}/**/*.parquet')"
                n = con.execute(f"""
                    SELECT COUNT(*) FROM (
                        SELECT transaction_hash, log_index
                        FROM {path}
                        GROUP BY transaction_hash, log_index
                        HAVING COUNT(*) > 1
                    )
                """).fetchone()[0]
                table_dupes += n
            total_dupes += table_dupes
            tables_checked.append(rel_path)

    assert total_dupes == 0, (
        f"{total_dupes} duplicate (transaction_hash, log_index) pairs "
        f"across {len(tables_checked)} tables"
    )
