#!/usr/bin/env python3
"""
Export SQLite event tables to v2 partitioned Parquet files.

Reads from the RPC scraper's SQLite database and writes Parquet files
in v2 format: all tables use 10K-block partitions nested inside 1M-block
directories, organized by contract, with identifiers as binary BLOBs.
Only blocks >= 33,605,403 are exported.

Mixed SQLite tables (e.g. order_filled contains rows from both
CTFExchange and NegRiskCtfExchange) are split by contract_address
and written to separate contract directories.

USAGE
    python3 export_parquet.py                         # export all pending
    python3 export_parquet.py --dry-run               # show what would be exported
    python3 export_parquet.py --table order_filled    # export one SQLite table
    python3 export_parquet.py --delete                # delete from SQLite after export
    python3 export_parquet.py --limit 5               # export at most 5 partitions per target

ENVIRONMENT
    RPC_DB_PATH         path to SQLite database (same as scraper)
    POLYGON_CONTRACT_EVENTS_V2_DIR  output directory for v2 Parquet files

Directory layout:
    <output>/<contract>/<event>/1M=<N>/10K=<N>/data.parquet
    <output>/<contract>/<event>/1M=<N>/10K=<N>/metadata.json

REQUIREMENTS
    pip install pyarrow python-dotenv
"""

import argparse
import hashlib
import json
import os
import shutil
import sqlite3
import subprocess
import sys
import tempfile
import time

from pathlib import Path

import pyarrow as pa
import pyarrow.parquet as pq
from dotenv import load_dotenv

# ---------------------------------------------------------------------------
# Configuration
# ---------------------------------------------------------------------------

_script_dir = os.path.dirname(os.path.abspath(__file__))
_project_root = Path(__file__).resolve().parent.parent

load_dotenv(_project_root / ".env")

DB_PATH = os.environ.get("RPC_DB_PATH", "")
if not DB_PATH:
    sys.exit("RPC_DB_PATH not set. Add it to .env.")
if not os.path.isabs(DB_PATH):
    DB_PATH = os.path.join(_script_dir, DB_PATH)

OUTPUT_DIR = os.environ.get("POLYGON_CONTRACT_EVENTS_V2_DIR", "")
if not OUTPUT_DIR:
    sys.exit("POLYGON_CONTRACT_EVENTS_V2_DIR not set. Add it to .env.")

# ---------------------------------------------------------------------------
# Import v2 schemas
# ---------------------------------------------------------------------------

sys.path.insert(0, str(Path(__file__).resolve().parent))
from v2_schemas import (
    START_BLOCK,
    CONTRACTS,
    SQLITE_TO_V2,
    is_mixed_sqlite_table,
    all_sqlite_table_names,
    get_v2_schema,
    get_arrow_schema,
    get_v2_column_names,
    _10K,
    _1M,
)

# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------


def format_duration(seconds):
    if seconds < 0:
        return "???"
    if seconds < 60:
        return f"{seconds:.0f}s"
    if seconds < 3600:
        m, s = divmod(int(seconds), 60)
        return f"{m}m{s:02d}s"
    h, remainder = divmod(int(seconds), 3600)
    m, s = divmod(remainder, 60)
    return f"{h}h{m:02d}m"


def format_number(n):
    return f"{n:,}"


def get_max_complete_block(conn):
    """Return the highest contiguous fully-scraped block."""
    rows = conn.execute(
        "SELECT from_block, to_block FROM completed_ranges ORDER BY from_block"
    ).fetchall()
    if not rows:
        return -1
    frontier = -1
    for from_b, to_b in rows:
        if from_b > frontier + 1:
            break
        frontier = max(frontier, to_b)
    return frontier


def dest_path_for_partition(output_dir, contract, event, block_start):
    """Return directory for a v2 10K partition."""
    m_start = (block_start // _1M) * _1M
    return os.path.join(output_dir, contract, event,
                        f"1M={m_start}", f"10K={block_start}")


def find_existing_partitions(output_dir, contract, event):
    """Return set of 10K block_start values already exported."""
    existing = set()
    event_dir = os.path.join(output_dir, contract, event)
    if not os.path.isdir(event_dir):
        return existing
    for m_entry in os.listdir(event_dir):
        if not m_entry.startswith("1M="):
            continue
        m_path = os.path.join(event_dir, m_entry)
        if not os.path.isdir(m_path):
            continue
        for k_entry in os.listdir(m_path):
            if not k_entry.startswith("10K="):
                continue
            k_path = os.path.join(m_path, k_entry)
            if os.path.isfile(os.path.join(k_path, "data.parquet")):
                try:
                    existing.add(int(k_entry.split("=")[1]))
                except (ValueError, IndexError):
                    pass
    return existing


def cleanup_orphaned_temp_dirs(output_dir):
    """Remove .export_* temp directories left behind by interrupted exports.

    The export_partition function creates temporary directories with names like
    .export_ContractName_event_name_blockStart_random inside 1M partition
    directories. If the process is killed, these persist and their data.parquet
    files get picked up by downstream glob queries, causing duplicate rows.
    """
    removed = 0
    if not os.path.isdir(output_dir):
        return removed
    for contract_entry in os.scandir(output_dir):
        if not contract_entry.is_dir():
            continue
        for event_entry in os.scandir(contract_entry.path):
            if not event_entry.is_dir():
                continue
            for m_entry in os.scandir(event_entry.path):
                if not m_entry.is_dir() or not m_entry.name.startswith("1M="):
                    continue
                for child in os.scandir(m_entry.path):
                    if child.is_dir() and child.name.startswith(".export_"):
                        shutil.rmtree(child.path)
                        removed += 1
    return removed


def get_sqlite_column_names(conn, table):
    """Return list of column names for a SQLite table."""
    rows = conn.execute(f"PRAGMA table_info({table})").fetchall()
    return [r[1] for r in rows]


def _sha256_file(path):
    h = hashlib.sha256()
    with open(path, "rb") as f:
        for chunk in iter(lambda: f.read(1 << 20), b""):
            h.update(chunk)
    return f"sha256:{h.hexdigest()}"


def _git_commit():
    try:
        return subprocess.check_output(
            ["git", "rev-parse", "--short", "HEAD"],
            cwd=os.path.dirname(os.path.abspath(__file__)),
            stderr=subprocess.DEVNULL,
        ).decode().strip()
    except Exception:
        return "unknown"


_GIT_COMMIT = _git_commit()


def export_partition(conn, sqlite_table, contract, event, block_start,
                     block_end, output_dir, dry_run=False):
    """Export one 10K partition from SQLite to Parquet.

    Reads raw data from SQLite (hex strings, int64), converts to typed
    Parquet columns (binary BLOBs, uint32), and writes a Parquet file.

    For mixed tables (e.g. order_filled shared by CTFExchange and
    NegRiskCtfExchange), filters rows by contract_address.

    Returns (row_count, file_size_bytes).
    """
    dest_dir = dest_path_for_partition(output_dir, contract, event, block_start)
    v2_schema_spec = get_v2_schema(contract, event)
    arrow_schema = get_arrow_schema(contract, event)

    # Build the SELECT — only query columns present in the schema
    # SQLite column names match the schema column names (minus excluded ones)
    v2_col_names = get_v2_column_names(contract, event)
    sqlite_cols = get_sqlite_column_names(conn, sqlite_table)

    # Map v2 column names to their position in the SQLite result
    select_cols = []
    for col_name in v2_col_names:
        if col_name in sqlite_cols:
            select_cols.append(col_name)
        else:
            select_cols.append(None)  # column not in SQLite, will be null

    # Build SQL select list (only existing columns)
    sql_cols = [c for c in select_cols if c is not None]
    col_list = ", ".join(sql_cols)

    # For mixed tables, filter by contract_address
    where_clause = "block_number >= ? AND block_number <= ?"
    params = [block_start, block_end]
    if is_mixed_sqlite_table(sqlite_table):
        where_clause += " AND contract_address = ?"
        params.append(CONTRACTS[contract].lower())

    if dry_run:
        count = conn.execute(
            f"SELECT COUNT(*) FROM {sqlite_table} "
            f"WHERE {where_clause}",
            params,
        ).fetchone()[0]
        return count, 0

    # Query data
    cursor = conn.execute(
        f"SELECT {col_list} FROM {sqlite_table} "
        f"WHERE {where_clause} "
        f"ORDER BY block_number, transaction_index, log_index",
        params,
    )
    rows = cursor.fetchall()

    # Build v2 Arrow table
    if rows:
        # Create a dict mapping sql_col_name → column_data
        sql_col_map = {}
        col_data = list(zip(*rows))
        for i, col_name in enumerate(sql_cols):
            sql_col_map[col_name] = col_data[i]

        # Convert each v2 column
        arrays = []
        for col_name, pa_type, converter in v2_schema_spec:
            if col_name in sql_col_map:
                raw_values = sql_col_map[col_name]
                converted = [converter(v) for v in raw_values]
            else:
                converted = [None] * len(rows)
            arrays.append(pa.array(converted, type=pa_type))
    else:
        arrays = [pa.array([], type=pa_type) for _, pa_type, _ in v2_schema_spec]

    arrow_table = pa.table(
        {spec[0]: arr for spec, arr in zip(v2_schema_spec, arrays)},
        schema=arrow_schema,
    )

    # Write atomically
    parent = os.path.dirname(dest_dir)
    os.makedirs(parent, exist_ok=True)
    tmp_dir = tempfile.mkdtemp(
        prefix=f".export_{contract}_{event}_{block_start}_",
        dir=parent,
    )
    try:
        tmp_parquet = os.path.join(tmp_dir, "data.parquet")
        tmp_meta = os.path.join(tmp_dir, "metadata.json")

        pq.write_table(
            arrow_table,
            tmp_parquet,
            compression="zstd",
            use_dictionary=True,
            write_statistics=True,
        )

        file_size = os.path.getsize(tmp_parquet)
        content_hash = _sha256_file(tmp_parquet)

        m_start = (block_start // _1M) * _1M
        partition_str = f"1M={m_start}/10K={block_start}"

        meta = {
            "contract": contract,
            "event": event,
            "version": "v2",
            "partition": partition_str,
            "created_at": time.strftime("%Y-%m-%dT%H:%M:%SZ", time.gmtime()),
            "source_script": os.path.basename(__file__),
            "git_commit": _GIT_COMMIT,
            "row_count": len(rows),
            "file_size_bytes": file_size,
            "content_hash": content_hash,
            "parameters": {
                "block_start": block_start,
                "block_end": block_end,
                "partition_size": _10K,
            },
            "status": "empty" if not rows else "complete",
        }
        with open(tmp_meta, "w") as f:
            json.dump(meta, f, indent=2)

        if os.path.exists(dest_dir):
            shutil.rmtree(tmp_dir)
            return len(rows), file_size
        shutil.move(tmp_dir, dest_dir)
        return len(rows), file_size

    except Exception:
        if os.path.exists(tmp_dir):
            shutil.rmtree(tmp_dir)
        raise


def verify_parquet(path):
    table = pq.read_table(path)
    return len(table)


def delete_partition_from_sqlite(conn, table, block_start, block_end):
    conn.execute(
        f"DELETE FROM {table} WHERE block_number >= ? AND block_number <= ?",
        (block_start, block_end),
    )
    conn.commit()


def _delete_already_exported(conn, already_exported, output_dir):
    """Delete SQLite rows for partitions already in v2 Parquet."""
    total = len(already_exported)
    print(f"\nCleaning up {total:,} already-exported partitions from SQLite...")
    deleted = 0
    skipped = 0
    pending = 0
    BATCH = 10

    for i, (sqlite_table, contract, event, block_start, block_end) in enumerate(already_exported, 1):
        pct = i / total * 100
        label = f"{contract}/{event}"
        print(f"  [{i:,}/{total:,} {pct:.0f}%] {label} {block_start:,}...", end=" ", flush=True)

        dest_dir = dest_path_for_partition(output_dir, contract, event, block_start)
        parquet_path = os.path.join(dest_dir, "data.parquet")
        if not os.path.isfile(parquet_path):
            print("SKIP (Parquet missing)")
            skipped += 1
        else:
            if is_mixed_sqlite_table(sqlite_table):
                conn.execute(
                    f"DELETE FROM {sqlite_table} "
                    f"WHERE block_number >= ? AND block_number <= ? "
                    f"AND contract_address = ?",
                    (block_start, block_end, CONTRACTS[contract].lower()),
                )
            else:
                conn.execute(
                    f"DELETE FROM {sqlite_table} "
                    f"WHERE block_number >= ? AND block_number <= ?",
                    (block_start, block_end),
                )
            deleted += 1
            pending += 1
            print("ok")

        if pending >= BATCH:
            print("  committing batch...", end=" ", flush=True)
            conn.commit()
            pending = 0
            print("done")

    if pending:
        print("  committing final batch...", end=" ", flush=True)
        conn.commit()
        print("done")

    print(f"  Done — deleted {deleted:,} partitions from SQLite ({skipped} skipped).")


# ---------------------------------------------------------------------------
# Progress display
# ---------------------------------------------------------------------------

class ProgressTracker:
    def __init__(self, total_partitions):
        self.total = total_partitions
        self.done = 0
        self.total_rows = 0
        self.total_bytes = 0
        self.start_time = time.monotonic()
        self.table_start_time = None
        self.current_table = None
        self.table_done = 0
        self.table_total = 0
        self.per_target = {}  # label -> {partitions, rows, bytes}

    def start_table(self, table, num_partitions):
        self.current_table = table
        self.table_start_time = time.monotonic()
        self.table_done = 0
        self.table_total = num_partitions
        if table not in self.per_target:
            self.per_target[table] = {"partitions": 0, "rows": 0, "bytes": 0}

    def record(self, rows, file_bytes):
        self.done += 1
        self.table_done += 1
        self.total_rows += rows
        self.total_bytes += file_bytes
        if self.current_table:
            t = self.per_target[self.current_table]
            t["partitions"] += 1
            t["rows"] += rows
            t["bytes"] += file_bytes

    def elapsed(self):
        return time.monotonic() - self.start_time

    def overall_eta(self):
        if self.done == 0:
            return -1
        rate = self.done / self.elapsed()
        remaining = self.total - self.done
        return remaining / rate

    def status_line(self, rows, file_bytes, block_start, elapsed_s):
        pct = (self.done / self.total * 100) if self.total > 0 else 0
        overall_eta = self.overall_eta()

        size_str = ""
        if file_bytes > 0:
            if file_bytes >= 1_048_576:
                size_str = f"{file_bytes / 1_048_576:.1f}MB"
            else:
                size_str = f"{file_bytes / 1024:.0f}KB"

        parts = [
            f"[{self.done}/{self.total} {pct:.0f}%]",
            f"blocks {block_start:,}",
        ]
        if rows > 0:
            row_str = f"{format_number(rows)} rows"
            if size_str:
                row_str += f" {size_str}"
            parts.append(row_str)
        else:
            parts.append("empty")

        parts.append(f"({elapsed_s:.1f}s)")

        if overall_eta >= 0 and self.done < self.total:
            parts.append(f"ETA {format_duration(overall_eta)}")

        return "  ".join(parts)


# ---------------------------------------------------------------------------
# Main export logic
# ---------------------------------------------------------------------------

def run_export(args):
    if not os.path.isfile(DB_PATH):
        sys.exit(f"Database not found: {DB_PATH}")

    # Clean up any temp directories left behind by interrupted exports
    orphans = cleanup_orphaned_temp_dirs(OUTPUT_DIR)
    if orphans:
        print(f"Cleaned up {orphans} orphaned temp director{'y' if orphans == 1 else 'ies'}")

    conn = sqlite3.connect(DB_PATH, timeout=30)
    conn.execute("PRAGMA journal_mode=WAL")

    max_complete = get_max_complete_block(conn)
    if max_complete < 0:
        sys.exit("No completed ranges found in database. Nothing to export.")

    print(f"Database: {DB_PATH}")
    print(f"Output:   {OUTPUT_DIR}")
    print(f"Start block: {START_BLOCK:,}")
    print(f"Fully scraped through block {max_complete:,}")
    print()

    # Check which SQLite tables exist
    sqlite_tables_on_disk = {
        r[0] for r in conn.execute(
            "SELECT name FROM sqlite_master WHERE type='table'"
        ).fetchall()
    }

    # Determine which SQLite tables to process
    if args.table:
        sqlite_tables = []
        for t in args.table:
            if t not in SQLITE_TO_V2:
                sys.exit(f"Unknown table: {t}\nValid tables: {sorted(SQLITE_TO_V2)}")
            sqlite_tables.append(t)
    else:
        sqlite_tables = all_sqlite_table_names()

    # Build work list: each item is (sqlite_table, contract, event, block_start, block_end)
    work = []
    already_exported = []

    for sqlite_table in sqlite_tables:
        if sqlite_table not in sqlite_tables_on_disk:
            continue

        row = conn.execute(
            f"SELECT"
            f" (SELECT MIN(block_number) FROM {sqlite_table}),"
            f" (SELECT MAX(block_number) FROM {sqlite_table})"
        ).fetchone()
        if row[0] is None:
            continue
        min_block = row[0]

        effective_min = max(min_block, START_BLOCK)
        first_partition = (effective_min // _10K) * _10K
        last_exportable = (max_complete // _10K) * _10K
        if last_exportable + _10K - 1 > max_complete:
            last_exportable -= _10K

        targets = SQLITE_TO_V2[sqlite_table]

        for contract, event in targets:
            label = f"{contract}/{event}"
            print(f"  Scanning {label}...", end="", flush=True)
            existing = find_existing_partitions(OUTPUT_DIR, contract, event)

            block_start = first_partition
            count = 0
            while block_start <= last_exportable:
                block_end = block_start + _10K - 1
                if block_start not in existing:
                    work.append((sqlite_table, contract, event, block_start, block_end))
                    count += 1
                    if args.limit and count >= args.limit:
                        break
                else:
                    already_exported.append((sqlite_table, contract, event, block_start, block_end))
                block_start += _10K

            if count > 0:
                print(f" {count} new ({len(existing)} existing)")
            else:
                print(f" up to date ({len(existing)} partitions)")

    if not work:
        print("Nothing to export — all partitions are up to date.")
        if args.delete and already_exported:
            _delete_already_exported(conn, already_exported, OUTPUT_DIR)
            print("Checkpointing WAL...", end=" ", flush=True)
            conn.execute("PRAGMA wal_checkpoint(TRUNCATE)")
            print("done")
            print("Running VACUUM to reclaim space...", end=" ", flush=True)
            conn.execute("VACUUM")
            print("done")
        conn.close()
        return

    # Group work by target for display
    work_by_target = {}
    for sqlite_table, contract, event, bs, be in work:
        key = f"{contract}/{event}"
        work_by_target.setdefault(key, []).append((sqlite_table, bs, be))

    print(f"\nPartitions to export: {len(work)}")
    for key in sorted(work_by_target):
        parts = work_by_target[key]
        print(f"  {key}: {len(parts)} partitions (10K)")
    print()

    if args.dry_run:
        print("Dry run — counting rows per partition:\n")
        total_rows = 0
        for sqlite_table, contract, event, block_start, block_end in work:
            rows, _ = export_partition(
                conn, sqlite_table, contract, event,
                block_start, block_end, OUTPUT_DIR, dry_run=True,
            )
            dest = dest_path_for_partition(OUTPUT_DIR, contract, event, block_start)
            rel = os.path.relpath(dest, OUTPUT_DIR)
            print(f"  {rel}: {format_number(rows)} rows")
            total_rows += rows
        print(f"\nTotal: {format_number(total_rows)} rows across {len(work)} partitions")
        conn.close()
        return

    # Run the export
    os.makedirs(OUTPUT_DIR, exist_ok=True)
    tracker = ProgressTracker(len(work))

    current_label = None

    for sqlite_table, contract, event, block_start, block_end in work:
        label = f"{contract}/{event}"
        if label != current_label:
            current_label = label
            section_count = sum(1 for _, c, e, _, _ in work
                                if f"{c}/{e}" == label)
            tracker.start_table(label, section_count)
            print(f"\n{'='*60}")
            print(f"  {label} — {section_count} partitions (10K)")
            print(f"{'='*60}")

        t0 = time.monotonic()
        rows, file_bytes = export_partition(
            conn, sqlite_table, contract, event,
            block_start, block_end, OUTPUT_DIR,
        )
        elapsed = time.monotonic() - t0

        tracker.record(rows, file_bytes)

        if args.delete and rows > 0:
            dest_dir = dest_path_for_partition(OUTPUT_DIR, contract, event, block_start)
            parquet_path = os.path.join(dest_dir, "data.parquet")
            try:
                verify_rows = verify_parquet(parquet_path)
                if verify_rows != rows:
                    print(
                        f"\n  WARNING: verification mismatch for {label} "
                        f"blocks {block_start}-{block_end}: "
                        f"wrote {rows} but read back {verify_rows}. "
                        f"Skipping delete."
                    )
                else:
                    if is_mixed_sqlite_table(sqlite_table):
                        conn.execute(
                            f"DELETE FROM {sqlite_table} "
                            f"WHERE block_number >= ? AND block_number <= ? "
                            f"AND contract_address = ?",
                            (block_start, block_end,
                             CONTRACTS[contract].lower()),
                        )
                        conn.commit()
                    else:
                        delete_partition_from_sqlite(
                            conn, sqlite_table, block_start, block_end,
                        )
            except Exception as e:
                print(
                    f"\n  WARNING: could not verify {parquet_path}: {e}. "
                    f"Skipping delete."
                )

        print(f"  {tracker.status_line(rows, file_bytes, block_start, elapsed)}")

    # Summary
    print(f"\n{'='*60}")
    print(f"  Export complete")
    print(f"{'='*60}")
    for label in sorted(tracker.per_target):
        t = tracker.per_target[label]
        size_str = ""
        if t["bytes"] >= 1_048_576:
            size_str = f"  {t['bytes'] / 1_048_576:.1f}MB"
        elif t["bytes"] > 0:
            size_str = f"  {t['bytes'] / 1024:.0f}KB"
        print(f"  {label}: {t['partitions']} partitions, {format_number(t['rows'])} rows{size_str}")
    print(f"  {'─'*56}")
    print(f"  Total:  {tracker.done} partitions, {format_number(tracker.total_rows)} rows")
    if tracker.total_bytes > 0:
        print(f"  Size:   {tracker.total_bytes / 1_073_741_824:.2f} GB")
    print(f"  Elapsed:     {format_duration(tracker.elapsed())}")
    if tracker.elapsed() > 0:
        rate = tracker.total_rows / tracker.elapsed()
        print(f"  Throughput:  {format_number(int(rate))} rows/s")
    print()

    if args.delete and already_exported:
        _delete_already_exported(conn, already_exported, OUTPUT_DIR)

    print("Checkpointing WAL...", end=" ", flush=True)
    conn.execute("PRAGMA wal_checkpoint(TRUNCATE)")
    print("done")

    if args.delete:
        print("Running VACUUM to reclaim space...", end=" ", flush=True)
        conn.execute("VACUUM")
        print("done")

    conn.close()


def main():
    parser = argparse.ArgumentParser(
        description="Export SQLite event tables to v2 partitioned Parquet files"
    )
    parser.add_argument("--dry-run", action="store_true",
                        help="Show what would be exported without writing files")
    parser.add_argument("--table", action="append",
                        help="Export only specific SQLite table(s) (can be repeated)")
    parser.add_argument("--delete", action="store_true",
                        help="Delete exported rows from SQLite after verification")
    parser.add_argument("--limit", type=int, default=0,
                        help="Max partitions to export per target (0 = unlimited)")
    args = parser.parse_args()
    run_export(args)


if __name__ == "__main__":
    main()
