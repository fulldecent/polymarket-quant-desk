"""
Write event data from SQLite to v2 partitioned Parquet files.

Pure library: takes data in, writes files out. No CLI, no config, no
env vars.

Directory layout:
    <root>/<contract>/<event>/1M=<N>/10K=<N>/data.parquet
    <root>/<contract>/<event>/1M=<N>/10K=<N>/metadata.json
"""

import hashlib
import json
import os
import shutil
import subprocess
import tempfile
import time

import pyarrow as pa
import pyarrow.parquet as pq

from .v2_schemas import (
    CONTRACTS,
    SQLITE_TO_V2,
    is_mixed_sqlite_table,
    get_v2_schema,
    get_arrow_schema,
    get_v2_column_names,
    _10K,
    _1M,
)


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
    """Remove .export_* temp directories left behind by interrupted writes."""
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


def get_sqlite_column_names(conn, table):
    """Return list of column names for a SQLite table."""
    rows = conn.execute(f"PRAGMA table_info({table})").fetchall()
    return [r[1] for r in rows]


def export_partition(conn, sqlite_table, contract, event, block_start,
                     block_end, output_dir):
    """Export one 10K partition from SQLite to Parquet.

    Reads raw data from SQLite (hex strings, int64), converts to typed
    Parquet columns (binary BLOBs, uint32), and writes a Parquet file
    atomically (temp dir then rename).

    For mixed tables (e.g. order_filled shared by CTFExchange and
    NegRiskCtfExchange), filters rows by contract_address.

    Returns (row_count, file_size_bytes).
    """
    dest_dir = dest_path_for_partition(output_dir, contract, event, block_start)

    # Already exported — skip
    if os.path.isfile(os.path.join(dest_dir, "data.parquet")):
        return -1, 0

    v2_schema_spec = get_v2_schema(contract, event)
    arrow_schema = get_arrow_schema(contract, event)

    v2_col_names = get_v2_column_names(contract, event)
    sqlite_cols = get_sqlite_column_names(conn, sqlite_table)

    select_cols = []
    for col_name in v2_col_names:
        if col_name in sqlite_cols:
            select_cols.append(col_name)
        else:
            select_cols.append(None)

    sql_cols = [c for c in select_cols if c is not None]
    col_list = ", ".join(sql_cols)

    where_clause = "block_number >= ? AND block_number <= ?"
    params = [block_start, block_end]
    if is_mixed_sqlite_table(sqlite_table):
        where_clause += " AND contract_address = ?"
        params.append(CONTRACTS[contract].lower())

    cursor = conn.execute(
        f"SELECT {col_list} FROM {sqlite_table} "
        f"WHERE {where_clause} "
        f"ORDER BY block_number, transaction_index, log_index",
        params,
    )
    rows = cursor.fetchall()

    if rows:
        sql_col_map = {}
        col_data = list(zip(*rows))
        for i, col_name in enumerate(sql_cols):
            sql_col_map[col_name] = col_data[i]

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
            "source_script": "scrape_events_rpc.py",
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
    """Read a Parquet file and return its row count."""
    table = pq.read_table(path)
    return len(table)
