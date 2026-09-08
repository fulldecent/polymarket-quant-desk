"""Ingestion must survive a sink worker holding a read snapshot on the same database."""

import sys
from pathlib import Path

import duckdb
import pytest

sys.path.insert(0, str(Path(__file__).resolve().parents[2]))

from _internal.persistence import HotStore, HotStoreConfig
from _internal.tables import SCRAPE_START_BLOCK, all_columns, all_targets, table_name

SCHEMA = str(Path(__file__).resolve().parents[2] / "schema.sql")
TARGET = all_targets()[0]
COLUMNS = all_columns(*TARGET)
TABLE = table_name(*TARGET)


@pytest.fixture
def store(tmp_path):
    hot = HotStore(
        str(tmp_path / "hot.db"), SCHEMA, config=HotStoreConfig(duckdb_temp_dir=str(tmp_path))
    )
    yield hot
    hot.close()


def declared_types(store: HotStore) -> dict[str, str]:
    rows = store.connection.execute(
        """
        SELECT column_name, data_type
        FROM information_schema.columns
        WHERE table_schema = 'main' AND table_name = ?
        """,
        (TABLE,),
    ).fetchall()
    return {name: kind for name, kind in rows}


def rows_for(store: HotStore, block: int, count: int = 5):
    declared = declared_types(store)
    out = []
    for i in range(count):
        row = {}
        for column in COLUMNS:
            kind = declared[column]
            if column == "block_number":
                row[column] = block
            elif kind == "UINTEGER":
                row[column] = i
            elif kind == "BLOB":
                row[column] = i.to_bytes(32, "big")
            else:
                row[column] = f"v{i}"
        out.append(row)
    return out


def open_reader(store: HotStore) -> duckdb.DuckDBPyConnection:
    """A second connection configured exactly as a sink worker's is."""
    return duckdb.connect(
        store.db_path,
        config={
            "memory_limit": store.config.duckdb_memory_limit,
            "threads": str(store.config.duckdb_threads),
            "temp_directory": store.config.duckdb_temp_dir,
        },
    )


def test_ingestion_continues_while_a_sink_holds_a_read_snapshot(store):
    """A live run died here: rewriting unchanged rows collides with the reader's snapshot."""
    block = SCRAPE_START_BLOCK + 1
    for _ in range(5):
        store.persist(
            from_block=block, to_block=block, rows_by_target={TARGET: rows_for(store, block)}
        )
        block += 2  # holes, so the ranges stay separate and get rewritten each time

    reader = open_reader(store)
    reader.execute("BEGIN TRANSACTION")
    reader.execute(f"SELECT count(*) FROM {TABLE}").fetchone()
    try:
        for _ in range(20):
            block += 2
            store.persist(
                from_block=block, to_block=block, rows_by_target={TARGET: rows_for(store, block)}
            )
    finally:
        reader.execute("ROLLBACK")
        reader.close()


def test_rows_survive_the_reader(store):
    block = SCRAPE_START_BLOCK + 1
    reader = open_reader(store)
    reader.execute("BEGIN TRANSACTION")
    try:
        result = store.persist(
            from_block=block, to_block=block, rows_by_target={TARGET: rows_for(store, block, 7)}
        )
    finally:
        reader.execute("ROLLBACK")
        reader.close()

    assert result.rows_inserted == 7
    landed = store.connection.execute(f"SELECT count(*) FROM {TABLE}").fetchone()[0]
    assert landed == 7
