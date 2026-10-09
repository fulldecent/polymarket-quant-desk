"""v4 grain, no transaction_hash, and path isolation from v3."""

import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parents[2]))

from _internal.tables import COMMON_COLUMNS, all_columns, all_targets


ROOT = Path(__file__).resolve().parents[2]
SCHEMA = (ROOT / "schema.sql").read_text()
MAIN = (ROOT / "main.py").read_text()


def test_common_columns_are_v4_grain_plus_timestamp():
    assert COMMON_COLUMNS == (
        "block_number",
        "block_timestamp",
        "transaction_index",
        "log_index",
    )


def test_no_event_table_declares_transaction_hash():
    assert "transaction_hash" not in COMMON_COLUMNS
    for contract, event in all_targets():
        cols = all_columns(contract, event)
        assert "transaction_hash" not in cols
        assert cols[:4] == COMMON_COLUMNS


def test_schema_sql_has_timestamp_and_no_hash_column():
    assert "block_timestamp      UINTEGER" in SCHEMA
    assert "There is no transaction_hash column." in SCHEMA
    # loaded_block_ranges and comments may mention the words; event tables must not.
    for line in SCHEMA.splitlines():
        stripped = line.strip()
        if stripped.startswith("--") or not stripped:
            continue
        assert "transaction_hash" not in stripped


def test_scraper_writes_only_v4_paths():
    assert "POLYGON_CONTRACT_EVENTS_V4_DIR" in MAIN
    assert 'polygon_contract_events_v4.db' in MAIN
    assert "POLYGON_CONTRACT_EVENTS_V3_DIR" not in MAIN
    assert "polygon_contract_events_v3.db" not in MAIN
