"""FeeRefunded.feeCharged is a uint256 topic stored as a decimal VARCHAR.

A live scrape died at persist with Arrow ArrowInvalid:

    Could not convert b'\\x00...!\\x98' with type bytes: was not a utf8 string

because the decoder emitted the 32-byte topic as bytes into a VARCHAR
column. 0x2198 == 8600 is a real feeCharged value.
"""

import sys
from pathlib import Path

import pytest
from eth_abi import encode as abi_encode

sys.path.insert(0, str(Path(__file__).resolve().parents[2]))
sys.path.insert(0, str(Path(__file__).resolve().parents[4]))

from _internal.errors import V3Error
from _internal.event_decoders import decode_log_strict
from _internal.parquet_sink import write_partition_files
from _internal.persistence import HotStore, HotStoreConfig
from _internal.tables import CONTRACTS_BY_NAME

SCHEMA = str(Path(__file__).resolve().parents[2] / "schema.sql")
FEE_MODULE = CONTRACTS_BY_NAME["FeeModuleCTF"].address
TOPIC0 = "0xb608d2bf25d8b4b744ba23ce2ea9802ea955e216c064a62f42152fbf98958d24"
ORDER_HASH = "11" * 32
RECEIVER = "22" * 20
FEE_CHARGED = 0x2198  # 8600; the bytes that blew up persist on block 81,278,019


def _topic_uint256(n: int) -> str:
    return "0x" + n.to_bytes(32, "big").hex()


def _fee_refunded_log():
    data = abi_encode(["uint256", "uint256"], [0, 100])
    return {
        "address": FEE_MODULE,
        "blockNumber": hex(81_278_019),
        "transactionIndex": "0x0",
        "transactionHash": "0x" + "ab" * 32,
        "logIndex": "0x0",
        "topics": [
            TOPIC0,
            "0x" + ORDER_HASH,
            "0x" + ("00" * 12) + RECEIVER,
            _topic_uint256(FEE_CHARGED),
        ],
        "data": "0x" + data.hex(),
    }


def test_fee_charged_is_a_decimal_string_not_bytes():
    contract, event, row = decode_log_strict(_fee_refunded_log())
    assert contract == "FeeModuleCTF"
    assert event == "fee_refunded"
    assert row["fee_charged"] == "8600"
    assert isinstance(row["fee_charged"], str)
    assert row["refund"] == "100"


def test_nul_padded_topic_bytes_are_rejected_at_persist(tmp_path):
    """Arrow would accept 156200 as 32-byte UTF-8; that encoding is poison."""
    store = HotStore(
        str(tmp_path / "hot.db"), SCHEMA, config=HotStoreConfig(duckdb_temp_dir=str(tmp_path))
    )
    try:
        _, _, row = decode_log_strict(_fee_refunded_log())
        row["fee_charged"] = (156200).to_bytes(32, "big").decode("utf-8")
        with pytest.raises(V3Error, match="not a uint256 decimal"):
            store.persist(
                from_block=81_278_019,
                to_block=81_278_019,
                rows_by_target={("FeeModuleCTF", "fee_refunded"): [row]},
            )
    finally:
        store.close()


def test_opening_hot_db_rejects_poisoned_fee_charged(tmp_path):
    db = str(tmp_path / "hot.db")
    store = HotStore(db, SCHEMA, config=HotStoreConfig(duckdb_temp_dir=str(tmp_path)))
    poison = (156200).to_bytes(32, "big").decode("utf-8")
    store.connection.execute(
        "INSERT INTO fee_module_ctf__fee_refunded "
        "(block_number, transaction_index, transaction_hash, log_index, "
        " order_hash, receiver, token_id, refund, fee_charged) VALUES "
        "(?, 0, ?, 0, ?, ?, ?, '0', ?)",
        [81_277_908, b"\xab" * 32, b"\x11" * 32, b"\x22" * 20, b"\x00" * 32, poison],
    )
    store.close()
    with pytest.raises(V3Error, match="not a uint256 decimal"):
        HotStore(db, SCHEMA, config=HotStoreConfig(duckdb_temp_dir=str(tmp_path)))


def test_sink_refuses_to_publish_poisoned_fee_charged(tmp_path):
    db = str(tmp_path / "hot.db")
    cold = tmp_path / "cold"
    cold.mkdir()
    store = HotStore(db, SCHEMA, config=HotStoreConfig(duckdb_temp_dir=str(tmp_path)))
    try:
        _, _, row = decode_log_strict(_fee_refunded_log())
        store.persist(
            from_block=81_270_000,
            to_block=81_279_999,
            rows_by_target={("FeeModuleCTF", "fee_refunded"): [row]},
        )
        poison = (156200).to_bytes(32, "big").decode("utf-8")
        store.connection.execute(
            "UPDATE fee_module_ctf__fee_refunded SET fee_charged = ?",
            [poison],
        )
        with pytest.raises(V3Error, match="not uint256 decimals"):
            write_partition_files(
                db,
                str(cold),
                81_270_000,
                duckdb_connect_config={
                    "memory_limit": store.config.duckdb_memory_limit,
                    "threads": str(store.config.duckdb_threads),
                    "temp_directory": str(tmp_path),
                },
            )
    finally:
        store.close()


def test_fee_refunded_row_persists_into_varchar_fee_charged(tmp_path):
    store = HotStore(
        str(tmp_path / "hot.db"), SCHEMA, config=HotStoreConfig(duckdb_temp_dir=str(tmp_path))
    )
    try:
        _, _, row = decode_log_strict(_fee_refunded_log())
        store.persist(
            from_block=81_278_019,
            to_block=81_278_019,
            rows_by_target={("FeeModuleCTF", "fee_refunded"): [row]},
        )
        got = store.connection.execute(
            "SELECT fee_charged FROM fee_module_ctf__fee_refunded"
        ).fetchone()[0]
        assert got == "8600"
    finally:
        store.close()
