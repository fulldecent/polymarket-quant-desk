"""Unit tests for token_and_usdc_flows_v2 main.py.

Focused regression coverage for the fee join behavioral assumption guard.
"""

from __future__ import annotations

import importlib.util
from pathlib import Path

import duckdb
import pyarrow as pa
import pyarrow.parquet as pq
import pytest


def _write_parquet(path: Path, rows: list[dict], schema: pa.Schema) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    cols = {name: [r[name] for r in rows] for name in schema.names}
    table = pa.table(cols, schema=schema)
    pq.write_table(table, path)


@pytest.fixture
def mod(tmp_path: Path, monkeypatch: pytest.MonkeyPatch):
    raw = tmp_path / "raw"
    out = tmp_path / "out"
    temp = tmp_path / "tmp"
    raw.mkdir(parents=True)
    out.mkdir(parents=True)
    temp.mkdir(parents=True)

    monkeypatch.setenv("POLYGON_CONTRACT_EVENTS_V3_DIR", str(raw))
    monkeypatch.setenv("TOKEN_AND_USDC_FLOWS_V2_DIR", str(out))
    monkeypatch.setenv("TEMP_DIR", str(temp))

    module_path = Path(__file__).resolve().parents[2] / "main.py"
    spec = importlib.util.spec_from_file_location("token_and_usdc_flows_v2_main_test", module_path)
    assert spec is not None and spec.loader is not None
    module = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(module)
    return module


def test_assert_unique_fills_per_tx_passes_for_exec_many_distinct_orders(mod, tmp_path: Path):
    """Multiple orders in one tx (exec_many style) is allowed when order_hash differs."""
    of_schema = pa.schema([
        pa.field("block_number", pa.uint32()),
        pa.field("transaction_index", pa.uint32()),
        pa.field("maker", pa.binary(20)),
        pa.field("taker", pa.binary(20)),
        pa.field("order_hash", pa.binary(32)),
    ])
    fr_schema = pa.schema([
        pa.field("block_number", pa.uint32()),
        pa.field("transaction_index", pa.uint32()),
        pa.field("order_hash", pa.binary(32)),
    ])

    raw = Path(mod.RAW)
    of_path = raw / "CTFExchange" / "order_filled" / "1M=82000000" / "10K=82000000" / "data.parquet"
    fr_path = raw / "FeeModuleCTF" / "fee_refunded" / "1M=82000000" / "10K=82000000" / "data.parquet"

    maker = bytes.fromhex("11" * 20)
    taker = bytes.fromhex("22" * 20)
    h1 = bytes.fromhex("aa" * 32)
    h2 = bytes.fromhex("bb" * 32)

    _write_parquet(
        of_path,
        [
            {"block_number": 82000001, "transaction_index": 7, "maker": maker, "taker": taker, "order_hash": h1},
            {"block_number": 82000001, "transaction_index": 7, "maker": maker, "taker": taker, "order_hash": h2},
        ],
        of_schema,
    )
    _write_parquet(
        fr_path,
        [
            {"block_number": 82000001, "transaction_index": 7, "order_hash": h1},
            {"block_number": 82000001, "transaction_index": 7, "order_hash": h2},
        ],
        fr_schema,
    )

    con = duckdb.connect()
    mod._assert_unique_fills_per_tx(
        con,
        "CTFExchange/order_filled",
        "FeeModuleCTF/fee_refunded",
        mod.CTF_EXCHANGE_HEX,
        82000000,
        82000000,
        mod.logging.getLogger("test"),
    )


def test_assert_unique_fills_per_tx_fails_on_duplicate_order_fill_in_same_tx(mod):
    """Two fills of same order_hash in one tx must fail fast."""
    of_schema = pa.schema([
        pa.field("block_number", pa.uint32()),
        pa.field("transaction_index", pa.uint32()),
        pa.field("maker", pa.binary(20)),
        pa.field("taker", pa.binary(20)),
        pa.field("order_hash", pa.binary(32)),
    ])

    raw = Path(mod.RAW)
    of_path = raw / "CTFExchange" / "order_filled" / "1M=82000000" / "10K=82000000" / "data.parquet"

    maker = bytes.fromhex("11" * 20)
    taker = bytes.fromhex("22" * 20)
    h1 = bytes.fromhex("aa" * 32)

    _write_parquet(
        of_path,
        [
            {"block_number": 82000001, "transaction_index": 7, "maker": maker, "taker": taker, "order_hash": h1},
            {"block_number": 82000001, "transaction_index": 7, "maker": maker, "taker": taker, "order_hash": h1},
        ],
        of_schema,
    )

    con = duckdb.connect()
    with pytest.raises(mod.FillAssumptionViolation):
        mod._assert_unique_fills_per_tx(
            con,
            "CTFExchange/order_filled",
            "FeeModuleCTF/fee_refunded",
            mod.CTF_EXCHANGE_HEX,
            82000000,
            82000000,
            mod.logging.getLogger("test"),
        )


def test_assert_unique_fills_per_tx_fails_on_duplicate_fee_refund_in_same_tx(mod):
    """Duplicate fee_refunded rows for same (tx,order_hash) must fail fast."""
    of_schema = pa.schema([
        pa.field("block_number", pa.uint32()),
        pa.field("transaction_index", pa.uint32()),
        pa.field("maker", pa.binary(20)),
        pa.field("taker", pa.binary(20)),
        pa.field("order_hash", pa.binary(32)),
    ])
    fr_schema = pa.schema([
        pa.field("block_number", pa.uint32()),
        pa.field("transaction_index", pa.uint32()),
        pa.field("order_hash", pa.binary(32)),
    ])

    raw = Path(mod.RAW)
    of_path = raw / "CTFExchange" / "order_filled" / "1M=82000000" / "10K=82000000" / "data.parquet"
    fr_path = raw / "FeeModuleCTF" / "fee_refunded" / "1M=82000000" / "10K=82000000" / "data.parquet"

    maker = bytes.fromhex("11" * 20)
    taker = bytes.fromhex("22" * 20)
    h1 = bytes.fromhex("aa" * 32)

    _write_parquet(
        of_path,
        [{"block_number": 82000001, "transaction_index": 7, "maker": maker, "taker": taker, "order_hash": h1}],
        of_schema,
    )
    _write_parquet(
        fr_path,
        [
            {"block_number": 82000001, "transaction_index": 7, "order_hash": h1},
            {"block_number": 82000001, "transaction_index": 7, "order_hash": h1},
        ],
        fr_schema,
    )

    con = duckdb.connect()
    with pytest.raises(mod.FillAssumptionViolation):
        mod._assert_unique_fills_per_tx(
            con,
            "CTFExchange/order_filled",
            "FeeModuleCTF/fee_refunded",
            mod.CTF_EXCHANGE_HEX,
            82000000,
            82000000,
            mod.logging.getLogger("test"),
        )
