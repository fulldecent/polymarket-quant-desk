"""Unit tests for condition_by_block_v1 aggregation against fixture fills."""

from __future__ import annotations

import importlib.util
import logging
import math
import sys
from pathlib import Path

import duckdb
import pyarrow as pa
import pyarrow.parquet as pq
import pytest

_DATASET_DIR = Path(__file__).resolve().parents[2]
_PROJECT_ROOT = Path(__file__).resolve().parents[4]
sys.path.insert(0, str(_PROJECT_ROOT))

from lib.partition_utils import partition_dir  # noqa: E402

K_VAL = 70_000_000
M_VAL = 70_000_000
COND_A = bytes.fromhex("aa" * 32)
COND_B = bytes.fromhex("bb" * 32)
ACCT = bytes.fromhex("11" * 20)


def _load_main():
    spec = importlib.util.spec_from_file_location(
        "condition_by_block_v1_main", _DATASET_DIR / "main.py"
    )
    mod = importlib.util.module_from_spec(spec)
    assert spec.loader is not None
    spec.loader.exec_module(mod)
    return mod


def _write_fills(base: Path, rows: list[dict]) -> Path:
    path = base / partition_dir(K_VAL) / "data.parquet"
    path.parent.mkdir(parents=True, exist_ok=True)
    if not rows:
        table = pa.table(
            {
                "block_number": pa.array([], type=pa.uint32()),
                "logical_fill_index": pa.array([], type=pa.uint32()),
                "account": pa.array([], type=pa.binary()),
                "condition_id": pa.array([], type=pa.binary()),
                "is_taker": pa.array([], type=pa.bool_()),
                "net_yes_tokens": pa.array([], type=pa.int64()),
                "gross_usdc": pa.array([], type=pa.int64()),
                "fee_usdc": pa.array([], type=pa.int64()),
            }
        )
    else:
        table = pa.table(
            {
                "block_number": pa.array([r["block_number"] for r in rows], type=pa.uint32()),
                "logical_fill_index": pa.array(
                    [r["logical_fill_index"] for r in rows], type=pa.uint32()
                ),
                "account": pa.array([r.get("account", ACCT) for r in rows], type=pa.binary()),
                "condition_id": pa.array([r["condition_id"] for r in rows], type=pa.binary()),
                "is_taker": pa.array([r.get("is_taker", True) for r in rows], type=pa.bool_()),
                "net_yes_tokens": pa.array([r["net_yes_tokens"] for r in rows], type=pa.int64()),
                "gross_usdc": pa.array([r["gross_usdc"] for r in rows], type=pa.int64()),
                "fee_usdc": pa.array([r.get("fee_usdc", 0) for r in rows], type=pa.int64()),
            }
        )
    pq.write_table(table, path)
    return path


def _run(tmp_path: Path, rows: list[dict]):
    mod = _load_main()
    fills = tmp_path / "fills"
    out = tmp_path / "out"
    _write_fills(fills, rows)
    mod.FILLS = str(fills)
    mod.OUT_DIR = str(out)
    con = duckdb.connect()
    log = logging.getLogger("test")
    n = mod.process_chunk(con, M_VAL, K_VAL, log)
    result_path = out / partition_dir(K_VAL) / "data.parquet"
    table = pq.read_table(result_path)
    return n, table, result_path


def test_yes_price_four_sides(tmp_path: Path):
    n, table, _ = _run(
        tmp_path,
        [
            # buy YES @ 0.55
            {
                "block_number": K_VAL,
                "logical_fill_index": 0,
                "condition_id": COND_A,
                "net_yes_tokens": 1_000_000,
                "gross_usdc": 550_000,
                "fee_usdc": 10,
            },
            # sell YES @ 0.55 (maker of the same match)
            {
                "block_number": K_VAL,
                "logical_fill_index": 1,
                "condition_id": COND_A,
                "net_yes_tokens": -1_000_000,
                "gross_usdc": -550_000,
                "fee_usdc": 0,
                "is_taker": False,
            },
            # buy NO @ 0.40 => YES 0.60
            {
                "block_number": K_VAL + 1,
                "logical_fill_index": 0,
                "condition_id": COND_A,
                "net_yes_tokens": -2_000_000,
                "gross_usdc": 800_000,
                "fee_usdc": 4,
            },
            # sell NO @ 0.40 => YES 0.60
            {
                "block_number": K_VAL + 1,
                "logical_fill_index": 1,
                "condition_id": COND_A,
                "net_yes_tokens": 2_000_000,
                "gross_usdc": -800_000,
                "fee_usdc": 0,
                "is_taker": False,
            },
        ],
    )
    assert n == 2
    df = table.to_pandas()
    row0 = df[df["block_number"] == K_VAL].iloc[0]
    assert math.isclose(row0["open_yes_price"], 0.55)
    assert math.isclose(row0["close_yes_price"], 0.55)
    assert row0["matched_usdc"] == 550_000
    assert row0["matched_yes_tokens"] == 1_000_000
    assert row0["fee_usdc"] == 10
    assert row0["fill_count"] == 2
    row1 = df[df["block_number"] == K_VAL + 1].iloc[0]
    assert math.isclose(row1["open_yes_price"], 0.60)
    assert math.isclose(row1["high_yes_price"], 0.60)
    assert row1["matched_usdc"] == 800_000
    assert row1["fee_usdc"] == 4


def test_open_close_follow_logical_fill_index(tmp_path: Path):
    _, table, _ = _run(
        tmp_path,
        [
            {
                "block_number": K_VAL,
                "logical_fill_index": 2,
                "condition_id": COND_A,
                "net_yes_tokens": 1_000_000,
                "gross_usdc": 700_000,
            },
            {
                "block_number": K_VAL,
                "logical_fill_index": 0,
                "condition_id": COND_A,
                "net_yes_tokens": 1_000_000,
                "gross_usdc": 400_000,
            },
            {
                "block_number": K_VAL,
                "logical_fill_index": 1,
                "condition_id": COND_A,
                "net_yes_tokens": 1_000_000,
                "gross_usdc": 900_000,
            },
        ],
    )
    row = table.to_pandas().iloc[0]
    assert math.isclose(row["open_yes_price"], 0.40)
    assert math.isclose(row["high_yes_price"], 0.90)
    assert math.isclose(row["low_yes_price"], 0.40)
    assert math.isclose(row["close_yes_price"], 0.70)
    assert row["matched_usdc"] == (400_000 + 900_000 + 700_000) // 2


def test_empty_fills_writes_zero_row_file(tmp_path: Path):
    n, table, path = _run(tmp_path, [])
    assert n == 0
    assert table.num_rows == 0
    assert (path.parent / "metadata.json").exists()
    pf = pq.ParquetFile(path)
    by_name = {pf.schema.column(i).name: pf.schema.column(i) for i in range(pf.metadata.num_columns)}
    assert by_name["matched_yes_tokens"].logical_type.type == "INT"
    assert "isSigned=false" in str(by_name["matched_yes_tokens"].logical_type)
    assert by_name["condition_id"].physical_type == "BYTE_ARRAY"
    assert by_name["condition_id"].logical_type.type == "NONE"


def test_zero_amount_fails_fast(tmp_path: Path):
    with pytest.raises(RuntimeError, match="net_yes_tokens = 0 or gross_usdc = 0"):
        _run(
            tmp_path,
            [
                {
                    "block_number": K_VAL,
                    "logical_fill_index": 0,
                    "condition_id": COND_A,
                    "net_yes_tokens": 0,
                    "gross_usdc": 100,
                }
            ],
        )


def test_two_conditions_sorted(tmp_path: Path):
    _, table, _ = _run(
        tmp_path,
        [
            {
                "block_number": K_VAL,
                "logical_fill_index": 0,
                "condition_id": COND_B,
                "net_yes_tokens": 1_000_000,
                "gross_usdc": 500_000,
            },
            {
                "block_number": K_VAL,
                "logical_fill_index": 1,
                "condition_id": COND_A,
                "net_yes_tokens": 1_000_000,
                "gross_usdc": 500_000,
            },
        ],
    )
    ids = table.column("condition_id").to_pylist()
    assert ids == sorted(ids)
    assert ids == [COND_A, COND_B]
