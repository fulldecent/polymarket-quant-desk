"""Unit tests for account_by_10k_v1 aggregation and entropy."""

from __future__ import annotations

import importlib.util
import logging
import math
import sys
from pathlib import Path

import duckdb
import pyarrow as pa
import pyarrow.parquet as pq

_DATASET_DIR = Path(__file__).resolve().parents[2]
_PROJECT_ROOT = Path(__file__).resolve().parents[4]
sys.path.insert(0, str(_PROJECT_ROOT))

from lib.partition_utils import partition_dir  # noqa: E402

K_VAL = 70_000_000
M_VAL = 70_000_000
EARLY_K = 60_000_000
COND_A = bytes.fromhex("aa" * 32)
COND_B = bytes.fromhex("bb" * 32)
ACCT = bytes.fromhex("11" * 20)
ACCT_2 = bytes.fromhex("22" * 20)


def _load_main():
    spec = importlib.util.spec_from_file_location(
        "account_by_10k_v1_main", _DATASET_DIR / "main.py"
    )
    mod = importlib.util.module_from_spec(spec)
    assert spec.loader is not None
    spec.loader.exec_module(mod)
    return mod


def _run(tmp_path: Path, rows: list[dict], k_val: int = K_VAL):
    mod = _load_main()
    fills = tmp_path / "fills"
    out = tmp_path / "out"
    path = fills / partition_dir(k_val) / "data.parquet"
    path.parent.mkdir(parents=True, exist_ok=True)
    table = pa.table(
        {
            "block_number": pa.array([r["block_number"] for r in rows], type=pa.uint32()),
            "logical_fill_index": pa.array(
                [r.get("logical_fill_index", i) for i, r in enumerate(rows)], type=pa.uint32()
            ),
            "account": pa.array([r.get("account", ACCT) for r in rows], type=pa.binary()),
            "condition_id": pa.array(
                [r.get("condition_id", COND_A) for r in rows], type=pa.binary()
            ),
            "is_taker": pa.array([r.get("is_taker", True) for r in rows], type=pa.bool_()),
            "net_yes_tokens": pa.array(
                [r.get("net_yes_tokens", 1_000_000) for r in rows], type=pa.int64()
            ),
            "gross_usdc": pa.array([r.get("gross_usdc", 500_000) for r in rows], type=pa.int64()),
            "fee_usdc": pa.array([r.get("fee_usdc", 0) for r in rows], type=pa.int64()),
        }
    )
    pq.write_table(table, path)
    mod.FILLS = str(fills)
    mod.OUT_DIR = str(out)
    n = mod.process_chunk(duckdb.connect(), (k_val // 1_000_000) * 1_000_000, k_val, logging.getLogger("test"))
    return n, pq.read_table(out / partition_dir(k_val) / "data.parquet")


def test_counts_volume_and_maker_ratio(tmp_path: Path):
    n, table = _run(
        tmp_path,
        [
            {"block_number": K_VAL, "is_taker": True, "condition_id": COND_A, "gross_usdc": 100, "fee_usdc": 1},
            {"block_number": K_VAL + 1, "is_taker": False, "condition_id": COND_B, "gross_usdc": -50, "fee_usdc": 2},
            {"block_number": K_VAL + 2, "is_taker": False, "condition_id": COND_A, "gross_usdc": 20, "fee_usdc": 0},
        ],
    )
    assert n == 1
    row = table.to_pandas().iloc[0]
    assert row["fill_count"] == 3
    assert row["condition_count"] == 2
    assert row["volume_usdc"] == 170
    assert row["fee_usdc"] == 3
    assert row["maker_fills"] == 2
    assert row["taker_fills"] == 1
    assert math.isclose(row["maker_fill_ratio"], 2 / 3)
    assert row["first_fill_block"] == K_VAL
    assert row["last_fill_block"] == K_VAL + 2


def test_entropy_null_below_threshold(tmp_path: Path):
    rows = [
        {"block_number": K_VAL, "is_taker": True}
        for _ in range(99)
    ]
    _, table = _run(tmp_path, rows)
    row = table.to_pandas().iloc[0]
    assert row["taker_fills"] == 99
    assert pa.compute.is_null(table.column("taker_hour_entropy"))[0].as_py()


def test_entropy_zero_when_all_one_hour(tmp_path: Path):
    rows = [{"block_number": K_VAL, "is_taker": True} for _ in range(100)]
    _, table = _run(tmp_path, rows)
    row = table.to_pandas().iloc[0]
    assert math.isclose(row["taker_hour_entropy"], 0.0)
    assert pa.compute.is_null(table.column("maker_hour_entropy"))[0].as_py()


def test_entropy_ln2_for_two_equal_hours(tmp_path: Path):
    rows = [{"block_number": K_VAL, "is_taker": True} for _ in range(50)]
    rows += [{"block_number": K_VAL + 1714, "is_taker": True} for _ in range(50)]
    _, table = _run(tmp_path, rows)
    row = table.to_pandas().iloc[0]
    assert math.isclose(row["taker_hour_entropy"], math.log(2), rel_tol=1e-9)


def test_entropy_null_before_min_block(tmp_path: Path):
    rows = [{"block_number": EARLY_K, "is_taker": True} for _ in range(100)]
    _, table = _run(tmp_path, rows, k_val=EARLY_K)
    assert pa.compute.is_null(table.column("taker_hour_entropy"))[0].as_py()
    assert pa.compute.is_null(table.column("maker_hour_entropy"))[0].as_py()
