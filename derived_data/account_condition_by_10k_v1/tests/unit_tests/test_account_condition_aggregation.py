"""Unit tests for account_condition_by_10k_v1 aggregation."""

from __future__ import annotations

import importlib.util
import logging
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
COND_A = bytes.fromhex("aa" * 32)
COND_B = bytes.fromhex("bb" * 32)
ACCT_1 = bytes.fromhex("11" * 20)
ACCT_2 = bytes.fromhex("22" * 20)


def _load_main():
    spec = importlib.util.spec_from_file_location(
        "account_condition_by_10k_v1_main", _DATASET_DIR / "main.py"
    )
    mod = importlib.util.module_from_spec(spec)
    assert spec.loader is not None
    spec.loader.exec_module(mod)
    return mod


def _run(tmp_path: Path, rows: list[dict]):
    mod = _load_main()
    fills = tmp_path / "fills"
    out = tmp_path / "out"
    path = fills / partition_dir(K_VAL) / "data.parquet"
    path.parent.mkdir(parents=True, exist_ok=True)
    table = pa.table(
        {
            "block_number": pa.array([r["block_number"] for r in rows], type=pa.uint32()),
            "logical_fill_index": pa.array(
                [r.get("logical_fill_index", i) for i, r in enumerate(rows)], type=pa.uint32()
            ),
            "account": pa.array([r["account"] for r in rows], type=pa.binary()),
            "condition_id": pa.array([r["condition_id"] for r in rows], type=pa.binary()),
            "is_taker": pa.array([r.get("is_taker", True) for r in rows], type=pa.bool_()),
            "net_yes_tokens": pa.array([r["net_yes_tokens"] for r in rows], type=pa.int64()),
            "gross_usdc": pa.array([r["gross_usdc"] for r in rows], type=pa.int64()),
            "fee_usdc": pa.array([r.get("fee_usdc", 0) for r in rows], type=pa.int64()),
        }
    )
    pq.write_table(table, path)
    mod.FILLS = str(fills)
    mod.OUT_DIR = str(out)
    n = mod.process_chunk(duckdb.connect(), M_VAL, K_VAL, logging.getLogger("test"))
    return n, pq.read_table(out / partition_dir(K_VAL) / "data.parquet")


def test_groups_account_condition_and_does_not_halve_volume(tmp_path: Path):
    n, table = _run(
        tmp_path,
        [
            {
                "block_number": K_VAL + 1,
                "account": ACCT_1,
                "condition_id": COND_A,
                "net_yes_tokens": 1_000_000,
                "gross_usdc": 550_000,
                "fee_usdc": 5,
            },
            {
                "block_number": K_VAL + 3,
                "account": ACCT_1,
                "condition_id": COND_A,
                "net_yes_tokens": -400_000,
                "gross_usdc": -200_000,
                "fee_usdc": 1,
                "is_taker": False,
            },
            {
                "block_number": K_VAL + 2,
                "account": ACCT_1,
                "condition_id": COND_B,
                "net_yes_tokens": 100_000,
                "gross_usdc": 50_000,
                "fee_usdc": 0,
            },
            {
                "block_number": K_VAL + 4,
                "account": ACCT_2,
                "condition_id": COND_A,
                "net_yes_tokens": 10_000,
                "gross_usdc": 4_000,
                "fee_usdc": 0,
            },
        ],
    )
    assert n == 3
    df = table.to_pandas()
    df["account"] = df["account"].apply(bytes)
    df["condition_id"] = df["condition_id"].apply(bytes)
    pair = df[(df["account"] == ACCT_1) & (df["condition_id"] == COND_A)].iloc[0]
    assert pair["fill_count"] == 2
    assert pair["volume_usdc"] == 750_000
    assert pair["volume_yes_tokens"] == 1_400_000
    assert pair["net_yes_tokens"] == 600_000
    assert pair["fee_usdc"] == 6
    assert pair["first_fill_block"] == K_VAL + 1
    assert pair["last_fill_block"] == K_VAL + 3
    keys = list(zip(df["account"], df["condition_id"]))
    assert keys == sorted(keys)
