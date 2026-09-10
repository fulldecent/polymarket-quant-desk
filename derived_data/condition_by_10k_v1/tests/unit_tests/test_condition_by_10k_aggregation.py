"""Unit tests for condition_by_10k_v1 aggregation against fixture inputs."""

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
COND_OTHER = bytes.fromhex("cc" * 32)
ACCT = bytes.fromhex("11" * 20)


def _load_main():
    spec = importlib.util.spec_from_file_location(
        "condition_by_10k_v1_main", _DATASET_DIR / "main.py"
    )
    mod = importlib.util.module_from_spec(spec)
    assert spec.loader is not None
    spec.loader.exec_module(mod)
    return mod


def _write_parquet(path: Path, table: pa.Table) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    pq.write_table(table, path)


def _setup(tmp_path: Path, *, fills_rows: list[dict], resolutions: list[dict], mapped: list[bytes]):
    mod = _load_main()
    fills = tmp_path / "fills"
    raw = tmp_path / "raw"
    token_map = tmp_path / "token_map"
    out = tmp_path / "out"
    fills_path = fills / partition_dir(K_VAL) / "data.parquet"
    if fills_rows:
        _write_parquet(
            fills_path,
            pa.table(
                {
                    "block_number": pa.array(
                        [r["block_number"] for r in fills_rows], type=pa.uint32()
                    ),
                    "logical_fill_index": pa.array(
                        [r["logical_fill_index"] for r in fills_rows], type=pa.uint32()
                    ),
                    "account": pa.array(
                        [r.get("account", ACCT) for r in fills_rows], type=pa.binary()
                    ),
                    "condition_id": pa.array(
                        [r["condition_id"] for r in fills_rows], type=pa.binary()
                    ),
                    "is_taker": pa.array(
                        [r.get("is_taker", True) for r in fills_rows], type=pa.bool_()
                    ),
                    "net_yes_tokens": pa.array(
                        [r["net_yes_tokens"] for r in fills_rows], type=pa.int64()
                    ),
                    "gross_usdc": pa.array([r["gross_usdc"] for r in fills_rows], type=pa.int64()),
                    "fee_usdc": pa.array(
                        [r.get("fee_usdc", 0) for r in fills_rows], type=pa.int64()
                    ),
                }
            ),
        )
    else:
        _write_parquet(
            fills_path,
            pa.table(
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
            ),
        )
    if resolutions:
        res_path = (
            raw / "ConditionalTokens" / "condition_resolution" / partition_dir(K_VAL) / "data.parquet"
        )
        _write_parquet(
            res_path,
            pa.table(
                {
                    "block_number": pa.array(
                        [r["block_number"] for r in resolutions], type=pa.uint32()
                    ),
                    "log_index": pa.array([r["log_index"] for r in resolutions], type=pa.uint32()),
                    "condition_id": pa.array(
                        [r["condition_id"] for r in resolutions], type=pa.binary()
                    ),
                    "outcome_slot_count": pa.array(
                        [r.get("outcome_slot_count", 2) for r in resolutions], type=pa.uint32()
                    ),
                    "payout_numerators": pa.array(
                        [r.get("payout_numerators", '["1","0"]') for r in resolutions],
                        type=pa.string(),
                    ),
                }
            ),
        )
    map_path = token_map / "1M=0" / "10K=0" / "data.parquet"
    _write_parquet(
        map_path,
        pa.table({"condition_id": pa.array(mapped, type=pa.binary())}),
    )
    mod.FILLS = str(fills)
    mod.RAW = str(raw)
    mod.TOKEN_MAP = str(token_map)
    mod.OUT_DIR = str(out)
    con = duckdb.connect()
    mod._load_polymarket_conditions(con)
    n = mod.process_chunk(con, M_VAL, K_VAL, logging.getLogger("test"))
    result = pq.read_table(out / partition_dir(K_VAL) / "data.parquet")
    return n, result


def test_fills_and_resolution(tmp_path: Path):
    n, table = _setup(
        tmp_path,
        fills_rows=[
            {
                "block_number": K_VAL,
                "logical_fill_index": 0,
                "condition_id": COND_A,
                "net_yes_tokens": 1_000_000,
                "gross_usdc": 550_000,
                "fee_usdc": 3,
            },
            {
                "block_number": K_VAL,
                "logical_fill_index": 1,
                "condition_id": COND_A,
                "net_yes_tokens": -1_000_000,
                "gross_usdc": -550_000,
                "fee_usdc": 0,
                "is_taker": False,
            },
        ],
        resolutions=[
            {
                "block_number": K_VAL + 9,
                "log_index": 4,
                "condition_id": COND_A,
                "payout_numerators": '["0","1"]',
            }
        ],
        mapped=[COND_A],
    )
    assert n == 1
    row = table.to_pandas().iloc[0]
    assert math.isclose(row["open_yes_price"], 0.55)
    assert row["matched_usdc"] == 550_000
    assert row["fee_usdc"] == 3
    assert row["fill_count"] == 2
    assert row["resolved_block"] == K_VAL + 9
    assert row["resolved_log_index"] == 4
    assert row["outcome_slot_count"] == 2
    assert row["payout_numerators"] == '["0","1"]'


def test_resolution_only_mapped_condition(tmp_path: Path):
    n, table = _setup(
        tmp_path,
        fills_rows=[],
        resolutions=[
            {
                "block_number": K_VAL + 1,
                "log_index": 8,
                "condition_id": COND_B,
            }
        ],
        mapped=[COND_B],
    )
    assert n == 1
    row = table.to_pandas().iloc[0]
    assert row["fill_count"] == 0
    assert row["matched_usdc"] == 0
    assert pa.compute.is_null(table.column("open_yes_price"))[0].as_py()
    assert row["resolved_block"] == K_VAL + 1


def test_unmapped_resolution_dropped(tmp_path: Path):
    n, table = _setup(
        tmp_path,
        fills_rows=[],
        resolutions=[
            {
                "block_number": K_VAL + 1,
                "log_index": 8,
                "condition_id": COND_OTHER,
            }
        ],
        mapped=[COND_A],
    )
    assert n == 0
    assert table.num_rows == 0


def test_non_binary_resolution_fails(tmp_path: Path):
    with pytest.raises(RuntimeError, match="outcome_slot_count"):
        _setup(
            tmp_path,
            fills_rows=[],
            resolutions=[
                {
                    "block_number": K_VAL,
                    "log_index": 1,
                    "condition_id": COND_A,
                    "outcome_slot_count": 3,
                    "payout_numerators": '["1","0","0"]',
                }
            ],
            mapped=[COND_A],
        )


def test_duplicate_resolution_fails(tmp_path: Path):
    with pytest.raises(RuntimeError, match="more than one condition_resolution"):
        _setup(
            tmp_path,
            fills_rows=[],
            resolutions=[
                {"block_number": K_VAL, "log_index": 1, "condition_id": COND_A},
                {"block_number": K_VAL, "log_index": 2, "condition_id": COND_A},
            ],
            mapped=[COND_A],
        )
