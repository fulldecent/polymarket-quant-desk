"""Consecutive 10K coverage for condition_by_10k_v1."""

from __future__ import annotations

import os
import sys
from pathlib import Path

import pytest
from dotenv import load_dotenv

_project_root = Path(__file__).resolve().parents[4]
load_dotenv(_project_root / ".env")
sys.path.insert(0, str(_project_root))

from lib.partition_utils import partition_start  # noqa: E402
from raw_data.polygon_contract_events_v3 import SCRAPE_START_BLOCK  # noqa: E402

_PARTITION_10K_SIZE = 10_000
_PARTITION_1M_SIZE = 1_000_000
START_PARTITION_10K = partition_start(SCRAPE_START_BLOCK)
ENV = "CONDITION_BY_10K_V1_DIR"


def _output_dir() -> Path:
    val = os.environ.get(ENV, "")
    if not val:
        pytest.skip(f"{ENV} is not set")
    out = Path(val)
    if not out.exists():
        pytest.skip(f"{ENV} does not exist: {out}")
    return out


def _landed_10k(out: Path) -> list[int]:
    ks = []
    for m_dir in out.glob("1M=*"):
        if not m_dir.is_dir():
            continue
        for k_dir in m_dir.glob("10K=*"):
            if k_dir.is_dir() and not k_dir.name.endswith(".tmp"):
                ks.append(int(k_dir.name.split("=")[1]))
    return sorted(ks)


def test_starts_at_polymarket_start_partition():
    ks = _landed_10k(_output_dir())
    if not ks:
        pytest.skip("no partitions found")
    assert ks[0] == START_PARTITION_10K


def test_partitions_are_consecutive_no_gaps():
    ks = _landed_10k(_output_dir())
    if not ks:
        pytest.skip("no partitions found")
    expected = list(range(ks[0], ks[-1] + _PARTITION_10K_SIZE, _PARTITION_10K_SIZE))
    missing = sorted(set(expected) - set(ks))
    assert not missing, f"gaps: {missing[:10]}"


def test_each_partition_has_data_and_metadata():
    incomplete = []
    for m_dir in _output_dir().glob("1M=*"):
        if not m_dir.is_dir():
            continue
        for k_dir in m_dir.glob("10K=*"):
            if not k_dir.is_dir() or k_dir.name.endswith(".tmp"):
                continue
            if not ((k_dir / "data.parquet").exists() and (k_dir / "metadata.json").exists()):
                incomplete.append(f"{m_dir.name}/{k_dir.name}")
    assert not incomplete, incomplete[:10]


def test_1m_label_matches_10k_partition():
    mismatched = []
    for m_dir in _output_dir().glob("1M=*"):
        if not m_dir.is_dir():
            continue
        m_val = int(m_dir.name.split("=")[1])
        for k_dir in m_dir.glob("10K=*"):
            if not k_dir.is_dir() or k_dir.name.endswith(".tmp"):
                continue
            k_val = int(k_dir.name.split("=")[1])
            expected_m = (k_val // _PARTITION_1M_SIZE) * _PARTITION_1M_SIZE
            if expected_m != m_val:
                mismatched.append(f"{m_dir.name}/{k_dir.name}")
    assert not mismatched, mismatched[:10]
