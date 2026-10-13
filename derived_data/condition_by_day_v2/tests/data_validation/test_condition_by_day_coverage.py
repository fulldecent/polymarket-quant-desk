"""Contiguous UTC-day coverage for condition_by_day_v2.

A missing day inside the manifested range is a failure.
"""

from __future__ import annotations

import os
import sys
from pathlib import Path

import pytest
from dotenv import load_dotenv

_project_root = Path(__file__).resolve().parents[4]
load_dotenv(_project_root / ".env")
sys.path.insert(0, str(_project_root))

from lib.ymd_partition import (  # noqa: E402
    assert_contiguous_day_keys,
    day_key_from_hive_dir,
    iter_manifest_day_keys,
)

ENV = "CONDITION_BY_DAY_V2_DIR"
RAW_ENV = "POLYGON_CONTRACT_EVENTS_V4_DIR"


def _output_dir() -> Path:
    val = os.environ.get(ENV, "")
    if not val:
        pytest.skip(f"{ENV} is not set")
    out = Path(val)
    if not out.exists():
        pytest.skip(f"{ENV} does not exist: {out}")
    return out


def _landed_days(out: Path) -> list[int]:
    keys = []
    for day_path in out.glob("year=*/month=*/day=*"):
        if not day_path.is_dir() or day_path.name.endswith(".tmp"):
            continue
        keys.append(day_key_from_hive_dir(day_path))
    return sorted(set(keys))


def _manifested_days() -> list[int]:
    raw = os.environ.get(RAW_ENV, "")
    if not raw:
        pytest.skip(f"{RAW_ENV} is not set")
    keys = iter_manifest_day_keys(raw)
    if not keys:
        pytest.skip("no raw manifests")
    assert_contiguous_day_keys(keys)
    return keys


def test_landed_prefix_matches_manifests():
    landed = _landed_days(_output_dir())
    if not landed:
        pytest.skip("no day folders found")
    manifested = _manifested_days()
    assert landed[0] == manifested[0], (
        f"first landed day {landed[0]} != first manifested day {manifested[0]}"
    )
    expected = [key for key in manifested if key <= landed[-1]]
    landed_set = set(landed)
    missing = [key for key in expected if key not in landed_set]
    extra = [key for key in landed if key not in set(expected)]
    assert not missing, f"missing days inside manifested range: {missing[:10]}"
    assert not extra, f"landed days outside manifested prefix: {extra[:10]}"


def test_each_day_has_data_and_metadata():
    incomplete = []
    for day_path in _output_dir().glob("year=*/month=*/day=*"):
        if not day_path.is_dir() or day_path.name.endswith(".tmp"):
            continue
        if not ((day_path / "data.parquet").exists() and (day_path / "metadata.json").exists()):
            incomplete.append(day_path.as_posix())
    assert not incomplete, incomplete[:10]
