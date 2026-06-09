"""Validate that token_id_map_v1 covers every consecutive 10K partition with no gaps.

The derived dataset must include the Polymarket start partition
(``1M=33000000/10K=33600000``, the partition containing ``SCRAPE_START_BLOCK``)
and then every subsequent 10K partition in strict 10,000-block steps with no
gaps, up to the highest landed partition. Each partition is its own immutable
folder containing ``data.parquet`` (possibly zero rows) and ``metadata.json``.
"""
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


def _output_dir() -> Path:
    val = os.environ.get("TOKEN_ID_MAP_V1_DIR", "")
    if not val:
        pytest.skip("TOKEN_ID_MAP_V1_DIR is not set")
    out = Path(val)
    if not out.exists():
        pytest.skip(f"TOKEN_ID_MAP_V1_DIR does not exist: {out}")
    return out


def _landed_10k_partitions(out: Path) -> list[int]:
    """Return sorted 10K start blocks of every landed partition folder."""
    ks: list[int] = []
    for m_dir in out.glob("1M=*"):
        if not m_dir.is_dir():
            continue
        for k_dir in m_dir.glob("10K=*"):
            if not k_dir.is_dir():
                continue
            ks.append(int(k_dir.name.split("=")[1]))
    return sorted(ks)


def test_starts_at_polymarket_start_partition():
    """The first landed partition is the one containing SCRAPE_START_BLOCK."""
    out = _output_dir()
    ks = _landed_10k_partitions(out)
    assert ks, "no partitions found in token_id_map_v1"
    assert ks[0] == START_PARTITION_10K, (
        f"first partition is 10K={ks[0]}, expected 10K={START_PARTITION_10K} "
        f"(the partition containing SCRAPE_START_BLOCK={SCRAPE_START_BLOCK})"
    )


def test_partitions_are_consecutive_no_gaps():
    """Every 10K partition from the start to the max is present, no gaps."""
    out = _output_dir()
    ks = _landed_10k_partitions(out)
    assert ks, "no partitions found in token_id_map_v1"

    expected = list(range(ks[0], ks[-1] + _PARTITION_10K_SIZE, _PARTITION_10K_SIZE))
    missing = sorted(set(expected) - set(ks))
    assert not missing, (
        f"gaps detected: {len(missing)} missing 10K partitions; "
        f"first few: {missing[:10]}"
    )


def test_each_partition_has_data_and_metadata():
    """Every landed partition folder has both data.parquet and metadata.json."""
    out = _output_dir()
    incomplete: list[str] = []
    for m_dir in out.glob("1M=*"):
        if not m_dir.is_dir():
            continue
        for k_dir in m_dir.glob("10K=*"):
            if not k_dir.is_dir():
                continue
            has_data = (k_dir / "data.parquet").exists()
            has_meta = (k_dir / "metadata.json").exists()
            if not (has_data and has_meta):
                incomplete.append(f"{m_dir.name}/{k_dir.name} (data={has_data}, metadata={has_meta})")
    assert not incomplete, f"partitions missing data.parquet or metadata.json: {incomplete[:10]}"


def test_1m_label_matches_10k_partition():
    """Each 10K partition lives under the correct 1M parent folder."""
    out = _output_dir()
    mismatched: list[str] = []
    for m_dir in out.glob("1M=*"):
        if not m_dir.is_dir():
            continue
        m_val = int(m_dir.name.split("=")[1])
        for k_dir in m_dir.glob("10K=*"):
            if not k_dir.is_dir():
                continue
            k_val = int(k_dir.name.split("=")[1])
            expected_m = (k_val // _PARTITION_1M_SIZE) * _PARTITION_1M_SIZE
            if expected_m != m_val:
                mismatched.append(f"{m_dir.name}/{k_dir.name} (expected 1M={expected_m})")
    assert not mismatched, f"10K partitions under wrong 1M parent: {mismatched[:10]}"
