from pathlib import Path

import pytest

from raw_data.polygon_contract_events_v3 import get_sunk_frontier
from raw_data.polygon_contract_events_v3._internal.errors import V3Error
from raw_data.polygon_contract_events_v3._internal.tables import (
    PARTITION_SIZE_10K,
    PARTITION_SIZE_1M,
    SCRAPE_START_BLOCK,
)


def _write_manifest_success(cold_root: Path, partition_start: int) -> None:
    k1m = (partition_start // PARTITION_SIZE_1M) * PARTITION_SIZE_1M
    manifest_path = (
        cold_root
        / "manifests"
        / f"1M={k1m}"
        / f"10K={partition_start}"
        / "_SUCCESS"
    )
    manifest_path.parent.mkdir(parents=True, exist_ok=True)
    manifest_path.write_bytes(b"")


def test_get_sunk_frontier_uses_contiguous_manifest_success(tmp_path: Path) -> None:
    p0 = (SCRAPE_START_BLOCK // PARTITION_SIZE_10K) * PARTITION_SIZE_10K

    assert get_sunk_frontier(str(tmp_path)) == SCRAPE_START_BLOCK - 1

    _write_manifest_success(tmp_path, p0)
    assert get_sunk_frontier(str(tmp_path)) == p0 + PARTITION_SIZE_10K - 1

    _write_manifest_success(tmp_path, p0 + PARTITION_SIZE_10K)
    assert get_sunk_frontier(str(tmp_path)) == p0 + (2 * PARTITION_SIZE_10K) - 1


def test_get_sunk_frontier_fails_fast_on_manifest_gap(tmp_path: Path) -> None:
    p0 = (SCRAPE_START_BLOCK // PARTITION_SIZE_10K) * PARTITION_SIZE_10K

    _write_manifest_success(tmp_path, p0)
    _write_manifest_success(tmp_path, p0 + 2 * PARTITION_SIZE_10K)

    with pytest.raises(V3Error, match="non-contiguous manifest frontier"):
        get_sunk_frontier(str(tmp_path))
