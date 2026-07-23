from pathlib import Path

import pytest

from raw_data.polygon_contract_events_v3._internal.errors import V3Error
from raw_data.polygon_contract_events_v3._internal.parquet_sink import (
    _expected_targets_for_partition,
    cleanup_temp_dirs_after_frontier,
    roll_forward_manifests_to_exhaustion,
)
from raw_data.polygon_contract_events_v3._internal.tables import (
    PARTITION_SIZE_10K,
    PARTITION_SIZE_1M,
    SCRAPE_START_BLOCK,
)


def _manifest_success_path(cold_root: Path, partition_start: int) -> Path:
    k1m = (partition_start // PARTITION_SIZE_1M) * PARTITION_SIZE_1M
    return (
        cold_root
        / "manifests"
        / f"1M={k1m}"
        / f"10K={partition_start}"
        / "_SUCCESS"
    )


def test_roll_forward_fails_fast_when_expected_partition_missing_metadata(tmp_path: Path) -> None:
    cold_root = tmp_path
    p0 = (SCRAPE_START_BLOCK // PARTITION_SIZE_10K) * PARTITION_SIZE_10K
    p1 = p0 + PARTITION_SIZE_10K
    k1m = (p1 // PARTITION_SIZE_1M) * PARTITION_SIZE_1M

    # Existing frontier at p0 so roll-forward evaluates p1 next.
    success0 = _manifest_success_path(cold_root, p0)
    success0.parent.mkdir(parents=True, exist_ok=True)
    success0.write_bytes(b"")

    expected_targets = _expected_targets_for_partition(p1)
    assert expected_targets, "expected at least one target for p1"

    # Create every expected folder for p1 with data.parquet present.
    # Leave exactly one missing metadata.json to trigger fail-fast.
    first = True
    for contract, event in expected_targets:
        partition_dir = cold_root / contract / event / f"1M={k1m}" / f"10K={p1}"
        partition_dir.mkdir(parents=True, exist_ok=True)
        (partition_dir / "data.parquet").write_bytes(b"PAR1")
        if not first:
            (partition_dir / "metadata.json").write_text("{}", encoding="utf-8")
        first = False

    with pytest.raises(V3Error, match="missing required metadata.json"):
        roll_forward_manifests_to_exhaustion(str(cold_root))


def test_cleanup_temp_dirs_after_frontier_removes_tmp_and_manifest_partition(tmp_path: Path) -> None:
    cold_root = tmp_path
    p0 = (SCRAPE_START_BLOCK // PARTITION_SIZE_10K) * PARTITION_SIZE_10K
    p1 = p0 + PARTITION_SIZE_10K
    k1m = (p1 // PARTITION_SIZE_1M) * PARTITION_SIZE_1M

    # Frontier at p0.
    success0 = _manifest_success_path(cold_root, p0)
    success0.parent.mkdir(parents=True, exist_ok=True)
    success0.write_bytes(b"")

    # Create a contract/event tmp folder for first unsunk partition p1.
    tmp_dir = cold_root / "ConditionalTokens" / "condition_preparation" / f"1M={k1m}" / f"10K={p1}.tmp"
    tmp_dir.mkdir(parents=True, exist_ok=True)

    # Create defensive manifest tmp folder for p1.
    manifest_tmp = cold_root / "manifests" / f"1M={k1m}" / f"10K={p1}.tmp"
    manifest_tmp.mkdir(parents=True, exist_ok=True)

    removed = cleanup_temp_dirs_after_frontier(str(cold_root))

    assert removed >= 2
    assert not tmp_dir.exists()
    assert not manifest_tmp.exists()


def test_roll_forward_treats_empty_partition_dir_as_incomplete(tmp_path: Path) -> None:
    cold_root = tmp_path
    p0 = (SCRAPE_START_BLOCK // PARTITION_SIZE_10K) * PARTITION_SIZE_10K
    p1 = p0 + PARTITION_SIZE_10K
    k1m = (p1 // PARTITION_SIZE_1M) * PARTITION_SIZE_1M

    success0 = _manifest_success_path(cold_root, p0)
    success0.parent.mkdir(parents=True, exist_ok=True)
    success0.write_bytes(b"")

    expected_targets = _expected_targets_for_partition(p1)
    assert expected_targets, "expected at least one target for p1"

    contract, event = expected_targets[0]
    empty_partition_dir = cold_root / contract / event / f"1M={k1m}" / f"10K={p1}"
    empty_partition_dir.mkdir(parents=True, exist_ok=True)

    written = roll_forward_manifests_to_exhaustion(str(cold_root))

    assert written == 0
    assert not empty_partition_dir.exists()
