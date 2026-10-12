from pathlib import Path

import pytest

from raw_data.polygon_contract_events_v4._internal.errors import V4Error
from raw_data.polygon_contract_events_v4._internal.parquet_sink import (
    _expected_targets_for_partition,
    cleanup_temp_dirs_after_frontier,
    get_sunk_frontier,
    roll_forward_manifests_to_exhaustion,
)
from raw_data.polygon_contract_events_v4._internal.tables import (
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

    with pytest.raises(V4Error, match="missing required metadata.json"):
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


def test_roll_forward_does_not_publish_first_partition_on_empty_cold(tmp_path: Path) -> None:
    p0 = (SCRAPE_START_BLOCK // PARTITION_SIZE_10K) * PARTITION_SIZE_10K
    written = roll_forward_manifests_to_exhaustion(str(tmp_path))

    assert written == 0
    assert get_sunk_frontier(str(tmp_path)) == SCRAPE_START_BLOCK - 1
    assert not _manifest_success_path(tmp_path, p0).is_file()


def _aligned_partitions() -> tuple[int, int, int]:
    p0 = (SCRAPE_START_BLOCK // PARTITION_SIZE_10K) * PARTITION_SIZE_10K
    return p0, p0 + PARTITION_SIZE_10K, p0 + 2 * PARTITION_SIZE_10K


def _plant_success(cold_root: Path, partition_start: int) -> None:
    success = _manifest_success_path(cold_root, partition_start)
    success.parent.mkdir(parents=True, exist_ok=True)
    success.write_bytes(b"")


def _event_dir(cold_root: Path, contract: str, event: str, partition_start: int) -> Path:
    k1m = (partition_start // PARTITION_SIZE_1M) * PARTITION_SIZE_1M
    return cold_root / contract / event / f"1M={k1m}" / f"10K={partition_start}"


def _write_expected_targets(
    cold_root: Path,
    partition_start: int,
    *,
    with_metadata: bool = True,
    skip_first_meta: bool = False,
) -> list[Path]:
    folders: list[Path] = []
    first = True
    for contract, event in _expected_targets_for_partition(partition_start):
        folder = _event_dir(cold_root, contract, event, partition_start)
        folder.mkdir(parents=True, exist_ok=True)
        (folder / "data.parquet").write_bytes(b"PAR")
        if with_metadata and not (skip_first_meta and first):
            (folder / "metadata.json").write_text("{}", encoding="utf-8")
        folders.append(folder)
        first = False
    return folders


def test_cleanup_wipes_parquet_only_unpublished_partition(tmp_path: Path) -> None:
    p0, p1, _p2 = _aligned_partitions()
    _plant_success(tmp_path, p0)
    folders = _write_expected_targets(tmp_path, p1, with_metadata=False)

    cleanup_temp_dirs_after_frontier(str(tmp_path))
    written = roll_forward_manifests_to_exhaustion(str(tmp_path))

    assert written == 0
    assert not _manifest_success_path(tmp_path, p1).is_file()
    for folder in folders:
        assert not folder.exists()
    assert _manifest_success_path(tmp_path, p0).is_file()


def test_cleanup_wipes_mixed_unpublished_partition_including_complete_siblings(
    tmp_path: Path,
) -> None:
    p0, p1, _p2 = _aligned_partitions()
    _plant_success(tmp_path, p0)
    folders = _write_expected_targets(tmp_path, p1, skip_first_meta=True)

    cleanup_temp_dirs_after_frontier(str(tmp_path))
    written = roll_forward_manifests_to_exhaustion(str(tmp_path))

    assert written == 0
    assert not _manifest_success_path(tmp_path, p1).is_file()
    for folder in folders:
        assert not folder.exists()


def test_cleanup_leaves_complete_unpublished_partition_for_roll_forward(
    tmp_path: Path,
) -> None:
    p0, p1, _p2 = _aligned_partitions()
    _plant_success(tmp_path, p0)
    folders = _write_expected_targets(tmp_path, p1, with_metadata=True)

    cleanup_temp_dirs_after_frontier(str(tmp_path))
    written = roll_forward_manifests_to_exhaustion(str(tmp_path))

    assert written == 1
    assert _manifest_success_path(tmp_path, p1).is_file()
    for folder in folders:
        assert (folder / "data.parquet").is_file()
        assert (folder / "metadata.json").is_file()


def test_cleanup_does_not_touch_sunk_partition_folders(tmp_path: Path) -> None:
    p0, p1, _p2 = _aligned_partitions()
    _plant_success(tmp_path, p0)
    sunk = _write_expected_targets(tmp_path, p0, with_metadata=True)
    _write_expected_targets(tmp_path, p1, with_metadata=False)

    cleanup_temp_dirs_after_frontier(str(tmp_path))

    for folder in sunk:
        assert (folder / "data.parquet").is_file()
        assert (folder / "metadata.json").is_file()
    assert _manifest_success_path(tmp_path, p0).is_file()


def test_cleanup_wipes_later_dirty_10k_and_leaves_complete_first_unsunk(
    tmp_path: Path,
) -> None:
    p0, p1, p2 = _aligned_partitions()
    _plant_success(tmp_path, p0)
    complete = _write_expected_targets(tmp_path, p1, with_metadata=True)
    dirty = _write_expected_targets(tmp_path, p2, with_metadata=False)

    cleanup_temp_dirs_after_frontier(str(tmp_path))
    written = roll_forward_manifests_to_exhaustion(str(tmp_path))

    assert written == 1
    assert _manifest_success_path(tmp_path, p1).is_file()
    assert not _manifest_success_path(tmp_path, p2).is_file()
    for folder in complete:
        assert folder.exists()
    for folder in dirty:
        assert not folder.exists()


def test_cleanup_then_roll_forward_recovers_from_missing_metadata(tmp_path: Path) -> None:
    p0, p1, _p2 = _aligned_partitions()
    _plant_success(tmp_path, p0)
    _write_expected_targets(tmp_path, p1, skip_first_meta=True)

    cleanup_temp_dirs_after_frontier(str(tmp_path))
    written = roll_forward_manifests_to_exhaustion(str(tmp_path))

    assert written == 0
    assert not any(tmp_path.glob(f"**/10K={p1}"))
