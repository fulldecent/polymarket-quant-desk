"""The manifest is what makes a partition sunk, so nothing may outrun it."""

import shutil
import sys
from pathlib import Path

import pytest

sys.path.insert(0, str(Path(__file__).resolve().parents[2]))
sys.path.insert(0, str(Path(__file__).resolve().parents[4]))

from _internal.errors import V3Error
from _internal.parquet_sink import (
    _expected_targets_for_partition,
    get_sunk_frontier,
    publish_manifest,
)
from _internal.tables import PARTITION_SIZE_10K, SCRAPE_START_BLOCK
from lib.partition_utils import partition_dir

# The first partition predates every contract deployment, so nothing is expected inside it and it
# publishes trivially. Real target folders only start with the partition after it.
FIRST = (SCRAPE_START_BLOCK // PARTITION_SIZE_10K) * PARTITION_SIZE_10K
SECOND = FIRST + PARTITION_SIZE_10K
THIRD = SECOND + PARTITION_SIZE_10K


def write_partition(cold_root: Path, partition_start: int, *, withhold: int = 0) -> None:
    """Lay down the Parquet output a sink worker produces, optionally leaving targets unwritten."""
    cold_root.mkdir(parents=True, exist_ok=True)
    targets = _expected_targets_for_partition(partition_start)
    assert targets, f"partition {partition_start} expects no targets; pick another"
    for contract, event in targets[: len(targets) - withhold]:
        d = cold_root / contract / event / partition_dir(partition_start)
        d.mkdir(parents=True, exist_ok=True)
        (d / "data.parquet").write_bytes(b"parquet")
        (d / "metadata.json").write_text("{}")


def manifest_exists(cold_root: Path, partition_start: int) -> bool:
    return (cold_root / "manifests" / partition_dir(partition_start) / "_SUCCESS").is_file()


def test_parquet_on_disk_alone_does_not_move_the_frontier(tmp_path):
    publish_manifest(str(tmp_path), FIRST)
    write_partition(tmp_path, SECOND)

    assert get_sunk_frontier(str(tmp_path)) == FIRST + PARTITION_SIZE_10K - 1, (
        "a partition with Parquet but no manifest is written, not sunk"
    )


def test_publishing_declares_the_partition_sunk(tmp_path):
    publish_manifest(str(tmp_path), FIRST)
    write_partition(tmp_path, SECOND)

    assert publish_manifest(str(tmp_path), SECOND) is True

    assert get_sunk_frontier(str(tmp_path)) == SECOND + PARTITION_SIZE_10K - 1


def test_incomplete_partition_is_not_declared_sunk(tmp_path):
    publish_manifest(str(tmp_path), FIRST)
    write_partition(tmp_path, SECOND, withhold=1)

    assert publish_manifest(str(tmp_path), SECOND) is False
    assert not manifest_exists(tmp_path, SECOND)
    assert get_sunk_frontier(str(tmp_path)) == FIRST + PARTITION_SIZE_10K - 1


def test_publishing_is_idempotent_so_a_crash_can_resume(tmp_path):
    publish_manifest(str(tmp_path), FIRST)
    write_partition(tmp_path, SECOND)
    assert publish_manifest(str(tmp_path), SECOND) is True

    # A crash between publishing and committing brings us back to the same partition.
    assert publish_manifest(str(tmp_path), SECOND) is True
    assert get_sunk_frontier(str(tmp_path)) == SECOND + PARTITION_SIZE_10K - 1


def test_frontier_refuses_to_read_past_a_hole_in_the_manifests(tmp_path):
    manifests = tmp_path / "manifests"
    write_partition(tmp_path, SECOND)
    write_partition(tmp_path, THIRD)
    publish_manifest(str(tmp_path), FIRST)
    publish_manifest(str(tmp_path), SECOND)

    # Fabricate the out-of-order state that must never be mistaken for progress.
    shutil.rmtree(manifests / partition_dir(SECOND))
    publish_dir = manifests / partition_dir(THIRD)
    publish_dir.mkdir(parents=True, exist_ok=True)
    (publish_dir / "_SUCCESS").write_bytes(b"")

    with pytest.raises(V3Error, match="non-contiguous"):
        get_sunk_frontier(str(tmp_path))


def test_a_partition_cannot_be_declared_sunk_before_its_predecessor(tmp_path):
    publish_manifest(str(tmp_path), FIRST)
    write_partition(tmp_path, SECOND)
    write_partition(tmp_path, THIRD)

    with pytest.raises(V3Error, match="refusing to declare"):
        publish_manifest(str(tmp_path), THIRD)

    assert not manifest_exists(tmp_path, THIRD)
    assert get_sunk_frontier(str(tmp_path)) == FIRST + PARTITION_SIZE_10K - 1


def test_the_manifest_not_the_file_listing_is_the_frontier(tmp_path):
    publish_manifest(str(tmp_path), FIRST)
    write_partition(tmp_path, SECOND)
    publish_manifest(str(tmp_path), SECOND)
    frontier = get_sunk_frontier(str(tmp_path))

    contract, event = _expected_targets_for_partition(SECOND)[0]
    shutil.rmtree(tmp_path / contract / event / partition_dir(SECOND))

    assert get_sunk_frontier(str(tmp_path)) == frontier
