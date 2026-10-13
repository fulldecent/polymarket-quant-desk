"""Startup cleanup and roll-forward follow UTC day folders."""

import sys
from pathlib import Path

import pytest

sys.path.insert(0, str(Path(__file__).resolve().parents[2]))
sys.path.insert(0, str(Path(__file__).resolve().parents[4]))
sys.path.insert(0, str(Path(__file__).resolve().parent))

from _internal.errors import V4Error
from _internal.parquet_sink import (
    _expected_targets_for_day,
    cleanup_temp_dirs_after_frontier,
    get_sunk_frontier,
    roll_forward_manifests_to_exhaustion,
)
from _internal.tables import SCRAPE_START_BLOCK
from day_layout import (
    D0,
    D1,
    D2,
    bounds,
    day_tmp_rel,
    manifest_path,
    plant_day,
)
from lib.ymd_partition import day_dir


def test_roll_forward_fails_fast_when_a_target_is_missing_metadata(tmp_path: Path) -> None:
    plant_day(tmp_path, D0, *bounds(0), manifest=True)
    plant_day(tmp_path, D1, *bounds(1), skip_first_metadata=True)

    with pytest.raises(V4Error, match="missing required metadata.json"):
        roll_forward_manifests_to_exhaustion(str(tmp_path))


def test_cleanup_removes_day_tmp_folders_above_the_frontier(tmp_path: Path) -> None:
    plant_day(tmp_path, D0, *bounds(0), manifest=True)
    event_tmp = tmp_path / "ConditionalTokens" / "condition_preparation" / day_tmp_rel(D1)
    event_tmp.mkdir(parents=True)
    manifest_tmp = tmp_path / "manifests" / day_tmp_rel(D1)
    manifest_tmp.mkdir(parents=True)

    removed = cleanup_temp_dirs_after_frontier(str(tmp_path))

    assert removed >= 2
    assert not event_tmp.exists()
    assert not manifest_tmp.exists()


def test_roll_forward_removes_an_empty_target_folder(tmp_path: Path) -> None:
    plant_day(tmp_path, D0, *bounds(0), manifest=True)
    targets = _expected_targets_for_day(bounds(1)[1])
    plant_day(tmp_path, D1, *bounds(1))
    empty = tmp_path / targets[0][0] / targets[0][1] / day_dir(D1)
    for child in empty.iterdir():
        child.unlink()

    written = roll_forward_manifests_to_exhaustion(str(tmp_path))

    assert written == 0
    assert not empty.exists()


def test_roll_forward_does_not_invent_a_first_day(tmp_path: Path) -> None:
    written = roll_forward_manifests_to_exhaustion(str(tmp_path))

    assert written == 0
    assert get_sunk_frontier(str(tmp_path)) == SCRAPE_START_BLOCK - 1
    assert not manifest_path(tmp_path, D0).is_file()


def test_roll_forward_with_the_header_day_still_needs_files(tmp_path: Path) -> None:
    written = roll_forward_manifests_to_exhaustion(str(tmp_path), first_day_key=D0)

    assert written == 0
    assert not manifest_path(tmp_path, D0).is_file()


def test_cleanup_wipes_parquet_only_unpublished_day(tmp_path: Path) -> None:
    plant_day(tmp_path, D0, *bounds(0), manifest=True)
    folders = plant_day(tmp_path, D1, *bounds(1), parquet_only=True)

    cleanup_temp_dirs_after_frontier(str(tmp_path))
    written = roll_forward_manifests_to_exhaustion(str(tmp_path))

    assert written == 0
    assert not manifest_path(tmp_path, D1).is_file()
    for folder in folders:
        assert not folder.exists()
    assert manifest_path(tmp_path, D0).is_file()


def test_cleanup_wipes_a_day_that_is_only_partly_complete(tmp_path: Path) -> None:
    plant_day(tmp_path, D0, *bounds(0), manifest=True)
    folders = plant_day(tmp_path, D1, *bounds(1), skip_first_metadata=True)

    cleanup_temp_dirs_after_frontier(str(tmp_path))
    written = roll_forward_manifests_to_exhaustion(str(tmp_path))

    assert written == 0
    assert not manifest_path(tmp_path, D1).is_file()
    for folder in folders:
        assert not folder.exists()


def test_cleanup_leaves_a_complete_unpublished_day_for_roll_forward(tmp_path: Path) -> None:
    plant_day(tmp_path, D0, *bounds(0), manifest=True)
    folders = plant_day(tmp_path, D1, *bounds(1))

    cleanup_temp_dirs_after_frontier(str(tmp_path))
    written = roll_forward_manifests_to_exhaustion(str(tmp_path))

    assert written == 1
    assert manifest_path(tmp_path, D1).is_file()
    for folder in folders:
        assert (folder / "data.parquet").is_file()
        assert (folder / "metadata.json").is_file()


def test_cleanup_does_not_touch_folders_at_or_below_the_frontier(tmp_path: Path) -> None:
    plant_day(tmp_path, D0, *bounds(0), manifest=True)
    sunk = plant_day(tmp_path, D0, *bounds(0))
    plant_day(tmp_path, D1, *bounds(1), parquet_only=True)

    cleanup_temp_dirs_after_frontier(str(tmp_path))

    for folder in sunk:
        assert (folder / "data.parquet").is_file()
        assert (folder / "metadata.json").is_file()
    assert manifest_path(tmp_path, D0).is_file()


def test_cleanup_wipes_a_later_dirty_day_and_keeps_a_complete_one(tmp_path: Path) -> None:
    plant_day(tmp_path, D0, *bounds(0), manifest=True)
    complete = plant_day(tmp_path, D1, *bounds(1))
    dirty = plant_day(tmp_path, D2, *bounds(2), parquet_only=True)

    cleanup_temp_dirs_after_frontier(str(tmp_path))
    written = roll_forward_manifests_to_exhaustion(str(tmp_path))

    assert written == 1
    assert manifest_path(tmp_path, D1).is_file()
    assert not manifest_path(tmp_path, D2).is_file()
    for folder in complete:
        assert folder.exists()
    for folder in dirty:
        assert not folder.exists()


def test_cleanup_then_roll_forward_drops_a_day_missing_metadata(tmp_path: Path) -> None:
    plant_day(tmp_path, D0, *bounds(0), manifest=True)
    plant_day(tmp_path, D1, *bounds(1), skip_first_metadata=True)

    cleanup_temp_dirs_after_frontier(str(tmp_path))
    written = roll_forward_manifests_to_exhaustion(str(tmp_path))

    assert written == 0
    leaf = day_dir(D1).split("/")[-1]
    assert not any(path.name == leaf for path in tmp_path.rglob("day=*"))
