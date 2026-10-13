"""The manifest is what makes a UTC day sunk, so nothing may outrun it."""

import shutil
import sys
from pathlib import Path

import pytest

sys.path.insert(0, str(Path(__file__).resolve().parents[2]))
sys.path.insert(0, str(Path(__file__).resolve().parents[4]))
sys.path.insert(0, str(Path(__file__).resolve().parent))

from _internal.errors import V4Error
from _internal.parquet_sink import (
    _expected_targets_for_day,
    get_sunk_frontier,
    publish_manifest,
)
from _internal.tables import SCRAPE_START_BLOCK
from day_layout import D0, D1, D2, bounds, manifest_path, plant_day
from lib.ymd_partition import day_dir


def test_parquet_on_disk_alone_does_not_move_the_frontier(tmp_path):
    plant_day(tmp_path, D0, *bounds(0), manifest=True)
    plant_day(tmp_path, D1, *bounds(1))

    assert get_sunk_frontier(str(tmp_path)) == bounds(0)[1]


def test_publishing_declares_the_day_sunk(tmp_path):
    plant_day(tmp_path, D0, *bounds(0), manifest=True)
    plant_day(tmp_path, D1, *bounds(1))

    assert publish_manifest(str(tmp_path), D1) is True
    assert get_sunk_frontier(str(tmp_path)) == bounds(1)[1]


def test_incomplete_day_is_not_declared_sunk(tmp_path):
    plant_day(tmp_path, D0, *bounds(0), manifest=True)
    plant_day(tmp_path, D1, *bounds(1), withhold=1)

    assert publish_manifest(str(tmp_path), D1) is False
    assert not manifest_path(tmp_path, D1).is_file()
    assert get_sunk_frontier(str(tmp_path)) == bounds(0)[1]


def test_publishing_is_idempotent_so_a_crash_can_resume(tmp_path):
    plant_day(tmp_path, D0, *bounds(0), manifest=True)
    plant_day(tmp_path, D1, *bounds(1))
    assert publish_manifest(str(tmp_path), D1) is True

    assert publish_manifest(str(tmp_path), D1) is True
    assert get_sunk_frontier(str(tmp_path)) == bounds(1)[1]


def test_frontier_refuses_to_read_past_a_hole_in_the_manifests(tmp_path):
    plant_day(tmp_path, D0, *bounds(0), manifest=True)
    plant_day(tmp_path, D1, *bounds(1), manifest=True)
    plant_day(tmp_path, D2, *bounds(2))

    shutil.rmtree(tmp_path / "manifests" / day_dir(D1))
    hole = tmp_path / "manifests" / day_dir(D2)
    hole.mkdir(parents=True)
    (hole / "_SUCCESS").write_bytes(b"")

    with pytest.raises(V4Error, match="skip"):
        get_sunk_frontier(str(tmp_path))


def test_a_day_cannot_be_declared_sunk_before_its_predecessor(tmp_path):
    plant_day(tmp_path, D0, *bounds(0), manifest=True)
    plant_day(tmp_path, D1, *bounds(1))
    plant_day(tmp_path, D2, *bounds(2))

    with pytest.raises(V4Error, match="refusing to declare"):
        publish_manifest(str(tmp_path), D2)

    assert not manifest_path(tmp_path, D2).is_file()
    assert get_sunk_frontier(str(tmp_path)) == bounds(0)[1]


def test_the_manifest_not_the_file_listing_is_the_frontier(tmp_path):
    plant_day(tmp_path, D0, *bounds(0), manifest=True)
    plant_day(tmp_path, D1, *bounds(1))
    assert publish_manifest(str(tmp_path), D1) is True
    frontier = get_sunk_frontier(str(tmp_path))

    contract, event = _expected_targets_for_day(bounds(1)[1])[0]
    shutil.rmtree(tmp_path / contract / event / day_dir(D1))

    assert get_sunk_frontier(str(tmp_path)) == frontier


def test_an_empty_day_is_not_declared_sunk(tmp_path):
    tmp_path.mkdir(parents=True, exist_ok=True)
    assert publish_manifest(str(tmp_path), D0) is False
    assert not manifest_path(tmp_path, D0).is_file()
    assert get_sunk_frontier(str(tmp_path)) == SCRAPE_START_BLOCK - 1


def test_the_first_span_expects_contracts_deployed_by_its_last_block():
    contracts = {c for c, _ in _expected_targets_for_day(bounds(0)[1])}
    assert "ConditionalTokens" in contracts
    assert "CTFExchange" in contracts
    assert "UmaCtfAdapter" in contracts
    assert "FeeModuleCTF" not in contracts
