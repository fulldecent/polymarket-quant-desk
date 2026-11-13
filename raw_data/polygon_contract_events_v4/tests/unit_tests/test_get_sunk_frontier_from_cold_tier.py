"""The sunk frontier is the last contiguous manifested day's max_block."""

import sys
from pathlib import Path

import pytest

sys.path.insert(0, str(Path(__file__).resolve().parent))

from day_layout import D0, D1, D2, bounds, plant_day
from raw_data.polygon_contract_events_v4 import get_sunk_frontier
from raw_data.polygon_contract_events_v4._internal.errors import V4Error
from raw_data.polygon_contract_events_v4._internal.tables import SCRAPE_START_BLOCK


def test_no_manifests_leave_the_frontier_at_scrape_start(tmp_path: Path) -> None:
    assert get_sunk_frontier(str(tmp_path)) == SCRAPE_START_BLOCK - 1


def test_contiguous_manifests_end_at_the_last_days_max_block(tmp_path: Path) -> None:
    first0, last0 = bounds(0)
    _first1, last1 = bounds(1)
    plant_day(tmp_path, D0, first0, last0, manifest=True)
    assert get_sunk_frontier(str(tmp_path)) == last0

    plant_day(tmp_path, D1, *bounds(1), manifest=True)
    assert get_sunk_frontier(str(tmp_path)) == last1


def test_a_skipped_day_is_not_a_frontier(tmp_path: Path) -> None:
    plant_day(tmp_path, D0, *bounds(0), manifest=True)
    plant_day(tmp_path, D2, *bounds(2), manifest=True)

    with pytest.raises(V4Error, match="skip"):
        get_sunk_frontier(str(tmp_path))


def test_the_first_manifest_must_be_the_header_day_when_one_is_given(
    tmp_path: Path,
) -> None:
    plant_day(tmp_path, D1, *bounds(1), manifest=True)

    with pytest.raises(V4Error, match="lowest manifest"):
        get_sunk_frontier(str(tmp_path), first_day_key=D0)
