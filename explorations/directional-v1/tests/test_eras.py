"""Required Stage B generalization eras."""

from __future__ import annotations

import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))

from eras import ERAS, REQUIRED_CATEGORIES  # noqa: E402


def test_required_categories_present():
    cats = {e["category"] for e in ERAS}
    assert set(REQUIRED_CATEGORIES).issubset(cats)


def test_era_ids_unique_and_ordered_bounds():
    ids = [e["id"] for e in ERAS]
    assert len(ids) == len(set(ids))
    for e in ERAS:
        assert e["start"] < e["end"], e["id"]
        assert e["id"]
        assert e["title"]
        assert e["why"]


def test_military_before_after_pairs():
    pairs = [
        ("oct7_before", "oct7_after"),
        ("yemen_before", "yemen_after"),
        ("rough_rider_before", "rough_rider_after"),
        ("iran_before", "iran_after"),
    ]
    by_id = {e["id"]: e for e in ERAS}
    for a, b in pairs:
        assert by_id[a]["end"] == by_id[b]["start"], (a, b)
        assert by_id[a]["category"] == "us_military"


def test_covers_named_seasons():
    ids = {e["id"] for e in ERAS}
    assert "us_election_2024" in ids
    assert "fifa_wc_2022" in ids and "fifa_wc_2026" in ids
    assert {"sb_2023", "sb_2024", "sb_2025", "sb_2026"}.issubset(ids)
    assert "cwc_2025" in ids
    assert "ticket_shock_2024" in ids
    assert "inauguration_2025" in ids
