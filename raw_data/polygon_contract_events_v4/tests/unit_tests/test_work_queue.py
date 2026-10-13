"""Carving requests out of the pending ranges must never lose or duplicate a block."""

import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parents[2]))
sys.path.insert(0, str(Path(__file__).resolve().parents[4]))

import main as scraper


def blocks_covered(ranges, *, taken=()):
    """Every block still queued, plus every block handed out."""
    covered = set()
    for from_b, to_b in list(ranges) + list(taken):
        covered.update(range(from_b, to_b + 1))
    return covered


def test_carving_takes_the_span_off_the_front():
    ranges = [(100, 999)]

    chunk = scraper._take_chunk(ranges, 100)

    assert chunk == (100, 199)
    assert ranges == [(200, 999)]


def test_carving_a_whole_range_removes_it():
    ranges = [(100, 199), (500, 599)]

    chunk = scraper._take_chunk(ranges, 100)

    assert chunk == (100, 199)
    assert ranges == [(500, 599)]


def test_a_span_wider_than_the_range_is_clipped_to_it():
    """Gaps are often far smaller than the span density allows; the extra must not spill over."""
    ranges = [(100, 163), (500, 599)]

    chunk = scraper._take_chunk(ranges, 10_000)

    assert chunk == (100, 163), "must not read past the end of a gap"
    assert ranges == [(500, 599)]


def test_a_returned_chunk_merges_back_with_its_remainder():
    ranges = [(100, 999)]
    chunk = scraper._take_chunk(ranges, 100)

    scraper._return_chunk(ranges, chunk)

    assert ranges == [(100, 999)], "a refused chunk should rejoin the gap it came from"


def test_a_returned_chunk_goes_back_even_with_nothing_left_behind():
    ranges = []

    scraper._return_chunk(ranges, (100, 199))

    assert ranges == [(100, 199)]


def test_a_returned_chunk_does_not_merge_across_a_hole():
    ranges = [(500, 599)]

    scraper._return_chunk(ranges, (100, 199))

    assert ranges == [(100, 199), (500, 599)], "separate gaps must stay separate"


def test_no_block_is_lost_or_duplicated_while_working_through_fragmented_gaps():
    # Shaped like a real run: a few wide gaps and several tiny ones.
    original = [(1_000, 3_781), (3_848, 3_911), (3_976, 4_606), (4_794, 4_924), (5_090, 5_187)]
    ranges = list(original)
    expected = blocks_covered(original)

    handed_out = []
    spans = [7, 5_000, 1, 64, 300, 2, 10_000, 33]
    step = 0
    while ranges:
        chunk = scraper._take_chunk(ranges, spans[step % len(spans)])
        step += 1
        # Refuse every third chunk, as the provider would.
        if step % 3 == 0:
            scraper._return_chunk(ranges, chunk)
        else:
            handed_out.append(chunk)
        assert blocks_covered(ranges, taken=handed_out) == expected, "coverage changed at step"

    assert blocks_covered([], taken=handed_out) == expected
    assert sum(t - f + 1 for f, t in handed_out) == len(expected), "a block was scraped twice"


def _lane(**overrides):
    lane = scraper._RpcLane(
        index=0,
        url="https://example.invalid",
        label="example",
        client=None,
    )
    lane.pool = object()
    for key, value in overrides.items():
        setattr(lane, key, value)
    return lane


def test_a_parked_lane_does_not_accept_work_until_the_cooldown_ends(monkeypatch):
    lane = _lane()
    monkeypatch.setattr(scraper.time, "monotonic", lambda: 100.0)

    scraper._park_lane_after_failure(lane)

    assert lane.cooldown_until == 101.0
    assert scraper._lane_accepts_work(lane, now=100.5) is False
    assert scraper._lane_accepts_work(lane, now=101.0) is True


def test_a_long_throttle_sleep_is_the_cooldown(monkeypatch):
    lane = _lane()
    lane.optimizer._active = lane.optimizer.params.__class__(
        workers=1,
        result_budget=1,
        sleep_after_response_sec=5.0,
    )
    monkeypatch.setattr(scraper.time, "monotonic", lambda: 100.0)

    scraper._park_lane_after_failure(lane)

    assert lane.cooldown_until == 105.0
    assert scraper._lanes_cooling_down([lane], 104.0) == [lane]
    assert scraper._lanes_cooling_down([lane], 105.0) == []
