"""Ramping to the provider's envelope, then hill-climbing throughput around it."""

import sys
from pathlib import Path

import pytest

sys.path.insert(0, str(Path(__file__).resolve().parents[2]))

from _internal.epoch_optimizer import (
    AXES,
    LATENCY_DEGRADE_FACTOR,
    MIN_GAIN,
    EpochOptimizer,
    Params,
    Phase,
)


class FakeClock:
    def __init__(self) -> None:
        self.now = 0.0

    def __call__(self) -> float:
        return self.now

    def advance(self, seconds: float) -> None:
        self.now += seconds


def make_optimizer(**kwargs) -> tuple[EpochOptimizer, FakeClock]:
    clock = FakeClock()
    defaults = dict(worker_cap=64, clock=clock)
    defaults.update(kwargs)
    return EpochOptimizer(**defaults), clock


def feed_ok(opt: EpochOptimizer, count: int, *, logs: int = 10**9, blocks: int = 100,
            elapsed_sec: float = 0.1) -> None:
    for _ in range(count):
        opt.record_ok(blocks=blocks, logs=logs, elapsed_sec=elapsed_sec)


def finish_ramp(opt: EpochOptimizer) -> None:
    """Put the optimizer into the search phase without walking the ramp."""
    opt.record_timeout()
    opt.start_epoch()


def run_epoch(opt: EpochOptimizer, clock: FakeClock, blocks: int, seconds: float = 20.0):
    """Run one whole epoch at the current parameters and return its report."""
    opt.start_epoch()
    opt.record_blocks(blocks)
    clock.advance(seconds)
    return opt.end_epoch()


# ---------------------------------------------------------------------------
# Ramp
# ---------------------------------------------------------------------------

def test_starts_at_one_worker_one_log_and_no_sleep():
    opt, _ = make_optimizer()

    assert opt.params == Params(workers=1, result_budget=1, sleep_after_response_sec=0.0)
    assert opt.is_ramping


def test_budget_doubles_while_responses_keep_filling_it():
    opt, _ = make_optimizer()

    budgets = []
    for _ in range(5):
        opt.record_ok(blocks=100, logs=10**9, elapsed_sec=0.1)
        budgets.append(opt.params.result_budget)

    assert budgets == [2, 4, 8, 16, 32]


def test_a_sparse_response_does_not_grow_the_budget():
    """Early history returns almost nothing; that says nothing about how large a response may be."""
    opt, _ = make_optimizer()
    feed_ok(opt, 5)
    budget = opt.params.result_budget

    opt.record_ok(blocks=100_000, logs=1, elapsed_sec=0.1)

    assert opt.params.result_budget == budget


def test_workers_double_once_per_round_trip():
    opt, _ = make_optimizer()
    seen = []

    for _ in range(15):
        before = opt.params.workers
        opt.record_ok(blocks=100, logs=10**9, elapsed_sec=0.1)
        if opt.params.workers != before:
            seen.append(opt.params.workers)

    # Doublings land after 1, then 2, then 4, then 8 further responses: one per generation.
    assert seen == [2, 4, 8, 16], seen


def test_a_single_burst_cannot_multiply_concurrency_repeatedly():
    opt, _ = make_optimizer()
    feed_ok(opt, 4)
    workers = opt.params.workers

    feed_ok(opt, workers)  # one generation arriving at once

    assert opt.params.workers == workers * 2


def test_concurrency_ramp_stops_when_per_block_latency_climbs():
    opt, _ = make_optimizer()
    for _ in range(6):
        opt.record_ok(blocks=100, logs=10**9, elapsed_sec=1.0)
    workers_before = opt.params.workers

    for _ in range(30):
        # Same span, far slower: requests are queueing behind each other.
        opt.record_ok(blocks=100, logs=10**9, elapsed_sec=1.0 * LATENCY_DEGRADE_FACTOR * 5)

    assert opt.params.workers == workers_before


def test_a_bigger_response_is_not_mistaken_for_queueing():
    opt, _ = make_optimizer()

    # Latency rises in step with the blocks covered, which is what a bigger request should cost.
    blocks = 100
    for _ in range(8):
        opt.record_ok(blocks=blocks, logs=10**9, elapsed_sec=0.01 * blocks)
        blocks *= 2

    assert opt.params.workers > 2, "per-block latency was flat, so concurrency should keep growing"


def test_ramp_hands_over_to_the_search_once_both_parameters_stop():
    opt, _ = make_optimizer(worker_cap=2)
    feed_ok(opt, 3)
    assert opt.is_ramping

    opt.record_rejection()  # budget has met a wall

    assert opt.phase is Phase.SEARCH


def test_no_epoch_boundary_during_the_ramp():
    opt, clock = make_optimizer(min_epoch_sec=15.0)
    opt.start_epoch()

    clock.advance(10_000.0)

    assert opt.is_ramping
    assert not opt.should_end_epoch(), "the ramp measures nothing, so it has no deadline"


def test_the_ramp_hands_over_even_when_nothing_pushes_back():
    """A live run spent its whole length ramping and never measured anything."""
    opt, clock = make_optimizer(ramp_max_sec=60.0)
    feed_ok(opt, 5)
    assert opt.is_ramping

    clock.advance(61.0)
    opt.record_ok(blocks=100, logs=10**9, elapsed_sec=0.1)

    assert opt.phase is Phase.SEARCH


def test_the_ramp_is_not_cut_short_before_its_deadline():
    opt, clock = make_optimizer(ramp_max_sec=60.0)
    feed_ok(opt, 5)

    clock.advance(59.0)
    opt.record_ok(blocks=100, logs=10**9, elapsed_sec=0.1)

    assert opt.is_ramping


def test_throttling_stops_the_worker_ramp_but_not_the_budget_ramp():
    opt, _ = make_optimizer()
    feed_ok(opt, 4)
    budget_before = opt.params.result_budget

    opt.record_throttled(429)

    assert opt.is_ramping, "rate limiting says nothing about response size"
    opt.record_ok(blocks=100, logs=10**9, elapsed_sec=0.1)
    assert opt.params.result_budget > budget_before


# ---------------------------------------------------------------------------
# Search
# ---------------------------------------------------------------------------

def test_first_search_epoch_is_a_baseline_then_a_trial_follows():
    opt, clock = make_optimizer()
    finish_ramp(opt)

    report = run_epoch(opt, clock, blocks=100)

    assert report.reason == "baseline"
    assert report.score == pytest.approx(5.0)
    assert report.next_params != report.params


def test_score_is_blocks_per_second_over_the_whole_epoch():
    opt, clock = make_optimizer()
    finish_ramp(opt)

    report = run_epoch(opt, clock, blocks=900, seconds=30.0)

    assert report.blocks == 900
    assert report.duration_sec == pytest.approx(30.0)
    assert report.score == pytest.approx(30.0)


def test_improving_trial_is_committed_and_pushes_the_same_parameter_further():
    opt, clock = make_optimizer()
    finish_ramp(opt)
    run_epoch(opt, clock, blocks=100)

    incumbent = opt._incumbent
    trial = opt.params
    changed = [axis for axis in AXES if getattr(trial, axis) != getattr(incumbent, axis)]
    assert len(changed) == 1, "a trial changes exactly one parameter"

    report = run_epoch(opt, clock, blocks=400)

    assert report.accepted
    assert getattr(report.next_params, changed[0]) > getattr(trial, changed[0])


def test_trial_within_noise_is_not_committed():
    opt, clock = make_optimizer()
    finish_ramp(opt)
    run_epoch(opt, clock, blocks=100)

    report = run_epoch(opt, clock, blocks=int(100 * (1 + MIN_GAIN / 2)))

    assert not report.accepted


def test_rejected_trial_moves_on_rather_than_repeating_itself():
    opt, clock = make_optimizer()
    finish_ramp(opt)
    run_epoch(opt, clock, blocks=100)
    first_trial = opt.params

    run_epoch(opt, clock, blocks=1)

    assert opt.params != first_trial


def test_search_rotates_over_every_parameter():
    opt, clock = make_optimizer()
    finish_ramp(opt)
    run_epoch(opt, clock, blocks=100)

    touched = set()
    for _ in range(len(AXES) * 2):
        trial = opt.params
        touched.update(a for a in AXES if getattr(trial, a) != getattr(opt._incumbent, a))
        run_epoch(opt, clock, blocks=1)

    assert touched == set(AXES), touched


def test_stale_incumbent_is_re_measured_after_a_full_rotation():
    opt, clock = make_optimizer()
    finish_ramp(opt)
    run_epoch(opt, clock, blocks=100)

    reasons = [run_epoch(opt, clock, blocks=1).reason for _ in range(len(AXES) * 2)]

    assert "reverted, re-measuring" in reasons
    assert opt.params == opt._incumbent


def test_search_steps_are_finer_than_ramp_steps():
    opt, _ = make_optimizer()
    finish_ramp(opt)
    from_here = Params(workers=16, result_budget=1_000, sleep_after_response_sec=0.0)

    assert opt._step(from_here, "workers", 1).workers == 20
    assert EpochOptimizer._grow(from_here.workers, 256) == 32


def test_a_step_always_moves_at_least_one_whole_unit():
    opt, _ = make_optimizer()
    finish_ramp(opt)
    small = Params(workers=1, result_budget=1, sleep_after_response_sec=0.0)

    # 1 * 1.25 rounds back to 1, which would waste an epoch measuring the incumbent again.
    assert opt._step(small, "workers", 1).workers == 2
    assert opt._step(small, "result_budget", 1).result_budget == 2


# ---------------------------------------------------------------------------
# Adverse signals
# ---------------------------------------------------------------------------

def test_a_refused_range_ends_the_budget_ramp_and_spoils_the_epoch():
    opt, clock = make_optimizer()
    finish_ramp(opt)
    run_epoch(opt, clock, blocks=100)

    opt.start_epoch()
    opt.record_rejection()
    clock.advance(20.0)
    report = opt.end_epoch()

    assert not report.accepted
    assert "refused" in report.reason


def test_timeout_eases_off_concurrency_and_response_size():
    opt, clock = make_optimizer()
    finish_ramp(opt)
    opt._active = Params(workers=16, result_budget=800, sleep_after_response_sec=0.0)

    opt.start_epoch()
    opt.record_timeout()

    assert opt.params.workers == 8
    assert opt.params.result_budget == 400
    clock.advance(20.0)
    assert opt.end_epoch().reason == "backed off, request timed out"


def test_rate_limit_introduces_sleep_and_halves_workers():
    opt, _ = make_optimizer()
    finish_ramp(opt)
    opt._active = Params(workers=16, result_budget=800, sleep_after_response_sec=0.0)

    opt.record_throttled(429)

    assert opt.params.workers == 8
    assert opt.params.sleep_after_response_sec > 0.0


def test_repeated_rate_limits_keep_increasing_the_sleep():
    opt, _ = make_optimizer()
    finish_ramp(opt)
    opt._active = Params(workers=16, result_budget=800, sleep_after_response_sec=0.0)

    opt.record_throttled(429)
    first = opt.params.sleep_after_response_sec
    opt.record_throttled(429)

    assert opt.params.sleep_after_response_sec > first


def test_server_error_eases_concurrency_without_adding_sleep():
    opt, _ = make_optimizer()
    finish_ramp(opt)
    opt._active = Params(workers=16, result_budget=800, sleep_after_response_sec=0.0)

    opt.record_throttled(503)

    assert opt.params.workers == 8
    assert opt.params.sleep_after_response_sec == 0.0


def test_spoiled_epoch_is_re_measured_rather_than_scored():
    opt, clock = make_optimizer()
    finish_ramp(opt)
    run_epoch(opt, clock, blocks=100)

    opt.start_epoch()
    opt.record_blocks(999_999)
    opt.record_timeout()
    clock.advance(20.0)
    report = opt.end_epoch()

    assert not report.accepted
    assert report.next_params == opt.params
    # The inflated score must not become the bar that later epochs have to clear.
    assert run_epoch(opt, clock, blocks=100).reason == "baseline"


# ---------------------------------------------------------------------------
# Bounds and epoch sizing
# ---------------------------------------------------------------------------

def test_parameters_stay_within_their_bounds():
    opt, clock = make_optimizer(worker_cap=4)
    feed_ok(opt, 20)
    for _ in range(60):
        run_epoch(opt, clock, blocks=1_000)
        assert 1 <= opt.params.workers <= 4
        assert opt.params.result_budget >= 1
        assert 0.0 <= opt.params.sleep_after_response_sec <= 5.0


def test_concurrency_is_not_capped_at_some_default():
    """No flag is passed, so the ramp must be free to find a large worker count on its own."""
    opt, _ = make_optimizer(worker_cap=256)

    feed_ok(opt, 400)

    assert opt.params.workers > 64


def test_epoch_length_scales_with_request_latency():
    opt, _ = make_optimizer(min_epoch_sec=15.0, max_epoch_sec=120.0, epoch_latency_multiple=30.0)
    assert opt.epoch_duration_sec == 15.0, "no latency seen yet, so use the floor"

    opt.record_ok(blocks=1, logs=0, elapsed_sec=2.0)
    assert opt.epoch_duration_sec == pytest.approx(60.0)

    for _ in range(50):
        opt.record_ok(blocks=1, logs=0, elapsed_sec=30.0)
    assert opt.epoch_duration_sec == 120.0, "clamped so the search cannot stall"


def test_epoch_ends_only_once_its_duration_has_elapsed():
    opt, clock = make_optimizer(min_epoch_sec=15.0)
    finish_ramp(opt)
    opt.start_epoch()

    clock.advance(14.9)
    assert not opt.should_end_epoch()
    clock.advance(0.2)
    assert opt.should_end_epoch()


def test_rejects_impossible_construction():
    with pytest.raises(ValueError):
        EpochOptimizer(worker_cap=0)
    with pytest.raises(ValueError):
        EpochOptimizer(worker_cap=1, min_epoch_sec=30, max_epoch_sec=10)
