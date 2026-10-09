"""Sizing requests when event density varies by orders of magnitude along the chain."""

import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parents[2]))

from _internal.chunk_planner import GROWTH_FACTOR, INITIAL_SPAN, ChunkPlanner


def drive(planner: ChunkPlanner, *, budget: int, density: float, rounds: int,
          provider_result_limit: int | None = None,
          provider_span_limit: int | None = None) -> list[int]:
    """Run the planner against a modelled provider, returning the spans it asked for."""
    spans = []
    for _ in range(rounds):
        span = planner.next_span(budget)
        spans.append(span)
        logs = int(span * density)
        too_many = provider_result_limit is not None and logs > provider_result_limit
        too_wide = provider_span_limit is not None and span > provider_span_limit
        if too_many or too_wide:
            planner.record_rejection(blocks=span)
        else:
            planner.record_success(blocks=span, logs=logs)
    return spans


def test_starts_at_one_block():
    assert ChunkPlanner().next_span(10_000) == INITIAL_SPAN


def test_span_grows_geometrically_rather_than_jumping_at_an_unknown_limit():
    planner = ChunkPlanner()
    spans = drive(planner, budget=1_000_000, density=0.0, rounds=6)

    assert spans == [1, 2, 4, 8, 16, 32], spans


def test_sparse_history_reaches_very_wide_spans():
    """Early history holds about one event per thousand blocks; requests must widen accordingly."""
    planner = ChunkPlanner()
    drive(planner, budget=10_000, density=0.001, rounds=40)

    assert planner.next_span(10_000) > 100_000, planner.describe()


def test_dense_region_converges_on_the_result_budget():
    """208 events per block was measured on real partitions; the budget must drive the span down."""
    planner = ChunkPlanner()
    planner.record_success(blocks=8, logs=8 * 208)

    # Growth stays geometric off proven ground rather than leaping at the budget in one go.
    assert planner.next_span(10_000) == 16

    spans = drive(planner, budget=10_000, density=208.0, rounds=5)

    assert 40 <= spans[-1] <= 60, f"{spans} should settle near a 10k-result response"


def test_span_shrinks_when_density_rises_mid_scrape():
    planner = ChunkPlanner()
    drive(planner, budget=10_000, density=0.01, rounds=30)
    wide = planner.next_span(10_000)

    # The scrape moves into a busy stretch.
    for _ in range(5):
        span = planner.next_span(10_000)
        planner.record_success(blocks=span, logs=int(span * 150))

    assert planner.next_span(10_000) < wide / 100, "the planner must follow the data down"


def test_empty_stretches_do_not_stall_the_scrape():
    planner = ChunkPlanner()
    spans = drive(planner, budget=10_000, density=0.0, rounds=25)

    assert spans[-1] > 1_000_000, "a region with no events should be swallowed whole"


# ---------------------------------------------------------------------------
# Discovering the provider's limits
# ---------------------------------------------------------------------------

def test_result_limit_is_discovered_and_respected():
    planner = ChunkPlanner()
    drive(planner, budget=1_000_000, density=80.0, rounds=40, provider_result_limit=10_000)

    span = planner.next_span(1_000_000)
    assert span * 80 <= 10_000, f"span {span} would ask for more than the provider allows"
    assert planner.results_refused is not None


def test_block_span_limit_is_discovered_and_respected():
    planner = ChunkPlanner()
    drive(planner, budget=10_000, density=0.0, rounds=40, provider_span_limit=50_000)

    assert planner.next_span(10_000) <= 50_000
    assert planner.span_refused is not None


def test_a_limit_is_bracketed_rather_than_walked_down_one_block_at_a_time():
    planner = ChunkPlanner()
    spans = drive(planner, budget=10_000, density=0.0, rounds=40, provider_span_limit=50_000)

    rejections = sum(1 for span in spans if span > 50_000)
    assert rejections <= 12, f"{rejections} refusals to find one limit is a linear search"
    assert spans[-1] > 40_000, f"should settle near the real limit, got {spans[-1]:,}"


def test_a_dense_rejection_does_not_cap_the_block_span():
    """The bug this planner exists to prevent: one busy region crippling every later sparse one."""
    planner = ChunkPlanner()
    planner.record_success(blocks=8, logs=658)

    charged = planner.record_rejection(blocks=128)

    assert charged == "results"
    assert planner.span_refused is None, "a dense rejection says nothing about block count"


def test_a_sparse_rejection_does_not_pin_the_result_budget_below_proven_ground():
    planner = ChunkPlanner()
    planner.record_success(blocks=100_000, logs=50)

    planner.record_rejection(blocks=200_000)

    # Whichever limit takes the blame, a request already proven to work must stay available.
    assert planner.next_span(10_000) >= 100_000


def test_a_block_limit_is_recognised_where_results_cannot_explain_it():
    planner = ChunkPlanner()
    planner.record_success(blocks=100_000, logs=0)

    charged = planner.record_rejection(blocks=200_000)

    assert charged == "blocks"
    assert planner.span_refused == 200_000
    assert planner.results_refused is None


def test_recovering_from_a_dense_region_into_a_sparse_one():
    """Density collapses 6x between adjacent partitions in the real data."""
    planner = ChunkPlanner()
    drive(planner, budget=10_000, density=200.0, rounds=20, provider_result_limit=10_000)
    dense_span = planner.next_span(10_000)

    drive(planner, budget=10_000, density=0.001, rounds=40, provider_result_limit=10_000)

    assert planner.next_span(10_000) > dense_span * 100, (
        "a limit learned in a busy region must not hold the scrape back later"
    )


def test_success_beyond_a_learned_limit_clears_it():
    planner = ChunkPlanner()
    planner.record_success(blocks=100_000, logs=0)
    planner.record_rejection(blocks=200_000)
    assert planner.span_refused == 200_000

    # The provider accepts what it previously refused, so the limit was not real.
    planner.record_success(blocks=200_000, logs=0)

    assert planner.span_refused is None


def test_probing_stays_below_a_refused_span():
    planner = ChunkPlanner()
    planner.record_success(blocks=100_000, logs=0)
    planner.record_rejection(blocks=200_000)

    assert planner.next_span(10_000) < 200_000, "never ask again for a size already refused"


def test_planner_never_returns_a_span_below_one():
    planner = ChunkPlanner()
    planner.record_success(blocks=1, logs=1_000_000)

    assert planner.next_span(1) >= 1


# ---------------------------------------------------------------------------
# Probing beyond proven ground
# ---------------------------------------------------------------------------

def test_a_non_probing_span_never_exceeds_proven_ground():
    planner = ChunkPlanner()
    planner.record_success(blocks=100, logs=0)

    assert planner.next_span(10_000, probe=True) == 200
    assert planner.next_span(10_000, probe=False) == 100


def test_planner_reports_when_the_next_span_would_be_exploratory():
    planner = ChunkPlanner()
    planner.record_success(blocks=100, logs=0)
    assert planner.would_probe(10_000)

    # In a dense region the budget alone holds the span below proven ground, so nothing is risked.
    planner.record_success(blocks=100, logs=100_000)
    assert not planner.would_probe(10_000)


def test_one_bad_probe_costs_one_request_not_a_whole_generation():
    """A live run wasted 64 requests because every worker used the same untried span."""
    planner = ChunkPlanner()
    planner.record_success(blocks=100, logs=500)
    workers = 64
    provider_span_limit = 150

    refused = 0
    probe_outstanding = False
    for _ in range(workers):
        probing = not probe_outstanding and planner.would_probe(10_000)
        span = planner.next_span(10_000, probe=probing)
        if span > provider_span_limit:
            refused += 1
            probe_outstanding = probing

    assert refused <= 1, f"{refused} requests were refused for one untried span"
