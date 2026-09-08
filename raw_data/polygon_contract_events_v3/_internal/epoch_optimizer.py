"""
Throughput optimizer for the JSON-RPC scrape loop.

Pure library: no I/O, no globals, no logging. Three parameters are tuned, and nothing about the
provider is configured up front — no requests-per-second setting, no block-span limit, no
concurrency setting:

* ``workers``                  — how many ``eth_getLogs`` requests may be in flight at once.
* ``block_span``               — how many blocks one ``eth_getLogs`` call covers.
* ``sleep_after_response_sec`` — how long a worker thread pauses after any response before it
  picks up more work. This is the only throttle.

The run has two phases.

**Ramp.** Starting from one worker and one block, growth is driven by each response rather than by
a measured window: the block span doubles on every success, and the worker count doubles every few
successes. Waiting a full measurement window per doubling would spend minutes crawling before the
parameters were anywhere near useful, and during the ramp there is nothing to measure anyway —
each step is plainly better than the last. A parameter stops ramping when the provider objects to
it, or, for concurrency, when per-block latency starts climbing: requests queueing is the signal
that the useful concurrency has already been passed. Latency is tracked per block so that growing
the span, which lengthens a response for a good reason, is not mistaken for queueing.

**Search.** Once both ramps have stopped, the parameters sit near the provider's envelope and the
remaining question is which local adjustment is actually faster. A single response cannot answer
that, so an epoch does: parameters are held still, the epoch runs to its deadline, the orchestrator
stops submitting and waits for in-flight requests to drain, and the score is every block credited
divided by the wall time from epoch start to the end of the drain — the drain is inside the
measurement rather than distorting it. Epoch length is derived from observed latency so that many
requests land inside each window.

The search is a coordinate hill-climb. Each epoch is a trial that moves exactly one parameter; a
trial that beats the incumbent by ``MIN_GAIN`` is committed and the next trial pushes the same
parameter the same way. A rejected trial reverts, then retries the same parameter in the opposite
direction, then rotates to the next parameter. After a full rotation with nothing accepted, one
epoch re-measures the incumbent, because block density and provider load drift and a stale
incumbent score would freeze the search.

Adverse signals are never held until a boundary. A rejected range, a timeout or a throttling
response backs the parameters off immediately; during search it also spoils the epoch, whose score
would otherwise describe more than one parameter set. Permanent errors are not this module's
business — the caller fails fast on those.
"""

from __future__ import annotations

import time
from dataclasses import dataclass, replace
from enum import Enum
from typing import Callable

MIN_WORKERS: int = 1
MIN_RESULT_BUDGET: int = 1
MIN_SLEEP_STEP_SEC: float = 0.005
MAX_SLEEP_SEC: float = 5.0

MAX_WORKERS: int = 256
"""Guard against runaway thread creation, not a tuning knob: the ramp stops far below this once
the provider pushes back or latency climbs."""

RAMP_FACTOR: float = 2.0
"""Growth per ramp step. Doubling, so the ramp is over in a handful of round trips."""

LATENCY_DEGRADE_FACTOR: float = 2.0
"""Stop ramping concurrency once per-block latency reaches this multiple of the best seen."""

RAMP_MAX_SEC: float = 60.0
"""Hand over to the measured search after this long, however the ramp is going.

The ramp only stops on its own when the provider objects or latency degrades. Neither happens if
the bottleneck is on our side of the wire, so without a deadline the ramp keeps adding workers
that cannot help and nothing is ever actually measured."""

SEARCH_FACTOR: float = 1.25
"""Step multiplier for the local search once ramping is done."""

MIN_GAIN: float = 0.05
"""A trial must beat the incumbent by this fraction to be committed. Set above the noise floor of
one epoch's measurement so the search does not chase jitter."""

AXES: tuple[str, ...] = ("workers", "result_budget", "sleep_after_response_sec")


class Phase(Enum):
    RAMP = "ramp"
    SEARCH = "search"


@dataclass(frozen=True)
class Params:
    """One point in the parameter space."""

    workers: int
    result_budget: int
    sleep_after_response_sec: float

    def describe(self) -> str:
        return (
            f"workers={self.workers} budget={self.result_budget:,} logs "
            f"sleep={self.sleep_after_response_sec:.3f}s"
        )


@dataclass(frozen=True)
class EpochReport:
    """What one epoch measured and what the optimizer decided as a result."""

    params: Params
    """The parameters that were in force (for a spoiled epoch, the ones it ended with)."""

    blocks: int
    duration_sec: float
    score: float
    """Blocks per second, the objective being maximized."""

    accepted: bool
    reason: str
    next_params: Params

    def describe(self) -> str:
        return (
            f"{self.score:,.0f} blk/s over {self.duration_sec:.0f}s "
            f"[{self.params.describe()}] {self.reason}"
        )


class EpochOptimizer:
    """Ramp to the provider's envelope, then hill-climb blocks/second around it.

    Not thread-safe: call from the orchestrator thread only. Worker threads read ``params``
    (an immutable snapshot) and never mutate the optimizer.
    """

    def __init__(
        self,
        *,
        worker_cap: int = MAX_WORKERS,
        min_epoch_sec: float = 15.0,
        max_epoch_sec: float = 120.0,
        epoch_latency_multiple: float = 30.0,
        ramp_max_sec: float = RAMP_MAX_SEC,
        clock: Callable[[], float] = time.monotonic,
    ) -> None:
        if worker_cap < MIN_WORKERS:
            raise ValueError(f"worker_cap must be >= {MIN_WORKERS}")
        if min_epoch_sec <= 0 or max_epoch_sec < min_epoch_sec:
            raise ValueError("epoch bounds must satisfy 0 < min_epoch_sec <= max_epoch_sec")

        self._worker_cap = worker_cap
        self._min_epoch_sec = min_epoch_sec
        self._max_epoch_sec = max_epoch_sec
        self._epoch_latency_multiple = epoch_latency_multiple
        self._ramp_max_sec = ramp_max_sec
        self._clock = clock
        self._ramp_start = self._clock()

        # One worker, one log, no throttle: everything else is discovered.
        self._active = Params(workers=1, result_budget=1, sleep_after_response_sec=0.0)
        self._incumbent = self._active
        self._incumbent_score: float | None = None

        self._phase = Phase.RAMP
        self._budget_ramping = True
        self._workers_ramping = True
        self._consecutive_ok = 0

        self._axis = 0
        self._direction = 1
        self._rejects_since_accept = 0

        self._mean_latency_sec = 0.0
        self._mean_block_latency_sec = 0.0
        self._best_block_latency_sec = 0.0

        self._epoch_start = self._clock()
        self._epoch_blocks = 0
        self._spoiled = False
        self._spoil_reason = ""

    # ------------------------------------------------------------------
    # Read-only state
    # ------------------------------------------------------------------

    @property
    def params(self) -> Params:
        return self._active

    @property
    def phase(self) -> Phase:
        return self._phase

    @property
    def is_ramping(self) -> bool:
        return self._phase is Phase.RAMP

    @property
    def mean_latency_sec(self) -> float:
        return self._mean_latency_sec

    @property
    def epoch_duration_sec(self) -> float:
        """How long a search epoch should run.

        Scaled to observed request latency so that many requests complete inside the window and
        the drain at the end is a small share of it. Clamped so a fast provider still gets a
        window long enough to average over, and a slow one does not stall the search.
        """
        if self._mean_latency_sec <= 0.0:
            return self._min_epoch_sec
        target = self._epoch_latency_multiple * self._mean_latency_sec
        return min(self._max_epoch_sec, max(self._min_epoch_sec, target))

    def should_end_epoch(self) -> bool:
        if self._phase is Phase.RAMP:
            return False
        return self._clock() - self._epoch_start >= self.epoch_duration_sec

    # ------------------------------------------------------------------
    # Per-response feedback
    # ------------------------------------------------------------------

    def start_epoch(self) -> None:
        self._epoch_start = self._clock()
        self._epoch_blocks = 0
        self._spoiled = False
        self._spoil_reason = ""

    def record_blocks(self, blocks: int) -> None:
        """Credit blocks whose rows are durable, including any landing during the drain."""
        self._epoch_blocks += blocks

    def record_ok(self, *, blocks: int, logs: int, elapsed_sec: float) -> bool:
        """Credit one successful response. Returns True if the parameters changed."""
        self._observe_latency(elapsed_sec, blocks)
        if self._phase is not Phase.RAMP:
            return False

        if self._clock() - self._ramp_start >= self._ramp_max_sec:
            self._budget_ramping = False
            self._workers_ramping = False
            self._maybe_begin_search()
            return False

        changed = False

        # Only grow the budget off a response that actually filled it. A sparse region returns
        # fewer logs than asked for, which says nothing about how large a response may be.
        if self._budget_ramping and logs >= self._active.result_budget:
            grown = self._grow(self._active.result_budget, 2**62)
            if grown != self._active.result_budget:
                self._active = replace(self._active, result_budget=grown)
                changed = True

        if self._workers_ramping:
            if self._latency_degraded():
                self._workers_ramping = False
            else:
                self._consecutive_ok += 1
                # Double once per round trip, after a full generation of in-flight requests has
                # come back. Counting responses instead would let one batch multiply concurrency
                # many times over before the provider had any chance to object.
                if self._consecutive_ok >= self._active.workers:
                    self._consecutive_ok = 0
                    grown = self._grow(self._active.workers, self._worker_cap)
                    if grown != self._active.workers:
                        self._active = replace(self._active, workers=grown)
                        changed = True
                    else:
                        self._workers_ramping = False

        self._maybe_begin_search()
        return changed

    @staticmethod
    def _grow(value: int, ceiling: int) -> int:
        return min(ceiling, max(value + 1, round(value * RAMP_FACTOR)))

    def _observe_latency(self, elapsed_sec: float, chunk_span: int) -> None:
        if elapsed_sec <= 0.0:
            return
        if self._mean_latency_sec <= 0.0:
            self._mean_latency_sec = elapsed_sec
        else:
            self._mean_latency_sec = 0.8 * self._mean_latency_sec + 0.2 * elapsed_sec

        # Per block, so a longer response caused by a larger span does not look like queueing.
        per_block = elapsed_sec / max(chunk_span, 1)
        if self._mean_block_latency_sec <= 0.0:
            self._mean_block_latency_sec = per_block
        else:
            self._mean_block_latency_sec = 0.8 * self._mean_block_latency_sec + 0.2 * per_block
        if (
            self._best_block_latency_sec <= 0.0
            or self._mean_block_latency_sec < self._best_block_latency_sec
        ):
            self._best_block_latency_sec = self._mean_block_latency_sec

    def _latency_degraded(self) -> bool:
        if self._best_block_latency_sec <= 0.0:
            return False
        return (
            self._mean_block_latency_sec
            > LATENCY_DEGRADE_FACTOR * self._best_block_latency_sec
        )

    def _maybe_begin_search(self) -> None:
        if self._phase is Phase.RAMP and not self._budget_ramping and not self._workers_ramping:
            self._phase = Phase.SEARCH
            self._incumbent = self._active
            self._incumbent_score = None
            self.start_epoch()

    # ------------------------------------------------------------------
    # Adverse signals
    # ------------------------------------------------------------------

    def record_rejection(self) -> None:
        """The provider refused a range as too large.

        Which limit was hit, and what span to use next, is ``chunk_planner``'s business. All that
        matters here is that the budget has met a wall, so it stops ramping.
        """
        self._budget_ramping = False
        self._spoil("range refused")
        self._maybe_begin_search()

    def record_timeout(self) -> None:
        """A request timed out: both concurrency and response size are suspect, so ease off each."""
        self._budget_ramping = False
        self._workers_ramping = False
        self._active = replace(
            self._active,
            workers=max(MIN_WORKERS, self._active.workers // 2),
            result_budget=max(MIN_RESULT_BUDGET, self._active.result_budget // 2),
        )
        self._spoil("request timed out")
        self._maybe_begin_search()

    def record_throttled(self, status_code: int | None) -> None:
        """The provider pushed back (HTTP 429, or 5xx that outlived the client's retries).

        This is about request rate, not response size, so the budget ramp is left alone.
        """
        self._workers_ramping = False
        sleep = self._active.sleep_after_response_sec
        if status_code == 429:
            # Being rate limited is what the sleep knob exists for; concurrency alone will not fix it.
            sleep = min(MAX_SLEEP_SEC, sleep * 2 if sleep > 0 else MIN_SLEEP_STEP_SEC * 4)
        self._active = replace(
            self._active,
            workers=max(MIN_WORKERS, self._active.workers // 2),
            sleep_after_response_sec=round(sleep, 4),
        )
        self._spoil(f"HTTP {status_code}" if status_code else "provider pushed back")
        self._maybe_begin_search()

    def _spoil(self, reason: str) -> None:
        # Only a scored window can be spoiled; the ramp measures nothing.
        if self._phase is Phase.SEARCH:
            self._spoiled = True
            self._spoil_reason = reason

    # ------------------------------------------------------------------
    # Epoch decision
    # ------------------------------------------------------------------

    def end_epoch(self) -> EpochReport:
        """Score the finished epoch, decide what to keep, and publish the next parameters.

        Call only after in-flight work has drained, so the score covers the full cost of the
        parameters it is attributed to.
        """
        duration = max(self._clock() - self._epoch_start, 1e-9)
        score = self._epoch_blocks / duration
        measured = self._active

        if self._spoiled:
            # More than one parameter set was in force, so the number describes neither. Adopt the
            # backed-off parameters and re-measure them.
            self._incumbent = self._active
            self._incumbent_score = None
            self._rejects_since_accept = 0
            return self._report(measured, score, duration, False, f"backed off, {self._spoil_reason}")

        if self._incumbent_score is None:
            self._incumbent = measured
            self._incumbent_score = score
            self._active = self._propose()
            return self._report(measured, score, duration, True, "baseline")

        if score > self._incumbent_score * (1.0 + MIN_GAIN):
            gain = score / self._incumbent_score - 1.0
            self._incumbent = measured
            self._incumbent_score = score
            self._rejects_since_accept = 0
            self._active = self._propose()
            return self._report(measured, score, duration, True, f"kept, +{gain * 100:.0f}%")

        self._rejects_since_accept += 1
        self._advance_axis()
        if self._rejects_since_accept >= len(AXES) * 2:
            # A full rotation found nothing. Re-measure the incumbent before searching again.
            self._rejects_since_accept = 0
            self._incumbent_score = None
            self._active = self._incumbent
            return self._report(measured, score, duration, False, "reverted, re-measuring")
        self._active = self._propose()
        return self._report(measured, score, duration, False, "reverted")

    def _report(
        self,
        measured: Params,
        score: float,
        duration: float,
        accepted: bool,
        reason: str,
    ) -> EpochReport:
        return EpochReport(
            params=measured,
            blocks=self._epoch_blocks,
            duration_sec=duration,
            score=score,
            accepted=accepted,
            reason=reason,
            next_params=self._active,
        )

    # ------------------------------------------------------------------
    # Coordinate search
    # ------------------------------------------------------------------

    def _advance_axis(self) -> None:
        """Try the other direction on this parameter, or move to the next parameter."""
        if self._direction > 0:
            self._direction = -1
        else:
            self._direction = 1
            self._axis = (self._axis + 1) % len(AXES)

    def _propose(self) -> Params:
        """Return the next trial: the incumbent with one parameter stepped."""
        for _ in range(len(AXES) * 2):
            candidate = self._step(self._incumbent, AXES[self._axis], self._direction)
            if candidate != self._incumbent:
                return candidate
            # That parameter is already at a bound in that direction; nothing to learn from it.
            self._advance_axis()
        return self._incumbent

    def _step(self, params: Params, axis: str, direction: int) -> Params:
        if axis == "workers":
            value = params.workers
            if direction > 0:
                stepped = min(self._worker_cap, max(value + 1, round(value * SEARCH_FACTOR)))
            else:
                stepped = max(MIN_WORKERS, min(value - 1, int(value / SEARCH_FACTOR)))
            return replace(params, workers=stepped)

        if axis == "result_budget":
            value = params.result_budget
            if direction > 0:
                stepped = max(value + 1, round(value * SEARCH_FACTOR))
            else:
                stepped = max(MIN_RESULT_BUDGET, min(value - 1, int(value / SEARCH_FACTOR)))
            return replace(params, result_budget=stepped)

        value = params.sleep_after_response_sec
        if direction > 0:
            stepped = (
                MIN_SLEEP_STEP_SEC if value <= 0.0 else min(MAX_SLEEP_SEC, value * SEARCH_FACTOR)
            )
        else:
            stepped = 0.0 if value <= MIN_SLEEP_STEP_SEC else value / SEARCH_FACTOR
        return replace(params, sleep_after_response_sec=round(stepped, 4))
