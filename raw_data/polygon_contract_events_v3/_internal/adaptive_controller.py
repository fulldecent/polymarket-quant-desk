"""
Adaptive controller for the JSON-RPC scrape loop.

Pure library: no I/O, no globals, no logging. Takes one outcome at a time
(``record_ok`` / ``record_slow`` / ``record_timeout`` / ``record_split``)
and exposes the current concurrency and block-span targets that the
orchestrator should use for the next batch of work.

The controller implements a TCP-slow-start style ramp-up plus
multiplicative back-off on adverse signals. Factored out as a small
state machine so the main loop is easier to read and the policy is
independently testable.

Two parameters are tracked together:

* ``active_workers``: how many ``eth_getLogs`` requests may be in flight
  at once. Bounded below by 1 and above by ``worker_ceiling``.
* ``block_span``: how many blocks one ``eth_getLogs`` call covers.
  Bounded below by ``MIN_BLOCK_SPAN = 1`` and above by ``span_ceiling``.

Both grow on a streak of "ok" outcomes and shrink on the first "slow"
or "timeout". The block span also bumps up when the provider's
response was well below its result limit ("low utilization"), since
that is direct evidence we can ask for more blocks per call.

State transitions for the block span:

  * slow_start phase: span doubles on each "ok" until the first
    adverse signal. After that, the span_ceiling is locked to the
    last span before the adverse event (minus one) and we leave
    slow start.
  * After slow start: span grows multiplicatively toward the ceiling
    when utilization is low; halves on "slow" or "timeout".

State transitions for the worker count:

  * slow_start phase: workers double after every
    ``GROW_WORKERS_SLOW_START`` consecutive "ok" outcomes.
  * After slow start: workers grow by one every
    ``GROW_WORKERS_LINEAR`` consecutive "ok" outcomes.
  * "timeout" halves workers and ends slow start.
  * "slow" does not change workers (it shrinks span instead).

The controller never speaks to the RPC client or the persistence layer.
It is a small state machine consulted between dispatches.
"""

from __future__ import annotations

from dataclasses import dataclass


# Tunables exposed as module constants so tests can poke at them
# without having to construct an ``AdaptiveController`` instance.
# Calibrated for Polygon JSON-RPC providers with a ~10k-result
# ``eth_getLogs`` ceiling.
MIN_BLOCK_SPAN: int = 1
GROW_WORKERS_SLOW_START: int = 3
GROW_WORKERS_LINEAR: int = 20
SPAN_BUMP_UTILIZATION: float = 0.5
RESULT_LIMIT_FOR_UTILIZATION: int = 10_000
"""Most providers truncate ``eth_getLogs`` at ~10k results. We treat
``raw_log_count / RESULT_LIMIT_FOR_UTILIZATION`` as the utilization
ratio that drives span-bump decisions."""


@dataclass
class AdaptiveSnapshot:
    """A read-only view of the controller's current state."""

    active_workers: int
    block_span: int
    span_ceiling: int
    span_in_slow_start: bool
    workers_in_slow_start: bool
    consecutive_ok: int


class AdaptiveController:
    """Drive ``active_workers`` and ``block_span`` based on outcomes.

    Call the appropriate ``record_*`` method for every completed RPC
    request. After each call, read ``active_workers`` and ``block_span``
    to discover the new target values. ``changed_span`` and
    ``changed_workers`` on the returned record indicate whether the
    caller needs to re-chunk pending work or grow the worker pool.

    The controller does NOT itself rebuild queues, submit work, or grow
    pools — it just publishes the target values. The orchestrator does
    those things.

    Thread safety: not thread-safe. Call from the orchestrator thread
    only. The v3 design has all RPC-result handling on one thread, so
    this matches.
    """

    def __init__(
        self,
        *,
        worker_ceiling: int,
        max_block_span: int,
    ) -> None:
        if worker_ceiling < 1:
            raise ValueError("worker_ceiling must be >= 1")
        if max_block_span < 1:
            raise ValueError("max_block_span must be >= 1")

        self._worker_ceiling: int = worker_ceiling
        self._max_block_span: int = max_block_span

        # Initial state: TCP slow-start with one worker, one-block spans.
        self._active_workers: int = 1
        self._block_span: int = 1
        self._span_ceiling: int = max_block_span
        self._span_in_slow_start: bool = True
        self._workers_in_slow_start: bool = True
        self._consecutive_ok: int = 0

    # ------------------------------------------------------------------
    # Read-only accessors
    # ------------------------------------------------------------------

    @property
    def active_workers(self) -> int:
        return self._active_workers

    @property
    def block_span(self) -> int:
        return self._block_span

    @property
    def worker_ceiling(self) -> int:
        return self._worker_ceiling

    @property
    def span_ceiling(self) -> int:
        return self._span_ceiling

    @property
    def span_in_slow_start(self) -> bool:
        return self._span_in_slow_start

    @property
    def workers_in_slow_start(self) -> bool:
        return self._workers_in_slow_start

    def snapshot(self) -> AdaptiveSnapshot:
        return AdaptiveSnapshot(
            active_workers=self._active_workers,
            block_span=self._block_span,
            span_ceiling=self._span_ceiling,
            span_in_slow_start=self._span_in_slow_start,
            workers_in_slow_start=self._workers_in_slow_start,
            consecutive_ok=self._consecutive_ok,
        )

    # ------------------------------------------------------------------
    # Outcome recorders
    # ------------------------------------------------------------------

    def record_ok(
        self,
        *,
        chunk_span: int,
        raw_log_count: int,
    ) -> tuple[bool, bool]:
        """Record a successful fetch.

        Args:
            chunk_span: Inclusive blocks covered by the completed
                request (``to_block - from_block + 1``). Required so we
                only bump the span when the completed work was at the
                current target — otherwise a leftover small chunk could
                erroneously trigger growth.
            raw_log_count: Number of logs the provider returned. A
                value well below the provider's limit means we can
                safely ask for more blocks per call.

        Returns:
            ``(span_changed, workers_changed)`` — flags telling the
            caller whether to re-chunk the queue and/or grow the
            executor pool. Either flag implies a new target value is
            already published on ``self.block_span`` /
            ``self.active_workers``.
        """
        self._consecutive_ok += 1
        span_changed = self._maybe_bump_span(chunk_span, raw_log_count)
        workers_changed = self._maybe_grow_workers()
        return span_changed, workers_changed

    def record_slow(
        self,
        *,
        chunk_span: int,
    ) -> tuple[bool, bool]:
        """Record a fetch that succeeded but was slower than the
        configured threshold.

        Slow responses shrink the block span but do not change the
        worker count: the underlying transport is willing to serve more
        requests, just slower per request.

        Returns ``(span_changed, workers_changed)``.
        """
        # A slow fetch counts as "ok" for the worker streak — the
        # request did succeed — but we reset progress toward span
        # growth and immediately shrink the span if the completed
        # chunk matches the current target.
        self._consecutive_ok += 1
        span_changed = False
        if chunk_span <= self._block_span and self._block_span > MIN_BLOCK_SPAN:
            if self._span_in_slow_start:
                self._span_in_slow_start = False
                self._span_ceiling = max(MIN_BLOCK_SPAN, self._block_span - 1)
            new_span = max(MIN_BLOCK_SPAN, self._block_span // 2)
            if new_span != self._block_span:
                self._block_span = new_span
                span_changed = True
        return span_changed, False

    def record_timeout(self) -> tuple[bool, bool]:
        """Record a timeout. Halves both span and workers, exits slow
        start for both.

        Returns ``(span_changed, workers_changed)``.
        """
        self._consecutive_ok = 0

        span_changed = False
        if self._span_in_slow_start:
            self._span_in_slow_start = False
            self._span_ceiling = max(MIN_BLOCK_SPAN, self._block_span - 1)
        if self._block_span > MIN_BLOCK_SPAN:
            new_span = max(MIN_BLOCK_SPAN, self._block_span // 2)
            if new_span != self._block_span:
                self._block_span = new_span
                span_changed = True

        workers_changed = False
        self._workers_in_slow_start = False
        if self._active_workers > 1:
            new_workers = max(1, self._active_workers // 2)
            if new_workers != self._active_workers:
                self._active_workers = new_workers
                workers_changed = True

        return span_changed, workers_changed

    def record_split(self) -> tuple[bool, bool]:
        """Record a "range too large" rejection.

        Treated like a timeout for span purposes: shrink and exit slow
        start. Worker count is unchanged because the provider is happily
        serving requests, just not this size.

        Returns ``(span_changed, workers_changed)``.
        """
        self._consecutive_ok = 0
        span_changed = False
        if self._span_in_slow_start:
            self._span_in_slow_start = False
            self._span_ceiling = max(MIN_BLOCK_SPAN, self._block_span - 1)
        if self._block_span > MIN_BLOCK_SPAN:
            new_span = max(MIN_BLOCK_SPAN, self._block_span // 2)
            if new_span != self._block_span:
                self._block_span = new_span
                span_changed = True
        return span_changed, False

    def record_error(self) -> None:
        """Record a generic transport-level error. Does not change
        targets but resets the consecutive-ok streak so the next worker
        growth waits for fresh evidence of stability.
        """
        self._consecutive_ok = 0

    # ------------------------------------------------------------------
    # Internal helpers
    # ------------------------------------------------------------------

    def _maybe_bump_span(self, chunk_span: int, raw_log_count: int) -> bool:
        """Grow the block span if the just-completed chunk was at the
        current target and the response was well below the provider's
        result limit.
        """
        if chunk_span < self._block_span:
            return False
        utilization = raw_log_count / RESULT_LIMIT_FOR_UTILIZATION
        if utilization >= SPAN_BUMP_UTILIZATION:
            return False

        max_span = self._max_block_span if self._span_in_slow_start else self._span_ceiling
        if self._block_span >= max_span:
            return False

        old_span = self._block_span
        if utilization > 0:
            multiplier = min(SPAN_BUMP_UTILIZATION / utilization, 4.0)
            new_span = min(max_span, int(self._block_span * multiplier))
        else:
            factor = 4 if self._span_in_slow_start else 2
            new_span = min(max_span, self._block_span * factor)
        new_span = max(new_span, MIN_BLOCK_SPAN)
        if new_span != old_span:
            self._block_span = new_span
            return True
        return False

    def _maybe_grow_workers(self) -> bool:
        """Grow the worker count after a streak of successful fetches."""
        if self._active_workers >= self._worker_ceiling:
            return False
        threshold = (
            GROW_WORKERS_SLOW_START
            if self._workers_in_slow_start
            else GROW_WORKERS_LINEAR
        )
        if self._consecutive_ok < threshold:
            return False

        old_workers = self._active_workers
        if self._workers_in_slow_start:
            self._active_workers = min(self._worker_ceiling, self._active_workers * 2)
        else:
            self._active_workers = min(self._worker_ceiling, self._active_workers + 1)

        if self._active_workers == self._worker_ceiling and self._workers_in_slow_start:
            # Hitting the ceiling ends slow start: future growth has
            # nowhere to go and any next adverse event should not
            # halve us back into a doubling ramp.
            self._workers_in_slow_start = False

        self._consecutive_ok = 0
        return self._active_workers != old_workers
