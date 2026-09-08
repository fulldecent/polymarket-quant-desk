"""
Decide how many blocks one ``eth_getLogs`` request should cover.

Pure library: no I/O, no globals, no logging.

Event density along the chain varies by orders of magnitude. Early history holds one event per
thousands of blocks; busy periods run to hundreds of events per block; outages leave stretches
with none at all. No single block span serves all of that, so the span is not a tuned constant
here — it is computed for each request from a running estimate of events per block, aimed at a
result budget.

Nothing about the provider is assumed. Two separate limits are discovered, each held as a bracket
between the largest value proven to work and the smallest value known to be refused:

* a **result limit** — the provider refuses a response carrying too many logs;
* a **span limit** — the provider refuses a range covering too many blocks, however few logs it
  holds.

While a limit has never been hit the bracket is open and the value grows geometrically, probing
rather than leaping. Once a refusal closes the bracket, the next probe is its midpoint, so the
true limit is found in a handful of requests instead of being walked down one block at a time.

Going wider than a span already proven to work is exploration, and the caller is expected to keep
one such probe in flight at a time (see ``would_probe``). Otherwise every request in a generation
is sized alike, and a single bad guess is refused as many times as there are workers.

Telling the two limits apart matters. A refusal in a dense region says nothing about how many
*blocks* the provider accepts, and recording it as a span limit would hold back every sparser
region for the rest of the run. So the span is blamed only when the range cannot have held more
logs than a request that already succeeded; otherwise the response size is blamed, and never
below a size already proven to work.
"""

from __future__ import annotations

INITIAL_SPAN: int = 1
"""Start at one block and let measurement open it up."""

GROWTH_FACTOR: int = 2
"""How fast a value may grow while its bracket is still open."""

DENSITY_ALPHA: float = 0.3
"""Weight of the newest sample in the events-per-block estimate. High enough to follow a change in
density within a few requests, low enough not to chase a single unusual block."""


class ChunkPlanner:
    """Size each request from measured event density and discovered provider limits.

    Not thread-safe: call from the orchestrator thread only.
    """

    def __init__(self) -> None:
        self._density: float | None = None
        self._span_ok = 0
        self._span_refused: int | None = None
        self._results_ok = 0
        self._results_refused: int | None = None

    # ------------------------------------------------------------------
    # What has been learned
    # ------------------------------------------------------------------

    @property
    def density(self) -> float | None:
        """Estimated events per block, or None until the first response."""
        return self._density

    @property
    def span_refused(self) -> int | None:
        """Smallest block span the provider has refused, or None if it never has."""
        return self._span_refused

    @property
    def results_refused(self) -> int | None:
        """Smallest result count believed to be refused, or None if none has been."""
        return self._results_refused

    @property
    def max_ok_span(self) -> int:
        return self._span_ok

    def predicted_results(self, blocks: int) -> float | None:
        if self._density is None:
            return None
        return self._density * blocks

    def describe(self) -> str:
        density = "?" if self._density is None else f"{self._density:.3g}"
        span = "?" if self._span_refused is None else f"<{self._span_refused:,}"
        results = "?" if self._results_refused is None else f"<{self._results_refused:,}"
        return f"density={density}/blk span_limit={span} result_limit={results}"

    # ------------------------------------------------------------------
    # Planning
    # ------------------------------------------------------------------

    def next_span(self, result_budget: int, *, probe: bool = True) -> int:
        """Blocks to request next, aiming for ``result_budget`` logs.

        With ``probe=False`` the span never exceeds one already proven to work. Going wider is
        exploration and can be refused, and with many requests in flight every one of them would
        be sized alike and bounce together, so the caller keeps that to one request at a time.
        """
        budget = min(max(1, result_budget), self._result_ceiling())

        if self._density is not None and self._density > 0:
            span = max(1, int(budget / self._density))
        else:
            # Nothing found yet, so response size is not the binding constraint; probe wider.
            span = self._span_ceiling()

        span = min(span, self._span_ceiling())
        if not probe and self._span_ok > 0:
            span = min(span, self._span_ok)
        return max(1, span)

    def would_probe(self, result_budget: int) -> bool:
        """True if the next span would go beyond ground already proven to work."""
        return self.next_span(result_budget) > self.next_span(result_budget, probe=False)

    def _span_ceiling(self) -> int:
        if self._span_refused is None:
            return max(INITIAL_SPAN, self._span_ok * GROWTH_FACTOR)
        # Bracket closed: halve the remaining gap, settling on the largest proven span.
        midpoint = self._span_ok + (self._span_refused - self._span_ok) // 2
        return max(INITIAL_SPAN, midpoint)

    def _result_ceiling(self) -> int:
        if self._results_refused is None:
            return 2**62
        midpoint = self._results_ok + (self._results_refused - self._results_ok) // 2
        return max(1, midpoint)

    # ------------------------------------------------------------------
    # Feedback
    # ------------------------------------------------------------------

    def record_success(self, *, blocks: int, logs: int) -> None:
        """One request covering ``blocks`` blocks came back with ``logs`` events."""
        if blocks <= 0:
            return
        sample = logs / blocks
        if self._density is None:
            self._density = sample
        else:
            self._density = (1 - DENSITY_ALPHA) * self._density + DENSITY_ALPHA * sample

        self._span_ok = max(self._span_ok, blocks)
        self._results_ok = max(self._results_ok, logs)

        # A value that was thought refused has now worked, so that bracket was wrong.
        if self._span_refused is not None and blocks >= self._span_refused:
            self._span_refused = None
        if self._results_refused is not None and logs >= self._results_refused:
            self._results_refused = None

    def record_rejection(self, *, blocks: int) -> str:
        """The provider refused a range of ``blocks`` blocks as too large.

        Returns ``"results"`` or ``"blocks"`` — which limit took the blame.
        """
        blocks = max(1, blocks)
        predicted = self.predicted_results(blocks)

        cannot_be_results = predicted is not None and predicted <= self._results_ok
        cannot_be_span = blocks <= self._span_ok

        if cannot_be_results and not cannot_be_span:
            self._span_refused = (
                blocks if self._span_refused is None else min(self._span_refused, blocks)
            )
            return "blocks"

        # Blamed on response size, but never below a size already proven to work, so an unlucky
        # attribution cannot ratchet the scrape downwards.
        estimate = max(int(predicted or 0), self._results_ok + 1)
        self._results_refused = (
            estimate if self._results_refused is None else min(self._results_refused, estimate)
        )
        return "results"
