#!/usr/bin/env python3
"""
Scrape Polymarket event logs from Polygon into the v3 hot DB and cold Parquet tree.

This is the v3 main program. It owns process lifecycle, CLI flags, env loading, signal handling,
the status line, and the orchestration that ties together the four library modules:

    * ``_internal.rpc_client.RpcClient`` — thread-safe Polygon JSON-RPC client.
        * ``_internal.event_decoders.decode_batch`` — decode each ``eth_getLogs`` response into
        ``(contract, event, row)`` triples.
    * ``_internal.persistence.HotStore`` — owns the hot DuckDB, atomic event ingestion,
        ``loaded_block_ranges`` bookkeeping, sink commit. Every write against the hot DB happens on
        this thread (the main thread).
    * ``_internal.parquet_sink.write_partition_files`` — runs in a background pool. Opens its own
        read-only DuckDB connection on the hot DB, writes one 10K partition's complete set of
        Parquet files, returns.

The control loop is:

  1. Open the HotStore. Reconcile the hot DB's progress table against whatever Parquet files
      already exist on the cold tier, so a crash between a sink worker's rename and the
      orchestrator's ``commit_sink`` is recovered automatically.
  2. Ask the chain for the current head block.
  3. Compute gaps from ``SCRAPE_START_BLOCK`` to the head, print the startup banner.
  4. Schedule ``eth_getLogs`` requests covering those gaps, adaptively growing concurrency and
      block span on success and backing off on adverse signals (see ``_internal.adaptive_controller``).
  5. As each RPC completes, decode the logs and call ``store.persist`` in a single atomic
      transaction. Any 10K partition that just became ready is submitted to the sink pool.
  6. As sink workers complete (in commit-frontier order), call
     ``store.commit_sink(partition_start)`` to delete the partition's
     hot rows and advance the sunk frontier.
    7. When the queue drains, re-check the chain head; if new gaps appeared, rebuild the queue and
         continue. Stop when within ``--lag-tolerance`` blocks of the head.

CTRL-C is honored at every point in the loop. The first interrupt sets a shared stop flag that
propagates into the RPC client, the persistence layer, the sink workers, and the rechunker. The
second interrupt hard-exits the process. Partition writes are atomic per file (temp-then-rename);
any partition whose Parquet files reached disk but whose ``commit_sink`` did not run will be
picked up by ``reconcile_with_cold_tier`` on the next startup.

USAGE
    python main.py                          # all contracts, all gaps to head
    python main.py --max-calls 100          # stop after 100 RPC calls
    python main.py --parallel 4             # concurrent worker ceiling
    python main.py --lag-tolerance 10       # stop when within 10 blocks
    python main.py --sink-workers 2         # concurrent sink workers

ENVIRONMENT (all required, set in .env)
    POLYGON_CONTRACT_EVENTS_V3_HOT_DB    hot DuckDB ``.db`` file
    POLYGON_CONTRACT_EVENTS_V3_DIR       cold-tier root directory
    POLYGON_RPC_URL                      Polygon JSON-RPC endpoint
    POLYGON_RPC_MAX_GETLOGS_BLOCK_SPAN   ceiling on per-call block span
    POLYGON_RPC_MAX_REQUESTS_PER_SECOND  rate limit
    TEMP_DIR                             DuckDB spill directory
"""

from __future__ import annotations

import argparse
import concurrent.futures
import os
import shutil
import signal
import subprocess
import sys
import threading
import time
from concurrent.futures import Future, ThreadPoolExecutor
from datetime import datetime, timezone
from pathlib import Path
from typing import TextIO

from dotenv import load_dotenv

# Make ``_internal`` importable when run as a script.
sys.path.insert(0, str(Path(__file__).resolve().parent))

from _internal.adaptive_controller import AdaptiveController
from _internal.errors import (
    DuplicateRowError,
    OperationCancelled,
    PartitionFrontierError,
    SchemaMismatchError,
    V3Error,
)
from _internal.event_decoders import (
    WANTED_ADDRESSES,
    WANTED_TOPICS,
    decode_batch_strict,
)
from _internal.parquet_sink import (
    PartitionWriteResult,
    cleanup_temp_dirs_after_frontier,
    get_sunk_frontier as get_manifest_frontier,
    read_manifest_frontier,
    remove_orphan_temp_files,
    roll_forward_manifests_to_exhaustion,
    write_partition_files,
)
from _internal.persistence import HotStore, HotStoreConfig
from _internal.rpc_client import (
    RpcClient,
    RpcClientConfig,
    RPCError,
    RPCTimeout,
    TooManyResults,
)
from _internal.tables import (
    PARTITION_SIZE_10K,
    SCRAPE_START_BLOCK,
)


# ---------------------------------------------------------------------------
# Module-level constants
# ---------------------------------------------------------------------------

# Path to ``schema.sql`` next to this file.
_SCHEMA_SQL = str(Path(__file__).resolve().parent / "schema.sql")

# Tuning knobs that are stable enough to live as constants rather than CLI flags.
SLOW_FETCH_THRESHOLD_SEC: float = 20.0
"""An ``eth_getLogs`` that succeeds but takes longer than this is treated as a "slow" outcome by
the adaptive controller — the request itself is kept, but the controller shrinks the per-call
block span."""

RPC_CALL_TIMEOUT_SEC: float = 30.0
RPC_BLOCK_NUMBER_TIMEOUT_SEC: float = 5.0
RPC_MAX_RETRIES: int = 3
RPC_BACKOFF_BASE_SEC: float = 0.5

POLL_TIMEOUT_SEC: float = 0.5
"""How long ``concurrent.futures.wait`` blocks before re-rendering the status line.

Picked at half the user-visible refresh rate so the heartbeat thread is the one driving display
updates, not this loop.
"""

HEARTBEAT_INTERVAL_SEC: float = 0.5
"""Status line refresh interval.

The user-stated requirement is "at least every 1000 ms"; we run at twice that rate so a single
slow tick does not blow the budget.
"""

FORCE_EXIT_AFTER_INTERRUPTS: int = 2
"""Second CTRL-C bypasses orderly shutdown and ``os._exit``s."""

MAX_CHUNK_RETRIES: int = 3
"""How many times to retry one ``(from_block, to_block)`` chunk on a transport-level error before
giving up on it and continuing past it.

(Gaps are recomputed on every chain-head poll, so a skipped chunk will be retried on the next
loop.)
"""

POLYGON_BLOCK_TIME_SEC: float = 2.0


# ---------------------------------------------------------------------------
# Stop event — accessed by signal handler, heartbeat thread, RPC client, sink workers, and
# persistence layer. Single source of truth for "should we be shutting down right now."
# ---------------------------------------------------------------------------

_stop_event = threading.Event()


# ---------------------------------------------------------------------------
# Environment loading
# ---------------------------------------------------------------------------

def _load_environment(*, allow_dirty: bool = False) -> dict[str, str]:
    """Read every required env var from the root ``.env`` and validate it.

    Exits the process with a clear message on any missing or invalid value. The returned dict has
    the env-var name as key and the raw string value (already validated as non-empty) as value.
    """
    project_root = Path(__file__).resolve().parent.parent.parent

    _assert_git_clean(project_root, allow_dirty=allow_dirty)

    load_dotenv(project_root / ".env")

    required = [
        "POLYGON_CONTRACT_EVENTS_V3_HOT_DB",
        "POLYGON_CONTRACT_EVENTS_V3_DIR",
        "POLYGON_RPC_URL",
        "POLYGON_RPC_MAX_GETLOGS_BLOCK_SPAN",
        "POLYGON_RPC_MAX_REQUESTS_PER_SECOND",
        "TEMP_DIR",
    ]
    missing = [k for k in required if not os.environ.get(k)]
    if missing:
        sys.exit(
            "Missing required environment variables: "
            + ", ".join(missing)
            + "\nSet them in the repo-root .env file. See raw_data/"
            "polygon_contract_events_v3/main.py docstring."
        )

    db_path = os.environ["POLYGON_CONTRACT_EVENTS_V3_HOT_DB"]
    db_parent = os.path.dirname(os.path.abspath(db_path))
    if not os.path.isdir(db_parent):
        sys.exit(
            f"POLYGON_CONTRACT_EVENTS_V3_HOT_DB parent directory does not exist: {db_parent}\n"
            f"Create it (e.g. mkdir -p {db_parent}) before running."
        )
    if not os.access(db_parent, os.W_OK):
        sys.exit(f"POLYGON_CONTRACT_EVENTS_V3_HOT_DB parent is not writable: {db_parent}")

    cold_root = os.environ["POLYGON_CONTRACT_EVENTS_V3_DIR"]
    if not os.path.isdir(cold_root):
        sys.exit(
            f"POLYGON_CONTRACT_EVENTS_V3_DIR does not exist: {cold_root}\n"
            f"Ensure the cold-tier volume is mounted."
        )
    if not os.access(cold_root, os.W_OK):
        sys.exit(f"POLYGON_CONTRACT_EVENTS_V3_DIR is not writable: {cold_root}")

    temp_dir = os.environ["TEMP_DIR"]
    if not os.path.isdir(temp_dir):
        sys.exit(
            f"TEMP_DIR does not exist: {temp_dir}\nCreate it before running."
        )

    try:
        max_span = int(os.environ["POLYGON_RPC_MAX_GETLOGS_BLOCK_SPAN"])
        max_rps = int(os.environ["POLYGON_RPC_MAX_REQUESTS_PER_SECOND"])
    except ValueError as e:
        sys.exit(f"Failed to parse RPC limits as integers: {e}")
    if max_span < 1:
        sys.exit(f"POLYGON_RPC_MAX_GETLOGS_BLOCK_SPAN must be >= 1; got {max_span}")
    if max_rps < 1:
        sys.exit(f"POLYGON_RPC_MAX_REQUESTS_PER_SECOND must be >= 1; got {max_rps}")

    return {
        "db_path": db_path,
        "cold_root": cold_root,
        "rpc_url": os.environ["POLYGON_RPC_URL"],
        "max_block_span": str(max_span),
        "max_rps": str(max_rps),
        "temp_dir": temp_dir,
    }


def _assert_git_clean(project_root: Path, *, allow_dirty: bool = False) -> None:
    """Fail fast when the repository has uncommitted changes.

    Metadata embeds ``git_commit``. To keep provenance strict, we refuse to write if the working
    tree is dirty unless the caller explicitly opts in via ``--run-dirty``.
    """
    try:
        result = subprocess.run(
            ["git", "status", "--porcelain"],
            cwd=project_root,
            check=True,
            capture_output=True,
            text=True,
        )
    except FileNotFoundError:
        sys.exit("git executable was not found; cannot verify clean working tree")
    except subprocess.CalledProcessError as e:
        sys.exit(f"failed to check git status: {e}")

    lines = [line for line in result.stdout.splitlines() if line.strip()]
    if lines:
        if allow_dirty:
            preview = "\n".join(lines[:20])
            suffix = "\n..." if len(lines) > 20 else ""
            print(
                "WARNING: git working tree is dirty, but --run-dirty was set. Proceeding anyway.\n"
                f"Dirty entries:\n{preview}{suffix}"
            )
            return
        preview = "\n".join(lines[:20])
        suffix = "\n..." if len(lines) > 20 else ""
        sys.exit(
            "Refusing to start because git working tree is dirty. "
            "Commit or stash changes first.\n"
            f"Dirty entries:\n{preview}{suffix}"
        )


# ---------------------------------------------------------------------------
# Rate limiter
# ---------------------------------------------------------------------------

class RateLimiter:
    """Token-bucket-ish rate limiter shared across worker threads.

    Keeps a global "ideal time at which the next call should have happened" and sleeps the caller
    until we have caught up. Simple, fair, and avoids the "burst then idle" pattern of a naive
    sliding window.

    Thread-safe.
    """

    def __init__(self, max_calls_per_sec: float) -> None:
        self._max_calls_per_sec = max_calls_per_sec
        self._start_time: float | None = None
        self._total_calls = 0
        self._lock = threading.Lock()

    @property
    def total_calls(self) -> int:
        return self._total_calls

    def wait(self) -> None:
        """Block until it is safe to issue one more call.

        Sleeps in short slices so a CTRL-C can interrupt promptly.
        """
        with self._lock:
            if self._start_time is None:
                self._start_time = time.monotonic()
            self._total_calls += 1
            required = self._total_calls / self._max_calls_per_sec
            actual = time.monotonic() - self._start_time
            sleep_for = required - actual
        while sleep_for > 0:
            if _stop_event.is_set():
                return
            chunk = min(sleep_for, 0.1)
            time.sleep(chunk)
            sleep_for -= chunk


# ---------------------------------------------------------------------------
# Display helpers
# ---------------------------------------------------------------------------

def _term_width() -> int:
    try:
        w = shutil.get_terminal_size().columns
        return w if w > 0 else 120
    except OSError:
        return 120


def _format_duration(seconds: float) -> str:
    if seconds < 0 or seconds != seconds:  # NaN-safe
        return "--:--:--"
    h = int(seconds) // 3600
    m = (int(seconds) % 3600) // 60
    s = int(seconds) % 60
    return f"{h:02d}:{m:02d}:{s:02d}"


def _file_size_str(path: str) -> str:
    try:
        size = float(os.path.getsize(path))
    except OSError:
        return "?"
    for unit in ("B", "KB", "MB", "GB"):
        if size < 1024:
            return f"{size:.1f} {unit}" if unit != "B" else f"{int(size)} {unit}"
        size /= 1024
    return f"{size:.1f} TB"


def _blocks_behind_str(blocks: int) -> str:
    seconds = blocks * POLYGON_BLOCK_TIME_SEC
    if seconds < 120:
        return f"{seconds:.0f}s"
    if seconds < 7200:
        return f"{seconds / 60:.0f}m"
    if seconds < 172800:
        return f"{seconds / 3600:.1f}h"
    return f"{seconds / 86400:.1f}d"


def _fmt_range(from_block: int, to_block: int) -> str:
    return f"{from_block:,}\u2013{to_block:,}"


def _next_partition_fill_pct(
    *,
    store: HotStore,
    sunk_frontier: int,
    chain_head: int,
) -> float | None:
    """Return fill percentage for the next sink partition.

    Counts how many blocks in the next 10k partition are present in hot DB unsunk ranges,
    regardless of contiguity.
    """
    if chain_head < SCRAPE_START_BLOCK:
        return None

    next_partition_start = (
        (max(sunk_frontier, SCRAPE_START_BLOCK - 1) + 1) // PARTITION_SIZE_10K
    ) * PARTITION_SIZE_10K
    next_partition_end = next_partition_start + PARTITION_SIZE_10K - 1

    loaded = 0
    for from_b, to_b, _ in store.list_loaded_ranges(include_sunk=False):
        if to_b < next_partition_start:
            continue
        if from_b > next_partition_end:
            break
        overlap_start = max(from_b, next_partition_start)
        overlap_end = min(to_b, next_partition_end)
        if overlap_start <= overlap_end:
            loaded += overlap_end - overlap_start + 1

    return min(100.0, (loaded / PARTITION_SIZE_10K) * 100.0)


# ---------------------------------------------------------------------------
# Status line state and rendering
# ---------------------------------------------------------------------------
#
# A single ``_live`` dict is updated from the main loop; the heartbeat thread reads it and
# re-prints the status line. Updates are coarse-grained and racy by design — the values are scalar
# and the worst that happens on a torn read is one stale digit on screen for ~500 ms before the
# next refresh.
#
# ``_print_lock`` serializes the only two writers to stdout: the heartbeat thread (status line)
# and ``_print_message`` (log lines). Without it, an inline log line printed during a heartbeat
# re-render would interleave its bytes with the status line.

_live: dict[str, object] = {}
_print_lock = threading.Lock()

# Persistent run log (opened once per invocation)
_log_file: TextIO | None = None


def _set_phase(text: str) -> None:
    _live["phase"] = text
    _live["phase_since"] = time.monotonic()


def _open_run_log() -> None:
    """Create and open a timestamped .log file for this scraper run."""
    global _log_file
    if _log_file is not None:
        return
    ts = datetime.now(timezone.utc).strftime("%Y-%m-%dT%H%M%SZ")
    log_dir = Path(__file__).resolve().parent / "logs"
    log_dir.mkdir(parents=True, exist_ok=True)
    log_path = log_dir / f"main-{ts}.log"
    _log_file = open(log_path, "a", encoding="utf-8")
    # Do not write a header here — _print_banner will be the first real content.
    _log_file.flush()


def _print_status_line(text: str) -> None:
    """Re-render the current status line in place (no newline)."""
    width = _term_width()
    with _print_lock:
        print(f"\r{text}".ljust(width)[:width], end="", flush=True)


def _print_message(text: str) -> None:
    """Print a normal log line above the status line.

    Wipes the status line first, prints the message, then re-renders
    the status line so the live display does not appear to scroll.
    Also appends the line to the persistent .log file if open.
    """
    width = _term_width()
    with _print_lock:
        print(f"\r{' ' * width}\r{text}", flush=True)
        line = _build_status_line()
        if line:
            print(f"\r{line}".ljust(width)[:width], end="", flush=True)
    # Also write to the run log (plain text, no carriage returns)
    global _log_file
    if _log_file is not None:
        _log_file.write(text + "\n")
        _log_file.flush()


def _build_status_line() -> str:
    s = _live
    run_start_value = s.get("run_start", 0)
    run_start = float(run_start_value) if isinstance(run_start_value, (int, float)) else 0.0
    if not run_start:
        return ""
    elapsed = time.monotonic() - run_start

    phase = str(s.get("phase", "")) or "running"

    # Startup phases want a different layout: no throughput is meaningful yet, just show what we
    # are waiting on.
    startup_task = s.get("startup_task")
    if startup_task:
        sunk_to_value = s.get("sunk_to", -1)
        sunk_to = int(sunk_to_value) if isinstance(sunk_to_value, int) else -1
        has_sunk = sunk_to > (SCRAPE_START_BLOCK - 1)
        parts = [
            f"[{_format_duration(elapsed)}]",
            f"sunk: {sunk_to:,}" if has_sunk else "sunk: none",
            f"phase:{phase}",
        ]
        startup_done = s.get("startup_done")
        startup_total = s.get("startup_total")
        startup_detail = s.get("startup_detail")
        if startup_total is not None and startup_done is not None:
            parts.append(f"{startup_task}:{startup_done:,}/{startup_total:,}")
        elif startup_done is not None:
            parts.append(f"{startup_task}:{startup_done:,}")
        if startup_detail:
            parts.append(str(startup_detail))
        return "  ".join(parts)

    blocks_done_value = s.get("blocks_done", 0)
    events_inserted_value = s.get("events_inserted", 0)
    calls_value = s.get("calls", 0)
    chain_head_value = s.get("chain_head", 0)
    sunk_to_value = s.get("sunk_to", -1)
    blocks_done = int(blocks_done_value) if isinstance(blocks_done_value, int) else 0
    events_inserted = int(events_inserted_value) if isinstance(events_inserted_value, int) else 0
    calls = int(calls_value) if isinstance(calls_value, int) else 0
    chain_head = int(chain_head_value) if isinstance(chain_head_value, int) else 0
    sunk_to = int(sunk_to_value) if isinstance(sunk_to_value, int) else -1
    next_pct = s.get("next_pct")
    has_sunk = sunk_to > (SCRAPE_START_BLOCK - 1)

    blk_s = blocks_done / elapsed if elapsed > 1 and blocks_done > 0 else 0.0
    ev_s = events_inserted / elapsed if elapsed > 1 else 0.0
    req_s = calls / elapsed if elapsed > 1 and calls > 0 else 0.0

    remaining_to_head = max(0, chain_head - max(sunk_to, SCRAPE_START_BLOCK - 1))
    eta = (
        _format_duration(remaining_to_head / blk_s)
        if blk_s > 0 and remaining_to_head > 0
        else "--:--:--"
    )

    if chain_head >= SCRAPE_START_BLOCK:
        head_text = f"{chain_head:,}"
        if isinstance(next_pct, (int, float)):
            next_text = f"{float(next_pct):.2f}%"
        else:
            next_text = "n/a"
    else:
        head_text = "n/a"
        next_text = "n/a"

    parts = [
        f"[{_format_duration(elapsed)}]",
        f"sunk: {sunk_to:,}" if has_sunk else "sunk: none",
        f"next: {next_text}",
        f"head: {head_text}",
        f"eta: {eta}",
    ]
    if phase and phase != "running":
        parts.append(f"phase:{phase}")

    return "  ".join(parts)


def _heartbeat_loop(stop: threading.Event) -> None:
    """Re-render the status line on a fixed cadence.

    Runs on its own thread until ``stop`` fires. The interval is ``HEARTBEAT_INTERVAL_SEC``
    (500 ms by default) which satisfies the "status output at least every 1000 ms" mission
    requirement with room to spare.
    """
    while not stop.wait(HEARTBEAT_INTERVAL_SEC):
        line = _build_status_line()
        if line:
            _print_status_line(line)


# ---------------------------------------------------------------------------
# RPC worker function
# ---------------------------------------------------------------------------

_worker_id_lock = threading.Lock()
_worker_id_counter = 0
_worker_ids: dict[int, int] = {}


def _get_worker_id() -> int:
    """Assign a small monotonic integer to each worker thread.

    Used only for cosmetic console output ("w:3"). The mapping is keyed by
    ``threading.get_ident()`` so reused thread IDs would collide, but ``ThreadPoolExecutor``
    reuses threads across requests by design, so the collision is what we want — one ID per worker.
    """
    tid = threading.get_ident()
    with _worker_id_lock:
        if tid not in _worker_ids:
            global _worker_id_counter
            _worker_id_counter += 1
            _worker_ids[tid] = _worker_id_counter
        return _worker_ids[tid]


def _fetch_range(
    *,
    from_block: int,
    to_block: int,
    rpc_client: RpcClient,
    rate_limiter: RateLimiter,
) -> dict:
    """Fetch and decode logs for one block range.

    Returns a dict with a ``status`` field, one of:

      * ``"ok"``        — fetch succeeded within ``SLOW_FETCH_THRESHOLD_SEC``.
      * ``"slow"``      — fetch succeeded but was over the threshold.
      * ``"split"``     — provider rejected the range as too large.
      * ``"timeout"``   — provider timed out.
      * ``"error"``     — any other transport-level error.
      * ``"cancelled"`` — ``_stop_event`` was set before the request finished.

    For ``ok`` and ``slow``, the dict also has ``rows_by_target``
    (``dict[(contract, event), list[row]]``), ``raw_log_count``, and
    ``elapsed_sec``.

    For ``error``, the dict has ``error_message``.

    The function never raises: the caller deals with structured outcomes only. Mid-flight
    cancellation via ``_stop_event`` returns ``"cancelled"`` rather than propagating
    ``OperationCancelled``.
    """
    wid = _get_worker_id()
    if _stop_event.is_set():
        return {"status": "cancelled", "wid": wid, "elapsed_sec": 0.0}
    rate_limiter.wait()
    if _stop_event.is_set():
        return {"status": "cancelled", "wid": wid, "elapsed_sec": 0.0}

    t0 = time.monotonic()
    try:
        logs = rpc_client.get_logs(
            addresses=WANTED_ADDRESSES,
            from_block=from_block,
            to_block=to_block,
            topics=[WANTED_TOPICS],
        )
    except TooManyResults:
        return {"status": "split", "wid": wid, "elapsed_sec": time.monotonic() - t0}
    except RPCTimeout:
        return {"status": "timeout", "wid": wid, "elapsed_sec": time.monotonic() - t0}
    except OperationCancelled:
        return {"status": "cancelled", "wid": wid, "elapsed_sec": time.monotonic() - t0}
    except (RPCError, RuntimeError) as e:
        return {
            "status": "error",
            "wid": wid,
            "elapsed_sec": time.monotonic() - t0,
            "error_message": f"{type(e).__name__}: {e}",
        }

    elapsed = time.monotonic() - t0
    raw_log_count = len(logs)

    # Strict decoding: a decoder bug must be fatal because silently dropping a malformed log would
    # create a hidden gap that no later assertion can detect. The orchestrator catches the
    # exception below.
    try:
        triples = decode_batch_strict(logs)
    except Exception as e:
        return {
            "status": "error",
            "wid": wid,
            "elapsed_sec": elapsed,
            "error_message": f"decode failure: {type(e).__name__}: {e}",
            "fatal": True,
        }

    rows_by_target: dict[tuple[str, str], list[dict]] = {}
    for contract, event, row in triples:
        rows_by_target.setdefault((contract, event), []).append(row)

    return {
        "status": "slow" if elapsed > SLOW_FETCH_THRESHOLD_SEC else "ok",
        "wid": wid,
        "elapsed_sec": elapsed,
        "rows_by_target": rows_by_target,
        "raw_log_count": raw_log_count,
    }


# ---------------------------------------------------------------------------
# Work queue helpers
# ---------------------------------------------------------------------------

def _build_chunks(
    gaps: list[tuple[int, int]],
    block_span: int,
) -> list[tuple[int, int]]:
    """Slice each gap into ``block_span``-sized chunks.

    Always preserves chunk boundaries within a gap: a gap of 17 blocks with span 5 becomes
    ``[(g, g+4), (g+5, g+9), (g+10, g+14), (g+15, g+16)]``.
    Gaps are never merged across non-contiguous boundaries.
    """
    out: list[tuple[int, int]] = []
    for gap_from, gap_to in gaps:
        cursor = gap_from
        while cursor <= gap_to:
            chunk_to = min(cursor + block_span - 1, gap_to)
            out.append((cursor, chunk_to))
            cursor = chunk_to + 1
    return out


def _rechunk_pending(
    pending: list[tuple[int, int]],
    block_span: int,
) -> list[tuple[int, int]]:
    """Merge contiguous pending chunks and re-slice at the new ``block_span``.

    Called whenever the adaptive controller changes the block span: chunks that were sized for the
    old span are coalesced into runs and then re-cut at the new size so the queue reflects the new
    policy.

    The input may be in arbitrary order — SPLIT and TIMEOUT outcomes push retry chunks to the
    front of the queue, so by the time we rechunk, ``pending`` is not sorted. We sort first, then
    run the merge-pass over the sorted list. Without this, the merge would drop any chunk whose
    ``from`` block is below the running ``runs[-1][1]`` watermark — exactly the SPLIT halves that
    need to be retried, which has been observed leaving "swiss cheese" gaps in
    ``loaded_block_ranges``.
    """
    if not pending:
        return []
    sorted_pending = sorted(pending)
    runs = [[sorted_pending[0][0], sorted_pending[0][1]]]
    for a, b in sorted_pending[1:]:
        if a <= runs[-1][1] + 1:
            runs[-1][1] = max(runs[-1][1], b)
        else:
            runs.append([a, b])
    return _build_chunks([(r[0], r[1]) for r in runs], block_span)


# ---------------------------------------------------------------------------
# Sink orchestration
# ---------------------------------------------------------------------------
#
# ``write_partition_files`` runs in a background pool. The orchestrator tracks each in-flight
# write by its ``partition_start`` so it can call ``HotStore.commit_sink`` in commit-frontier
# order when futures finish.
#
# Frontier order matters: ``commit_sink`` only accepts the partition immediately above the current
# sunk frontier. The sink pool may finish out of order (faster partitions complete first), so the
# orchestrator buffers completed-but-not-yet-committed results and drains them in order.

class _SinkOrchestrator:
    """Manage the sink worker pool and frontier-ordered commits.

    Owns:
      * a ``ThreadPoolExecutor`` with at most ``--sink-workers`` threads;
      * a map ``Future -> partition_start`` for in-flight writes;
      * a buffer of completed-but-not-yet-committed ``(partition_start, write_result)`` entries,
        keyed by ``partition_start`` so the orchestrator can pop them in frontier order.

    Used pattern:

        sink = _SinkOrchestrator(store, db_path, cold_root, workers)
        for p in result.ready_partitions:
            sink.submit(p)
        sink.drain_ready()       # called from the main loop periodically
        sink.shutdown(wait=True)  # at the end
    """

    def __init__(
        self,
        store: HotStore,
        db_path: str,
        cold_root: str,
        max_workers: int,
    ) -> None:
        self._store = store
        self._db_path = db_path
        self._cold_root = cold_root
        self._executor = ThreadPoolExecutor(
            max_workers=max_workers,
            thread_name_prefix="sink",
        )
        self._in_flight: dict[Future[PartitionWriteResult], int] = {}
        self._completed: dict[int, PartitionWriteResult] = {}
        self._submitted_partitions: set[int] = set()
        self._fatal_error: BaseException | None = None

    # --------------------------------------------------------------
    # Public API
    # --------------------------------------------------------------

    @property
    def in_flight(self) -> int:
        return len(self._in_flight)

    @property
    def pending_commit(self) -> int:
        return len(self._completed)

    @property
    def fatal_error(self) -> BaseException | None:
        return self._fatal_error

    def submit(self, partition_start: int) -> None:
        """Schedule one partition for sinking.

        Idempotent in the sense that already-submitted partitions are skipped — useful because
        ``HotStore.persist`` may surface the same partition both via its ``ready_partitions`` field
        and via a startup ``list_10k_partitions_ready_to_sink`` call.
        """
        if partition_start in self._submitted_partitions:
            return
        if _stop_event.is_set():
            return
        self._submitted_partitions.add(partition_start)
        # Pass the orchestrator's hot-DB connection so the worker can open a cursor on it (DuckDB
        # does not permit a second ``duckdb.connect()`` against an already-open ``.db`` file in
        # the same process, but cursors off the same connection are thread-safe and see committed
        # rows via MVCC).
        future = self._executor.submit(
            write_partition_files,
            self._db_path,
            self._cold_root,
            partition_start,
            connection=self._store.connection,
            stop_event=_stop_event,
        )
        self._in_flight[future] = partition_start

    def reap_completed(self) -> None:
        """Move any finished futures into the commit-pending buffer.

        Non-blocking: only checks ``future.done()`` for each in-flight future. The actual commit
        happens in ``commit_ready``, which is also non-blocking (it just checks the buffer head
        against the sunk frontier).
        """
        done = [f for f in self._in_flight if f.done()]
        for fut in done:
            partition_start = self._in_flight.pop(fut)
            try:
                result = fut.result()
            except BaseException as e:  # noqa: BLE001
                # Any worker exception is fatal: the cold tier could be inconsistent (some temp
                # files unrenamed, some renamed in earlier partitions). Surface to the main loop
                # so it can stop everything cleanly. Don't re-raise here so we can keep reaping
                # the rest of the in-flight set for a cleaner shutdown.
                self._submitted_partitions.discard(partition_start)
                self._fatal_error = e
                _print_message(
                    f"  SINK FAIL  10K={partition_start:,}  "
                    f"{type(e).__name__}: {e}"
                )
                continue
            self._completed[partition_start] = result

    def commit_ready(self) -> int:
        """Commit every completed partition that is at the sunk frontier.

        Returns the number of commits performed. Each call advances the sunk frontier by exactly
        that many 10K partitions. Stops as soon as the next-expected partition is not yet in the
        buffer.

        Must be called from the orchestrator thread because
        ``HotStore.commit_sink`` is single-threaded.
        """
        n_committed = 0
        while self._completed:
            sunk_frontier = self._store.get_sunk_frontier()
            if sunk_frontier <= SCRAPE_START_BLOCK - 1:
                expected_next = (SCRAPE_START_BLOCK // PARTITION_SIZE_10K) * PARTITION_SIZE_10K
            else:
                expected_next = ((sunk_frontier + 1) // PARTITION_SIZE_10K) * PARTITION_SIZE_10K
            if expected_next not in self._completed:
                return n_committed
            write_result = self._completed.pop(expected_next)
            try:
                self._store.commit_sink(expected_next)
            except PartitionFrontierError as e:
                # The library should not get here if we are computing ``expected_next`` correctly,
                # so treat any frontier rejection as fatal rather than silently dropping the
                # commit.
                self._fatal_error = e
                _print_message(
                    f"  COMMIT_SINK FRONTIER ERROR  10K={expected_next:,}: {e}"
                )
                return n_committed
            except V3Error as e:
                self._fatal_error = e
                _print_message(
                    f"  COMMIT_SINK FAIL  10K={expected_next:,}: {e}"
                )
                return n_committed
            n_committed += 1
            mb = write_result.bytes_total / (1024 * 1024)
            partition_end = expected_next + PARTITION_SIZE_10K - 1
            _print_message(
                f"  <- sunk {partition_end:,}"
                f"  rows: {write_result.rows_total:,}"
                f"  bytes: {mb:.1f} MiB"
                f"  {write_result.elapsed_ms / 1000:.1f}s"
            )
        return n_committed

    def shutdown(self, wait: bool) -> None:
        """Stop accepting new submissions and (optionally) wait for the pool."""
        self._executor.shutdown(wait=wait, cancel_futures=not wait)


# ---------------------------------------------------------------------------
# Banner
# ---------------------------------------------------------------------------

def _print_banner(
    *,
    db_path: str,
    cold_root: str,
    manifest_frontier: int,
    sunk_frontier: int,
    loaded_frontier: int,
    chain_head: int,
    gaps: list[tuple[int, int]],
    total_gap_blocks: int,
    unsunk_ranges: int,
) -> None:
    """Print the once-per-run startup summary above the live status line.

    Items shown:

        * absolute paths in use;
        * progress of the sunk and loaded frontiers;
        * chain head;
        * gap list (truncated to a few entries) with total backlog size.
    """
    now = datetime.now(timezone.utc).isoformat()
    banner_lines = [
        f"# main — start at {now}",
        f"hot db:               {db_path} ({_file_size_str(db_path)})",
        f"cold root:            {cold_root}",
        "",
    ]
    if sunk_frontier <= SCRAPE_START_BLOCK - 1:
        banner_lines.append("sunk parquet to:      none")
    else:
        banner_lines.append(f"sunk parquet to:      {sunk_frontier:,}")
    if manifest_frontier < 0:
        banner_lines.append("manifest frontier:    none")
    else:
        banner_lines.append(f"manifest frontier:    {manifest_frontier:,}")
    if loaded_frontier < 0:
        banner_lines.append("loaded hot frontier:  none")
    else:
        banner_lines.append(f"loaded hot frontier:  {loaded_frontier:,}")
    if unsunk_ranges > 0:
        banner_lines.append(f"unsunk ranges in hot: {unsunk_ranges}")
    banner_lines.append(f"chain head:           {chain_head:,}")
    banner_lines.append("")
    if gaps:
        banner_lines.append(
            f"blocks to scrape:     {total_gap_blocks:,} blocks "
            f"(~{_blocks_behind_str(total_gap_blocks)} behind), "
            f"{len(gaps)} range{'s' if len(gaps) != 1 else ''}"
        )
        for f, t in gaps[:6]:
            banner_lines.append(f"    {f:,}–{t:,} ({t - f + 1:,} blocks)")
        if len(gaps) > 6:
            banner_lines.append(f"    ... {len(gaps) - 6} more ...")
    else:
        banner_lines.append("blocks to scrape:     0 (caught up)")
    banner_lines.append("")

    for line in banner_lines:
        print(line)
        if _log_file is not None:
            _log_file.write(line + "\n")
    if _log_file is not None:
        _log_file.flush()


# ---------------------------------------------------------------------------
# Final summary
# ---------------------------------------------------------------------------

def _print_summary(
    *,
    store: HotStore | None,
    manifest_frontier: int,
    chain_head: int,
    caught_up_confirmed: bool,
    blocks_done: int,
    events_inserted: int,
    rate_limiter: RateLimiter,
    elapsed: float,
    fatal_reason: str | None,
) -> None:
    summary_lines = []
    summary_lines.append("")
    summary_lines.append("=" * 70)
    summary_lines.append(f"run complete  ({_format_duration(elapsed)})")
    summary_lines.append("=" * 70)

    if fatal_reason:
        summary_lines.append("")
        summary_lines.append("status")
        summary_lines.append(f"  FATAL: {fatal_reason}")
    elif caught_up_confirmed:
        summary_lines.append("")
        summary_lines.append("status")
        summary_lines.append("  CAUGHT UP - no new blocks to scrape")
    elif blocks_done == 0:
        summary_lines.append("")
        summary_lines.append("status")
        summary_lines.append("  NO PROGRESS - run ended without confirmed caught-up state")
    else:
        summary_lines.append("")
        summary_lines.append("status")
        summary_lines.append("  OK")

    sunk = None
    loaded = None
    lag = chain_head - SCRAPE_START_BLOCK
    if store is not None:
        sunk = store.get_sunk_frontier()
        loaded = store.get_loaded_frontier()
        summary_lines.append("")
        summary_lines.append("frontier evidence")
        summary_lines.append(
            f"  manifest frontier:   {manifest_frontier:,}"
            if manifest_frontier >= 0
            else "  manifest frontier:   none"
        )
        summary_lines.append(
            f"  sunk parquet to:     {sunk:,}"
            if sunk is not None and sunk > (SCRAPE_START_BLOCK - 1)
            else "  sunk parquet to:     none"
        )
        summary_lines.append(
            f"  loaded hot frontier:  {loaded:,}" if loaded is not None and loaded >= 0 else "  loaded hot frontier: none"
        )

    if store is not None:
        hot_db_blocks = sum(
            (to_b - from_b + 1)
            for from_b, to_b, _ in store.list_loaded_ranges(include_sunk=False)
        )
        summary_lines.append("")
        summary_lines.append("progress")
        summary_lines.append(
            f"  sunk parquet to:      {sunk:,}"
            if sunk is not None and sunk > (SCRAPE_START_BLOCK - 1)
            else "  sunk parquet to:      none"
        )
        summary_lines.append(
            f"  manifest frontier:    {manifest_frontier:,}"
            if manifest_frontier >= 0
            else "  manifest frontier:    none"
        )
        summary_lines.append(f"  hot database:         {hot_db_blocks:,} blocks")
        summary_lines.append(f"  chain head:           {chain_head:,}")
        summary_lines.append(f"  blocks processed:     +{blocks_done:,} this run")
    summary_lines.append("")
    summary_lines.append("throughput")
    blk_s = blocks_done / elapsed if elapsed > 1 and blocks_done > 0 else 0.0
    ev_s = events_inserted / elapsed if elapsed > 1 else 0.0
    cps = rate_limiter.total_calls / elapsed if elapsed > 1 and rate_limiter.total_calls else 0.0
    summary_lines.append(f"  blocks/sec:  {blk_s:,.0f}")
    summary_lines.append(f"  events/sec:  {ev_s:,.0f}")
    summary_lines.append(f"  API calls:   {rate_limiter.total_calls:,}  ({cps:.1f}/s effective)")

    for line in summary_lines:
        print(line)
        if _log_file is not None:
            _log_file.write(line + "\n")
    if _log_file is not None:
        _log_file.flush()


# ---------------------------------------------------------------------------
# Main
# ---------------------------------------------------------------------------

def main() -> None:
    # ----- Signal handling. Install BEFORE anything else so even a CTRL-C during env loading is
    # honored. ---------------------------------------------------------
    interrupt_count = [0]

    def _on_sigint(_sig, _frame):
        interrupt_count[0] += 1
        _stop_event.set()
        if interrupt_count[0] >= FORCE_EXIT_AFTER_INTERRUPTS:
            # User said "really, stop". Bypass orderly shutdown — every file in flight has either
            # been renamed atomically (visible only after success) or is a ``.tmp-*`` file that
            # ``remove_orphan_temp_files`` will clean at next startup.
            print("\n  Force exit (second interrupt).")
            os._exit(1)
        # First interrupt: raise so cooperating code can shut down cleanly.
        raise KeyboardInterrupt

    signal.signal(signal.SIGINT, _on_sigint)
    signal.signal(signal.SIGTERM, _on_sigint)

    # ----- Parse args ------------------------------------------------
    parser = argparse.ArgumentParser(
        description="Scrape Polymarket event logs from Polygon JSON-RPC into the v3 hot DB and cold Parquet tree.",
    )
    parser.add_argument(
        "--parallel", type=int, default=8,
        help="ceiling on concurrent RPC workers (default: 8)",
    )
    parser.add_argument(
        "--sink-workers", type=int, default=1,
        help="concurrent Parquet sink workers (default: 1)",
    )
    parser.add_argument(
        "--run-dirty",
        action="store_true",
        help="allow startup even when git working tree is dirty",
    )
    parser.add_argument(
        "--max-calls", type=int, default=None,
        help="stop after this many RPC calls (default: no limit)",
    )
    parser.add_argument(
        "--lag-tolerance", type=int, default=2,
        help="stop when within N blocks of chain head (default: 2)",
    )
    args = parser.parse_args()

    if args.parallel < 1:
        sys.exit(f"--parallel must be >= 1; got {args.parallel}")
    if args.sink_workers < 1:
        sys.exit(f"--sink-workers must be >= 1; got {args.sink_workers}")
    if args.max_calls is not None and args.max_calls < 1:
        sys.exit(f"--max-calls must be >= 1; got {args.max_calls}")
    if args.lag_tolerance < 0:
        sys.exit(f"--lag-tolerance must be >= 0; got {args.lag_tolerance}")

    env = _load_environment(allow_dirty=args.run_dirty)
    max_block_span = int(env["max_block_span"])
    max_rps = int(env["max_rps"])

    # ----- Wire up state --------------------------------------------
    run_start = time.monotonic()
    _live.update({
        "run_start": run_start,
        "phase": "startup",
        "phase_since": run_start,
        "max_workers": args.parallel,
        "max_span": max_block_span,
    })

    heartbeat_stop = threading.Event()
    heartbeat = threading.Thread(
        target=_heartbeat_loop, args=(heartbeat_stop,),
        name="heartbeat", daemon=True,
    )
    heartbeat.start()

    # Open the persistent run log as early as possible
    _open_run_log()

    rate_limiter = RateLimiter(max_rps)
    rpc_client = RpcClient(
        env["rpc_url"],
        config=RpcClientConfig(
            call_timeout_sec=RPC_CALL_TIMEOUT_SEC,
            block_number_timeout_sec=RPC_BLOCK_NUMBER_TIMEOUT_SEC,
            max_retries=RPC_MAX_RETRIES,
            retry_backoff_base_sec=RPC_BACKOFF_BASE_SEC,
        ),
        stop_event=_stop_event,
    )

    store: HotStore | None = None
    sink: _SinkOrchestrator | None = None
    rpc_pool: ThreadPoolExecutor | None = None
    rpc_futures: dict[Future[dict], tuple[int, int]] = {}

    fatal_reason: str | None = None
    system_exit_requested = False
    manifest_frontier = SCRAPE_START_BLOCK - 1
    sunk_frontier = SCRAPE_START_BLOCK - 1
    loaded_frontier = SCRAPE_START_BLOCK - 1
    caught_up_confirmed = False
    blocks_done = 0
    events_inserted = 0
    chain_head = 0

    try:
        # --- HotStore -----------------------------------------------
        _set_phase("opening hot db")
        try:
            store = HotStore(
                env["db_path"],
                _SCHEMA_SQL,
                config=HotStoreConfig(duckdb_temp_dir=env["temp_dir"]),
            )
        except SchemaMismatchError as e:
            sys.exit(
                f"Hot DB schema mismatch at {env['db_path']}: {e}\n"
                f"Delete the hot DB file and re-run to recreate it from schema.sql."
            )
        except (FileNotFoundError, V3Error) as e:
            sys.exit(f"Could not open hot DB at {env['db_path']}: {e}")

        # Seed status line with current sunk frontier so startup phases can show it.
        start_sunk_frontier = store.get_sunk_frontier()
        _live["sunk_to"] = (
            start_sunk_frontier if start_sunk_frontier > (SCRAPE_START_BLOCK - 1) else -1
        )

        # --- Chain head and gap discovery ---------------------------
        # Read the chain head early so startup can prove whether the cold tier is already caught
        # up even if later startup phases return early.
        _set_phase("querying chain head")
        try:
            chain_head = rpc_client.get_block_number()
        except (RPCError, RuntimeError) as e:
            sys.exit(f"FATAL: could not read chain head: {type(e).__name__}: {e}")

        if chain_head < SCRAPE_START_BLOCK:
            sys.exit(
                f"FATAL: RPC returned chain head {chain_head:,}, which is before "
                f"SCRAPE_START_BLOCK {SCRAPE_START_BLOCK:,}. The RPC endpoint may be "
                f"returning incorrect data or is in a failed state."
            )

        _print_message(f"Chain head query complete: {chain_head:,}.")

        # --- Clean orphan temp files left over from prior crashes ----
        _live["startup_task"] = "cleaning"
        _live["startup_detail"] = "scanning for .tmp-* files"
        try:
            orphans = remove_orphan_temp_files(env["cold_root"], older_than_seconds=60.0)
        except OSError as e:
            sys.exit(f"FATAL: could not scan cold tier for orphan temp files: {e}")
        _live["startup_task"] = None
        _live["startup_detail"] = None
        if orphans:
            _print_message(f"Removed {orphans} orphaned ``.tmp-*`` file(s) from prior runs.")

        # --- Read manifest frontier --------------------------------
        _set_phase("reading frontier")

        def _frontier_progress(*, op, phase, rows_done=None, rows_total=None,
                               elapsed_ms=None, message=""):
            _live["startup_task"] = "frontier"
            if rows_total is not None and rows_done is not None:
                _live["startup_detail"] = f"{phase}:{rows_done:,}/{rows_total:,}"
            elif rows_done is not None:
                _live["startup_detail"] = f"{phase}:{rows_done:,}"
            else:
                _live["startup_detail"] = phase
            if message:
                _live["startup_detail"] = f"{_live['startup_detail']} {message}"
            _live["startup_done"] = rows_done
            _live["startup_total"] = rows_total

        try:
            manifest_frontier, manifest_partition_count = read_manifest_frontier(
                env["cold_root"],
                progress_cb=_frontier_progress,
            )
        except V3Error as e:
            sys.exit(f"FATAL: manifest frontier read failed: {e}")
        finally:
            _live["startup_task"] = None
            _live["startup_done"] = None
            _live["startup_total"] = None
            _live["startup_detail"] = None

        _print_message(
            "Manifest frontier read complete: "
            f"frontier={manifest_frontier:,}, partitions={manifest_partition_count:,}."
        )

        # --- Roll manifest frontier forward -------------------------
        _set_phase("rolling forward frontier")

        def _manifest_progress(*, op, phase, rows_done=None, rows_total=None,
                               elapsed_ms=None, message=""):
            _live["startup_task"] = "manifest"
            if rows_total is not None and rows_done is not None:
                _live["startup_detail"] = f"{phase}:{rows_done:,}/{rows_total:,}"
            elif rows_done is not None:
                _live["startup_detail"] = f"{phase}:{rows_done:,}"
            else:
                _live["startup_detail"] = phase
            if message:
                _live["startup_detail"] = f"{_live['startup_detail']} {message}"
            _live["startup_done"] = rows_done
            _live["startup_total"] = rows_total

        try:
            published_manifests = roll_forward_manifests_to_exhaustion(
                env["cold_root"],
                progress_cb=_manifest_progress,
                stop_event=_stop_event,
            )
        except OperationCancelled:
            raise KeyboardInterrupt
        except V3Error as e:
            sys.exit(f"FATAL: manifest startup checks failed: {e}")
        finally:
            _live["startup_task"] = None
            _live["startup_done"] = None
            _live["startup_total"] = None
            _live["startup_detail"] = None

        _print_message(
            "Manifest roll-forward complete: "
            f"published={published_manifests:,} partition(s)."
        )

        # --- Cleanup after frontier ---------------------------------
        _set_phase("cleanup after frontier")
        _live["startup_task"] = "cleanup"
        _live["startup_detail"] = "post-frontier temp cleanup"
        try:
            cleaned_manifest = cleanup_temp_dirs_after_frontier(
                env["cold_root"],
                progress_cb=_manifest_progress,
            )
        except V3Error as e:
            sys.exit(f"FATAL: post-frontier cleanup failed: {e}")
        finally:
            _live["startup_task"] = None
            _live["startup_done"] = None
            _live["startup_total"] = None
            _live["startup_detail"] = None

        _print_message(
            "Post-frontier cleanup complete: "
            f"removed={cleaned_manifest:,} path(s)."
        )

        manifest_frontier = get_manifest_frontier(env["cold_root"])
        _live["manifest_to"] = manifest_frontier if manifest_frontier >= 0 else -1

        # --- Reconcile hot DB progress with cold tier ----------------
        # If we crashed between a sink worker's rename and the orchestrator's commit_sink, the
        # Parquet files reached disk but the hot DB still has the rows. Reconcile fixes that
        # without re-scraping.
        _set_phase("reconciling cold tier")

        def _reconcile_progress(*, op, phase, rows_done=None, rows_total=None,
                                elapsed_ms=None, message=""):
            _live["startup_task"] = "reconciling"
            if phase == "scan":
                _live["startup_detail"] = f"scan:{rows_done or 0:,}"
                if rows_total:
                    _live["startup_detail"] = f"scan:{rows_done or 0:,}/{rows_total:,}"
            elif phase == "update":
                _live["startup_detail"] = f"update:{rows_done or 0:,}"
            else:
                _live["startup_detail"] = message or phase
            _live["startup_done"] = rows_done
            _live["startup_total"] = rows_total
            # Keep sunk frontier live during reconcile so startup status reflects progress.
            current_sunk = store.get_sunk_frontier()
            _live["sunk_to"] = current_sunk if current_sunk > (SCRAPE_START_BLOCK - 1) else -1

        try:
            newly_sunk = store.reconcile_with_cold_tier(
                env["cold_root"],
                progress_cb=_reconcile_progress,
                stop_event=_stop_event,
                manifest_frontier=manifest_frontier,
            )
        except OperationCancelled:
            raise KeyboardInterrupt
        except V3Error as e:
            sys.exit(f"FATAL: reconcile_with_cold_tier failed: {e}")
        finally:
            _live["startup_task"] = None
            _live["startup_done"] = None
            _live["startup_total"] = None
            _live["startup_detail"] = None
        if newly_sunk:
            _print_message(f"Reconciled {newly_sunk:,} partition(s) from existing cold-tier files.")

        # --- Sink orchestrator --------------------------------------
        sink = _SinkOrchestrator(
            store=store,
            db_path=env["db_path"],
            cold_root=env["cold_root"],
            max_workers=args.sink_workers,
        )

        # Anything already in the hot DB and ready to sink at startup (e.g. a clean shutdown after
        # persist but before sink) should go to the sink pool immediately.
        backlog = store.list_10k_partitions_ready_to_sink()
        for p in backlog:
            sink.submit(p)
        if backlog:
            _print_message(f"Submitted {len(backlog):,} pre-existing ready partition(s) to the sink pool.")

        gaps = store.find_gaps(SCRAPE_START_BLOCK, chain_head)
        total_gap_blocks = sum(t - f + 1 for f, t in gaps)
        sunk_frontier = store.get_sunk_frontier()
        loaded_frontier = store.get_loaded_frontier()
        unsunk_ranges = len(store.list_loaded_ranges(include_sunk=False))

        _print_banner(
            db_path=env["db_path"],
            cold_root=env["cold_root"],
            manifest_frontier=manifest_frontier,
            sunk_frontier=sunk_frontier,
            loaded_frontier=loaded_frontier,
            chain_head=chain_head,
            gaps=gaps,
            total_gap_blocks=total_gap_blocks,
            unsunk_ranges=unsunk_ranges,
        )

        _live.update({
            "total_blocks": total_gap_blocks,
            "blocks_done": 0,
            "events_inserted": 0,
            "calls": 0,
            "active_workers": 1,
            "span": 1,
            "in_flight_rpc": 0,
            "sink_pending": sink.in_flight,
            "sunk_to": sunk_frontier if sunk_frontier >= 0 else -1,
            "next_pct": _next_partition_fill_pct(
                store=store,
                sunk_frontier=sunk_frontier,
                chain_head=chain_head,
            ),
            "chain_head": chain_head,
        })
        _set_phase("running")

        if not gaps and sink.in_flight == 0 and sink.pending_commit == 0:
            caught_up_confirmed = True
            lag = chain_head - loaded_frontier if loaded_frontier >= 0 else chain_head - SCRAPE_START_BLOCK
            _print_banner(
                db_path=env["db_path"],
                cold_root=env["cold_root"],
                manifest_frontier=manifest_frontier,
                sunk_frontier=sunk_frontier,
                loaded_frontier=loaded_frontier,
                chain_head=chain_head,
                gaps=[],
                total_gap_blocks=0,
                unsunk_ranges=unsunk_ranges,
            )
            print(
                "caught-up proof: "
                f"manifest_frontier={manifest_frontier:,} "
                f"sunk_frontier={sunk_frontier:,} "
                f"loaded_frontier={loaded_frontier:,} "
                f"chain_head={chain_head:,} "
                f"lag={lag:,} blocks",
                flush=True,
            )
            print()
            print("=" * 70)
            print("ALREADY CAUGHT UP")
            print("=" * 70)
            print(f"  loaded frontier:  {loaded_frontier:,}")
            print(f"  manifest frontier: {manifest_frontier:,}")
            print(f"  chain head:       {chain_head:,}")
            print(f"  lag:              {lag:,} blocks (tolerance: {args.lag_tolerance})")
            print()
            print("No new work to do. Exiting cleanly.")
            print()
            return

        # --- Set up the adaptive controller and work queue ----------
        controller = AdaptiveController(
            worker_ceiling=args.parallel,
            max_block_span=max_block_span,
        )
        pending_chunks: list[tuple[int, int]] = _build_chunks(gaps, controller.block_span)
        chunk_failures: dict[tuple[int, int], int] = {}

        # RPC worker pool. We don't use ``concurrent.futures.wait``'s timeout-only-on-
        # FIRST_COMPLETED with a huge pool; one pool sized to ``--parallel`` is plenty. The
        # controller's ``active_workers`` value caps how many we submit at once.
        rpc_pool = ThreadPoolExecutor(
            max_workers=args.parallel,
            thread_name_prefix="rpc",
        )

        def _submit_one(chunk: tuple[int, int]) -> bool:
            """Submit one chunk. Returns False on call-count limit."""
            if _stop_event.is_set():
                return False
            if args.max_calls is not None and rate_limiter.total_calls >= args.max_calls:
                return False
            fut = rpc_pool.submit(
                _fetch_range,
                from_block=chunk[0], to_block=chunk[1],
                rpc_client=rpc_client, rate_limiter=rate_limiter,
            )
            rpc_futures[fut] = chunk
            return True

        def _top_up_in_flight() -> None:
            """Keep ``len(rpc_futures) == controller.active_workers``
            by submitting from the pending queue.
            """
            while pending_chunks and len(rpc_futures) < controller.active_workers:
                chunk = pending_chunks.pop(0)
                if not _submit_one(chunk):
                    # Put it back so we don't lose it.
                    pending_chunks.insert(0, chunk)
                    return

        _top_up_in_flight()
        if not rpc_futures:
            fatal_reason = (
                "No work could be submitted at startup (call-count limit hit "
                "before scraping began?)."
            )
            _print_message(f"FATAL: {fatal_reason}")
            return

        # --- Main loop ----------------------------------------------
        while rpc_futures and not _stop_event.is_set() and sink.fatal_error is None:
            done, _ = concurrent.futures.wait(
                rpc_futures,
                timeout=POLL_TIMEOUT_SEC,
                return_when=concurrent.futures.FIRST_COMPLETED,
            )

            # Even if no RPC future completed, run sink bookkeeping so
            # commits don't pile up.
            sink.reap_completed()
            sink.commit_ready()
            if sink.fatal_error is not None:
                fatal_reason = f"sink worker failed: {sink.fatal_error}"
                _stop_event.set()
                break

            _live["in_flight_rpc"] = len(rpc_futures)
            _live["sink_pending"] = sink.in_flight + sink.pending_commit
            _live["calls"] = rate_limiter.total_calls
            _live["active_workers"] = controller.active_workers
            _live["span"] = controller.block_span
            _live["blocks_done"] = blocks_done
            _live["events_inserted"] = events_inserted
            sf = store.get_sunk_frontier()
            _live["sunk_to"] = sf if sf >= 0 else -1
            _live["next_pct"] = _next_partition_fill_pct(
                store=store,
                sunk_frontier=sf,
                chain_head=chain_head,
            )

            if not done:
                continue

            span_changed_any = False

            for fut in done:
                chunk = rpc_futures.pop(fut)
                from_b, to_b = chunk
                chunk_span = to_b - from_b + 1
                try:
                    result = fut.result()
                except BaseException as e:  # noqa: BLE001
                    # ``_fetch_range`` is supposed to translate every error into a structured
                    # outcome. An exception here would mean we found a code path that doesn't, so
                    # fail loudly rather than silently retry.
                    fatal_reason = (
                        f"_fetch_range raised unexpectedly on "
                        f"[{from_b:,}-{to_b:,}]: {type(e).__name__}: {e}"
                    )
                    _print_message(f"FATAL: {fatal_reason}")
                    _stop_event.set()
                    break

                status = result["status"]

                if status == "cancelled":
                    # Shutting down. Put the chunk back at the front so
                    # the next run picks it up. (find_gaps will do so
                    # because nothing was committed for this range.)
                    continue

                if status in ("ok", "slow"):
                    raw_log_count = int(result["raw_log_count"])
                    elapsed_s = float(result["elapsed_sec"])
                    rows_by_target: dict[tuple[str, str], list[dict]] = result["rows_by_target"]
                    wid = int(result["wid"])

                    # Persist into the hot DB. Single atomic transaction.
                    try:
                        persist_result = store.persist(
                            from_block=from_b,
                            to_block=to_b,
                            rows_by_target=rows_by_target,
                            stop_event=_stop_event,
                        )
                    except OperationCancelled:
                        # Treat as cancelled-chunk: bail without advancing accounting.
                        continue
                    except DuplicateRowError as e:
                        fatal_reason = f"duplicate event in [{from_b:,}-{to_b:,}]: {e}"
                        _print_message(f"FATAL: {fatal_reason}")
                        _stop_event.set()
                        break
                    except (ValueError, V3Error) as e:
                        # If we are already shutting down, the DuckDB transaction was almost
                        # certainly aborted by the connection interrupt that the SIGINT handler
                        # triggers; that is expected and not fatal. The chunk just stays unrecorded
                        # in ``loaded_block_ranges`` and will be picked up again via ``find_gaps``
                        # on the next run.
                        if _stop_event.is_set():
                            continue
                        fatal_reason = (
                            f"persist failed for [{from_b:,}-{to_b:,}]: "
                            f"{type(e).__name__}: {e}"
                        )
                        _print_message(f"FATAL: {fatal_reason}")
                        _stop_event.set()
                        break

                    events_inserted += persist_result.rows_inserted
                    blocks_done += chunk_span

                    # Refresh next_pct for accurate status line display
                    sf = store.get_sunk_frontier()
                    _live["next_pct"] = _next_partition_fill_pct(
                        store=store,
                        sunk_frontier=sf,
                        chain_head=chain_head,
                    )

                    # Compute how many blocks in this range fall beyond the next sink partition
                    next_partition_start = (
                        (max(sf, SCRAPE_START_BLOCK - 1) + 1) // PARTITION_SIZE_10K
                    ) * PARTITION_SIZE_10K
                    next_partition_end = next_partition_start + PARTITION_SIZE_10K - 1
                    beyond_blks = max(0, to_b - next_partition_end)

                    # Print after persist succeeds
                    _suffix = f" (beyond next: {beyond_blks:,} blks)" if beyond_blks > 0 else ""
                    _print_message(
                        f"  -> blks: {chunk_span:,}"
                        f"  evts: {raw_log_count:,}"
                        f"  {elapsed_s:.1f}s"
                        f"{_suffix}"
                    )

                    for p in persist_result.ready_partitions:
                        sink.submit(p)

                    # Tell the controller. ``record_ok``/``record_slow`` return whether the targets
                    # changed.
                    if status == "ok":
                        span_changed, workers_changed = controller.record_ok(
                            chunk_span=chunk_span,
                            raw_log_count=raw_log_count,
                        )
                    else:
                        span_changed, workers_changed = controller.record_slow(
                            chunk_span=chunk_span,
                        )
                    if span_changed:
                        _print_message(
                            f"  span -> {controller.block_span:,}"
                            f"  (slow-start={'on' if controller.span_in_slow_start else 'off'})"
                        )
                        span_changed_any = True
                    if workers_changed:
                        _print_message(
                            f"  workers -> {controller.active_workers}"
                            f"  (slow-start={'on' if controller.workers_in_slow_start else 'off'})"
                        )

                elif status == "split":
                    elapsed_s = float(result["elapsed_sec"])
                    wid = int(result["wid"])
                    _print_message(
                        f"  SPLIT  w:{wid}  blks:{chunk_span:,}  "
                        f"[{from_b:,}-{to_b:,}]  {elapsed_s:.1f}s"
                    )
                    if chunk_span <= 1:
                        # Provider rejected a single-block range. Skip rather than loop forever;
                        # the chunk stays out of ``loaded_block_ranges`` so the next run will retry
                        # it via ``find_gaps``.
                        _print_message(
                            f"  Single-block range {from_b} rejected. "
                            "Skipping; will be retried on next run."
                        )
                        controller.record_split()
                        continue
                    mid = (from_b + to_b) // 2
                    pending_chunks.insert(0, (mid + 1, to_b))
                    pending_chunks.insert(0, (from_b, mid))
                    span_changed, _ = controller.record_split()
                    if span_changed:
                        _print_message(f"  span -> {controller.block_span:,}  (after SPLIT)")
                        span_changed_any = True

                elif status == "timeout":
                    elapsed_s = float(result["elapsed_sec"])
                    wid = int(result["wid"])
                    _print_message(
                        f"  TMOUT  w:{wid}  blks:{chunk_span:,}  "
                        f"[{from_b:,}-{to_b:,}]  {elapsed_s:.1f}s"
                    )
                    pending_chunks.insert(0, chunk)
                    span_changed, workers_changed = controller.record_timeout()
                    if span_changed:
                        _print_message(f"  span -> {controller.block_span:,}  (after TMOUT)")
                        span_changed_any = True
                    if workers_changed:
                        _print_message(f"  workers -> {controller.active_workers}  (after TMOUT)")

                elif status == "error":
                    err_msg = str(result.get("error_message", ""))
                    wid = int(result["wid"])
                    elapsed_s = float(result["elapsed_sec"])
                    fatal = bool(result.get("fatal", False))
                    if fatal:
                        # Decode failure: never retry — there's a code bug to fix first.
                        fatal_reason = (
                            f"fatal error on [{from_b:,}-{to_b:,}]: {err_msg}"
                        )
                        _print_message(f"FATAL: {fatal_reason}")
                        _stop_event.set()
                        break

                    _print_message(
                        f"  ERROR  w:{wid}  blks:{chunk_span:,}  "
                        f"[{from_b:,}-{to_b:,}]  {elapsed_s:.1f}s  err: {err_msg}"
                    )
                    controller.record_error()
                    chunk_failures[chunk] = chunk_failures.get(chunk, 0) + 1
                    if chunk_failures[chunk] < MAX_CHUNK_RETRIES:
                        pending_chunks.insert(0, chunk)
                    else:
                        _print_message(
                            f"  Giving up on [{from_b:,}-{to_b:,}] "
                            f"after {MAX_CHUNK_RETRIES} retries; "
                            "will be retried on next run."
                        )

                else:
                    # Unknown status — defensive: should never happen.
                    fatal_reason = (
                        f"_fetch_range returned unknown status {status!r} "
                        f"on [{from_b:,}-{to_b:,}]"
                    )
                    _print_message(f"FATAL: {fatal_reason}")
                    _stop_event.set()
                    break

            # If the span changed, the remaining queue is sized to the
            # old span. Re-cut at the new size.
            if span_changed_any:
                pending_chunks = _rechunk_pending(pending_chunks, controller.block_span)

            # Top up in-flight work and run sink bookkeeping again.
            _top_up_in_flight()
            sink.reap_completed()
            sink.commit_ready()
            if sink.fatal_error is not None:
                fatal_reason = f"sink worker failed: {sink.fatal_error}"
                _stop_event.set()
                break

            # If we've exhausted the call limit, drain in-flight and stop.
            if (
                args.max_calls is not None
                and rate_limiter.total_calls >= args.max_calls
                and not pending_chunks
            ):
                _print_message(
                    f"  Reached --max-calls limit ({args.max_calls}). "
                    "Draining in-flight work."
                )

            # If the queue is empty and nothing is in flight, check the chain head and either
            # rebuild the queue or stop.
            if not pending_chunks and not rpc_futures:
                _set_phase("rechecking chain head")
                try:
                    new_head = rpc_client.get_block_number()
                except (RPCError, RuntimeError) as e:
                    fatal_reason = f"failed to recheck chain head: {type(e).__name__}: {e}"
                    _print_message(f"FATAL: {fatal_reason}")
                    _stop_event.set()
                    break

                new_gaps = store.find_gaps(SCRAPE_START_BLOCK, new_head)
                new_gap_blocks = sum(t - f + 1 for f, t in new_gaps)

                if new_gap_blocks <= args.lag_tolerance:
                    if sink.in_flight or sink.pending_commit:
                        _print_message(
                            f"  Within lag tolerance ({new_gap_blocks:,} <= "
                            f"{args.lag_tolerance}) but sink pipeline still has "
                            f"{sink.in_flight} in-flight / {sink.pending_commit} pending. "
                            "Waiting for sink to drain..."
                        )
                        _set_phase("draining sink")
                        chain_head = new_head
                        # Stay in the loop; we'll fall through with no RPC work and just let sink
                        # complete.
                        # Avoid a busy loop by sleeping a beat.
                        time.sleep(0.5)
                        continue
                    caught_up_confirmed = True
                    chain_head = new_head
                    _print_message(
                        f"  Within lag tolerance ({new_gap_blocks:,} <= "
                        f"{args.lag_tolerance}). Done."
                    )
                    break

                _print_message(
                    f"  Chain advanced to {new_head:,} "
                    f"(+{new_gap_blocks - sum(t - f + 1 for f, t in gaps):,} blocks since startup; "
                    f"{new_gap_blocks:,} remaining)."
                )
                chain_head = new_head
                _live["chain_head"] = chain_head
                gaps = new_gaps
                pending_chunks = _build_chunks(new_gaps, controller.block_span)
                _live["total_blocks"] = blocks_done + new_gap_blocks
                _set_phase("running")
                _top_up_in_flight()
                if not rpc_futures:
                    if args.max_calls is not None and rate_limiter.total_calls >= args.max_calls:
                        fatal_reason = (
                            f"--max-calls limit ({args.max_calls}) reached "
                            f"while {new_gap_blocks:,} blocks still behind."
                        )
                    else:
                        fatal_reason = (
                            "No work could be submitted after queue rebuild "
                            f"while {new_gap_blocks:,} blocks still behind."
                        )
                    _print_message(f"FATAL: {fatal_reason}")
                    break

        # End of main loop.

    except KeyboardInterrupt:
        # First CTRL-C: fall through to the finally block which drains what it can and exits
        # cleanly.
        _stop_event.set()

    except SystemExit as e:
        system_exit_requested = True
        if e.code not in (None, 0):
            fatal_reason = str(e.code)
            print(f"FATAL: {fatal_reason}", flush=True)
        raise

    except BaseException as e:  # noqa: BLE001
        # Last-resort safety net — anything we didn't already classify. Print the traceback above
        # the summary so a crash inside an otherwise-untyped code path is debuggable.
        import traceback as _tb
        fatal_reason = f"unhandled exception in main loop: {type(e).__name__}: {e}"
        _stop_event.set()
        _print_message(f"FATAL: {fatal_reason}")
        _print_message(_tb.format_exc())

    finally:
        # Close the run log file if it was opened
        global _log_file
        if _log_file is not None:
            try:
                _log_file.write("# main finished\n")
                _log_file.flush()
                _log_file.close()
            except Exception:
                pass
            _log_file = None

        # --- Orderly shutdown ---------------------------------------
        # 1. Cancel anything we can in the RPC pool.
        if rpc_pool is not None:
            for f in list(rpc_futures):
                f.cancel()
            rpc_pool.shutdown(wait=False, cancel_futures=True)

        # 2. Let the sink pool finish what it has started, then commit every result that lands in
        #    frontier order. We do NOT submit new sink work past this point; any incomplete
        #    partition will be picked up by ``reconcile_with_cold_tier`` on the next start.
        if sink is not None:
            _set_phase("draining sink")
            # Wait for sink futures with a generous timeout. Two CTRL-Cs bypass this via the
            # os._exit in the SIGINT handler.
            sink.shutdown(wait=True)
            # Final pass: reap and commit anything that finished while we were shutting down.
            sink.reap_completed()
            if store is not None:
                try:
                    sink.commit_ready()
                except Exception as e:  # noqa: BLE001
                    _print_message(f"  final commit_ready failed: {e}")
            if sink.fatal_error is not None and fatal_reason is None:
                fatal_reason = f"sink worker failed: {sink.fatal_error}"

        # 3. Stop the heartbeat.
        heartbeat_stop.set()
        heartbeat.join(timeout=2.0)

        # 4. Erase the live status line so the terminal isn't ugly.
        sys.stdout.write(f"\r{' ' * _term_width()}\r")
        sys.stdout.flush()

        # 5. Print the summary.
        try:
            if system_exit_requested:
                pass
            else:
                final_chain_head: int | None = None
                try:
                    final_chain_head = rpc_client.get_block_number()
                except Exception:
                    final_chain_head = None

                head_for_output = final_chain_head if final_chain_head is not None else chain_head
                elapsed = time.monotonic() - run_start
                _print_summary(
                    store=store,
                    manifest_frontier=manifest_frontier,
                    chain_head=head_for_output,
                    caught_up_confirmed=caught_up_confirmed,
                    blocks_done=blocks_done,
                    events_inserted=events_inserted,
                    rate_limiter=rate_limiter,
                    elapsed=elapsed,
                    fatal_reason=fatal_reason,
                )
        except Exception as e:  # noqa: BLE001
            print(f"\n(summary print failed: {type(e).__name__}: {e})")

        # 6. Close the hot DB connection. The lib enforces all writes were already committed
        #    atomically, so there's no flush.
        if store is not None:
            try:
                store.close()
            except Exception:  # noqa: BLE001
                pass

        # 7. Close the RPC client (per-thread connections — main thread only).
        try:
            rpc_client.close()
        except Exception:  # noqa: BLE001
            pass

        sys.stdout.flush()
        sys.exit(1 if fatal_reason else 0)


if __name__ == "__main__":
    main()
