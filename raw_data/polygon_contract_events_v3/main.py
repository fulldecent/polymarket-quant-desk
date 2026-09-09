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
  4. Carve requests off those gaps, sizing each one from measured event density
      (``_internal.chunk_planner``) and hill-climbing concurrency, result budget and post-response
      sleep on blocks/second in fixed-parameter epochs (``_internal.epoch_optimizer``).
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
    python main.py --lag-tolerance 10       # stop when within 10 blocks
    python main.py --sink-workers 2         # concurrent sink workers

Concurrency, request size and request rate are not configurable. Event density varies by orders of
magnitude along the chain and every provider enforces different limits, so the scraper starts at
one request of one block and measures its way up, discovering the provider's limits from refusals.

ENVIRONMENT (all required, set in .env)
    HOT_DIR                              regenerable working state; this program keeps
                                         ``polygon_contract_events_v3.db`` there
    SCRATCH_DIR                          DuckDB spill; safe to wipe between runs
    POLYGON_CONTRACT_EVENTS_V3_DIR       cold-tier root directory
    POLYGON_RPC_URL                      Polygon JSON-RPC endpoint
"""

from __future__ import annotations

import argparse
import concurrent.futures
import json
import math
import os
import signal
import subprocess
import sys
import threading
import time
import urllib.parse
from collections import Counter
from concurrent.futures import Future, ThreadPoolExecutor
from datetime import datetime, timezone
from pathlib import Path
from typing import Any

from rich.console import Console, Group
from rich.live import Live
from rich.progress import (
    BarColumn,
    Progress,
    ProgressColumn,
    SpinnerColumn,
    Task,
    TaskID,
    TextColumn,
    TimeElapsedColumn,
    TimeRemainingColumn,
)
from rich.text import Text
from rich.theme import Theme

from dotenv import load_dotenv

# Make ``_internal`` importable when run as a script.
_project_root = Path(__file__).resolve().parent.parent.parent
sys.path.insert(0, str(Path(__file__).resolve().parent))
sys.path.insert(0, str(_project_root))

from lib.env import require_directory_env  # noqa: E402
from lib.git_utils import assert_git_clean  # noqa: E402

from _internal.chunk_planner import ChunkPlanner
from _internal.epoch_optimizer import MAX_WORKERS, EpochOptimizer
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
    publish_manifest,
    read_manifest_frontier,
    roll_forward_manifests_to_exhaustion,
    write_partition_files,
)
from _internal.persistence import HotStore, HotStoreConfig
from _internal.rpc_client import (
    RpcClient,
    RpcClientConfig,
    HTTPError,
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
RPC_CALL_TIMEOUT_SEC: float = 30.0
RPC_BLOCK_NUMBER_TIMEOUT_SEC: float = 5.0
RPC_MAX_RETRIES: int = 3
RPC_BACKOFF_BASE_SEC: float = 0.5

POLL_TIMEOUT_SEC: float = 0.5
"""How long ``concurrent.futures.wait`` blocks before the loop refreshes the footer.

Kept well under a second so progress never looks stalled.
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
# Stop event — accessed by signal handler, RPC client, sink workers, and persistence layer.
# Single source of truth for "should we be shutting down right now."
# ---------------------------------------------------------------------------

_stop_event = threading.Event()


# ---------------------------------------------------------------------------
# Environment loading
# ---------------------------------------------------------------------------

def _load_environment() -> dict[str, str]:
    """Read every required env var from the root ``.env`` and validate it.

    Exits the process with a clear message on any missing or invalid value. The returned dict has
    the env-var name as key and the raw string value (already validated as non-empty) as value.
    """
    project_root = Path(__file__).resolve().parent.parent.parent

    assert_git_clean(project_root)

    load_dotenv(project_root / ".env")

    required = [
        "HOT_DIR",
        "SCRATCH_DIR",
        "POLYGON_CONTRACT_EVENTS_V3_DIR",
        "POLYGON_RPC_URL",
    ]
    missing = [k for k in required if not os.environ.get(k)]
    if missing:
        sys.exit(
            "Missing required environment variables: "
            + ", ".join(missing)
            + "\nSet them in the repo-root .env file."
        )

    hot_dir = require_directory_env("HOT_DIR")
    # Named after the dataset it serves, so several tools can share one hot directory.
    db_path = os.path.join(hot_dir, "polygon_contract_events_v3.db")

    cold_root = os.environ["POLYGON_CONTRACT_EVENTS_V3_DIR"]
    if not os.path.isdir(cold_root):
        sys.exit(
            f"POLYGON_CONTRACT_EVENTS_V3_DIR does not exist: {cold_root}\n"
            f"Ensure the cold-tier volume is mounted."
        )
    if not os.access(cold_root, os.W_OK):
        sys.exit(f"POLYGON_CONTRACT_EVENTS_V3_DIR is not writable: {cold_root}")

    scratch_dir = require_directory_env("SCRATCH_DIR")

    return {
        "db_path": db_path,
        "cold_root": cold_root,
        "rpc_url": os.environ["POLYGON_RPC_URL"],
        "scratch_dir": scratch_dir,
    }


# ---------------------------------------------------------------------------
# Display helpers
# ---------------------------------------------------------------------------

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


def _next_partition_progress(store: HotStore, sunk_frontier: int) -> tuple[int, int]:
    """Return ``(partition_start, blocks_loaded)`` for the partition currently being filled.

    Counts blocks present in hot-DB unsunk ranges regardless of contiguity.
    """
    next_start = (
        (max(sunk_frontier, SCRAPE_START_BLOCK - 1) + 1) // PARTITION_SIZE_10K
    ) * PARTITION_SIZE_10K
    next_end = next_start + PARTITION_SIZE_10K - 1

    loaded = 0
    for from_b, to_b, _ in store.list_loaded_ranges(include_sunk=False):
        if to_b < next_start:
            continue
        if from_b > next_end:
            break
        overlap_start = max(from_b, next_start)
        overlap_end = min(to_b, next_end)
        if overlap_start <= overlap_end:
            loaded += overlap_end - overlap_start + 1

    return next_start, min(loaded, PARTITION_SIZE_10K)


# ---------------------------------------------------------------------------
# Console output
# ---------------------------------------------------------------------------
#
# One sticky footer holds every activity that is running right now. A row is added when work
# starts and removed when it finishes, so the footer never shows a completed step. Everything
# else — including the full startup detail the footer omits — goes to the per-run JSONL log.

_COLOR_DONE = "green"
_COLOR_TODO = "magenta"

console = Console(
    theme=Theme({"progress.elapsed": _COLOR_DONE, "progress.remaining": _COLOR_TODO})
)
_print_lock = threading.Lock()


def _mask_partition(partition_start: int) -> str:
    """Render a 10K partition start with its four variable digits masked: ``92,93X,XXX``."""
    out: list[str] = []
    masked = 0
    for ch in reversed(f"{partition_start:,}"):
        if ch.isdigit() and masked < 4:
            out.append("X")
            masked += 1
        else:
            out.append(ch)
    return "".join(reversed(out))


class JSONLRunLogger:
    """Write machine-readable run events to a per-run JSONL file."""

    def __init__(self, log_path: Path) -> None:
        self._path = log_path
        self._handle = open(log_path, "a", encoding="utf-8")

    def log(self, event: str, *, message: str, **extra: Any) -> None:
        payload = {
            "ts": datetime.now(timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ"),
            "event": event,
            "message": message,
            **extra,
        }
        self._handle.write(json.dumps(payload, separators=(",", ":"), ensure_ascii=False) + "\n")
        self._handle.flush()

    def close(self) -> None:
        try:
            self._handle.close()
        except Exception:
            pass


class _DoneOfTotalColumn(ProgressColumn):
    """``done/total``, each number coloured like the bar segment it stands for."""

    def render(self, task: Task) -> Text:
        total = int(task.total) if task.total is not None else 0
        return Text.assemble(
            (f"{int(task.completed):,}", _COLOR_DONE),
            "/",
            (f"{total:,}", _COLOR_TODO),
        )


def _shannon_entropy(text: str) -> float:
    counts = Counter(text)
    length = len(text)
    return -sum((n / length) * math.log2(n / length) for n in counts.values())


def _describe_rpc_endpoint(url: str) -> str:
    """Return ``host key`` with the key masked, e.g. ``lb.drpc.live AmP...UMv``.

    The key is whichever part of the URL looks least like a word: API keys are long and
    high-entropy, while path components such as ``polygon`` or ``v3`` are neither. Only the first
    and last three characters are shown, so a screen recording or a pasted log never carries the
    whole credential.
    """
    parsed = urllib.parse.urlparse(url)
    host = parsed.hostname or url

    candidates = [segment for segment in parsed.path.split("/") if len(segment) >= 8]
    candidates += [value for _, value in urllib.parse.parse_qsl(parsed.query) if len(value) >= 8]
    if not candidates:
        return host

    key = max(candidates, key=lambda c: (_shannon_entropy(c), len(c)))
    return f"{host} {key[:3]}...{key[-3:]}"


# Footer rows, top to bottom. The overall run bar sits at the very bottom.
_BAR_ORDER = ("partition", "main")


class RichRunStatus:
    """Sticky footer showing only the work that is running right now.

    A row is added when an activity starts and removed when it finishes, so a completed step
    never lingers on screen. Log lines printed through ``console`` scroll above the footer.
    """

    def __init__(self) -> None:
        self._spinners = Progress(
            SpinnerColumn(),
            TextColumn("{task.description}"),
            console=console,
        )
        self._bars: dict[str, Progress] = {}
        self._live = Live(console=console, refresh_per_second=8, transient=True)
        self._lock = threading.Lock()
        self._spinner_tasks: dict[str, TaskID] = {}
        self._bar_tasks: dict[str, TaskID] = {}
        self._bar_descriptions: dict[str, str] = {}

    def start(self) -> None:
        self._live.start()

    def stop(self) -> None:
        self._live.stop()

    def spinner(self, key: str, description: str) -> None:
        """Show an indeterminate row for ``key``, replacing any earlier text for it."""
        with self._lock:
            task_id = self._spinner_tasks.get(key)
            if task_id is None:
                task_id = self._spinners.add_task(description, total=None)
                self._spinner_tasks[key] = task_id
            self._spinners.update(task_id, description=description)
            self._refresh()

    def bar(self, key: str, description: str, completed: int, total: int) -> None:
        """Update the ``key`` bar.

        A changed description means a new unit of work (the next partition), so the row's
        elapsed time and ETA restart from zero rather than carrying the previous one's clock.
        """
        with self._lock:
            progress = self._bars.get(key)
            if progress is None:
                progress = self._make_bar()
                self._bars[key] = progress
                self._bar_tasks[key] = progress.add_task(description, total=max(total, 1))
                self._bar_descriptions[key] = description
            task_id = self._bar_tasks[key]
            if self._bar_descriptions[key] != description:
                progress.reset(
                    task_id,
                    description=description,
                    completed=completed,
                    total=max(total, 1),
                )
                self._bar_descriptions[key] = description
            else:
                progress.update(
                    task_id,
                    description=description,
                    completed=completed,
                    total=max(total, 1),
                )
            self._refresh()

    def clear(self, key: str) -> None:
        with self._lock:
            task_id = self._spinner_tasks.pop(key, None)
            if task_id is not None:
                self._spinners.remove_task(task_id)
            self._bars.pop(key, None)
            self._bar_tasks.pop(key, None)
            self._bar_descriptions.pop(key, None)
            self._refresh()

    def _make_bar(self) -> Progress:
        return Progress(
            SpinnerColumn(),
            TextColumn("{task.description}"),
            BarColumn(complete_style=_COLOR_DONE, finished_style=_COLOR_DONE, style=_COLOR_TODO),
            _DoneOfTotalColumn(),
            TimeElapsedColumn(),
            TimeRemainingColumn(),
            console=console,
        )

    def _refresh(self) -> None:
        rows: list[Progress] = []
        if self._spinners.tasks:
            rows.append(self._spinners)
        rows.extend(self._bars[key] for key in _BAR_ORDER if key in self._bars)
        self._live.update(Group(*rows))


status_ui = RichRunStatus()

# Persistent run log (opened once per invocation)
_run_logger: JSONLRunLogger | None = None


def _open_run_log() -> Path:
    """Create and open a timestamped .log file for this scraper run."""
    global _run_logger
    if _run_logger is not None:
        return _run_logger._path
    ts = datetime.now(timezone.utc).strftime("%Y-%m-%dT%H%M%SZ")
    log_dir = Path(__file__).resolve().parent / "logs"
    log_dir.mkdir(parents=True, exist_ok=True)
    _run_logger = JSONLRunLogger(log_dir / f"main-{ts}.log")
    return _run_logger._path


def _log_event(event: str, message: str, **extra: Any) -> None:
    """Record one run event. Never touches the screen."""
    if _run_logger is not None:
        _run_logger.log(event, message=message, **extra)


def _print_message(text: str | Text) -> None:
    """Print one line above the sticky footer and record it to the run log."""
    with _print_lock:
        console.print(text, markup=False, highlight=False)
    _log_event("message", text.plain if isinstance(text, Text) else text)


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


def _interruptible_sleep(seconds: float) -> None:
    """Sleep in short slices so a CTRL-C is honored promptly."""
    remaining = seconds
    while remaining > 0:
        if _stop_event.is_set():
            return
        slice_sec = min(remaining, 0.1)
        time.sleep(slice_sec)
        remaining -= slice_sec


def _fetch_range(
    *,
    from_block: int,
    to_block: int,
    rpc_client: RpcClient,
    sleep_after_response_sec: float,
) -> dict:
    """Fetch and decode logs for one block range.

    Returns a dict with a ``status`` field, one of:

      * ``"ok"``        — fetch succeeded.
      * ``"split"``     — provider rejected the range as too large.
      * ``"timeout"``   — provider timed out.
      * ``"throttled"`` — provider pushed back (HTTP 429, or a 5xx that outlived the client's
        retries). Carries ``http_status``.
      * ``"error"``     — other transport-level error, worth a bounded retry.
      * ``"fatal"``     — permanent failure (bad credentials, malformed request, decoder bug).
        Carries ``error_message``.
      * ``"cancelled"`` — ``_stop_event`` was set before the request finished.

    For ``ok`` the dict also has ``rows_by_target`` (``dict[(contract, event), list[row]]``),
    ``raw_log_count``, and ``elapsed_sec``.

    The function never raises: the caller deals with structured outcomes only. Mid-flight
    cancellation via ``_stop_event`` returns ``"cancelled"`` rather than propagating
    ``OperationCancelled``.

    ``sleep_after_response_sec`` is the optimizer's throttle: this thread pauses for that long
    after the provider answers, before the orchestrator can hand it more work.
    """
    wid = _get_worker_id()
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
        _interruptible_sleep(sleep_after_response_sec)
        return {"status": "split", "wid": wid, "elapsed_sec": time.monotonic() - t0}
    except RPCTimeout:
        _interruptible_sleep(sleep_after_response_sec)
        return {"status": "timeout", "wid": wid, "elapsed_sec": time.monotonic() - t0}
    except OperationCancelled:
        return {"status": "cancelled", "wid": wid, "elapsed_sec": time.monotonic() - t0}
    except HTTPError as e:
        _interruptible_sleep(sleep_after_response_sec)
        if e.is_retryable:
            # Survived the client's own retries, so the provider is genuinely overloaded.
            return {
                "status": "throttled",
                "wid": wid,
                "elapsed_sec": time.monotonic() - t0,
                "http_status": e.status_code,
            }
        # 401/403 and friends never fix themselves; retrying just burns the backlog.
        return {
            "status": "fatal",
            "wid": wid,
            "elapsed_sec": time.monotonic() - t0,
            "error_message": f"HTTP {e.status_code} from the RPC provider",
        }
    except (RPCError, RuntimeError) as e:
        _interruptible_sleep(sleep_after_response_sec)
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
            "status": "fatal",
            "wid": wid,
            "elapsed_sec": elapsed,
            "error_message": f"decode failure: {type(e).__name__}: {e}",
        }

    rows_by_target: dict[tuple[str, str], list[dict]] = {}
    for contract, event, row in triples:
        rows_by_target.setdefault((contract, event), []).append(row)

    _interruptible_sleep(sleep_after_response_sec)

    return {
        "status": "ok",
        "wid": wid,
        "elapsed_sec": elapsed,
        "rows_by_target": rows_by_target,
        "raw_log_count": raw_log_count,
    }


# ---------------------------------------------------------------------------
# Work queue helpers
# ---------------------------------------------------------------------------

def _take_chunk(ranges: list[tuple[int, int]], span: int) -> tuple[int, int]:
    """Carve up to ``span`` blocks off the front of ``ranges``, shrinking it in place."""
    from_block, to_block = ranges[0]
    chunk_to = min(from_block + span - 1, to_block)
    if chunk_to >= to_block:
        ranges.pop(0)
    else:
        ranges[0] = (chunk_to + 1, to_block)
    return from_block, chunk_to


def _return_chunk(ranges: list[tuple[int, int]], chunk: tuple[int, int]) -> None:
    """Put an unfinished chunk back at the front, merging with what follows it."""
    from_block, to_block = chunk
    if ranges and ranges[0][0] == to_block + 1:
        ranges[0] = (from_block, ranges[0][1])
    else:
        ranges.insert(0, chunk)


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
        progress_cb: Any | None = None,
    ) -> None:
        self._store = store
        self._db_path = db_path
        self._cold_root = cold_root
        self._progress_cb = progress_cb
        self._executor = ThreadPoolExecutor(
            max_workers=max_workers,
            thread_name_prefix="sink",
        )
        self._sink_connect_config = {
            "memory_limit": self._store.config.duckdb_memory_limit,
            "threads": str(self._store.config.duckdb_threads),
            "temp_directory": self._store.config.duckdb_temp_dir,
        }
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
        # Sink workers use their own DuckDB connection with matching config.
        # This avoids cross-thread transactional contention on a shared
        # connection while still reading a consistent snapshot via MVCC.
        future = self._executor.submit(
            write_partition_files,
            self._db_path,
            self._cold_root,
            partition_start,
            duckdb_connect_config=self._sink_connect_config,
            progress_cb=self._progress_cb,
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
                # The manifest is what makes a partition sunk, so it goes first and in frontier
                # order. Only then may anything treat this partition as final.
                if not publish_manifest(self._cold_root, expected_next):
                    raise V3Error(
                        f"partition {expected_next} is missing Parquet files for one or more "
                        "targets, so it cannot be declared sunk"
                    )
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
    scratch_dir: str,
    log_path: Path,
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
        f"hot database:         {db_path} ({_file_size_str(db_path)})",
        f"raw:                  {cold_root}",
        f"scratch:              {scratch_dir}",
        f"log:                  {log_path}",
        "",
    ]
    if sunk_frontier <= SCRAPE_START_BLOCK - 1:
        banner_lines.append("sunk frontier:        none")
    else:
        banner_lines.append(f"sunk frontier:        {sunk_frontier:,}")
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

    # Only the paths go on screen; the frontier/gap detail belongs in the run log.
    console.print(
        f"hot database:         {db_path} ({_file_size_str(db_path)})", highlight=False
    )
    console.print(f"raw:                  {cold_root}", highlight=False)
    console.print(f"scratch:              {scratch_dir}", highlight=False)
    console.print(f"log:                  {log_path}", highlight=False)
    for line in banner_lines:
        _log_event("banner", line)


# ---------------------------------------------------------------------------
# Final summary
# ---------------------------------------------------------------------------

def _print_summary(
    *,
    store: HotStore | None,
    cold_root: str,
    chain_head: int,
    caught_up_confirmed: bool,
    blocks_done: int,
    events_inserted: int,
    calls_made: int,
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
        summary_lines.append("  caught up")
    elif blocks_done == 0:
        summary_lines.append("")
        summary_lines.append("status")
        summary_lines.append("  no progress - run ended without confirmed caught-up state")
    else:
        summary_lines.append("")
        summary_lines.append("status")
        summary_lines.append("  OK")

    sunk = None
    loaded = None
    lag = chain_head - SCRAPE_START_BLOCK
    if store is not None:
        # Read it back from the manifests rather than trusting in-memory state.
        sunk = get_manifest_frontier(cold_root)
        hot_db_blocks = sum(
            (to_b - from_b + 1)
            for from_b, to_b, _ in store.list_loaded_ranges(include_sunk=False)
        )
        summary_lines.append("")
        summary_lines.append("progress")
        summary_lines.append(
            f"  sunk frontier:        {sunk:,}"
            if sunk > (SCRAPE_START_BLOCK - 1)
            else "  sunk frontier:        none"
        )
        summary_lines.append(f"  hot database:         {hot_db_blocks:,} blocks")
        summary_lines.append(f"  chain head:           {chain_head:,}")
        summary_lines.append(f"  blocks processed:     +{blocks_done:,} this run")
    summary_lines.append("")
    summary_lines.append("throughput")
    blk_s = blocks_done / elapsed if elapsed > 1 and blocks_done > 0 else 0.0
    ev_s = events_inserted / elapsed if elapsed > 1 else 0.0
    cps = calls_made / elapsed if elapsed > 1 and calls_made else 0.0
    summary_lines.append(f"  blocks/sec:  {blk_s:,.0f}")
    summary_lines.append(f"  events/sec:  {ev_s:,.0f}")
    summary_lines.append(f"  API calls:   {calls_made:,}  ({cps:.1f}/s effective)")

    for line in summary_lines:
        console.print(line, highlight=False)
        _log_event("summary", line)


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
            # the next startup drops when it inspects that partition.
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
        "--sink-workers", type=int, default=1,
        help="concurrent Parquet sink workers (default: 1)",
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

    if args.sink_workers < 1:
        sys.exit(f"--sink-workers must be >= 1; got {args.sink_workers}")
    if args.max_calls is not None and args.max_calls < 1:
        sys.exit(f"--max-calls must be >= 1; got {args.max_calls}")
    if args.lag_tolerance < 0:
        sys.exit(f"--lag-tolerance must be >= 0; got {args.lag_tolerance}")

    env = _load_environment()

    # ----- Wire up state --------------------------------------------
    run_start = time.monotonic()

    # Open the persistent run log as early as possible
    log_path = _open_run_log()
    status_ui.start()
    status_ui.spinner("head", "Querying chain head")
    status_ui.spinner("startup", "Opening hot database")

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

    def _query_chain_head() -> int:
        return rpc_client.get_block_number()

    head_executor = ThreadPoolExecutor(max_workers=1, thread_name_prefix="startup-head")
    head_future = head_executor.submit(_query_chain_head)

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
    calls_made = 0
    chain_head = 0

    try:
        # --- HotStore -----------------------------------------------
        try:
            store = HotStore(
                env["db_path"],
                _SCHEMA_SQL,
                config=HotStoreConfig(duckdb_temp_dir=env["scratch_dir"]),
            )
        except SchemaMismatchError as e:
            sys.exit(
                f"Hot DB schema mismatch at {env['db_path']}: {e}\n"
                f"Delete the hot DB file and re-run to recreate it from schema.sql."
            )
        except (FileNotFoundError, V3Error) as e:
            sys.exit(f"Could not open hot DB at {env['db_path']}: {e}")

        # --- Read manifest frontier --------------------------------
        # Temp files under already-sunk partitions are not second-guessed: a published manifest
        # means that partition is final. An interrupted write is dropped when the partition it
        # belongs to is inspected.
        status_ui.spinner("startup", "Reading manifest frontier")

        def _frontier_progress(*, op, phase, rows_done=None, rows_total=None,
                               elapsed_ms=None, message="", partition=None):
            if partition is not None:
                status_ui.spinner(
                    "startup", f"Finding sunk partition {_mask_partition(partition)}"
                )

        try:
            manifest_frontier, _ = read_manifest_frontier(
                env["cold_root"],
                progress_cb=_frontier_progress,
            )
        except V3Error as e:
            sys.exit(f"FATAL: manifest frontier read failed: {e}")

        _print_message(f"Manifest frontier read complete: frontier={manifest_frontier:,}.")

        # --- Roll manifest frontier forward -------------------------
        status_ui.spinner("startup", "Touching up sunk partitions")

        def _manifest_progress(*, op, phase, rows_done=None, rows_total=None,
                               elapsed_ms=None, message="", partition=None):
            if partition is not None:
                status_ui.spinner(
                    "startup", f"Touching up sunk partition {_mask_partition(partition)}"
                )

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

        _print_message(
            "Manifest roll-forward complete: "
            f"published={published_manifests:,} partition(s)."
        )

        # --- Cleanup after frontier ---------------------------------
        status_ui.spinner("startup", "Cleaning temporary files at the frontier")

        def _cleanup_progress(*, op, phase, rows_done=None, rows_total=None,
                              elapsed_ms=None, message="", partition=None):
            if partition is not None:
                status_ui.spinner(
                    "startup",
                    f"Cleaning temporary files for partition {_mask_partition(partition)}",
                )

        try:
            cleaned_manifest = cleanup_temp_dirs_after_frontier(
                env["cold_root"],
                progress_cb=_cleanup_progress,
            )
        except V3Error as e:
            sys.exit(f"FATAL: post-frontier cleanup failed: {e}")

        _print_message(
            "Post-frontier cleanup complete: "
            f"removed={cleaned_manifest:,} path(s)."
        )

        manifest_frontier = get_manifest_frontier(env["cold_root"])
        # Manifest is the durable source of truth for sunk frontier at startup.
        store.sunk_frontier = manifest_frontier

        # --- Reconcile hot DB progress with cold tier ----------------
        # If we crashed between a sink worker's rename and the orchestrator's commit_sink, the
        # Parquet files reached disk but the hot DB still has the rows. Reconcile fixes that
        # without re-scraping.
        status_ui.spinner("startup", "Reading sunk partitions from the hot database")

        def _reconcile_progress(*, op, phase, rows_done=None, rows_total=None,
                                elapsed_ms=None, message="", partition=None):
            if phase == "scan":
                status_ui.spinner("startup", "Scanning the cold tier for sunk partitions")
            elif phase == "update":
                status_ui.spinner(
                    "startup", "Recording sunk partitions in the hot database"
                )

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
        if newly_sunk:
            _print_message(f"Reconciled {newly_sunk:,} partition(s) from existing cold-tier files.")

        # --- Sink orchestrator --------------------------------------
        def _sink_progress_cb(*, op, phase, rows_done=None, rows_total=None,
                              elapsed_ms=None, message="", partition=None):
            # Takes over the partition row: by the time a partition is being written, it is full.
            if op == "sink" and phase == "copy" and partition is not None:
                status_ui.bar(
                    "partition",
                    f"Sinking partition {_mask_partition(partition)}",
                    rows_done or 0,
                    rows_total or 1,
                )

        sink = _SinkOrchestrator(
            store=store,
            db_path=env["db_path"],
            cold_root=env["cold_root"],
            max_workers=args.sink_workers,
            progress_cb=_sink_progress_cb,
        )

        # Anything already in the hot DB and ready to sink at startup (e.g. a clean shutdown after
        # persist but before sink) should go to the sink pool immediately.
        backlog = store.list_10k_partitions_ready_to_sink()
        for p in backlog:
            sink.submit(p)
        if backlog:
            _print_message(f"Submitted {len(backlog):,} pre-existing ready partition(s) to the sink pool.")

        status_ui.clear("startup")

        # --- Chain head ---------------------------------------------
        # Queried in parallel with all of the startup disk work above; needed only now, to plan
        # the scrape. Any failure is fatal.
        try:
            chain_head = head_future.result()
        except Exception as e:  # noqa: BLE001
            sys.exit(f"FATAL: could not read chain head: {type(e).__name__}: {e}")
        finally:
            head_executor.shutdown(wait=False)
            status_ui.clear("head")

        if chain_head < SCRAPE_START_BLOCK:
            sys.exit(
                f"FATAL: RPC returned chain head {chain_head:,}, which is before "
                f"SCRAPE_START_BLOCK {SCRAPE_START_BLOCK:,}. The RPC endpoint may be "
                f"returning incorrect data or is in a failed state."
            )

        _print_message(f"Chain head: {chain_head:,}")

        # Work planning must ignore already-sunk history and only consider
        # unsunk coverage above the sunk frontier.
        gaps = store.find_gaps(SCRAPE_START_BLOCK, chain_head, include_sunk=False)
        total_gap_blocks = sum(t - f + 1 for f, t in gaps)
        sunk_frontier = store.get_sunk_frontier()
        loaded_frontier = store.get_loaded_frontier()
        unsunk_ranges = len(store.list_loaded_ranges(include_sunk=False))

        _print_banner(
            db_path=env["db_path"],
            cold_root=env["cold_root"],
            scratch_dir=env["scratch_dir"],
            log_path=log_path,
            sunk_frontier=sunk_frontier,
            loaded_frontier=loaded_frontier,
            chain_head=chain_head,
            gaps=gaps,
            total_gap_blocks=total_gap_blocks,
            unsunk_ranges=unsunk_ranges,
        )

        # The main bar counts only the blocks this run has to scrape, starting at zero.
        total_target = max(total_gap_blocks, 1)
        status_ui.bar("main", "Total progress", 0, total_target)

        if not gaps and sink.in_flight == 0 and sink.pending_commit == 0:
            caught_up_confirmed = True
            lag = chain_head - loaded_frontier if loaded_frontier >= 0 else chain_head - SCRAPE_START_BLOCK
            _print_banner(
                db_path=env["db_path"],
                cold_root=env["cold_root"],
                scratch_dir=env["scratch_dir"],
                log_path=log_path,
                sunk_frontier=sunk_frontier,
                loaded_frontier=loaded_frontier,
                chain_head=chain_head,
                gaps=[],
                total_gap_blocks=0,
                unsunk_ranges=unsunk_ranges,
            )
            _print_message(
                "caught-up proof: "
                f"sunk_frontier={sunk_frontier:,} "
                f"loaded_frontier={loaded_frontier:,} "
                f"chain_head={chain_head:,} "
                f"lag={lag:,} blocks"
            )
            _print_message("")
            _print_message("=" * 70)
            _print_message("already caught up")
            _print_message("=" * 70)
            _print_message(f"  sunk frontier:    {sunk_frontier:,}")
            _print_message(f"  loaded frontier:  {loaded_frontier:,}")
            _print_message(f"  chain head:       {chain_head:,}")
            _print_message(
                f"  lag:              {lag:,} blocks (tolerance: {args.lag_tolerance})"
            )
            _print_message("")
            _print_message("No new work to do. Exiting cleanly.")
            _print_message("")
            return

        # --- Set up the optimizer and work queue --------------------
        optimizer = EpochOptimizer()
        planner = ChunkPlanner()
        endpoint_label = _describe_rpc_endpoint(env["rpc_url"])
        # Ranges still to scrape, ascending. Chunks are carved off the front as workers free up,
        # so each request is sized for the density where it actually lands.
        pending_ranges: list[tuple[int, int]] = list(gaps)
        chunk_failures: dict[tuple[int, int], int] = {}
        draining_epoch = False
        probe_in_flight: tuple[int, int] | None = None

        # Sized to the safety cap; the optimizer decides how many of these threads are ever busy.
        rpc_pool = ThreadPoolExecutor(
            max_workers=MAX_WORKERS,
            thread_name_prefix="rpc",
        )

        def _submit_one(chunk: tuple[int, int]) -> bool:
            """Submit one chunk. Returns False on call-count limit."""
            nonlocal calls_made
            if _stop_event.is_set():
                return False
            if args.max_calls is not None and calls_made >= args.max_calls:
                return False
            fut = rpc_pool.submit(
                _fetch_range,
                from_block=chunk[0], to_block=chunk[1],
                rpc_client=rpc_client,
                sleep_after_response_sec=optimizer.params.sleep_after_response_sec,
            )
            rpc_futures[fut] = chunk
            calls_made += 1
            return True

        def _refresh_bars() -> None:
            """Repaint the footer from current progress."""
            if sink.in_flight == 0:
                # While a partition is being written the sink owns this row.
                partition_start_now, partition_loaded = _next_partition_progress(
                    store, store.get_sunk_frontier()
                )
                status_ui.bar(
                    "partition",
                    f"Partition {_mask_partition(partition_start_now)}",
                    partition_loaded,
                    PARTITION_SIZE_10K,
                )
            status_ui.bar(
                "main", "Total progress", min(blocks_done, total_target), total_target
            )
            if rpc_futures:
                run_seconds = time.monotonic() - run_start
                rate = blocks_done / run_seconds if run_seconds > 0 else 0.0
                status_ui.spinner(
                    "scrape", f"Scraping {endpoint_label}  {rate:,.0f} blk/s"
                )
            else:
                status_ui.clear("scrape")

        def _top_up_in_flight() -> None:
            """Fill the in-flight set up to the optimizer's worker count.

            Submits nothing while the epoch is draining, so the drain can actually finish.
            """
            nonlocal probe_in_flight
            if draining_epoch:
                return
            while pending_ranges and len(rpc_futures) < optimizer.params.workers:
                budget = optimizer.params.result_budget
                # Only one request at a time may try a span beyond proven ground, so a refusal
                # costs one request instead of every request currently in flight.
                probing = not probe_in_flight and planner.would_probe(budget)
                span = planner.next_span(budget, probe=probing)
                chunk = _take_chunk(pending_ranges, span)
                if not _submit_one(chunk):
                    _return_chunk(pending_ranges, chunk)
                    return
                if probing:
                    probe_in_flight = chunk

        optimizer.start_epoch()
        _top_up_in_flight()
        if not rpc_futures:
            fatal_reason = (
                "No work could be submitted at startup (call-count limit hit "
                "before scraping began?)."
            )
            _print_message(f"FATAL: {fatal_reason}")
            return

        # --- Main loop ----------------------------------------------
        while not _stop_event.is_set() and sink.fatal_error is None:
            if rpc_futures:
                done, _ = concurrent.futures.wait(
                    rpc_futures,
                    timeout=POLL_TIMEOUT_SEC,
                    return_when=concurrent.futures.FIRST_COMPLETED,
                )
            else:
                done = set()

            # Even if no RPC future completed, run sink bookkeeping so
            # commits don't pile up.
            sink.reap_completed()
            sink.commit_ready()
            if sink.fatal_error is not None:
                fatal_reason = f"sink worker failed: {sink.fatal_error}"
                _stop_event.set()
                break

            _refresh_bars()

            for fut in done:
                chunk = rpc_futures.pop(fut)
                from_b, to_b = chunk
                chunk_span = to_b - from_b + 1
                if probe_in_flight == chunk:
                    probe_in_flight = None
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

                if status == "ok":
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
                    optimizer.record_blocks(chunk_span)
                    planner.record_success(blocks=chunk_span, logs=raw_log_count)
                    if optimizer.record_ok(
                        blocks=chunk_span, logs=raw_log_count, elapsed_sec=elapsed_s
                    ):
                        _print_message(f"  ramp -> {optimizer.params.describe()}")
                    _refresh_bars()
                    sf = store.get_sunk_frontier()

                    # Blocks in this chunk that land beyond the partition being filled next.
                    next_partition_start = (
                        (max(sf, SCRAPE_START_BLOCK - 1) + 1) // PARTITION_SIZE_10K
                    ) * PARTITION_SIZE_10K
                    next_partition_end = next_partition_start + PARTITION_SIZE_10K - 1
                    overlap_start = max(from_b, next_partition_start)
                    overlap_end = min(to_b, next_partition_end)
                    contributing = max(0, overlap_end - overlap_start + 1)
                    beyond_blks = chunk_span - contributing

                    # Print after persist succeeds
                    _suffix = f" (beyond next: {beyond_blks:,} blks)" if beyond_blks > 0 else ""
                    line = Text("  -> blks: ")
                    line.append(f"{chunk_span:,}", style=_COLOR_DONE)
                    line.append(
                        f"  evts: {raw_log_count:,}  {elapsed_s:.1f}s{_suffix}"
                    )
                    _print_message(line)

                    for p in persist_result.ready_partitions:
                        sink.submit(p)

                elif status == "split":
                    elapsed_s = float(result["elapsed_sec"])
                    wid = int(result["wid"])
                    charged = planner.record_rejection(blocks=chunk_span)
                    optimizer.record_rejection()
                    _print_message(
                        f"  REFUSED  w:{wid}  blks:{chunk_span:,}  "
                        f"[{from_b:,}-{to_b:,}]  too many {charged}; {planner.describe()}"
                    )
                    if chunk_span <= 1:
                        # A single block cannot be split further. Leave it out of
                        # ``loaded_block_ranges`` so the next run retries it.
                        _print_message(
                            f"  Single-block range {from_b} refused. "
                            "Skipping; will be retried on next run."
                        )
                        continue
                    _return_chunk(pending_ranges, chunk)

                elif status == "timeout":
                    elapsed_s = float(result["elapsed_sec"])
                    wid = int(result["wid"])
                    _print_message(
                        f"  TMOUT  w:{wid}  blks:{chunk_span:,}  "
                        f"[{from_b:,}-{to_b:,}]  {elapsed_s:.1f}s"
                    )
                    _return_chunk(pending_ranges, chunk)
                    optimizer.record_timeout()
                    _print_message(f"  backing off -> {optimizer.params.describe()}")

                elif status == "throttled":
                    http_status = result.get("http_status")
                    wid = int(result["wid"])
                    _print_message(
                        f"  THROTTLED  w:{wid}  HTTP {http_status}  "
                        f"[{from_b:,}-{to_b:,}]"
                    )
                    _return_chunk(pending_ranges, chunk)
                    optimizer.record_throttled(http_status)
                    _print_message(f"  backing off -> {optimizer.params.describe()}")

                elif status == "fatal":
                    fatal_reason = (
                        f"fatal error on [{from_b:,}-{to_b:,}]: "
                        f"{result.get('error_message', '')}"
                    )
                    _print_message(f"FATAL: {fatal_reason}")
                    _stop_event.set()
                    break

                elif status == "error":
                    err_msg = str(result.get("error_message", ""))
                    wid = int(result["wid"])
                    elapsed_s = float(result["elapsed_sec"])
                    _print_message(
                        f"  ERROR  w:{wid}  blks:{chunk_span:,}  "
                        f"[{from_b:,}-{to_b:,}]  {elapsed_s:.1f}s  err: {err_msg}"
                    )
                    chunk_failures[chunk] = chunk_failures.get(chunk, 0) + 1
                    if chunk_failures[chunk] < MAX_CHUNK_RETRIES:
                        _return_chunk(pending_ranges, chunk)
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

            # --- Epoch boundary -------------------------------------
            # Stop submitting at the deadline, let in-flight work drain, then score the epoch: the
            # drain belongs inside the measurement it was caused by.
            if not draining_epoch and optimizer.should_end_epoch():
                draining_epoch = True
            if draining_epoch and not rpc_futures:
                report = optimizer.end_epoch()
                _print_message(f"  epoch: {report.describe()}")
                _print_message(f"  next:  {report.next_params.describe()}  {planner.describe()}")
                draining_epoch = False
                optimizer.start_epoch()

            # Top up in-flight work and run sink bookkeeping again.
            _top_up_in_flight()
            sink.reap_completed()
            sink.commit_ready()
            if sink.fatal_error is not None:
                fatal_reason = f"sink worker failed: {sink.fatal_error}"
                _stop_event.set()
                break

            if pending_ranges and not rpc_futures and not draining_epoch:
                # Nothing could be submitted, so the call budget is spent.
                if args.max_calls is not None:
                    _print_message(
                        f"  Reached --max-calls limit ({args.max_calls}). Stopping."
                    )
                break

            # If the queue is empty and nothing is in flight, check the chain head and either
            # rebuild the queue or stop.
            if not pending_ranges and not rpc_futures and not draining_epoch:
                status_ui.spinner("head", "Rechecking chain head")
                try:
                    new_head = rpc_client.get_block_number()
                except (RPCError, RuntimeError) as e:
                    fatal_reason = f"failed to recheck chain head: {type(e).__name__}: {e}"
                    _print_message(f"FATAL: {fatal_reason}")
                    _stop_event.set()
                    break
                finally:
                    status_ui.clear("head")

                # Recompute only unsunk gaps; sunk history is immutable and
                # must never be re-scheduled.
                new_gaps = store.find_gaps(SCRAPE_START_BLOCK, new_head, include_sunk=False)
                new_gap_blocks = sum(t - f + 1 for f, t in new_gaps)

                if new_gap_blocks <= args.lag_tolerance:
                    if sink.in_flight or sink.pending_commit:
                        _print_message(
                            f"  Within lag tolerance ({new_gap_blocks:,} <= "
                            f"{args.lag_tolerance}) but sink pipeline still has "
                            f"{sink.in_flight} in-flight / {sink.pending_commit} pending. "
                            "Waiting for sink to drain..."
                        )
                        status_ui.spinner("drain", "Draining the sink pool")
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
                gaps = new_gaps
                pending_ranges = list(new_gaps)
                total_target = max(blocks_done + new_gap_blocks, 1)
                status_ui.clear("drain")
                _top_up_in_flight()
                if not rpc_futures:
                    if args.max_calls is not None and calls_made >= args.max_calls:
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
        # --- Orderly shutdown ---------------------------------------
        # 0. Stop the startup head-check executor if it is still alive.
        head_executor.shutdown(wait=False, cancel_futures=True)

        # 1. Cancel anything we can in the RPC pool.
        if rpc_pool is not None:
            for f in list(rpc_futures):
                f.cancel()
            rpc_pool.shutdown(wait=False, cancel_futures=True)

        # 2. Let the sink pool finish what it has started, then commit every result that lands in
        #    frontier order. We do NOT submit new sink work past this point; any incomplete
        #    partition will be picked up by ``reconcile_with_cold_tier`` on the next start.
        if sink is not None:
            status_ui.spinner("drain", "Draining the sink pool")
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

        # 3. Tear down the sticky footer so the summary lands on a clean screen.
        status_ui.stop()

        # 4. Print the summary.
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
                    cold_root=env["cold_root"],
                    chain_head=head_for_output,
                    caught_up_confirmed=caught_up_confirmed,
                    blocks_done=blocks_done,
                    events_inserted=events_inserted,
                    calls_made=calls_made,
                    elapsed=elapsed,
                    fatal_reason=fatal_reason,
                )
        except Exception as e:  # noqa: BLE001
            console.print(f"\n(summary print failed: {type(e).__name__}: {e})")

        # 5. Close the hot DB connection. The lib enforces all writes were already committed
        #    atomically, so there's no flush.
        if store is not None:
            try:
                store.close()
            except Exception:  # noqa: BLE001
                pass

        # 6. Close the RPC client (per-thread connections — main thread only).
        try:
            rpc_client.close()
        except Exception:  # noqa: BLE001
            pass

        # 7. Close the run log last so shutdown and the summary are recorded.
        global _run_logger
        if _run_logger is not None:
            _run_logger.log("finished", message="main finished")
            _run_logger.close()
            _run_logger = None

        sys.stdout.flush()
        sys.exit(1 if fatal_reason else 0)


if __name__ == "__main__":
    main()
