#!/usr/bin/env python3
"""
Scrape Polymarket event logs from Polygon into the v4 hot DB and cold Parquet tree.

This is the v4 main program. It owns process lifecycle, CLI flags, env loading, signal handling,
the status line, and the orchestration that ties together the four library modules:

    * ``_internal.rpc_client.RpcClient`` — thread-safe Polygon JSON-RPC client.
        * ``_internal.event_decoders.decode_batch`` — decode each ``eth_getLogs`` response into
        ``(contract, event, row)`` triples.
    * ``_internal.persistence.HotStore`` — owns the hot DuckDB, atomic event ingestion,
        ``loaded_block_ranges`` bookkeeping, sink commit. Every write against the hot DB happens on
        this thread (the main thread).
    * ``_internal.parquet_sink.write_partition_files`` — runs in a background pool. Opens its own
        DuckDB connection on the hot DB, writes one UTC day's complete set of Parquet files,
        returns.

The control loop is:

  1. Open the HotStore. Reconcile the hot DB's progress table against whatever Parquet files
      already exist on the cold tier, so a crash between a sink worker's rename and the
      orchestrator's ``commit_sink`` is recovered automatically.
  2. Ask the chain for the current head block.
  3. Compute gaps from ``SCRAPE_START_BLOCK`` to ``chain_head - REORG_BUFFER_BLOCKS``, and print
      the startup banner. Blocks inside the reorg buffer are not sunk.
  4. Carve requests off those gaps, sizing each one from measured event density
      (``_internal.chunk_planner``) and hill-climbing concurrency, result budget and post-response
      sleep on blocks/second in fixed-parameter epochs (``_internal.epoch_optimizer``).
  5. As each RPC completes, decode the logs, record any UTC midnight the chunk crossed, and call
      ``store.persist`` in a single atomic transaction. Any closed day that is fully loaded and
      at or below the safe head is submitted to the sink pool.
  6. As sink workers complete (in calendar order), call ``store.commit_sink(day_key)`` to delete
      the day's hot rows and advance the sunk frontier.
  7. When the queue drains, re-check the chain head; if new gaps appeared below the safe head,
      rebuild the queue and continue. Stop when the gap up to the safe head is within
      ``--lag-tolerance`` blocks.

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
                                         ``polygon_contract_events_v4.db`` there
    SCRATCH_DIR                          DuckDB spill; safe to wipe between runs
    POLYGON_CONTRACT_EVENTS_V4_DIR       cold-tier root directory
    POLYGON_RPC_URL                      one or more Polygon JSON-RPC endpoints,
                                         separated by commas or whitespace. Each
                                         URL is an independent scrape lane.
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
from dataclasses import dataclass, field
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
from lib.run_logging import format_duration  # noqa: E402
from lib.ymd_partition import (  # noqa: E402
    day_key_from_timestamp,
    day_label,
    first_block_on_or_after,
    midnights_crossed,
)

from _internal.chunk_planner import ChunkPlanner
from _internal.epoch_optimizer import MAX_WORKERS, EpochOptimizer
from _internal.errors import (
    DuplicateRowError,
    OperationCancelled,
    PartitionFrontierError,
    SchemaMismatchError,
    V4Error,
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
    is_rate_limit_message,
)
from _internal.tables import (
    REORG_BUFFER_BLOCKS,
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
CHAIN_HEAD_ATTEMPTS: int = 4

POLL_TIMEOUT_SEC: float = 0.5
"""How long ``concurrent.futures.wait`` blocks before the loop refreshes the footer.

Kept well under a second so progress never looks stalled.
"""

MIN_LANE_COOLDOWN_SEC: float = 1.0
"""After a lane returns a failed chunk, keep it from taking new work for at least this long.

The chunk goes back to the front of the queue. The lane that failed it is idle, so the next
fill would hand the same chunk straight back. A throttled public endpoint then keeps a frontier
hole forever while healthier lanes scrape past it."""

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

def _parse_rpc_urls(raw: str) -> list[str]:
    """Split ``POLYGON_RPC_URL`` on commas and whitespace. Order-preserving unique.

    JSON-RPC URLs do not contain unencoded commas or spaces, so either delimiter is safe.
    Duplicate entries (the same URL twice) collapse to one lane.
    """
    urls: list[str] = []
    seen: set[str] = set()
    for token in raw.replace(",", " ").split():
        url = token.strip()
        if len(url) >= 2 and url[0] == url[-1] and url[0] in {'"', "'"}:
            url = url[1:-1].strip()
        if not url or url in seen:
            continue
        seen.add(url)
        urls.append(url)
    return urls


def _looks_like_rpc_url(url: str) -> bool:
    return url.startswith(("http://", "https://"))


def _load_environment() -> dict[str, Any]:
    """Read every required env var from the root ``.env`` and validate it.

    Exits the process with a clear message on any missing or invalid value.
    """
    project_root = Path(__file__).resolve().parent.parent.parent

    assert_git_clean(project_root)

    load_dotenv(project_root / ".env")

    required = [
        "HOT_DIR",
        "SCRATCH_DIR",
        "POLYGON_CONTRACT_EVENTS_V4_DIR",
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
    db_path = os.path.join(hot_dir, "polygon_contract_events_v4.db")

    cold_root = os.environ["POLYGON_CONTRACT_EVENTS_V4_DIR"]
    if not os.path.isdir(cold_root):
        sys.exit(
            f"POLYGON_CONTRACT_EVENTS_V4_DIR does not exist: {cold_root}\n"
            f"Ensure the cold-tier volume is mounted."
        )
    if not os.access(cold_root, os.W_OK):
        sys.exit(f"POLYGON_CONTRACT_EVENTS_V4_DIR is not writable: {cold_root}")

    scratch_dir = require_directory_env("SCRATCH_DIR")

    rpc_urls = _parse_rpc_urls(os.environ["POLYGON_RPC_URL"])
    if not rpc_urls:
        sys.exit(
            "POLYGON_RPC_URL is empty. Set one or more http(s) URLs in .env, "
            "separated by commas or whitespace."
        )
    bad = [url for url in rpc_urls if not _looks_like_rpc_url(url)]
    if bad:
        sys.exit(
            "POLYGON_RPC_URL entries must be http(s) URLs; got: " + ", ".join(bad)
        )

    return {
        "db_path": db_path,
        "cold_root": cold_root,
        "rpc_urls": rpc_urls,
        "scratch_dir": scratch_dir,
    }


# ---------------------------------------------------------------------------
# Display helpers
# ---------------------------------------------------------------------------

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


def _blocks_loaded_between(store: HotStore, start: int, end: int) -> int:
    """Count unsunk loaded blocks that overlap ``[start, end]``."""
    loaded = 0
    for from_b, to_b, _ in store.list_loaded_ranges(include_sunk=False):
        if to_b < start:
            continue
        if from_b > end:
            break
        overlap_start = max(from_b, start)
        overlap_end = min(to_b, end)
        if overlap_start <= overlap_end:
            loaded += overlap_end - overlap_start + 1
    return loaded


def _day_cuts_for_chunk(
    rpc_client: RpcClient,
    from_block: int,
    to_block: int,
    ts_by_block: dict[int, int],
) -> list[tuple[int, int]]:
    """Return ``(day_key, first_block)`` for each UTC midnight this chunk crosses.

    The left edge is the header of ``from_block - 1`` when that block is in
    range, so a midnight that falls between two chunks is still a cut at
    ``from_block``. Header time only moves forward, so each midnight has
    one first block.
    """
    need = [from_block, to_block]
    if from_block > SCRAPE_START_BLOCK:
        need.append(from_block - 1)
    missing = [block for block in need if block not in ts_by_block]
    if missing:
        ts_by_block.update(rpc_client.get_block_timestamps(missing))

    left_block = from_block - 1 if from_block > SCRAPE_START_BLOCK else from_block
    ts_left = ts_by_block[left_block]
    ts_right = ts_by_block[to_block]
    if ts_right < ts_left:
        raise V4Error(
            f"header time went backwards between block {left_block} and {to_block}"
        )

    def ts_at(block: int) -> int:
        cached = ts_by_block.get(block)
        if cached is None:
            ts_by_block.update(rpc_client.get_block_timestamps([block]))
            cached = ts_by_block[block]
        return cached

    cuts: list[tuple[int, int]] = []
    search_lo = left_block
    for midnight in midnights_crossed(ts_left, ts_right):
        cut_block = first_block_on_or_after(ts_at, search_lo, to_block, midnight)
        cuts.append((day_key_from_timestamp(midnight), cut_block))
        search_lo = cut_block
    return cuts


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


def _tuning_suffix(optimizer: EpochOptimizer, planner: ChunkPlanner) -> str:
    """``  threads ##  max blks/req ##`` for the scrape sticky line and speed updates."""
    max_blks = planner.next_span(optimizer.params.result_budget)
    return f"  threads {optimizer.params.workers}  max blks/req {max_blks:,}"


_SCRAPE_FAILED_GLYPH = Text("✗", style="red")


class _ScrapeGlyphColumn(SpinnerColumn):
    """Spinner while a key is working; a red ✗ once that key is dead."""

    def render(self, task: Task) -> Text:
        if task.fields.get("failed"):
            return _SCRAPE_FAILED_GLYPH
        return super().render(task)


def _unique_endpoint_labels(urls: list[str]) -> list[str]:
    """Masked ``host key`` labels, with `` (2)`` suffixes if two URLs mask the same way."""
    labels = [_describe_rpc_endpoint(url) for url in urls]
    counts: dict[str, int] = {}
    for label in labels:
        counts[label] = counts.get(label, 0) + 1
    seen: dict[str, int] = {}
    unique: list[str] = []
    for label in labels:
        if counts[label] == 1:
            unique.append(label)
            continue
        seen[label] = seen.get(label, 0) + 1
        unique.append(f"{label} ({seen[label]})")
    return unique


def _lane_footer_text(lane: "_RpcLane", *, run_seconds: float) -> str:
    """Sticky-footer copy for one RPC lane: spinning scrape line, or failed line."""
    if lane.fatal_reason is not None:
        return f"Failed {lane.label}  {lane.fatal_reason}"
    rate = lane.blocks_done / run_seconds if run_seconds > 0 else 0.0
    return (
        f"Scraping {lane.label}  {rate:,.0f} blk/s"
        f"{_tuning_suffix(lane.optimizer, lane.planner)}"
    )


def _fetch_fatal_is_process_wide(result: dict[str, Any]) -> bool:
    """Decode bugs and missing headers are data integrity; HTTP 401 is one key."""
    msg = str(result.get("error_message") or "")
    return msg.startswith("decode failure") or msg.startswith("missing header")


def _all_rpc_lanes_fatal(lanes: list["_RpcLane"]) -> bool:
    return bool(lanes) and all(not lane.live for lane in lanes)


def _all_lanes_fatal_message(lanes: list["_RpcLane"]) -> str:
    parts = [f"{lane.label}: {lane.fatal_reason}" for lane in lanes]
    return "all RPC endpoints failed: " + "; ".join(parts)


# Footer rows, top to bottom. The overall run bar sits at the very bottom.
_BAR_ORDER = ("partition", "main")


class RichRunStatus:
    """Sticky footer showing only the work that is running right now.

    A row is added when an activity starts and removed when it finishes, so a completed step
    never lingers on screen. Log lines printed through ``console`` scroll above the footer.

    RPC lanes are an exception: each endpoint keeps a row for the whole scrape. A live lane
    spins; a failed lane shows a red ✗ and stays put so the operator can see which key died.
    """

    def __init__(self) -> None:
        self._spinners = Progress(
            SpinnerColumn(),
            TextColumn("{task.description}"),
            console=console,
        )
        self._scrape = Progress(
            _ScrapeGlyphColumn(),
            TextColumn("{task.description}"),
            console=console,
        )
        self._bars: dict[str, Progress] = {}
        self._live = Live(console=console, refresh_per_second=8, transient=True)
        self._lock = threading.Lock()
        self._started = False
        self._spinner_tasks: dict[str, TaskID] = {}
        self._scrape_tasks: dict[str, TaskID] = {}
        self._bar_tasks: dict[str, TaskID] = {}
        self._bar_descriptions: dict[str, str] = {}

    def start(self) -> None:
        self._started = True
        self._live.start()
        with self._lock:
            self._refresh()

    def stop(self) -> None:
        self._live.stop()
        self._started = False

    def spinner(self, key: str, description: str) -> None:
        """Show an indeterminate row for ``key``, replacing any earlier text for it."""
        with self._lock:
            task_id = self._spinner_tasks.get(key)
            if task_id is None:
                task_id = self._spinners.add_task(description, total=None)
                self._spinner_tasks[key] = task_id
            self._spinners.update(task_id, description=description)
            self._refresh()

    def scrape(self, key: str, description: str, *, failed: bool = False) -> None:
        """Show one RPC-lane row. Failed rows keep a stopped ✗ instead of a spinner."""
        with self._lock:
            task_id = self._scrape_tasks.get(key)
            if task_id is None:
                task_id = self._scrape.add_task(
                    description, total=None, failed=failed
                )
                self._scrape_tasks[key] = task_id
            else:
                task = next(t for t in self._scrape.tasks if t.id == task_id)
                if task.fields.get("failed") and not failed:
                    # A dead key stays dead; do not resume its spinner.
                    pass
                else:
                    self._scrape.update(
                        task_id, description=description, failed=failed
                    )
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
        if not self._started:
            return
        rows: list[Progress] = []
        if self._spinners.tasks:
            rows.append(self._spinners)
        if self._scrape.tasks:
            rows.append(self._scrape)
        rows.extend(self._bars[key] for key in _BAR_ORDER if key in self._bars)
        self._live.update(Group(*rows) if rows else "")


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


def _chain_head_retry_wait_sec(attempt: int) -> float:
    """Backoff after a failed chain-head attempt (1-based). Capped at 8s."""
    return min(8.0, 2.0 * (2 ** (attempt - 1)))


def fetch_block_number_with_retries(
    get_block_number,
    *,
    attempts: int = CHAIN_HEAD_ATTEMPTS,
    sleep_fn=_interruptible_sleep,
    on_retry=None,
    is_stop=None,
) -> int:
    """Call ``get_block_number`` up to ``attempts`` times.

    Retryable HTTP / rate-limit errors print via ``on_retry(attempt, attempts, status, wait)``
    and sleep. Permanent errors raise immediately. Exhaustion raises ``RuntimeError``
    with the last status (for example ``HTTP 403``).
    """
    last_status = "no response"
    for attempt in range(1, attempts + 1):
        try:
            return get_block_number()
        except HTTPError as e:
            if not e.is_retryable:
                raise
            last_status = f"HTTP {e.status_code}"
        except RPCError as e:
            if not is_rate_limit_message(str(e)):
                raise
            last_status = "rate limited"
        if attempt == attempts:
            break
        wait = _chain_head_retry_wait_sec(attempt)
        if on_retry is not None:
            on_retry(attempt, attempts, last_status, wait)
        sleep_fn(wait)
        if is_stop is not None and is_stop():
            raise OperationCancelled("chain head query interrupted")
    raise RuntimeError(last_status)


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
    ``raw_log_count``, ``elapsed_sec``, and ``day_cuts`` (``list[(day_key, first_block)]``
    for each UTC midnight the chunk crossed, including a zero-log chunk).

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
        # 401/404 and friends never fix themselves; retrying just burns the backlog.
        return {
            "status": "fatal",
            "wid": wid,
            "elapsed_sec": time.monotonic() - t0,
            "error_message": f"HTTP {e.status_code} from the RPC provider",
        }
    except (RPCError, RuntimeError) as e:
        _interruptible_sleep(sleep_after_response_sec)
        if is_rate_limit_message(str(e)):
            return {
                "status": "throttled",
                "wid": wid,
                "elapsed_sec": time.monotonic() - t0,
                "http_status": 429,
            }
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

    block_numbers = {
        row["block_number"]
        for rows in rows_by_target.values()
        for row in rows
    }
    ts_by_block: dict[int, int] = {}
    try:
        if block_numbers:
            ts_by_block.update(rpc_client.get_block_timestamps(sorted(block_numbers)))
        day_cuts = _day_cuts_for_chunk(rpc_client, from_block, to_block, ts_by_block)
    except OperationCancelled:
        return {"status": "cancelled", "wid": wid, "elapsed_sec": time.monotonic() - t0}
    except HTTPError as e:
        _interruptible_sleep(sleep_after_response_sec)
        if e.is_retryable:
            return {
                "status": "throttled",
                "wid": wid,
                "elapsed_sec": time.monotonic() - t0,
                "http_status": e.status_code,
            }
        return {
            "status": "fatal",
            "wid": wid,
            "elapsed_sec": time.monotonic() - t0,
            "error_message": f"header fetch: HTTP {e.status_code} from the RPC provider",
        }
    except (ValueError, V4Error) as e:
        return {
            "status": "fatal",
            "wid": wid,
            "elapsed_sec": time.monotonic() - t0,
            "error_message": f"header day cut: {type(e).__name__}: {e}",
        }
    except Exception as e:
        _interruptible_sleep(sleep_after_response_sec)
        if is_rate_limit_message(str(e)):
            return {
                "status": "throttled",
                "wid": wid,
                "elapsed_sec": time.monotonic() - t0,
                "http_status": 429,
            }
        return {
            "status": "error",
            "wid": wid,
            "elapsed_sec": time.monotonic() - t0,
            "error_message": f"header fetch: {type(e).__name__}: {e}",
        }
    missing = block_numbers - set(ts_by_block)
    if missing:
        return {
            "status": "fatal",
            "wid": wid,
            "elapsed_sec": time.monotonic() - t0,
            "error_message": f"missing header timestamp for blocks {sorted(missing)[:8]}",
        }
    for rows in rows_by_target.values():
        for row in rows:
            row["block_timestamp"] = ts_by_block[row["block_number"]]

    _interruptible_sleep(sleep_after_response_sec)

    return {
        "status": "ok",
        "wid": wid,
        "elapsed_sec": elapsed,
        "rows_by_target": rows_by_target,
        "raw_log_count": raw_log_count,
        "day_cuts": day_cuts,
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


def _make_rpc_client(url: str) -> RpcClient:
    return RpcClient(
        url,
        config=RpcClientConfig(
            call_timeout_sec=RPC_CALL_TIMEOUT_SEC,
            block_number_timeout_sec=RPC_BLOCK_NUMBER_TIMEOUT_SEC,
            max_retries=RPC_MAX_RETRIES,
            retry_backoff_base_sec=RPC_BACKOFF_BASE_SEC,
        ),
        stop_event=_stop_event,
    )


@dataclass
class _RpcLane:
    """One JSON-RPC endpoint: its own planner, optimizer, workers, and footer line."""

    index: int
    url: str
    label: str
    client: RpcClient
    planner: ChunkPlanner = field(default_factory=ChunkPlanner)
    optimizer: EpochOptimizer = field(default_factory=EpochOptimizer)
    pool: ThreadPoolExecutor | None = None
    futures: dict[Future[dict], tuple[int, int]] = field(default_factory=dict)
    fatal_reason: str | None = None
    probe_in_flight: tuple[int, int] | None = None
    draining_epoch: bool = False
    blocks_done: int = 0
    cooldown_until: float = 0.0

    @property
    def live(self) -> bool:
        return self.fatal_reason is None

    @property
    def spinner_key(self) -> str:
        return f"scrape:{self.index}"


def _park_lane_after_failure(lane: _RpcLane) -> None:
    """Hold ``lane`` out of scheduling after it puts a chunk back on the queue."""
    wait = max(lane.optimizer.params.sleep_after_response_sec, MIN_LANE_COOLDOWN_SEC)
    lane.cooldown_until = time.monotonic() + wait


def _lane_accepts_work(lane: _RpcLane, *, now: float) -> bool:
    return (
        lane.live
        and not lane.draining_epoch
        and lane.pool is not None
        and now >= lane.cooldown_until
    )


def _lanes_cooling_down(lanes: list[_RpcLane], now: float) -> list[_RpcLane]:
    return [
        lane
        for lane in lanes
        if lane.live and lane.pool is not None and now < lane.cooldown_until
    ]


# ---------------------------------------------------------------------------
# Sink orchestration
# ---------------------------------------------------------------------------
#
# ``write_partition_files`` runs in a background pool. The orchestrator tracks each in-flight
# write by its UTC day key so it can call ``HotStore.commit_sink`` in calendar order when
# futures finish.
#
# Order matters: ``commit_sink`` only accepts the day that starts on the next unsunk block.
# The sink pool may finish out of order, so the orchestrator buffers completed results and
# drains them in that order.

class _SinkOrchestrator:
    """Manage the sink worker pool and frontier-ordered commits.

    Owns:
      * a ``ThreadPoolExecutor`` with at most ``--sink-workers`` threads;
      * a map ``Future -> day_key`` for in-flight writes;
      * a buffer of completed-but-not-yet-committed results, keyed by day key so the
        orchestrator can pop them in calendar order.

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

    def submit(self, day_key: int) -> None:
        """Schedule one closed UTC day for sinking.

        Already-submitted days are skipped. ``persist`` and a later
        ``list_days_ready_to_sink`` can both name the same day.
        """
        if day_key in self._submitted_partitions:
            return
        if _stop_event.is_set():
            return
        self._submitted_partitions.add(day_key)
        # Sink workers use their own DuckDB connection with matching config.
        # This avoids cross-thread transactional contention on a shared
        # connection while still reading a consistent snapshot via MVCC.
        future = self._executor.submit(
            write_partition_files,
            self._db_path,
            self._cold_root,
            day_key,
            duckdb_connect_config=self._sink_connect_config,
            progress_cb=self._progress_cb,
            stop_event=_stop_event,
        )
        self._in_flight[future] = day_key

    def reap_completed(self) -> None:
        """Move any finished futures into the commit-pending buffer.

        Non-blocking: only checks ``future.done()`` for each in-flight future. The actual commit
        happens in ``commit_ready``, which is also non-blocking (it just checks the buffer head
        against the sunk frontier).
        """
        done = [f for f in self._in_flight if f.done()]
        for fut in done:
            day_key = self._in_flight.pop(fut)
            try:
                result = fut.result()
            except BaseException as e:  # noqa: BLE001
                # Any worker exception is fatal: the cold tier could be inconsistent (some temp
                # files unrenamed, some renamed for an earlier day). Surface to the main loop
                # so it can stop everything cleanly. Don't re-raise here so we can keep reaping
                # the rest of the in-flight set for a cleaner shutdown.
                self._submitted_partitions.discard(day_key)
                self._fatal_error = e
                _print_message(
                    f"  SINK FAIL  {day_label(day_key)}  "
                    f"{type(e).__name__}: {e}"
                )
                continue
            self._completed[day_key] = result

    def commit_ready(self) -> int:
        """Commit every completed day that starts on the next unsunk block.

        Returns the number of commits performed. Stops as soon as that day
        is not yet in the buffer. An open day is not committable.

        Must be called from the orchestrator thread because
        ``HotStore.commit_sink`` is single-threaded.
        """
        n_committed = 0
        while self._completed:
            span = self._store.current_day_span()
            if span is None or span[2] is None:
                return n_committed
            expected_next = span[0]
            if expected_next not in self._completed:
                return n_committed
            write_result = self._completed.pop(expected_next)
            label = day_label(expected_next)
            try:
                # The manifest is what makes a day sunk, so it goes first and in
                # calendar order. Only then may anything treat this day as final.
                if not publish_manifest(self._cold_root, expected_next):
                    raise V4Error(
                        f"{label} is missing Parquet files for one or more "
                        "targets, so it cannot be declared sunk"
                    )
                self._store.commit_sink(expected_next)
            except PartitionFrontierError as e:
                self._fatal_error = e
                _print_message(
                    f"  COMMIT_SINK FRONTIER ERROR  {label}: {e}"
                )
                return n_committed
            except V4Error as e:
                self._fatal_error = e
                _print_message(
                    f"  COMMIT_SINK FAIL  {label}: {e}"
                )
                return n_committed
            n_committed += 1
            mb = write_result.bytes_total / (1024 * 1024)
            _print_message(
                f"  <- sunk {label}"
                f"  rows: {write_result.rows_total:,}"
                f"  bytes: {mb:.1f} MiB"
                f"  {write_result.elapsed_ms / 1000:.1f}s"
            )
            # This commit moved the frontier. The following day may already be
            # closed and loaded, so no later persist will see it as newly ready.
            for day_key in self._store.list_days_ready_to_sink():
                self.submit(day_key)
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
    rpc_labels: list[str],
    sunk_frontier: int,
    loaded_frontier: int,
    chain_head: int,
    safe_head: int,
    gaps: list[tuple[int, int]],
    total_gap_blocks: int,
    unsunk_ranges: int,
) -> None:
    """Print the once-per-run startup banner above the live status line.

    Screen shows ``output:`` (cold root, bold), ``log:``, and a blank line.
    Hot, scratch, frontiers, and gap detail go to the run log only.
    """
    now = datetime.now(timezone.utc).isoformat()
    banner_lines = [
        f"# main — start at {now}",
        f"hot database:         {db_path} ({_file_size_str(db_path)})",
        f"raw:                  {cold_root}",
        f"scratch:              {scratch_dir}",
        f"log:                  {log_path}",
        f"rpc endpoints:        {len(rpc_labels)}",
    ]
    for label in rpc_labels:
        banner_lines.append(f"rpc:                  {label}")
    banner_lines.append("")
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
    banner_lines.append(
        f"safe head:            {safe_head:,}  "
        f"(chain head minus {REORG_BUFFER_BLOCKS} blocks)"
    )
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

    # Screen: output + log + blank line. Hot, scratch, and frontier detail stay in the run log.
    _print_message(Text(f"output: {cold_root}", style="bold"))
    _print_message(f"log:    {log_path}")
    _print_message("")
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
    if fatal_reason:
        heading = "run failed"
    elif _stop_event.is_set():
        heading = "run interrupted"
    else:
        heading = "run complete"

    sunk = None
    if store is not None:
        sunk = get_manifest_frontier(cold_root)
    if sunk is None or sunk <= (SCRAPE_START_BLOCK - 1):
        frontier_note = "none"
    elif caught_up_confirmed:
        frontier_note = f"{sunk:,} (caught up)"
    else:
        frontier_note = f"{sunk:,}"

    summary_lines = [
        "",
        heading,
        f"   time: {format_duration(elapsed)}",
        f"   blocks: {blocks_done:,}",
        f"   events: {events_inserted:,}",
        f"   new frontier: {frontier_note}",
        "",
    ]
    if fatal_reason:
        summary_lines.insert(-1, f"   error: {fatal_reason}")

    for line in summary_lines:
        console.print(line, highlight=False)
        _log_event("summary", line)

    _log_event(
        "summary_detail",
        "throughput",
        chain_head=chain_head,
        calls_made=calls_made,
    )


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
        description="Scrape Polymarket event logs from Polygon JSON-RPC into the v4 hot DB and cold Parquet tree.",
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
        help=(
            "stop when the gap up to the safe head (chain head minus "
            f"{REORG_BUFFER_BLOCKS} blocks) is within N blocks (default: 2)"
        ),
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

    rpc_urls: list[str] = env["rpc_urls"]
    rpc_labels = _unique_endpoint_labels(rpc_urls)
    lanes: list[_RpcLane] = [
        _RpcLane(
            index=i,
            url=url,
            label=rpc_labels[i],
            client=_make_rpc_client(url),
        )
        for i, url in enumerate(rpc_urls)
    ]

    def _fail_lane(lane: _RpcLane, reason: str) -> bool:
        """Mark one endpoint dead. Returns True if every endpoint is now dead."""
        if lane.fatal_reason is None:
            lane.fatal_reason = reason
            _print_message(f"FATAL {lane.label}: {reason}")
            status_ui.scrape(
                lane.spinner_key,
                _lane_footer_text(lane, run_seconds=time.monotonic() - run_start),
                failed=True,
            )
        return _all_rpc_lanes_fatal(lanes)

    def _query_chain_head(*, label: str = "Querying chain head") -> int:
        last_errors: list[str] = []
        for lane in lanes:
            if not lane.live:
                continue

            def _on_retry(
                attempt: int,
                attempts: int,
                status: str,
                wait: float,
                *,
                _lane: _RpcLane = lane,
            ) -> None:
                _print_message(
                    f"  chain head {attempt}/{attempts}  {_lane.label}  "
                    f"{status}  retry in {wait:.0f}s"
                )
                status_ui.spinner(
                    "head", f"{label} {_lane.label} {attempt + 1}/{attempts}"
                )

            status_ui.spinner(
                "head", f"{label} {lane.label} 1/{CHAIN_HEAD_ATTEMPTS}"
            )
            try:
                return fetch_block_number_with_retries(
                    lane.client.get_block_number,
                    attempts=CHAIN_HEAD_ATTEMPTS,
                    on_retry=_on_retry,
                    is_stop=_stop_event.is_set,
                )
            except OperationCancelled:
                raise
            except Exception as e:  # noqa: BLE001
                reason = str(e)
                last_errors.append(f"{lane.label}: {reason}")
                _fail_lane(lane, reason)
        raise RuntimeError(
            "; ".join(last_errors) if last_errors else "no live RPC endpoints"
        )

    head_executor = ThreadPoolExecutor(max_workers=1, thread_name_prefix="startup-head")
    head_future = head_executor.submit(_query_chain_head)

    store: HotStore | None = None
    sink: _SinkOrchestrator | None = None

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
        except (FileNotFoundError, V4Error) as e:
            sys.exit(f"Could not open hot DB at {env['db_path']}: {e}")

        # --- Read manifest frontier --------------------------------
        # A published manifest means that day is final. Incomplete event folders above the
        # frontier are unpublished; cleanup below removes them before roll-forward, which
        # fail-fasts on a two-file violation.
        status_ui.spinner("startup", "Reading manifest frontier")

        def _frontier_progress(*, op, phase, rows_done=None, rows_total=None,
                               elapsed_ms=None, message="", partition=None):
            if partition is not None:
                status_ui.spinner(
                    "startup", f"Finding sunk day {day_label(partition)}"
                )

        try:
            manifest_frontier, _ = read_manifest_frontier(
                env["cold_root"],
                progress_cb=_frontier_progress,
            )
        except V4Error as e:
            sys.exit(f"FATAL: manifest frontier read failed: {e}")

        _print_message(f"Manifest frontier read complete: frontier={manifest_frontier:,}.")

        # --- Cleanup unpublished leftovers above the frontier -------
        # Must run before roll-forward. Incomplete day folders (parquet without metadata, mixed
        # targets) are unpublished junk; complete two-file days stay so roll-forward can publish
        # _SUCCESS.
        status_ui.spinner("startup", "Cleaning temporary files at the frontier")

        def _cleanup_progress(*, op, phase, rows_done=None, rows_total=None,
                              elapsed_ms=None, message="", partition=None):
            if partition is not None:
                status_ui.spinner(
                    "startup",
                    f"Cleaning temporary files for {day_label(partition)}",
                )

        try:
            cleaned_manifest = cleanup_temp_dirs_after_frontier(
                env["cold_root"],
                progress_cb=_cleanup_progress,
            )
        except V4Error as e:
            sys.exit(f"FATAL: post-frontier cleanup failed: {e}")

        _print_message(
            "Post-frontier cleanup complete: "
            f"removed={cleaned_manifest:,} path(s)."
        )

        # --- Chain head ---------------------------------------------
        # Queried in parallel with the startup disk work above. Roll-forward needs the
        # scrape-start header, so the head join happens before any day is published.
        try:
            chain_head = head_future.result()
        except OperationCancelled:
            raise KeyboardInterrupt
        except Exception as e:  # noqa: BLE001
            sys.exit(
                f"FATAL: could not read chain head "
                f"({CHAIN_HEAD_ATTEMPTS}/{CHAIN_HEAD_ATTEMPTS}): {e}"
            )
        finally:
            head_executor.shutdown(wait=False)
            status_ui.clear("head")

        if chain_head < SCRAPE_START_BLOCK:
            sys.exit(
                f"FATAL: RPC returned chain head {chain_head:,}, which is before "
                f"SCRAPE_START_BLOCK {SCRAPE_START_BLOCK:,}. The RPC endpoint may be "
                f"returning incorrect data or is in a failed state."
            )

        loaded_max = store.max_loaded_block()
        if loaded_max is not None and loaded_max > chain_head:
            sys.exit(
                f"FATAL: chain head {chain_head:,} is behind loaded block {loaded_max:,}. "
                "The RPC endpoint rewound into data already stored."
            )

        _log_event("chain_head", f"chain head {chain_head:,}", chain_head=chain_head)

        # --- First UTC day, from one header of SCRAPE_START_BLOCK ----
        status_ui.spinner("startup", "Reading the scrape-start header")
        first_day_key: int | None = None
        header_errors: list[str] = []
        for lane in lanes:
            if not lane.live:
                continue
            try:
                stamps = lane.client.get_block_timestamps([SCRAPE_START_BLOCK])
                first_day_key = day_key_from_timestamp(stamps[SCRAPE_START_BLOCK])
                break
            except OperationCancelled:
                raise KeyboardInterrupt
            except Exception as e:  # noqa: BLE001
                header_errors.append(f"{lane.label}: {e}")
                _fail_lane(lane, str(e))
        if first_day_key is None:
            detail = "; ".join(header_errors) if header_errors else "no live RPC endpoints"
            sys.exit(
                "FATAL: could not read the header of "
                f"SCRAPE_START_BLOCK {SCRAPE_START_BLOCK:,}: {detail}"
            )
        try:
            store.open_first_day(first_day_key)
        except V4Error as e:
            sys.exit(f"FATAL: {e}")

        # --- Roll manifest frontier forward -------------------------
        status_ui.spinner("startup", "Touching up sunk days")

        def _manifest_progress(*, op, phase, rows_done=None, rows_total=None,
                               elapsed_ms=None, message="", partition=None):
            if partition is not None:
                status_ui.spinner(
                    "startup", f"Touching up sunk day {day_label(partition)}"
                )

        try:
            published_manifests = roll_forward_manifests_to_exhaustion(
                env["cold_root"],
                first_day_key=first_day_key,
                progress_cb=_manifest_progress,
                stop_event=_stop_event,
            )
        except OperationCancelled:
            raise KeyboardInterrupt
        except V4Error as e:
            sys.exit(f"FATAL: manifest startup checks failed: {e}")

        _print_message(
            "Manifest roll-forward complete: "
            f"published={published_manifests:,} day(s)."
        )

        manifest_frontier = get_manifest_frontier(
            env["cold_root"], first_day_key=first_day_key
        )
        # Manifest is the durable source of truth for sunk frontier at startup.
        store.sunk_frontier = manifest_frontier

        # --- Reconcile hot DB progress with cold tier ----------------
        # If we crashed between a sink worker's rename and the orchestrator's commit_sink, the
        # Parquet files reached disk but the hot DB still has the rows. Reconcile fixes that
        # without re-scraping. Parquet with no manifest does not move the frontier.
        status_ui.spinner("startup", "Reading sunk days from the hot database")

        def _reconcile_progress(*, op, phase, rows_done=None, rows_total=None,
                                elapsed_ms=None, message="", partition=None):
            if phase == "scan":
                status_ui.spinner("startup", "Scanning the cold tier for sunk days")
            elif phase == "update":
                status_ui.spinner(
                    "startup", "Recording sunk days in the hot database"
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
        except V4Error as e:
            sys.exit(f"FATAL: reconcile_with_cold_tier failed: {e}")
        if newly_sunk:
            _print_message(f"Reconciled {newly_sunk:,} day(s) from existing cold-tier files.")

        # --- Sink orchestrator --------------------------------------
        # Spinner and bar for key "partition" live in different stores. clear() drops both,
        # so a mode switch clears first and then sets only one.
        part_mode: str | None = None
        spin_day: int | None = None
        spin_t0 = 0.0

        def _show_partition_spinner(description: str) -> None:
            nonlocal part_mode
            if part_mode != "spin":
                status_ui.clear("partition")
                part_mode = "spin"
            status_ui.spinner("partition", description)

        def _show_partition_bar(description: str, done: int, total: int) -> None:
            nonlocal part_mode
            if part_mode != "bar":
                status_ui.clear("partition")
                part_mode = "bar"
            status_ui.bar("partition", description, done, total)

        def _sink_progress_cb(*, op, phase, rows_done=None, rows_total=None,
                              elapsed_ms=None, message="", partition=None):
            # Takes over the day row while files for that day are copied.
            if op == "sink" and phase == "copy" and partition is not None:
                _show_partition_bar(
                    f"Sinking {day_label(partition)}",
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

        safe_head = chain_head - REORG_BUFFER_BLOCKS
        store.set_safe_head(safe_head)

        # A day that closed before the head was known waits here, not only inside persist.
        backlog = store.list_days_ready_to_sink()
        for p in backlog:
            sink.submit(p)
        if backlog:
            _print_message(
                f"Submitted {len(backlog):,} pre-existing ready day(s) to the sink pool."
            )

        status_ui.clear("startup")

        # Gaps stop at the safe head. Nothing inside the reorg buffer is fetched or sunk.
        if safe_head < SCRAPE_START_BLOCK:
            gaps = []
        else:
            gaps = store.find_gaps(SCRAPE_START_BLOCK, safe_head, include_sunk=False)
        total_gap_blocks = sum(t - f + 1 for f, t in gaps)
        sunk_frontier = store.get_sunk_frontier()
        loaded_frontier = store.get_loaded_frontier()
        unsunk_ranges = len(store.list_loaded_ranges(include_sunk=False))

        _print_banner(
            db_path=env["db_path"],
            cold_root=env["cold_root"],
            scratch_dir=env["scratch_dir"],
            log_path=log_path,
            rpc_labels=[lane.label for lane in lanes],
            sunk_frontier=sunk_frontier,
            loaded_frontier=loaded_frontier,
            chain_head=chain_head,
            safe_head=safe_head,
            gaps=gaps,
            total_gap_blocks=total_gap_blocks,
            unsunk_ranges=unsunk_ranges,
        )

        # The main bar counts only the blocks this run has to scrape, starting at zero.
        total_target = max(total_gap_blocks, 1)
        status_ui.bar("main", "Total progress", 0, total_target)

        if (
            safe_head >= SCRAPE_START_BLOCK
            and not gaps
            and sink.in_flight == 0
            and sink.pending_commit == 0
        ):
            caught_up_confirmed = True
            lag = safe_head - loaded_frontier if loaded_frontier >= 0 else safe_head - SCRAPE_START_BLOCK + 1
            _log_event(
                "caught-up proof",
                "caught up",
                sunk_frontier=sunk_frontier,
                loaded_frontier=loaded_frontier,
                chain_head=chain_head,
                lag=lag,
            )
            return

        # --- Set up per-endpoint planners and the shared work queue -
        # Ranges still to scrape, ascending. Chunks are carved off the front as workers free up,
        # so each request is sized for the density where it actually lands. Each RPC URL has its
        # own planner, optimizer and worker pool; they pull disjoint chunks from this queue.
        pending_ranges: list[tuple[int, int]] = list(gaps)
        chunk_failures: dict[tuple[int, int], int] = {}

        def _in_flight() -> bool:
            return any(lane.futures for lane in lanes)

        def _any_draining() -> bool:
            return any(lane.live and lane.draining_epoch for lane in lanes)

        def _submit_one(lane: _RpcLane, chunk: tuple[int, int]) -> bool:
            """Submit one chunk on ``lane``. Returns False on call-count limit."""
            nonlocal calls_made
            if _stop_event.is_set() or not lane.live or lane.pool is None:
                return False
            if args.max_calls is not None and calls_made >= args.max_calls:
                return False
            fut = lane.pool.submit(
                _fetch_range,
                from_block=chunk[0], to_block=chunk[1],
                rpc_client=lane.client,
                sleep_after_response_sec=lane.optimizer.params.sleep_after_response_sec,
            )
            lane.futures[fut] = chunk
            calls_made += 1
            return True

        def _refresh_bars() -> None:
            """Repaint the footer from current progress.

            An open day has no known length, so the row is a spinner. Once the
            next midnight has been seen, the closed span is a bar. A sink in
            flight owns the row and is not overwritten here.
            """
            nonlocal part_mode, spin_day, spin_t0
            if sink.in_flight == 0:
                span = store.current_day_span()
                if span is None:
                    status_ui.clear("partition")
                    part_mode = None
                else:
                    day_key, first, last = span
                    if last is None:
                        loaded = _blocks_loaded_between(store, first, 2**32 - 1)
                        if spin_day != day_key or part_mode != "spin":
                            spin_day = day_key
                            spin_t0 = time.monotonic()
                        elapsed_s = max(0.0, time.monotonic() - spin_t0)
                        _show_partition_spinner(
                            f"{day_label(day_key)}  blks: {loaded:,}  {elapsed_s:.0f}s"
                        )
                    else:
                        _show_partition_bar(
                            day_label(day_key),
                            _blocks_loaded_between(store, first, last),
                            last - first + 1,
                        )
            status_ui.bar(
                "main", "Total progress", min(blocks_done, total_target), total_target
            )
            run_seconds = time.monotonic() - run_start
            for lane in lanes:
                status_ui.scrape(
                    lane.spinner_key,
                    _lane_footer_text(lane, run_seconds=run_seconds),
                    failed=lane.fatal_reason is not None,
                )

        def _top_up_in_flight() -> None:
            """Fill each live lane up to its optimizer's worker count.

            Round-robins so a slow key does not keep every early block. Submits nothing on a
            lane that is draining an epoch, so that drain can actually finish.
            """
            if _stop_event.is_set():
                return
            now = time.monotonic()
            live = [lane for lane in lanes if _lane_accepts_work(lane, now=now)]
            if not live:
                return
            progressed = True
            while pending_ranges and progressed:
                progressed = False
                for lane in live:
                    if not pending_ranges:
                        return
                    if len(lane.futures) >= lane.optimizer.params.workers:
                        continue
                    budget = lane.optimizer.params.result_budget
                    # Only one request at a time per lane may try a span beyond proven
                    # ground, so a refusal costs one request instead of every request
                    # currently in flight on that key.
                    probing = (
                        lane.probe_in_flight is None and lane.planner.would_probe(budget)
                    )
                    span = lane.planner.next_span(budget, probe=probing)
                    chunk = _take_chunk(pending_ranges, span)
                    if not _submit_one(lane, chunk):
                        _return_chunk(pending_ranges, chunk)
                        return
                    if probing:
                        lane.probe_in_flight = chunk
                    progressed = True

        for lane in lanes:
            if lane.live:
                lane.pool = ThreadPoolExecutor(
                    max_workers=MAX_WORKERS,
                    thread_name_prefix=f"rpc{lane.index}",
                )
                lane.optimizer.start_epoch()
                status_ui.scrape(
                    lane.spinner_key,
                    _lane_footer_text(lane, run_seconds=0.0),
                )
            else:
                status_ui.scrape(
                    lane.spinner_key,
                    _lane_footer_text(lane, run_seconds=0.0),
                    failed=True,
                )

        _top_up_in_flight()
        _refresh_bars()
        if not _in_flight() and pending_ranges:
            if _all_rpc_lanes_fatal(lanes):
                fatal_reason = _all_lanes_fatal_message(lanes)
            else:
                fatal_reason = (
                    "No work could be submitted at startup (call-count limit hit "
                    "before scraping began?)."
                )
            _print_message(f"FATAL: {fatal_reason}")
            return

        # --- Main loop ----------------------------------------------
        while not _stop_event.is_set() and sink.fatal_error is None:
            all_futs = [fut for lane in lanes for fut in lane.futures]
            if all_futs:
                done, _ = concurrent.futures.wait(
                    all_futs,
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

            process_fatal = False
            for fut in done:
                lane = next(ln for ln in lanes if fut in ln.futures)
                chunk = lane.futures.pop(fut)
                from_b, to_b = chunk
                chunk_span = to_b - from_b + 1
                if lane.probe_in_flight == chunk:
                    lane.probe_in_flight = None
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
                    process_fatal = True
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
                            day_cuts=result.get("day_cuts") or (),
                            safe_head=store.safe_head,
                            stop_event=_stop_event,
                        )
                    except OperationCancelled:
                        # Treat as cancelled-chunk: bail without advancing accounting.
                        continue
                    except DuplicateRowError as e:
                        fatal_reason = f"duplicate event in [{from_b:,}-{to_b:,}]: {e}"
                        _print_message(f"FATAL: {fatal_reason}")
                        _stop_event.set()
                        process_fatal = True
                        break
                    except (ValueError, V4Error) as e:
                        # If we are already shutting down, the DuckDB transaction was almost
                        # certainly aborted by the connection interrupt that the SIGINT handler
                        # triggers; that is expected and not fatal. The chunk just stays unrecorded
                        # in ``loaded_block_ranges`` and will be picked up again via ``find_gaps``
                        # on the next run.
                        if _stop_event.is_set():
                            continue
                        # The chunk crossed into a day whose previous day is not
                        # open yet. Roll back already happened. Retry after the
                        # earlier chunk lands.
                        if "is not open" in str(e) and (_in_flight() or pending_ranges):
                            _return_chunk(pending_ranges, (from_b, to_b))
                            continue
                        fatal_reason = (
                            f"persist failed for [{from_b:,}-{to_b:,}]: "
                            f"{type(e).__name__}: {e}"
                        )
                        _print_message(f"FATAL: {fatal_reason}")
                        _stop_event.set()
                        process_fatal = True
                        break

                    events_inserted += persist_result.rows_inserted
                    blocks_done += chunk_span
                    lane.blocks_done += chunk_span
                    lane.optimizer.record_blocks(chunk_span)
                    lane.planner.record_success(blocks=chunk_span, logs=raw_log_count)
                    if lane.optimizer.record_ok(
                        blocks=chunk_span, logs=raw_log_count, elapsed_sec=elapsed_s
                    ):
                        _print_message(
                            f"Scraping {lane.label} updating speed"
                            f"{_tuning_suffix(lane.optimizer, lane.planner)}"
                        )
                        _log_event(
                            "ramp",
                            lane.optimizer.params.describe(),
                            endpoint=lane.label,
                        )
                    _refresh_bars()

                    line = Text("  -> blks: ")
                    line.append(f"{chunk_span:,}", style=_COLOR_DONE)
                    line.append(f"  evts: {raw_log_count:,}  {elapsed_s:.1f}s")
                    _print_message(line)

                    for p in persist_result.ready_partitions:
                        sink.submit(p)

                elif status == "split":
                    elapsed_s = float(result["elapsed_sec"])
                    wid = int(result["wid"])
                    charged = lane.planner.record_rejection(blocks=chunk_span)
                    lane.optimizer.record_rejection()
                    _print_message(
                        f"  REFUSED  {lane.label}  w:{wid}  blks:{chunk_span:,}  "
                        f"[{from_b:,}-{to_b:,}]  too many {charged}; {lane.planner.describe()}"
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
                        f"  TMOUT  {lane.label}  w:{wid}  blks:{chunk_span:,}  "
                        f"[{from_b:,}-{to_b:,}]  {elapsed_s:.1f}s"
                    )
                    _return_chunk(pending_ranges, chunk)
                    lane.optimizer.record_timeout()
                    _park_lane_after_failure(lane)
                    _print_message(
                        f"Scraping {lane.label} backing off"
                        f"{_tuning_suffix(lane.optimizer, lane.planner)}"
                    )
                    _log_event(
                        "backoff",
                        lane.optimizer.params.describe(),
                        endpoint=lane.label,
                    )

                elif status == "throttled":
                    http_status = result.get("http_status")
                    wid = int(result["wid"])
                    _print_message(
                        f"  THROTTLED  {lane.label}  w:{wid}  HTTP {http_status}  "
                        f"[{from_b:,}-{to_b:,}]"
                    )
                    # 500/502 is overload, not a discovered block-span limit.
                    # Recording it on the planner closed the span bracket at
                    # 11 and left the run fetching 10 blocks for hours.
                    lane.optimizer.record_throttled(http_status)
                    _return_chunk(pending_ranges, chunk)
                    _park_lane_after_failure(lane)
                    _print_message(
                        f"Scraping {lane.label} backing off"
                        f"{_tuning_suffix(lane.optimizer, lane.planner)}"
                    )
                    _log_event(
                        "backoff",
                        lane.optimizer.params.describe(),
                        endpoint=lane.label,
                    )

                elif status == "fatal":
                    err = str(result.get("error_message", ""))
                    if _fetch_fatal_is_process_wide(result):
                        fatal_reason = (
                            f"fatal error on [{from_b:,}-{to_b:,}]: {err}"
                        )
                        _print_message(f"FATAL: {fatal_reason}")
                        _stop_event.set()
                        process_fatal = True
                        break
                    _return_chunk(pending_ranges, chunk)
                    if _fail_lane(lane, err):
                        fatal_reason = _all_lanes_fatal_message(lanes)
                        _print_message(f"FATAL: {fatal_reason}")
                        _stop_event.set()
                        process_fatal = True
                        break

                elif status == "error":
                    err_msg = str(result.get("error_message", ""))
                    wid = int(result["wid"])
                    elapsed_s = float(result["elapsed_sec"])
                    _print_message(
                        f"  ERROR  {lane.label}  w:{wid}  blks:{chunk_span:,}  "
                        f"[{from_b:,}-{to_b:,}]  {elapsed_s:.1f}s  err: {err_msg}"
                    )
                    chunk_failures[chunk] = chunk_failures.get(chunk, 0) + 1
                    if chunk_failures[chunk] < MAX_CHUNK_RETRIES:
                        _return_chunk(pending_ranges, chunk)
                        _park_lane_after_failure(lane)
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
                    process_fatal = True
                    break

            if process_fatal:
                break

            # --- Epoch boundary -------------------------------------
            # Stop submitting at the deadline, let in-flight work drain, then score the epoch: the
            # drain belongs inside the measurement it was caused by. Each key has its own clock.
            for lane in lanes:
                if not lane.live:
                    continue
                if not lane.draining_epoch and lane.optimizer.should_end_epoch():
                    lane.draining_epoch = True
                if lane.draining_epoch and not lane.futures:
                    report = lane.optimizer.end_epoch()
                    _print_message(f"  epoch  {lane.label}: {report.describe()}")
                    _print_message(
                        f"  next   {lane.label}: {report.next_params.describe()}  "
                        f"{lane.planner.describe()}"
                    )
                    lane.draining_epoch = False
                    lane.optimizer.start_epoch()

            # Top up in-flight work and run sink bookkeeping again.
            _top_up_in_flight()
            sink.reap_completed()
            sink.commit_ready()
            if sink.fatal_error is not None:
                fatal_reason = f"sink worker failed: {sink.fatal_error}"
                _stop_event.set()
                break

            if pending_ranges and not _in_flight() and not _any_draining():
                # A parked lane is waiting so another endpoint can take the chunk
                # it returned. Stopping here would exit while that work is still queued.
                now = time.monotonic()
                cooling = _lanes_cooling_down(lanes, now)
                call_budget_spent = (
                    args.max_calls is not None and calls_made >= args.max_calls
                )
                if cooling and not call_budget_spent:
                    wait = min(
                        POLL_TIMEOUT_SEC,
                        max(0.0, min(lane.cooldown_until for lane in cooling) - now),
                    )
                    time.sleep(wait)
                    continue
                if args.max_calls is not None:
                    _print_message(
                        f"  Reached --max-calls limit ({args.max_calls}). Stopping."
                    )
                break

            # If the queue is empty and nothing is in flight, check the chain head and either
            # rebuild the queue or stop.
            if not pending_ranges and not _in_flight() and not _any_draining():
                status_ui.spinner("head", "Rechecking chain head")
                try:
                    new_head = _query_chain_head(label="Rechecking chain head")
                except OperationCancelled:
                    _stop_event.set()
                    break
                except (RPCError, RuntimeError) as e:
                    if _all_rpc_lanes_fatal(lanes):
                        fatal_reason = _all_lanes_fatal_message(lanes)
                    else:
                        fatal_reason = (
                            f"failed to recheck chain head "
                            f"({CHAIN_HEAD_ATTEMPTS}/{CHAIN_HEAD_ATTEMPTS}): {e}"
                        )
                    _print_message(f"FATAL: {fatal_reason}")
                    _stop_event.set()
                    break
                finally:
                    status_ui.clear("head")

                loaded_max = store.max_loaded_block()
                if loaded_max is not None and loaded_max > new_head:
                    fatal_reason = (
                        f"chain head {new_head:,} is behind loaded block {loaded_max:,}"
                    )
                    _print_message(f"FATAL: {fatal_reason}")
                    _stop_event.set()
                    break

                safe_head = new_head - REORG_BUFFER_BLOCKS
                store.set_safe_head(safe_head)
                for day_key in store.list_days_ready_to_sink():
                    sink.submit(day_key)

                # The reorg buffer has not cleared the scrape start. Wait.
                # Do not treat an empty gap as caught up.
                if safe_head < SCRAPE_START_BLOCK:
                    if new_head != chain_head:
                        _print_message(
                            f"  Chain head {new_head:,} is inside the "
                            f"{REORG_BUFFER_BLOCKS}-block buffer. Waiting."
                        )
                    chain_head = new_head
                    time.sleep(2.0)
                    continue

                # Recompute only unsunk gaps up to the safe head. Sunk history
                # is immutable and must never be re-scheduled.
                new_gaps = store.find_gaps(SCRAPE_START_BLOCK, safe_head, include_sunk=False)
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
                if not _in_flight():
                    if _all_rpc_lanes_fatal(lanes):
                        fatal_reason = _all_lanes_fatal_message(lanes)
                    elif args.max_calls is not None and calls_made >= args.max_calls:
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

        # 1. Cancel anything we can in each RPC pool.
        for lane in lanes:
            for fut in list(lane.futures):
                fut.cancel()
            if lane.pool is not None:
                lane.pool.shutdown(wait=False, cancel_futures=True)

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
                for lane in list(lanes):
                    try:
                        final_chain_head = lane.client.get_block_number()
                        break
                    except Exception:
                        continue

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

        # 6. Close each RPC client (per-thread connections — main thread only).
        for lane in lanes:
            try:
                lane.client.close()
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
