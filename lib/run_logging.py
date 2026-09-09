"""Operator-facing console and log contract for partition-producing derived jobs.

Screen is the cockpit: paths, one work-plan line, a sticky Total progress bar,
occasional heartbeats, and an honest end summary. No log-level chrome.

The log file is the full record (UTC). Every screen line is also in the file.
Per-partition chatter stays file-only.

The raw scraper uses a separate status-line renderer and does not use this module.
"""

from __future__ import annotations

import logging
import time
from datetime import datetime, timezone
from pathlib import Path

from rich.console import Console
from rich.progress import (
    BarColumn,
    Progress,
    ProgressColumn,
    SpinnerColumn,
    Task,
    TextColumn,
    TimeElapsedColumn,
    TimeRemainingColumn,
)
from rich.text import Text
from rich.theme import Theme

COLOR_DONE = "green"
COLOR_TODO = "magenta"

HEARTBEAT_EVERY_N = 50
HEARTBEAT_EVERY_SEC = 10.0

_LABEL_WIDTH = 22


class _UtcFormatter(logging.Formatter):
    """``logging`` formatter that stamps lines in UTC with a literal ``Z``."""

    converter = time.gmtime


class _DoneOfTotalColumn(ProgressColumn):
    """``done/total``, each number coloured like the bar segment it stands for."""

    def render(self, task: Task) -> Text:
        total = int(task.total) if task.total is not None else 0
        return Text.assemble(
            (f"{int(task.completed):,}", COLOR_DONE),
            "/",
            (f"{total:,}", COLOR_TODO),
        )


def make_console() -> Console:
    """Console themed so elapsed time is done-coloured and remaining is todo-coloured."""
    return Console(
        theme=Theme({"progress.elapsed": COLOR_DONE, "progress.remaining": COLOR_TODO})
    )


def make_total_progress(console: Console) -> Progress:
    """Sticky Total progress bar matching the raw scraper status line.

    Counts whatever unit the caller puts in ``total`` (derived jobs: 10K partitions).
    """
    return Progress(
        SpinnerColumn(),
        TextColumn("{task.description}"),
        BarColumn(complete_style=COLOR_DONE, finished_style=COLOR_DONE, style=COLOR_TODO),
        _DoneOfTotalColumn(),
        TimeElapsedColumn(),
        TimeRemainingColumn(),
        console=console,
    )


def format_duration(seconds: float) -> str:
    if seconds < 0 or seconds != seconds:  # NaN-safe
        return "--:--:--"
    h = int(seconds) // 3600
    m = (int(seconds) % 3600) // 60
    s = int(seconds) % 60
    return f"{h:02d}:{m:02d}:{s:02d}"


def format_frontier(value: int, *, none_below: int) -> str:
    """Render a block-number frontier, or ``none`` when it has not advanced."""
    if value <= none_below:
        return "none"
    return f"{value:,}"


def _open_file_logger(logger_name: str, script_file: str) -> tuple[logging.Logger, Path]:
    log_dir = Path(script_file).resolve().parent / "logs"
    log_dir.mkdir(parents=True, exist_ok=True)
    ts = datetime.now(timezone.utc).strftime("%Y-%m-%dT%H%M%SZ")
    log_path = log_dir / f"main-{ts}.log"

    file_handler = logging.FileHandler(log_path, encoding="utf-8")
    file_handler.setLevel(logging.DEBUG)
    file_handler.setFormatter(
        _UtcFormatter("%(asctime)s  %(levelname)-7s  %(message)s", datefmt="%Y-%m-%dT%H:%M:%SZ")
    )

    logger = logging.getLogger(logger_name)
    logger.setLevel(logging.DEBUG)
    logger.handlers.clear()
    logger.addHandler(file_handler)
    logger.propagate = False
    return logger, log_path


def setup_logging(logger_name: str, script_file: str, console: Console | None = None) -> logging.Logger:
    """Open the per-run UTC log file and return its logger.

    ``console`` is accepted for compatibility and ignored: derived producers that
    want the operator cockpit should use ``RunOutput`` instead.
    """
    logger, _path = _open_file_logger(logger_name, script_file)
    return logger


class RunOutput:
    """One write path for screen+log lines, plus the file-only logger."""

    def __init__(self, logger_name: str, script_file: str) -> None:
        self.console = make_console()
        self.log, self.log_path = _open_file_logger(logger_name, script_file)

    def print(self, text: str | Text) -> None:
        """Print one line above the progress bar and record it in the run log."""
        if isinstance(text, Text):
            self.console.print(text, highlight=False, markup=False)
            self.log.info(text.plain)
        else:
            self.console.print(text, highlight=False, markup=False)
            self.log.info(text)

    def log_only(self, message: str, *, level: int = logging.INFO) -> None:
        """Write to the run log without touching the screen."""
        self.log.log(level, message)

    def make_total_progress(self) -> Progress:
        return make_total_progress(self.console)


class PartitionHeartbeat:
    """Periodic ``-> partitions: N  rows: M  T.Ts`` line above the bar.

    Prints after ``HEARTBEAT_EVERY_N`` partitions, after ``HEARTBEAT_EVERY_SEC``
    seconds, and when the run's last partition completes. Counts in each line
    are since the previous heartbeat, matching the raw scraper's per-chunk
    ``-> blks:`` cadence rather than a cumulative total (the bar already has
    that).
    """

    def __init__(
        self,
        out: RunOutput,
        *,
        every_n: int = HEARTBEAT_EVERY_N,
        every_sec: float = HEARTBEAT_EVERY_SEC,
    ) -> None:
        self._out = out
        self._every_n = every_n
        self._every_sec = every_sec
        self._batch_n = 0
        self._batch_rows = 0
        self._batch_t0 = time.monotonic()

    def update(self, *, rows: int, done: int, total: int) -> None:
        self._batch_n += 1
        self._batch_rows += rows
        now = time.monotonic()
        due = (
            done == total
            or self._batch_n >= self._every_n
            or (now - self._batch_t0) >= self._every_sec
        )
        if due:
            self._emit(now)

    def flush(self) -> None:
        """Emit a partial batch (e.g. on interrupt). No-op when already printed."""
        if self._batch_n == 0:
            return
        self._emit(time.monotonic())

    def _emit(self, now: float) -> None:
        elapsed = now - self._batch_t0
        line = Text("  -> partitions: ")
        line.append(f"{self._batch_n:,}", style=COLOR_DONE)
        line.append(f"  rows: {self._batch_rows:,}  {elapsed:.1f}s")
        self._out.print(line)
        self._batch_n = 0
        self._batch_rows = 0
        self._batch_t0 = now


def print_paths(out: RunOutput, items: list[tuple[str, str]]) -> None:
    """Print labeled absolute paths (and the log path) at startup."""
    for label, value in items:
        out.print(f"{label + ':':<{_LABEL_WIDTH}}{value}")


def print_work_plan(
    out: RunOutput,
    *,
    frontier: int,
    total: int,
    already_landed: int,
    todo: int,
    sample: int = 0,
) -> None:
    """One work-plan line: total partitions, already landed, remaining this run."""
    line = Text()
    line.append(f"frontier={frontier:,}")
    line.append("  |  ")
    line.append(f"total={total:,}")
    line.append("  |  ")
    line.append(f"{already_landed:,} already landed", style=COLOR_DONE)
    line.append("  |  ")
    todo_text = f"{todo:,} to process"
    if sample:
        todo_text += f" (sample {sample:,})"
    line.append(todo_text, style=COLOR_TODO)
    out.print(line)


def print_run_summary(
    out: RunOutput,
    *,
    status: str,
    elapsed: float,
    partitions_done: int,
    rows_done: int,
    self_frontier: int,
    upstream_frontier: int,
    none_below: int,
) -> None:
    """Honest end-of-run block (OK / interrupted / nothing to do)."""
    status_style = {
        "OK": COLOR_DONE,
        "nothing to do": COLOR_DONE,
        "interrupted": COLOR_TODO,
    }.get(status)

    part_s = partitions_done / elapsed if elapsed > 1 and partitions_done else 0.0
    row_s = rows_done / elapsed if elapsed > 1 and rows_done else 0.0

    out.print("")
    out.print("=" * 70)
    out.print(f"run complete  ({format_duration(elapsed)})")
    out.print("=" * 70)
    out.print("")
    out.print("status")
    status_line = Text("  ")
    status_line.append(status, style=status_style)
    out.print(status_line)
    out.print("")
    out.print("progress")
    out.print(f"  partitions this run:  {partitions_done:,}")
    out.print(f"  rows this run:        {rows_done:,}")
    out.print(f"  self frontier:        {format_frontier(self_frontier, none_below=none_below)}")
    out.print(f"  upstream frontier:    {format_frontier(upstream_frontier, none_below=none_below)}")
    out.print("")
    out.print("throughput")
    out.print(f"  partitions/sec:  {part_s:,.2f}")
    out.print(f"  rows/sec:        {row_s:,.0f}")
    out.print("")
