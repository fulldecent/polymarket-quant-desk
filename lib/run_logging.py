"""Operator-facing console and log contract for partition-producing derived jobs.

Screen is the cockpit: bold output path, log path, blank line, a two-row sticky
footer, one line per sunk partition, and a compact run-complete block. No
log-level chrome.

The log file is the full record (UTC). Every screen line is also in the file.
Input, scratch, hot, and the work plan are log-only.

The raw scraper uses a separate status-line renderer and does not use this module.
"""

from __future__ import annotations

import logging
import threading
import time
from collections.abc import Callable
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

from .partition_utils import mask_partition

COLOR_DONE = "green"
COLOR_TODO = "magenta"

_DUCKDB_PROGRESS_POLL_SEC = 0.2


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


class _PercentColumn(ProgressColumn):
    """DuckDB-style percent for the current-partition row; blank when indeterminate."""

    def render(self, task: Task) -> Text:
        if not task.total:
            return Text("")
        pct = 100.0 * float(task.completed) / float(task.total)
        return Text(f"{pct:,.0f}%", style=COLOR_DONE)


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
        auto_refresh=False,
    )


def format_duration(seconds: float) -> str:
    """Compact duration for the end-of-run summary: ``0.4s``, ``36s``, ``1m 12s``, ``2h 3m``."""
    if seconds < 0 or seconds != seconds:  # NaN-safe
        return "0s"
    if seconds < 10:
        return f"{seconds:.1f}s"
    total = int(round(seconds))
    hours, rem = divmod(total, 3600)
    minutes, secs = divmod(rem, 60)
    if hours:
        return f"{hours}h {minutes}m" if minutes else f"{hours}h"
    if minutes:
        return f"{minutes}m {secs}s" if secs else f"{minutes}m"
    return f"{secs}s"


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


def configure_duckdb_progress(con: Any) -> None:
    """Track query progress without DuckDB printing its own bar to stderr."""
    con.execute("SET enable_progress_bar = true")
    con.execute("SET enable_progress_bar_print = false")
    con.execute("SET progress_bar_time = 0")


def execute_with_progress(
    con: Any,
    sql: str,
    *,
    on_progress: Callable[[float], None],
    params: Any = None,
    poll_sec: float = _DUCKDB_PROGRESS_POLL_SEC,
) -> Any:
    """Run ``con.execute`` while polling ``con.query_progress`` from a side thread.

    ``on_progress`` receives a 0–100 percentage. Values below 0 (idle / unknown)
    are not forwarded. If the connection has no ``query_progress``, the query
    just runs.
    """
    if not hasattr(con, "query_progress"):
        return con.execute(sql) if params is None else con.execute(sql, params)

    stop = threading.Event()

    def _poll() -> None:
        while not stop.wait(poll_sec):
            try:
                pct = float(con.query_progress())
            except Exception:
                continue
            if pct >= 0:
                on_progress(pct)

    thread = threading.Thread(target=_poll, name="duckdb-progress", daemon=True)
    thread.start()
    try:
        if params is None:
            return con.execute(sql)
        return con.execute(sql, params)
    finally:
        stop.set()
        thread.join(timeout=1.0)


class RunStatus:
    """Sticky two-row footer: current partition (or startup phase) + Total progress.

    The partition row's elapsed time restarts when its description changes (new
    partition or new phase). DuckDB percentages update the same description so
    the clock keeps running. Completed work is removed from the footer; the
    scrolling heartbeat is the paper trail.
    """

    def __init__(self, console: Console) -> None:
        self._console = console
        self._live = Live(console=console, refresh_per_second=8, transient=True)
        self._lock = threading.Lock()
        self._started = False
        self._partition_progress: Progress | None = None
        self._partition_task: TaskID | None = None
        self._partition_description = ""
        self._total_progress: Progress | None = None
        self._total_task: TaskID | None = None

    def start(self) -> None:
        if not self._started:
            self._live.start()
            self._started = True

    def stop(self) -> None:
        if self._started:
            self._live.stop()
            self._started = False

    def __enter__(self) -> RunStatus:
        self.start()
        return self

    def __exit__(self, *exc: object) -> None:
        self.stop()

    def partition(
        self,
        description: str,
        completed: int | None = None,
        total: int | None = None,
    ) -> None:
        """Show or update the current-partition row.

        ``total is None`` is indeterminate (phase with no DuckDB percent).
        Changing ``description`` resets elapsed time.
        """
        with self._lock:
            if self._partition_progress is None:
                self._partition_progress = self._make_partition_bar()
                self._partition_task = self._partition_progress.add_task(
                    description,
                    total=total,
                    completed=completed or 0,
                )
                self._partition_description = description
            elif self._partition_description != description:
                self._partition_progress.reset(
                    self._partition_task,
                    description=description,
                    completed=completed or 0,
                    total=total,
                )
                self._partition_description = description
            else:
                kwargs: dict[str, Any] = {"description": description}
                if total is not None:
                    kwargs["total"] = max(int(total), 1)
                    kwargs["completed"] = completed if completed is not None else 0
                elif completed is not None:
                    kwargs["completed"] = completed
                self._partition_progress.update(self._partition_task, **kwargs)
            self._refresh()

    def clear_partition(self) -> None:
        with self._lock:
            self._partition_progress = None
            self._partition_task = None
            self._partition_description = ""
            self._refresh()

    def total(self, completed: int, total: int) -> None:
        with self._lock:
            if self._total_progress is None:
                self._total_progress = make_total_progress(self._console)
                self._total_task = self._total_progress.add_task(
                    "Total progress",
                    total=max(total, 1),
                    completed=completed,
                )
            else:
                self._total_progress.update(
                    self._total_task,
                    completed=completed,
                    total=max(total, 1),
                )
            self._refresh()

    def _make_partition_bar(self) -> Progress:
        return Progress(
            SpinnerColumn(),
            TextColumn("{task.description}"),
            BarColumn(complete_style=COLOR_DONE, finished_style=COLOR_DONE, style=COLOR_TODO),
            _PercentColumn(),
            TimeElapsedColumn(),
            console=self._console,
            auto_refresh=False,
        )

    def _refresh(self) -> None:
        rows: list[Progress] = []
        if self._partition_progress is not None:
            rows.append(self._partition_progress)
        if self._total_progress is not None:
            rows.append(self._total_progress)
        self._live.update(Group(*rows) if rows else Group())


class PhaseWork:
    """Drive the current-partition footer row through named phases of one unit of work."""

    def __init__(self, status: RunStatus, prefix: str) -> None:
        self._status = status
        self._prefix = prefix
        self._desc = prefix

    def phase(self, name: str = "") -> None:
        """Set the row text to ``prefix`` or ``prefix  name``. Resets elapsed on change."""
        self._desc = f"{self._prefix}  {name}" if name else self._prefix
        self._status.partition(self._desc)

    def execute(self, con: Any, sql: str, params: Any = None) -> Any:
        """Run SQL, painting DuckDB's 0–100% onto the current phase when available.

        Stays indeterminate until DuckDB reports a non-negative percent, so a
        query with no progress signal still shows a moving spinner.
        """
        desc = self._desc

        def on_pct(pct: float) -> None:
            completed = int(min(max(pct, 0.0), 100.0))
            self._status.partition(desc, completed=completed, total=100)

        return execute_with_progress(con, sql, on_progress=on_pct, params=params)


def run_sql(con: Any, sql: str, params: Any = None, *, work: PhaseWork | None = None) -> Any:
    """``con.execute``, or ``work.execute`` when a phase tracker is attached."""
    if work is not None:
        return work.execute(con, sql, params)
    if params is None:
        return con.execute(sql)
    return con.execute(sql, params)


class RunOutput:
    """One write path for screen+log lines, plus the file-only logger and footer."""

    def __init__(self, logger_name: str, script_file: str) -> None:
        self.console = make_console()
        self.log, self.log_path = _open_file_logger(logger_name, script_file)
        self.status = RunStatus(self.console)

    def print(self, text: str | Text) -> None:
        """Print one line above the progress footer and record it in the run log."""
        if isinstance(text, Text):
            self.console.print(text, highlight=False, markup=False)
            self.log.info(text.plain)
        else:
            self.console.print(text, highlight=False, markup=False)
            self.log.info(text)

    def log_only(self, message: str, *, level: int = logging.INFO) -> None:
        """Write to the run log without touching the screen."""
        self.log.log(level, message)


def print_partition_sunk(
    out: RunOutput,
    partition: int,
    rows: int,
    elapsed: float,
) -> None:
    """One scrolling line after a 10K partition is published."""
    line = Text("  -> partition ")
    line.append(mask_partition(partition), style=COLOR_DONE)
    line.append(f"  rows {rows:,}  {elapsed:.1f}s")
    out.print(line)


def print_paths(
    out: RunOutput,
    *,
    output: str,
    extra: list[tuple[str, str]] | None = None,
) -> None:
    """Startup banner: bold output path, log path, blank line.

    Input, scratch, and hot paths belong in ``extra`` and are written to the run
    log only.
    """
    out.print(Text(f"output: {output}", style="bold"))
    out.print(f"log:    {out.log_path}")
    out.print("")
    if extra:
        for label, value in extra:
            out.log_only(f"{label}: {value}")


def print_work_plan(
    out: RunOutput,
    *,
    frontier: int,
    total: int,
    already_landed: int,
    todo: int,
    sample: int = 0,
) -> None:
    """Record the work plan in the run log. Not shown on screen."""
    todo_text = f"{todo:,} to process"
    if sample:
        todo_text += f" (sample {sample:,})"
    out.log_only(
        f"frontier={frontier:,}  |  total={total:,}  |  "
        f"{already_landed:,} already landed  |  {todo_text}"
    )


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
    """Compact end-of-run block."""
    if status == "interrupted":
        heading = "run interrupted"
    else:
        heading = "run complete"

    if self_frontier <= none_below:
        frontier_note = "none"
    elif (
        status != "interrupted"
        and upstream_frontier > none_below
        and self_frontier >= upstream_frontier
    ):
        frontier_note = f"{self_frontier:,} (matches all input datasets)"
    elif upstream_frontier > none_below:
        frontier_note = f"{self_frontier:,} (upstream {upstream_frontier:,})"
    else:
        frontier_note = f"{self_frontier:,}"

    out.print("")
    out.print(heading)
    out.print(f"   time: {format_duration(elapsed)}")
    out.print(f"   partitions: {partitions_done:,}")
    out.print(f"   rows: {rows_done:,}")
    out.print(f"   new frontier: {frontier_note}")
    out.print("")
