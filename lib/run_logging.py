"""Console logging and progress UI for partition-producing derived jobs.

Derived producers all share the same operator-facing UX contract: a single
sticky progress bar with ETA, log lines that scroll above it, and one
timestamped log file per run under a ``logs/`` folder next to the script
(``main-{ISO8601-zulu}.log``). This module centralizes that setup so every
derived producer behaves identically and the progress bar stays pinned.

The raw scraper deliberately uses a different bespoke status-line renderer and
does not use this module.
"""

from __future__ import annotations

import logging
from datetime import datetime, timezone
from pathlib import Path

from rich.console import Console
from rich.logging import RichHandler
from rich.progress import (
    BarColumn,
    MofNCompleteColumn,
    Progress,
    SpinnerColumn,
    TaskProgressColumn,
    TextColumn,
    TimeElapsedColumn,
    TimeRemainingColumn,
)


def setup_logging(logger_name: str, script_file: str, console: Console) -> logging.Logger:
    """Configure a logger that writes a per-run file and console lines via rich.

    Creates ``logs/main-{ts}.log`` (ISO 8601 zulu, colon-free) next to
    ``script_file`` for full DEBUG output, and attaches a ``RichHandler`` bound to
    the supplied ``console`` for INFO-level console output. Binding to the same
    ``Console`` used for the progress bar is what keeps the bar pinned while log
    lines scroll above it.

    Args:
        logger_name: Name for the returned logger (e.g. the dataset name).
        script_file: ``__file__`` of the calling producer; the ``logs/`` folder is
            created next to it.
        console: The shared rich ``Console`` also passed to ``make_progress``.

    Returns:
        A configured ``logging.Logger``.
    """
    log_dir = Path(script_file).resolve().parent / "logs"
    log_dir.mkdir(parents=True, exist_ok=True)
    ts = datetime.now(timezone.utc).strftime("%Y-%m-%dT%H%M%SZ")
    log_path = log_dir / f"main-{ts}.log"

    file_handler = logging.FileHandler(log_path)
    file_handler.setLevel(logging.DEBUG)
    file_handler.setFormatter(
        logging.Formatter("%(asctime)s  %(levelname)-7s  %(message)s", datefmt="%Y-%m-%dT%H:%M:%S")
    )

    console_handler = RichHandler(
        console=console,
        show_path=False,
        rich_tracebacks=True,
        omit_repeated_times=False,
    )
    console_handler.setLevel(logging.INFO)
    console_handler.setFormatter(logging.Formatter("%(message)s", datefmt="%Y-%m-%dT%H:%M:%S"))

    logger = logging.getLogger(logger_name)
    logger.setLevel(logging.DEBUG)
    logger.addHandler(file_handler)
    logger.addHandler(console_handler)
    return logger


def make_progress(console: Console) -> Progress:
    """Return the standard sticky progress bar (spinner, bar, M/N, elapsed, ETA).

    Bind it to the same ``console`` passed to ``setup_logging`` so log lines and
    the bar cooperate.
    """
    return Progress(
        SpinnerColumn(),
        TextColumn("[progress.description]{task.description}"),
        BarColumn(),
        TaskProgressColumn(),
        MofNCompleteColumn(),
        TimeElapsedColumn(),
        TimeRemainingColumn(),
        console=console,
    )
