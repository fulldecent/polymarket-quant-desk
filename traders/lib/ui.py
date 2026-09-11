"""Shared rich console + per-run log file for trader CLIs."""

from __future__ import annotations

import logging
import time
from datetime import datetime, timezone
from pathlib import Path

from rich.console import Console
from rich.live import Live
from rich.text import Text
from rich.theme import Theme

from lib.run_logging import format_duration

COLOR_DONE = "green"
COLOR_TODO = "magenta"


class _UtcFormatter(logging.Formatter):
    converter = time.gmtime


def format_hms(seconds: float) -> str:
    if seconds < 0 or seconds != seconds:
        return "0:00:00"
    total = int(seconds)
    h, rem = divmod(total, 3600)
    m, s = divmod(rem, 60)
    return f"{h}:{m:02d}:{s:02d}"


def shorten(value: str, *, head: int = 6, tail: int = 4) -> str:
    if not value:
        return "none"
    if len(value) <= head + tail + 1:
        return value
    return f"{value[:head]}…{value[-tail:]}"


def shorten_addr(addr: str) -> str:
    if not addr:
        return "none"
    if not addr.startswith("0x") or len(addr) < 10:
        return addr
    return f"{addr[:5]}…{addr[-3:]}"


class TraderUI:
    """Opening banner, scrolling lines, optional sticky wait footer, closing banner."""

    def __init__(self, program: str, script_file: str) -> None:
        self.program = program
        self.console = Console(
            theme=Theme(
                {"progress.elapsed": COLOR_DONE, "progress.remaining": COLOR_TODO}
            )
        )
        log_dir = Path(script_file).resolve().parent / "logs"
        log_dir.mkdir(parents=True, exist_ok=True)
        ts = datetime.now(timezone.utc).strftime("%Y-%m-%dT%H%M%SZ")
        self.log_path = log_dir / f"main-{ts}.log"
        handler = logging.FileHandler(self.log_path, encoding="utf-8")
        handler.setFormatter(
            _UtcFormatter("%(asctime)s  %(message)s", datefmt="%Y-%m-%dT%H:%M:%SZ")
        )
        self.log = logging.getLogger(f"trader.{program}")
        self.log.setLevel(logging.DEBUG)
        self.log.handlers.clear()
        self.log.addHandler(handler)
        self.log.propagate = False
        self._live: Live | None = None
        self._footer = Text("")

    def print(self, text: str | Text) -> None:
        self.console.print(text, highlight=False, markup=False)
        self.log.info(text.plain if isinstance(text, Text) else text)

    def log_only(self, message: str) -> None:
        self.log.info(message)

    def opening(self, *, account: str) -> None:
        """Match derived jobs: labeled paths, log line, blank line."""
        acc = account or "none"
        self.print(Text(f"account: {acc}", style="bold"))
        self.print(f"log:     {self.log_path}")
        self.print("")

    def heading(self, title: str, detail: str = "") -> None:
        self.print("")
        line = Text(title, style="bold")
        if detail:
            line.append(f"  {detail}")
        self.print(line)

    def start_footer(self, text: str) -> None:
        self.stop_footer()
        self._footer = Text(text)
        self._live = Live(
            self._footer,
            console=self.console,
            refresh_per_second=8,
            transient=True,
        )
        self._live.start()

    def update_footer(self, text: str) -> None:
        self._footer.plain = text
        if self._live is not None:
            self._live.update(self._footer)

    def stop_footer(self) -> None:
        if self._live is not None:
            self._live.stop()
            self._live = None

    def closing(self, status_line: str, elapsed: float) -> None:
        self.stop_footer()
        self.print("")
        ok = status_line.startswith("status=ok") or status_line == "ok"
        heading = "run complete" if ok else "run failed"
        self.print(Text(heading, style=COLOR_DONE if ok else COLOR_TODO))
        self.print(f"   time: {format_duration(elapsed)}")
        rest = status_line
        for prefix in ("status=ok", "status=failed"):
            if rest.startswith(prefix):
                rest = rest[len(prefix) :].strip("  ")
                break
        if rest and rest not in {"ok", "failed", "snapshot"}:
            self.print(f"   {rest}")
        self.print("")
