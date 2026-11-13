"""A sunk day must hand the sink the day that was already waiting behind it."""

import sys
from pathlib import Path
from types import SimpleNamespace

sys.path.insert(0, str(Path(__file__).resolve().parents[2]))
sys.path.insert(0, str(Path(__file__).resolve().parents[4]))

import main as scraper


class _Store:
    def __init__(self) -> None:
        self.config = SimpleNamespace(
            duckdb_memory_limit="1GB",
            duckdb_threads=1,
            duckdb_temp_dir="/tmp",
        )
        self.committed: list[int] = []
        self._span = (20220930, 100, 200)

    def current_day_span(self):
        return self._span

    def commit_sink(self, day_key: int) -> None:
        self.committed.append(day_key)
        self._span = (20221001, 201, 300)

    def list_days_ready_to_sink(self) -> list[int]:
        if self.committed:
            return [20221001]
        return []


def test_committing_a_day_submits_the_closed_day_waiting_behind_it(monkeypatch):
    submitted: list[int] = []
    monkeypatch.setattr(scraper, "publish_manifest", lambda _root, _day: True)
    store = _Store()
    orch = scraper._SinkOrchestrator(store, ":memory:", "/tmp/cold", 1)
    try:
        orch._completed[20220930] = scraper.PartitionWriteResult(
            day_key=20220930,
            files_written=1,
            rows_total=4,
            bytes_total=32,
            elapsed_ms=10,
        )
        orch.submit = submitted.append  # type: ignore[method-assign]
        committed = orch.commit_ready()
    finally:
        orch.shutdown(wait=False)

    assert committed == 1
    assert store.committed == [20220930]
    assert submitted == [20221001]
