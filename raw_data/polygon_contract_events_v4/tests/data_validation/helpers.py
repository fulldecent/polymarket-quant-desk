"""
Helpers for polygon_contract_events_v4 data validation tests.

Provides functions for reading v4 partitioned Parquet data, where the layout is:

    {RAW}/{contract}/{event}/year=YYYY/month=MM/day=DD/data.parquet

The integers these helpers call "ranges" are UTC day keys (YYYYMMDD).
Event-name lookups union across contracts automatically.
"""
import json
import os
import sys
from collections.abc import Callable, Iterator, Sequence
from pathlib import Path

from dotenv import load_dotenv
from lib.ymd_partition import day_dir, day_key_from_parts

_project_root = Path(__file__).resolve().parent.parent.parent.parent.parent
load_dotenv(_project_root / ".env")

RAW = os.environ.get("POLYGON_CONTRACT_EVENTS_V4_DIR", "")
if not RAW:
    sys.exit("POLYGON_CONTRACT_EVENTS_V4_DIR not set in .env")

SCRATCH_DIR = os.environ.get("SCRATCH_DIR", "")
if not SCRATCH_DIR:
    sys.exit("SCRATCH_DIR not set in .env")

# SQL expression for the 32-byte zero blob representing USDC (collateral).
# Asset IDs are BLOB(32) on disk; USDC is all zeros.
ZERO_ASSET_ID_SQL = "unhex(repeat('00', 32))"

_EVENT_LOCATIONS: dict[str, list[str]] | None = None
_PROGRESS_CB: Callable[[str], None] | None = None


def set_progress_callback(callback: Callable[[str], None] | None) -> None:
    """Install a callback for progress messages emitted by helper scans."""
    global _PROGRESS_CB
    _PROGRESS_CB = callback


def _emit_progress(message: str) -> None:
    if _PROGRESS_CB is not None:
        _PROGRESS_CB(message)


def _progress_step(total: int) -> int:
    return max(1, total // 50)


class _ProgressSequence(Sequence[tuple[int, list[str]]]):
    """Sequence wrapper that emits coarse progress while being iterated."""

    def __init__(self, items: list[tuple[int, list[str]]], *, label: str) -> None:
        self._items = items
        self._label = label

    def __len__(self) -> int:
        return len(self._items)

    def __getitem__(self, index: int) -> tuple[int, list[str]]:
        return self._items[index]

    def __iter__(self) -> Iterator[tuple[int, list[str]]]:
        total = len(self._items)
        if total == 0:
            return iter(())

        step = _progress_step(total)

        def _iterator() -> Iterator[tuple[int, list[str]]]:
            for idx, item in enumerate(self._items, start=1):
                if idx == 1 or idx == total or idx % step == 0:
                    percent = (idx * 100) // total
                    _emit_progress(f"{self._label}: {idx}/{total} ({percent}%)")
                yield item

        return _iterator()


def _event_locations() -> dict[str, list[str]]:
    """Return map from logical event name to contract/event paths on disk."""
    global _EVENT_LOCATIONS
    if _EVENT_LOCATIONS is not None:
        return _EVENT_LOCATIONS

    contract_entries = [e for e in os.scandir(RAW) if e.is_dir()]
    locations: dict[str, list[str]] = {}
    total_contracts = len(contract_entries)
    step = _progress_step(total_contracts) if total_contracts else 1

    for idx, contract_entry in enumerate(contract_entries, start=1):
        if idx == 1 or idx == total_contracts or idx % step == 0:
            percent = (idx * 100) // total_contracts if total_contracts else 100
            _emit_progress(
                f"indexing contract/event directories: {idx}/{total_contracts} ({percent}%)"
            )
        contract = contract_entry.name
        for event_entry in os.scandir(contract_entry.path):
            if not event_entry.is_dir():
                continue
            event = event_entry.name
            locations.setdefault(event, []).append(f"{contract}/{event}")

    _EVENT_LOCATIONS = locations
    return _EVENT_LOCATIONS


# Backward-compatible export used by existing tests.
EVENT_LOCATIONS = _event_locations()


def glob_all(event: str) -> str:
    """Read all available data for a logical event name, unioned across contracts."""
    paths = _event_locations().get(event, [])
    if not paths:
        raise ValueError(f"no data found for event {event!r}")
    if len(paths) == 1:
        return f"read_parquet('{RAW}/{paths[0]}/**/*.parquet')"
    path_list = ", ".join(f"'{RAW}/{p}/**/*.parquet'" for p in paths)
    return f"read_parquet([{path_list}])"


def _day_keys_for_path(event_path: str) -> set[int]:
    """UTC day keys that have ``data.parquet`` under one contract/event path."""
    full = os.path.join(RAW, event_path)
    if not os.path.isdir(full):
        return set()
    keys: set[int] = set()
    for year in os.scandir(full):
        if not year.is_dir() or not year.name.startswith("year="):
            continue
        y = int(year.name.split("=", 1)[1])
        for month in os.scandir(year.path):
            if not month.is_dir() or not month.name.startswith("month="):
                continue
            m = int(month.name.split("=", 1)[1])
            for day in os.scandir(month.path):
                if (
                    not day.is_dir()
                    or not day.name.startswith("day=")
                    or day.name.endswith(".tmp")
                ):
                    continue
                pq = os.path.join(day.path, "data.parquet")
                if not os.path.isfile(pq):
                    continue
                d = int(day.name.split("=", 1)[1])
                keys.add(day_key_from_parts(y, m, d))
    return keys


def complete_1m_ranges(event: str = "order_filled") -> list[int]:
    """Sorted UTC day keys present for every contract that has this event."""
    paths = _event_locations().get(event, [])
    if not paths:
        return []
    _emit_progress(f"computing complete day keys for {event} across {len(paths)} path(s)")
    sets = [_day_keys_for_path(p) for p in paths]
    return sorted(sets[0].intersection(*sets[1:]))


def _parquet_sources(paths: list[str], day_keys: list[int]) -> list[str]:
    sources: list[str] = []
    for path in paths:
        for day_key in day_keys:
            parquet = os.path.join(RAW, path, day_dir(day_key), "data.parquet")
            if os.path.isfile(parquet):
                sources.append(f"'{parquet}'")
    return sources


def _read_parquet_sql(sources: list[str]) -> str:
    if len(sources) == 1:
        return f"read_parquet({sources[0]})"
    return f"read_parquet([{', '.join(sources)}])"


def glob_complete(event: str, ranges: list[int]) -> str:
    """Read the given UTC days for a logical event, unioned across contracts."""
    paths = _event_locations().get(event, [])
    if not paths:
        raise ValueError(f"no data found for event {event!r}")
    sources = _parquet_sources(paths, ranges)
    if not sources:
        raise ValueError(f"no complete partitions found for {event!r}")
    return _read_parquet_sql(sources)


def complete_1m_ranges_for(event: str) -> set[int]:
    """UTC day keys present for every contract that has this event."""
    paths = _event_locations().get(event, [])
    if not paths:
        return set()
    sets = [_day_keys_for_path(p) for p in paths]
    return sets[0].intersection(*sets[1:])


V1_EXCHANGE_CONTRACTS = frozenset({"CTFExchange", "NegRiskCtfExchange"})


def glob_complete_contract_prefix(prefix: str, ranges: list[int]) -> str:
    """Read the given UTC days for events whose path starts with ``prefix``.

    Example: prefix="ConditionalTokens" matches ConditionalTokens/position_split,
    ConditionalTokens/positions_merge, etc., but not NegRiskAdapter/*.
    """
    all_locs = _event_locations()
    filtered_paths = [
        p for p in sum(all_locs.values(), []) if p == prefix or p.startswith(prefix + "/")
    ]
    if not filtered_paths:
        raise ValueError(f"no data found for contract prefix {prefix!r}")
    sources = _parquet_sources(filtered_paths, ranges)
    if not sources:
        raise ValueError(f"no complete partitions found for contract prefix {prefix!r}")
    return _read_parquet_sql(sources)


def glob_complete_v1(event: str, ranges: list[int]) -> str:
    """Like glob_complete but restricted to V1 exchange contracts only.

    Use for queries that reference V1-only schema columns like
    maker_asset_id and taker_asset_id, to avoid schema mismatch with
    V2 directories that share the same UTC days.
    """
    paths = [
        p for p in _event_locations().get(event, [])
        if p.split("/")[0] in V1_EXCHANGE_CONTRACTS
    ]
    if not paths:
        raise ValueError(f"no V1 data found for event {event!r}")
    sources = _parquet_sources(paths, ranges)
    if not sources:
        raise ValueError(f"no complete V1 partitions found for {event!r}")
    return _read_parquet_sql(sources)


def complete_1m_ranges_for_paths(paths: list[str]) -> set[int]:
    """UTC day keys present on every provided event path.

    Paths are relative to RAW, for example:
        - CTFExchange/order_filled
        - NegRiskCtfExchange/order_filled
    """
    if not paths:
        return set()
    sets = [_day_keys_for_path(p) for p in paths]
    return sets[0].intersection(*sets[1:])


def event_day_parquet(contract: str, event: str, day_key: int) -> Path:
    """Path of one event file for a UTC day. The file need not exist yet."""
    return Path(RAW) / contract / event / day_dir(day_key) / "data.parquet"


def parquet_covering_block(contract: str, event: str, block: int) -> str:
    """``data.parquet`` whose metadata span contains ``block``."""
    root = Path(RAW) / contract / event
    for meta in root.glob("year=*/month=*/day=*/metadata.json"):
        payload = json.loads(meta.read_text(encoding="utf-8"))
        params = payload.get("parameters") or {}
        if int(params["min_block"]) <= block <= int(params["max_block"]):
            parquet = meta.parent / "data.parquet"
            if parquet.is_file():
                return str(parquet)
    raise FileNotFoundError(f"no {contract}/{event} day covers block {block}")


def iter_partitions_with_files() -> "list[tuple[int, list[str]]]":
    """Return [(day_key, [parquet paths])], sorted by UTC day.

    Used by global-invariant assertions that need to scan every event row for
    one UTC day together. Each entry is one day across at most ~42 event tables.
    """
    by_day: dict[int, list[str]] = {}
    contract_entries = [e for e in os.scandir(RAW) if e.is_dir() and e.name != "manifests"]
    total_contracts = len(contract_entries)
    step = _progress_step(total_contracts) if total_contracts else 1

    for idx, contract in enumerate(contract_entries, start=1):
        if idx == 1 or idx == total_contracts or idx % step == 0:
            percent = (idx * 100) // total_contracts if total_contracts else 100
            _emit_progress(
                f"indexing parquet partitions: {idx}/{total_contracts} contracts ({percent}%)"
            )
        if not contract.is_dir():
            continue
        for event in os.scandir(contract.path):
            if not event.is_dir():
                continue
            rel = os.path.relpath(event.path, RAW)
            for day_key in _day_keys_for_path(rel):
                pq = os.path.join(event.path, day_dir(day_key), "data.parquet")
                by_day.setdefault(day_key, []).append(pq)
    items = sorted(by_day.items())
    _emit_progress(f"discovered {len(items)} parquet partition(s)")
    return _ProgressSequence(items, label="checking parquet partitions")
