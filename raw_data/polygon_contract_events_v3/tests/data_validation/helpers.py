"""
Helpers for polygon_contract_events_v3 data validation tests.

Provides functions for reading v3 partitioned Parquet data, where the layout is:

    {RAW}/{contract}/{event}/1M={N}/10K={N}/data.parquet

Event-name lookups union across contracts automatically.
"""
import os
import sys
from collections.abc import Callable, Iterator, Sequence
from pathlib import Path

from dotenv import load_dotenv

_project_root = Path(__file__).resolve().parent.parent.parent.parent.parent
load_dotenv(_project_root / ".env")

RAW = os.environ.get("POLYGON_CONTRACT_EVENTS_V3_DIR", "")
if not RAW:
    sys.exit("POLYGON_CONTRACT_EVENTS_V3_DIR not set in .env")

TEMP_DIR = "/Volumes/polymarket-quant-desk/tmp"
_10K_DIRS_PER_1M = 100

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


def _complete_1m_ranges_for_path(event_path: str) -> set[int]:
    """Return set of 1M values with exactly 100 10K sub-dirs."""
    full = os.path.join(RAW, event_path)
    if not os.path.isdir(full):
        return set()
    complete = set()
    for entry in os.scandir(full):
        if not entry.is_dir() or not entry.name.startswith("1M="):
            continue
        n_10k = sum(1 for e in os.scandir(entry.path) if e.is_dir())
        if n_10k == _10K_DIRS_PER_1M:
            complete.add(int(entry.name.split("=")[1]))
    return complete


def complete_1m_ranges(event: str = "order_filled") -> list[int]:
    """Return sorted list of 1M values that are complete across all contracts
    that have this event.  'Complete' = each contract's 1M dir has 100 10K
    sub-dirs, intersected across contracts."""
    paths = _event_locations().get(event, [])
    if not paths:
        return []
    _emit_progress(f"computing complete 1M ranges for {event} across {len(paths)} path(s)")
    sets = [_complete_1m_ranges_for_path(p) for p in paths]
    return sorted(sets[0].intersection(*sets[1:]))


def glob_complete(event: str, ranges: list[int]) -> str:
    """Read only complete 1M partitions for a logical event name, unioned
    across contracts."""
    paths = _event_locations().get(event, [])
    if not paths:
        raise ValueError(f"no data found for event {event!r}")
    all_parquet = []
    for p in paths:
        for v in ranges:
            d = os.path.join(RAW, p, f"1M={v}")
            if os.path.isdir(d):
                all_parquet.append(f"'{RAW}/{p}/1M={v}/**/*.parquet'")
    if not all_parquet:
        raise ValueError(f"no complete partitions found for {event!r}")
    if len(all_parquet) == 1:
        return f"read_parquet({all_parquet[0]})"
    return f"read_parquet([{', '.join(all_parquet)}])"


def complete_1m_ranges_for(event: str) -> set[int]:
    """Return set of 1M values complete for a specific event (intersected
    across contracts)."""
    paths = _event_locations().get(event, [])
    if not paths:
        return set()
    sets = [_complete_1m_ranges_for_path(p) for p in paths]
    return sets[0].intersection(*sets[1:])


V1_EXCHANGE_CONTRACTS = frozenset({"CTFExchange", "NegRiskCtfExchange"})


def glob_complete_contract_prefix(prefix: str, ranges: list[int]) -> str:
    """Read only complete 1M partitions for events whose contract path starts with a prefix.

    Example: prefix="ConditionalTokens" matches ConditionalTokens/position_split,
    ConditionalTokens/positions_merge, etc., but not NegRiskAdapter/*.
    """
    # Build a synthetic event map filtered by prefix
    all_locs = _event_locations()
    # The values are lists of "contract/event" paths; filter those starting with the prefix
    filtered_paths = [p for p in sum(all_locs.values(), []) if p == prefix or p.startswith(prefix + "/")]
    if not filtered_paths:
        raise ValueError(f"no data found for contract prefix {prefix!r}")

    all_parquet = []
    for p in filtered_paths:
        for v in ranges:
            d = os.path.join(RAW, p, f"1M={v}")
            if os.path.isdir(d):
                all_parquet.append(f"'{RAW}/{p}/1M={v}/**/*.parquet'")
    if not all_parquet:
        raise ValueError(f"no complete partitions found for contract prefix {prefix!r}")
    if len(all_parquet) == 1:
        return f"read_parquet({all_parquet[0]})"
    return f"read_parquet([{', '.join(all_parquet)}])"


def glob_complete_v1(event: str, ranges: list[int]) -> str:
    """Like glob_complete but restricted to V1 exchange contracts only.

    Use for queries that reference V1-only schema columns like
    maker_asset_id and taker_asset_id, to avoid schema mismatch with
    V2 directories that share the same 1M partition ranges.
    """
    paths = [p for p in _event_locations().get(event, [])
             if p.split("/")[0] in V1_EXCHANGE_CONTRACTS]
    if not paths:
        raise ValueError(f"no V1 data found for event {event!r}")
    all_parquet = []
    for p in paths:
        for v in ranges:
            d = os.path.join(RAW, p, f"1M={v}")
            if os.path.isdir(d):
                all_parquet.append(f"'{RAW}/{p}/1M={v}/**/*.parquet'")
    if not all_parquet:
        raise ValueError(f"no complete V1 partitions found for {event!r}")
    if len(all_parquet) == 1:
        return f"read_parquet({all_parquet[0]})"
    return f"read_parquet([{', '.join(all_parquet)}])"


def complete_1m_ranges_for_paths(paths: list[str]) -> set[int]:
        """Return set of 1M values complete across the provided event paths.

        Paths are relative to RAW, for example:
            - CTFExchange/order_filled
            - NegRiskCtfExchange/order_filled
        """
        if not paths:
                return set()
        sets = [_complete_1m_ranges_for_path(p) for p in paths]
        return sets[0].intersection(*sets[1:])


def iter_partitions_with_files() -> "list[tuple[int, list[str]]]":
    """Return [(10K partition start, [parquet paths across every event table that
    cover that 10K range])], sorted by partition start.

    Used by global-invariant assertions that need to scan every event row for
    a given block range together. Each partition entry is bounded in size
    (one 10K block range across at most ~42 event tables), which keeps
    per-partition aggregations memory-cheap.
    """
    # partition_start -> list of absolute parquet paths
    by_partition: dict[int, list[str]] = {}
    contract_entries = [e for e in os.scandir(RAW) if e.is_dir()]
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
            for m_entry in os.scandir(event.path):
                if not m_entry.is_dir() or not m_entry.name.startswith("1M="):
                    continue
                for k_entry in os.scandir(m_entry.path):
                    if not k_entry.is_dir() or not k_entry.name.startswith("10K="):
                        continue
                    pq = os.path.join(k_entry.path, "data.parquet")
                    if not os.path.isfile(pq):
                        continue
                    block_start = int(k_entry.name.split("=", 1)[1])
                    by_partition.setdefault(block_start, []).append(pq)
    items = sorted(by_partition.items())
    _emit_progress(f"discovered {len(items)} parquet partition(s)")
    return _ProgressSequence(items, label="checking parquet partitions")
