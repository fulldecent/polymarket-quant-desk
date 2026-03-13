"""
Helpers for polygon_contract_events_v2 assertions.

Provides functions for reading v2 partitioned Parquet data, where the layout is:

    {RAW}/{contract}/{event}/1M={N}/10K={N}/data.parquet

Event-name lookups union across contracts automatically.
"""
import os
import sys
from pathlib import Path

from dotenv import load_dotenv

_project_root = Path(__file__).resolve().parent.parent.parent
load_dotenv(_project_root / ".env")

RAW = os.environ.get("POLYGON_CONTRACT_EVENTS_V2_DIR", "")
if not RAW:
    sys.exit("POLYGON_CONTRACT_EVENTS_V2_DIR not set in .env")

TEMP_DIR = "/Volumes/Untitled/tmp"
_10K_DIRS_PER_1M = 100

# SQL expression for the 32-byte zero blob representing USDC (collateral).
# In v2 parquet, asset IDs are BLOB(32); USDC is all zeros.
ZERO_ASSET_ID_SQL = "unhex(repeat('00', 32))"

# Map from logical event name -> list of contract/event relative paths on disk.
EVENT_LOCATIONS: dict[str, list[str]] = {}

for _contract_entry in os.scandir(RAW):
    if not _contract_entry.is_dir():
        continue
    _contract = _contract_entry.name
    for _event_entry in os.scandir(_contract_entry.path):
        if not _event_entry.is_dir():
            continue
        _event = _event_entry.name
        EVENT_LOCATIONS.setdefault(_event, []).append(f"{_contract}/{_event}")


def glob_all(event: str) -> str:
    """Read all available data for a logical event name, unioned across contracts."""
    paths = EVENT_LOCATIONS.get(event, [])
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
    paths = EVENT_LOCATIONS.get(event, [])
    if not paths:
        return []
    sets = [_complete_1m_ranges_for_path(p) for p in paths]
    return sorted(sets[0].intersection(*sets[1:]))


def glob_complete(event: str, ranges: list[int]) -> str:
    """Read only complete 1M partitions for a logical event name, unioned
    across contracts."""
    paths = EVENT_LOCATIONS.get(event, [])
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
    paths = EVENT_LOCATIONS.get(event, [])
    if not paths:
        return set()
    sets = [_complete_1m_ranges_for_path(p) for p in paths]
    return sets[0].intersection(*sets[1:])
