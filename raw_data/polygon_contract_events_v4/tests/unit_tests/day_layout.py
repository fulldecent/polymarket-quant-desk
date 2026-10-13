"""Plant UTC-day cold folders for v4 unit tests."""

from __future__ import annotations

import json
from pathlib import Path

from lib.ymd_partition import day_dir, day_key_parts
from raw_data.polygon_contract_events_v4._internal.parquet_sink import (
    _expected_targets_for_day,
)
from raw_data.polygon_contract_events_v4._internal.tables import SCRAPE_START_BLOCK

D0 = 20201102
D1 = 20201103
D2 = 20201104


def bounds(index: int) -> tuple[int, int]:
    """A 1,000-block span starting at ``SCRAPE_START_BLOCK``."""
    first = SCRAPE_START_BLOCK + index * 1000
    return first, first + 999


def parameters(day_key: int, first: int, last: int) -> dict[str, int]:
    year, month, day = day_key_parts(day_key)
    return {
        "year": year,
        "month": month,
        "day": day,
        "min_block": first,
        "max_block": last,
    }


def plant_day(
    cold_root: Path,
    day_key: int,
    first: int,
    last: int,
    *,
    manifest: bool = False,
    withhold: int = 0,
    skip_first_metadata: bool = False,
    parquet_only: bool = False,
) -> list[Path]:
    """Write event folders for one day. Returns the folders created."""
    cold_root.mkdir(parents=True, exist_ok=True)
    targets = _expected_targets_for_day(last)
    if withhold:
        targets = targets[: len(targets) - withhold]
    rel = day_dir(day_key)
    folders: list[Path] = []
    payload = json.dumps({"parameters": parameters(day_key, first, last)})
    for index, (contract, event) in enumerate(targets):
        folder = cold_root / contract / event / rel
        folder.mkdir(parents=True, exist_ok=True)
        (folder / "data.parquet").write_bytes(b"PAR1")
        if not parquet_only and not (skip_first_metadata and index == 0):
            (folder / "metadata.json").write_text(payload, encoding="utf-8")
        folders.append(folder)
    if manifest:
        manifest_dir = cold_root / "manifests" / rel
        manifest_dir.mkdir(parents=True, exist_ok=True)
        (manifest_dir / "_SUCCESS").write_bytes(b"")
    return folders


def manifest_path(cold_root: Path, day_key: int) -> Path:
    return cold_root / "manifests" / day_dir(day_key) / "_SUCCESS"


def day_tmp_rel(day_key: int) -> str:
    rel = day_dir(day_key)
    leaf = Path(rel).name
    return str(Path(rel).with_name(leaf + ".tmp"))
