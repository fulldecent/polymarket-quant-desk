"""Helpers for computing frontiers of derived datasets.

Derived producers must never read past the minimum frontier of all their
upstream sources. This module provides a canonical way to compute the
highest complete partition for a derived dataset by scanning its output
directory structure.
"""

from __future__ import annotations

from pathlib import Path
from typing import Callable

from .partition_utils import (
    PARTITION_10K_LABEL,
    PARTITION_10K_SIZE,
    PARTITION_1M_LABEL,
    PARTITION_1M_SIZE,
    partition_end,
    partition_start,
)


def _partition_folder(base: Path, partition: int) -> Path:
    """Return ``base/1M=<m>/10K=<k>`` for a 10K-aligned partition start."""
    m_val = (partition // PARTITION_1M_SIZE) * PARTITION_1M_SIZE
    return base / f"{PARTITION_1M_LABEL}={m_val}" / f"{PARTITION_10K_LABEL}={partition}"


def scan_frontier_1M_10K_folders(
    base_path: str | Path,
    starting_partition: int,
    tmp_suffix: str,
    cb_progress: Callable[[int], None],
) -> int | None:
    """Return highest contiguous landed partition from ``starting_partition``.

    Contract:
    * ``starting_partition`` is required and interpreted as a block number;
      scanning starts at its containing 10K partition.
    * Presence of a partition folder is sufficient (no file checks inside).
    * Temporary sibling folder ``10K=<k>{tmp_suffix}`` invalidates that
      partition for frontier purposes.
    * If the starting partition folder is missing, return ``None``.
    * If the starting partition folder exists but its tmp sibling exists,
      return ``None``.
    * On every accepted contiguous partition, invoke ``cb_progress(partition)``.

    Args:
        base_path: Dataset root containing 1M/10K folders.
        starting_partition: First partition to probe (or any block inside it).
        tmp_suffix: Temp suffix used by atomic publish (for example, ``.tmp``).
        cb_progress: Called once per accepted contiguous partition.

    Returns:
        Highest contiguous 10K partition start, or ``None`` if the scan
        cannot start.
    """
    root = Path(base_path)
    if not root.is_dir():
        return None

    current = partition_start(starting_partition)
    current_dir = _partition_folder(root, current)
    current_tmp_dir = current_dir.with_name(current_dir.name + tmp_suffix)

    if not current_dir.is_dir():
        return None
    if current_tmp_dir.exists():
        return None

    cb_progress(current)
    latest = current

    while True:
        nxt = latest + PARTITION_10K_SIZE
        nxt_dir = _partition_folder(root, nxt)
        nxt_tmp_dir = nxt_dir.with_name(nxt_dir.name + tmp_suffix)

        if nxt_tmp_dir.exists():
            break
        if not nxt_dir.is_dir():
            break

        cb_progress(nxt)
        latest = nxt

    return latest



