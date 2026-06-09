"""Reusable partition discovery and planning for derived data jobs.

This module provides generic partition enumeration and planning logic for
derived jobs that read from multiple source directories and produce partitioned
output in frontier order.

The planner discovers partitions by scanning base directories for valid
1M/10K partition folder structure, filters to the upstream frontier, and
determines which partitions still need processing based on the presence of
the output partition folder.

Atomic folder rename guarantee
------------------------------
Both source and output partition directories are published via atomic rename
of a temp folder containing data.parquet and metadata.json. Therefore, the
mere existence of a partition directory (e.g., ``1M={M}/10K={K}/``) is a
sufficient signal that the partition is complete and immutable. The planner
relies on this property and does not inspect the contents of the partition
folder for discovery or completion checks.

Raw data sources
----------------
When a source base points to raw ``polygon_contract_events_v3`` data, do not
pass ``contract/event`` table paths (e.g., ``CTFExchange/order_filled``).
Those parquet files are not guaranteed to be immutable or stable until the
corresponding manifest entry is published. Instead, for raw sources, pass the
``manifests`` directory (relative to the raw root). The ``manifests/1M=.../10K=.../_SUCCESS``
files form the stable, contiguous frontier per the v3 data dictionary contract
(see ``raw_data/polygon_contract_events_v3/DATA_DICTIONARY.md``, section
"File immutability and atomic visibility").

Derived sources
---------------
When a source base points to another derived table's output directory, pass
that derived table's root (e.g., ``token_and_usdc_flows_v2``). The planner
will discover partitions by folder existence under that root.
"""

from __future__ import annotations

from pathlib import Path
from typing import NamedTuple

from lib.partition_utils import PARTITION_10K_LABEL, PARTITION_1M_LABEL, partition_end


class PartitionPlan(NamedTuple):
    """Result of partition planning.

    Attributes:
        all_partitions: All discovered partitions within frontier, sorted.
        completed_partitions: Partitions whose output folder exists under
            output_root (atomic folder rename guarantees data+metadata are
            present and immutable).
        todo_partitions: Partitions that need processing (all minus completed),
            sorted in processing order.
    """

    all_partitions: list[tuple[int, int]]
    completed_partitions: list[tuple[int, int]]
    todo_partitions: list[tuple[int, int]]


def plan_partitions(
    source_bases: list[str],
    source_root: str | Path,
    output_root: str | Path,
    frontier: int,
) -> PartitionPlan:
    """Discover partitions from source base directories and compute work plan.

    Scans each base directory under source_root for valid 1M/10K partition
    folders. A partition is considered present in a source if its directory
    exists (e.g., ``.../base/1M={M}/10K={K}/``). The planner does not require
    ``data.parquet`` inside the folder; atomic rename of the entire folder
    (data + metadata) ensures that folder visibility implies completeness.

    Collects unique (m, k) pairs across all provided bases, filters to
    partitions whose end block is <= frontier, then determines which
    partitions are complete (output folder exists) versus todo.

    Args:
        source_bases: List of base directory paths relative to source_root.
            Each base should directly contain ``1M=*/10K=*/`` subdirectories.
            Examples: ``["manifests"]`` for raw stable partitions;
            ``["token_and_usdc_flows_v2"]`` for a derived dependency.
            Do not pass raw ``contract/event`` paths for ``polygon_contract_events_v3``
            sources; use the manifest instead (see module docstring).
        source_root: Root directory under which source_bases are resolved.
        output_root: Root directory for this job's output partitions
            (e.g., ``TOKEN_AND_USDC_FLOWS_V2_DIR``).
        frontier: Upstream frontier block number; only partitions with
            ``partition_end(k) <= frontier`` are included.

    Returns:
        PartitionPlan with all_partitions, completed_partitions, and
        todo_partitions lists (all sorted).
    """
    source_root = Path(source_root)
    output_root = Path(output_root)

    # Discover all partitions from source bases (folder existence is enough)
    all_partitions: set[tuple[int, int]] = set()
    for base in source_bases:
        base_dir = source_root / base
        if not base_dir.exists():
            continue
        for m_dir in sorted(base_dir.iterdir()):
            if not m_dir.name.startswith(f"{PARTITION_1M_LABEL}="):
                continue
            m_val = int(m_dir.name.split("=")[1])
            for k_dir in sorted(m_dir.iterdir()):
                if not k_dir.name.startswith(f"{PARTITION_10K_LABEL}="):
                    continue
                k_val = int(k_dir.name.split("=")[1])
                # Folder existence is sufficient (atomic rename guarantee)
                if k_dir.exists():
                    all_partitions.add((m_val, k_val))

    # Filter to frontier and sort
    filtered = [
        (m, k)
        for m, k in all_partitions
        if partition_end(k) <= frontier
    ]
    all_sorted = sorted(filtered)

    # Determine completed vs todo based on output folder existence
    # (atomic rename guarantees data+metadata are present if folder exists)
    completed: list[tuple[int, int]] = []
    todo: list[tuple[int, int]] = []
    for m, k in all_sorted:
        out_dir = output_root / f"{PARTITION_1M_LABEL}={m}" / f"{PARTITION_10K_LABEL}={k}"
        if out_dir.exists():
            completed.append((m, k))
        else:
            todo.append((m, k))

    return PartitionPlan(
        all_partitions=all_sorted,
        completed_partitions=completed,
        todo_partitions=todo,
    )
