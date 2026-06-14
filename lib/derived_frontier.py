"""Helpers for computing frontiers of derived datasets.

Derived producers must never read past the minimum frontier of all their
upstream sources. This module provides a canonical way to compute the
highest complete partition for a derived dataset by scanning its output
directory structure.
"""

from __future__ import annotations

from pathlib import Path

from .partition_utils import PARTITION_10K_LABEL, PARTITION_1M_LABEL, partition_end


def get_derived_frontier(derived_root: str | Path) -> int:
    """Return the highest block for which a complete derived partition exists.

    Scans the output directory for ``1M=*/10K=*/data.parquet`` + ``metadata.json``
    pairs and returns the inclusive end block of the highest such partition.
    Returns ``SCRAPE_START_BLOCK - 1`` (effectively "nothing") if no partitions
    are present.

    This is the derived-dataset analogue of ``get_sunk_frontier`` for raw data.
    A derived producer should take the min of all upstream frontiers (raw cold
    + any prerequisite derived datasets) to decide what it may safely consume.

    Args:
        derived_root: Root directory of a derived dataset (e.g. TOKEN_ID_MAP_V1_DIR).

    Returns:
        Highest block number covered by a complete partition, or a sentinel
        below SCRAPE_START_BLOCK if nothing is landed.
    """
    from raw_data.polygon_contract_events_v3 import SCRAPE_START_BLOCK

    root = Path(derived_root)
    if not root.exists() or not root.is_dir():
        return SCRAPE_START_BLOCK - 1

    max_k: int | None = None
    for m_dir in root.glob(f"{PARTITION_1M_LABEL}=*"):
        if not m_dir.is_dir():
            continue
        for k_dir in m_dir.glob(f"{PARTITION_10K_LABEL}=*"):
            if not k_dir.is_dir():
                continue
            data_file = k_dir / "data.parquet"
            meta_file = k_dir / "metadata.json"
            if data_file.exists() and meta_file.exists():
                k_val = int(k_dir.name.split("=")[1])
                if max_k is None or k_val > max_k:
                    max_k = k_val

    if max_k is None:
        return SCRAPE_START_BLOCK - 1

    return partition_end(max_k)
