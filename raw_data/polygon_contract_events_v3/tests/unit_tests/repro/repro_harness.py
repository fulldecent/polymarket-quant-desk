#!/usr/bin/env python3
"""
Reproducibility test harness for polygon_contract_events_v3.

This script prepares a partial cold-tier seed from the live data and
provides the exact commands to run a fresh scrape against it, then
compare the newly generated partitions byte-for-byte against the
original cold tier.

The goal is to prove that the scraper produces canonical, reproducible
Parquet files: given the same input RPC responses and the same code,
the output files are identical.

Workflow
--------
1. Run this script to copy ~10 GB of live cold-tier data into a
   temporary location on the same filesystem as the live data.
   The script automatically determines the block number that yields
   approximately the requested size.

2. Run the scraper with a fresh hot DB and the temporary cold root,
   stopping after exactly 3 new partitions have been sunk.

3. Use the comparison logic (or tmp/compare_repro.py) to verify that
   the 3 freshly written partitions are byte-identical to the ones
   that already existed in the live cold tier.

Because the cold tier is append-only and the sink path is deterministic
(canonical sort order + DuckDB Parquet writer), any difference indicates
a non-reproducibility bug.

Usage
-----
    python raw_data/polygon_contract_events_v3/tests/unit_tests/repro/repro_harness.py \
        --source-cold /Volumes/polymarket-quant-desk/raw_data/cold/polygon_contract_events_v3 \
        --dest-cold /tmp/repro_cold_v3 \
        --target-size-gb 10

The script prints the exact block number it stopped at and the total
bytes copied. Use that block number (or a slightly higher one) when
configuring the scraper stop condition.
"""

from __future__ import annotations

import argparse
import os
import shutil
from pathlib import Path
from typing import List, Tuple


def discover_partitions(cold_root: Path) -> List[Tuple[int, Path, int]]:
    """
    Walk the cold tier and return a list of (partition_start, dir_path, total_bytes)
    for every 10K partition that contains at least one data.parquet.

    Partitions are returned in ascending block order.
    """
    partitions: dict[int, Tuple[Path, int]] = {}
    for parquet in cold_root.rglob("data.parquet"):
        try:
            part_dir = parquet.parent  # .../10K=33600000
            if not part_dir.name.startswith("10K="):
                continue
            p = int(part_dir.name.split("=", 1)[1])
            size = parquet.stat().st_size
            if p not in partitions:
                partitions[p] = (part_dir.parent, 0)  # placeholder
            # Accumulate size per partition
            prev_path, prev_size = partitions[p]
            partitions[p] = (prev_path, prev_size + size)
        except Exception:
            continue

    # Re-walk to get the actual directory for each partition
    result: List[Tuple[int, Path, int]] = []
    for p, (parent_dir, total_size) in sorted(partitions.items()):
        # parent_dir is the 1M=... directory; we need the 10K=... directory
        tenk_dir = parent_dir / f"10K={p}"
        if tenk_dir.is_dir():
            result.append((p, tenk_dir, total_size))
    return sorted(result)


def select_prefix_for_size(
    partitions: List[Tuple[int, Path, int]], target_bytes: int
) -> Tuple[List[Tuple[int, Path, int]], int]:
    """
    Return the longest prefix of partitions whose cumulative size is <= target_bytes,
    and the actual cumulative size achieved.
    """
    cumulative = 0
    selected: List[Tuple[int, Path, int]] = []
    for p, path, size in partitions:
        if cumulative + size > target_bytes and selected:
            break
        selected.append((p, path, size))
        cumulative += size
    return selected, cumulative


def copy_partitions(
    selected: List[Tuple[int, Path, int]], dest_root: Path, dry_run: bool = False
) -> int:
    """
    Copy the selected partition directories into dest_root, preserving the
    relative structure {contract}/{event}/1M=N/10K=K/.

    Returns the total number of bytes copied.
    """
    total_copied = 0
    for p, src_dir, size in selected:
        # src_dir is .../1M=33600000/10K=33600000
        # We need to replicate the parent chain under dest_root
        # Find the contract/event/1M=... relative path
        # Walk up until we find a directory that looks like a contract name
        parts = list(src_dir.parts)
        # Find the index of the contract directory (first component after cold_root)
        # We assume the structure is cold_root / Contract / Event / 1M=... / 10K=...
        # So we take the last 4 components for the relative path.
        rel = Path(*parts[-4:])  # Contract/Event/1M=.../10K=...
        dst_dir = dest_root / rel
        if not dry_run:
            dst_dir.parent.mkdir(parents=True, exist_ok=True)
            shutil.copytree(src_dir, dst_dir, dirs_exist_ok=True)
        total_copied += size
        print(f"  copied {rel} ({size / 1_000_000:.1f} MB)")
    return total_copied


def main() -> None:
    parser = argparse.ArgumentParser(description="Prepare a partial cold-tier seed for reproducibility testing.")
    parser.add_argument(
        "--source-cold",
        type=Path,
        required=True,
        help="Path to the live cold tier root (contains Contract/ directories)",
    )
    parser.add_argument(
        "--dest-cold",
        type=Path,
        required=True,
        help="Path to the temporary cold tier root that will receive the copy",
    )
    parser.add_argument(
        "--target-size-gb",
        type=float,
        default=10.0,
        help="Approximate size of data to copy (in GiB). Default: 10",
    )
    parser.add_argument(
        "--dry-run",
        action="store_true",
        help="Only compute and print what would be copied; do not actually copy files.",
    )
    args = parser.parse_args()

    source = args.source_cold.resolve()
    dest = args.dest_cold.resolve()
    target_bytes = int(args.target_size_gb * 1024 * 1024 * 1024)

    if not source.is_dir():
        parser.error(f"source-cold does not exist or is not a directory: {source}")

    print(f"Scanning source cold tier: {source}")
    partitions = discover_partitions(source)
    if not partitions:
        parser.error("No partitions with data.parquet found under source-cold")

    print(f"Found {len(partitions)} partitions in source.")
    selected, actual_bytes = select_prefix_for_size(partitions, target_bytes)

    if not selected:
        parser.error("No partitions selected (target too small?)")

    first_p = selected[0][0]
    last_p = selected[-1][0]
    print(f"Selected {len(selected)} partitions covering blocks [{first_p}, {last_p + 9999}]")
    print(f"Estimated size to copy: {actual_bytes / 1_000_000_000:.2f} GiB")

    if args.dry_run:
        print("\nDry run — nothing copied.")
        return

    print(f"\nCopying into: {dest}")
    dest.mkdir(parents=True, exist_ok=True)
    copied = copy_partitions(selected, dest, dry_run=False)
    print(f"\nDone. Copied {copied / 1_000_000_000:.2f} GiB into {dest}")
    print(f"Last partition start block: {last_p}")
    print(
        "\nNext steps:\n"
        f"  1. Create a fresh hot DB (e.g. /tmp/repro_hot.db)\n"
        f"  2. Run the scraper with POLYGON_CONTRACT_EVENTS_V3_DIR={dest}\n"
        f"     and stop after 3 sunk partitions (use --max-calls or manual stop).\n"
        f"  3. Compare the newly written partitions under {dest} against the\n"
        f"     original partitions under {source} for the same block ranges."
    )


if __name__ == "__main__":
    main()