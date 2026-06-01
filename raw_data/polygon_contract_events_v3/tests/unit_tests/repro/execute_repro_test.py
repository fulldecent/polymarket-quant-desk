#!/usr/bin/env python3
"""
Execute the full reproducibility test:

1. Use the seeded cold tier (~10 GB, blocks 33.6M-69.6M)
2. Create a fresh hot DB
3. Run the scraper until 3 new partitions are sunk
4. Compare the 3 new partitions byte-for-byte against the live cold tier

This proves that the scraper produces canonical, reproducible output.
"""

from __future__ import annotations

import os
import shutil
import signal
import subprocess
import sys
import time
from pathlib import Path
from typing import Set


def find_sunk_partitions(cold_root: Path) -> Set[int]:
    """Return the set of 10K partition start blocks that have at least one data.parquet."""
    parts: Set[int] = set()
    for p in cold_root.rglob("data.parquet"):
        try:
            part_dir = p.parent.name
            if part_dir.startswith("10K="):
                parts.add(int(part_dir.split("=", 1)[1]))
        except Exception:
            continue
    return parts


def main() -> None:
    # Paths (on the data volume for performance)
    seeded_cold = Path("/Volumes/polymarket-quant-desk/tmp/repro_cold_v3")
    fresh_hot = Path("/Volumes/polymarket-quant-desk/tmp/repro_hot.db")
    live_cold = Path("/Volumes/polymarket-quant-desk/raw_data/cold/polygon_contract_events_v3")

    if not seeded_cold.is_dir():
        print(f"ERROR: Seeded cold tier not found at {seeded_cold}")
        print("Run repro_harness.py first to create the seed.")
        sys.exit(1)

    # Fresh hot DB
    if fresh_hot.exists():
        print(f"Removing stale hot DB: {fresh_hot}")
        fresh_hot.unlink()

    # Environment for the scraper
    env = os.environ.copy()
    env["POLYGON_CONTRACT_EVENTS_V3_HOT_DB"] = str(fresh_hot)
    env["POLYGON_CONTRACT_EVENTS_V3_DIR"] = str(seeded_cold)
    # Keep the same RPC URL from the main .env

    print("=" * 70)
    print("REPRODUCIBILITY TEST")
    print("=" * 70)
    print(f"Seeded cold root : {seeded_cold}")
    print(f"Fresh hot DB     : {fresh_hot}")
    print(f"Live cold root   : {live_cold}")
    print()

    # Snapshot of partitions before we start
    initial_parts = find_sunk_partitions(seeded_cold)
    print(f"Initial partitions in seed: {len(initial_parts)}")
    if initial_parts:
        print(f"  Range: {min(initial_parts)} - {max(initial_parts)}")
    print()

    # Launch the scraper
    scraper = Path(__file__).resolve().parents[3] / "main.py"
    cmd = [sys.executable, str(scraper), "--parallel", "8"]

    print("Starting scraper...")
    print(f"Command: {' '.join(cmd)}")
    print()

    proc = subprocess.Popen(
        cmd,
        env=env,
        stdout=subprocess.PIPE,
        stderr=subprocess.STDOUT,
        text=True,
        bufsize=1,
    )

    # Monitor for new sunk partitions
    start_time = time.time()
    last_count = len(initial_parts)
    target_new = 3

    try:
        while True:
            time.sleep(10)  # Check every 10 seconds

            current_parts = find_sunk_partitions(seeded_cold)
            new_count = len(current_parts) - len(initial_parts)

            if new_count != last_count:
                print(f"[{time.time() - start_time:.0f}s] New partitions sunk: {new_count} / {target_new}")
                last_count = new_count

            if new_count >= target_new:
                print(f"\n✓ Reached target: {new_count} new partitions sunk.")
                break

            # Check if scraper died
            if proc.poll() is not None:
                print("\nScraper exited on its own.")
                break

    except KeyboardInterrupt:
        print("\nInterrupted by user.")
    finally:
        print("\nTerminating scraper...")
        proc.send_signal(signal.SIGINT)
        try:
            proc.wait(timeout=30)
        except subprocess.TimeoutExpired:
            print("Forcing kill...")
            proc.kill()
            proc.wait()

    # Final state
    final_parts = find_sunk_partitions(seeded_cold)
    newly_sunk = sorted(final_parts - initial_parts)

    print("\n" + "=" * 70)
    print("SCRAPE COMPLETE")
    print("=" * 70)
    print(f"Newly sunk partitions: {newly_sunk}")

    if not newly_sunk:
        print("No new partitions were sunk. Exiting.")
        return

    # Compare the newly sunk partitions against the live cold tier
    print("\nComparing newly sunk partitions against live cold tier...")
    differences = []

    for p in newly_sunk:
        # Find all files for this partition in the seeded tree
        for seeded_file in seeded_cold.rglob(f"10K={p}/data.parquet"):
            # Compute the corresponding path in the live cold tier
            rel = seeded_file.relative_to(seeded_cold)
            live_file = live_cold / rel

            if not live_file.exists():
                differences.append(f"Missing in live: {rel}")
                continue

            # Byte compare
            if seeded_file.read_bytes() != live_file.read_bytes():
                differences.append(f"Byte mismatch: {rel}")
            else:
                print(f"  ✓ {rel} matches")

    print("\n" + "=" * 70)
    if differences:
        print("REPRODUCIBILITY TEST FAILED")
        print("=" * 70)
        for d in differences:
            print(f"  - {d}")
        sys.exit(1)
    else:
        print("REPRODUCIBILITY TEST PASSED")
        print("=" * 70)
        print(f"All {len(newly_sunk)} newly sunk partitions are byte-identical")
        print("to the corresponding partitions in the live cold tier.")
        print()
        print("This proves that the scraper produces canonical, reproducible output.")


if __name__ == "__main__":
    main()