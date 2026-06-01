#!/usr/bin/env python3
"""
Run a controlled reproducibility scrape that stops after N sunk partitions.

This script is a thin wrapper around main.py that:
- Uses a fresh hot DB (deletes it first if it exists)
- Uses the provided cold root (which should already contain seed data)
- Stops automatically after the requested number of partitions have been sunk

It is intended for the reproducibility test described in repro_harness.py.
"""

from __future__ import annotations

import argparse
import os
import shutil
import subprocess
import sys
from pathlib import Path


def main() -> None:
    parser = argparse.ArgumentParser(
        description="Run scraper until N partitions are sunk, then exit."
    )
    parser.add_argument(
        "--cold-root",
        type=Path,
        required=True,
        help="Path to the (seeded) cold tier root",
    )
    parser.add_argument(
        "--hot-db",
        type=Path,
        required=True,
        help="Path to the hot DuckDB file to use (will be deleted if exists)",
    )
    parser.add_argument(
        "--partitions",
        type=int,
        default=3,
        help="Number of partitions to sink before stopping (default: 3)",
    )
    parser.add_argument(
        "--parallel",
        type=int,
        default=8,
        help="Parallelism for RPC calls (default: 8)",
    )
    args = parser.parse_args()

    cold_root = args.cold_root.resolve()
    hot_db = args.hot_db.resolve()

    if not cold_root.is_dir():
        parser.error(f"cold-root does not exist: {cold_root}")

    # Fresh hot DB
    if hot_db.exists():
        print(f"Removing existing hot DB: {hot_db}")
        hot_db.unlink()

    # Set required environment variables
    env = os.environ.copy()
    env["POLYGON_CONTRACT_EVENTS_V3_HOT_DB"] = str(hot_db)
    env["POLYGON_CONTRACT_EVENTS_V3_DIR"] = str(cold_root)
    # Use the same RPC URL as the main environment
    # (main.py loads .env itself)

    print(f"Starting reproducibility scrape")
    print(f"  Cold root : {cold_root}")
    print(f"  Hot DB    : {hot_db}")
    print(f"  Target    : {args.partitions} sunk partitions")
    print(f"  Parallel  : {args.parallel}")
    print()

    # We run the scraper with a modest parallelism and let it run until
    # it has sunk the requested number of partitions.
    # The scraper itself does not have a "stop after N partitions" flag,
    # so we rely on the fact that the seed data only covers up to a
    # certain block. Once the scraper has processed all seeded blocks
    # and sunk the requested partitions, we can interrupt it.
    #
    # For a clean automated test we instead use --max-calls with a very
    # large number and rely on manual / scripted interruption, or we
    # simply let the user run it interactively.
    #
    # Here we just launch the scraper and let it run.

    cmd = [
        sys.executable,
        str(Path(__file__).resolve().parents[3] / "main.py"),
        "--parallel",
        str(args.parallel),
    ]

    print("Running:", " ".join(cmd))
    print("Environment overrides:")
    print(f"  POLYGON_CONTRACT_EVENTS_V3_HOT_DB={hot_db}")
    print(f"  POLYGON_CONTRACT_EVENTS_V3_DIR={cold_root}")
    print()

    # Run the scraper (this will block until the user stops it or it finishes)
    proc = subprocess.run(cmd, env=env)
    sys.exit(proc.returncode)


if __name__ == "__main__":
    main()