"""Shared primitives for atomic write/publish and safe temp cleanup.

Standardizes the pattern of:
  1. Creating a temp location for writing
  2. Atomically publishing (renaming) temp to final location
  3. Cleaning up temp artifacts on failure or startup

This module ensures that:
  - Interrupted writes leave only removable temp artifacts
  - Successful writes remain atomic from the reader perspective
  - Existing invariants (immutability, no-overwrite guarantees) are preserved
"""

from __future__ import annotations

import os
import shutil
import tempfile
import contextlib
from dataclasses import dataclass
from pathlib import Path
from typing import Iterator, Optional


@dataclass(frozen=True)
class TempLocation:
    """Metadata about a created temp location."""
    path: Path
    """Absolute path to the temp directory."""

    final_path: Path
    """Absolute path where this temp will be published."""

    temp_suffix: str
    """Suffix used to mark this as temporary (used for cleanup)."""


def create_temp_location(
    parent_dir: str | Path,
    final_name: str,
    temp_suffix: str = ".tmp",
) -> TempLocation:
    """Create a temp directory for writing, with predictable naming for cleanup.
    
    Args:
        parent_dir: Parent directory where temp will live (alongside final location).
        final_name: Name of the final (non-temp) path after publish.
        temp_suffix: Suffix to mark this as temporary (default: ".tmp").
    
    Returns:
        TempLocation with paths and cleanup metadata.
    
    Guarantees:
        - Returns immediately; does not check if parent_dir exists.
        - Temp directory name is predictable for cleanup.
        - Temp directory is created and is empty.
    
    Example:
        >>> temp = create_temp_location("/data/partitions", "1M=1000000/10K=1000000")
        >>> # Write to temp.path
        >>> publish_atomically(temp)  # Or use with cleanup_on_failure(temp)
    """
    parent_dir = Path(parent_dir)
    final_path = parent_dir / final_name
    temp_name = f"{final_name}{temp_suffix}"
    temp_path = parent_dir / temp_name

    # Create parent directory
    parent_dir.mkdir(parents=True, exist_ok=True)

    # Remove any stale temp directory
    if temp_path.exists():
        shutil.rmtree(temp_path, ignore_errors=True)

    # Create fresh temp directory
    temp_path.mkdir(parents=False, exist_ok=False)

    return TempLocation(
        path=temp_path,
        final_path=final_path,
        temp_suffix=temp_suffix,
    )


def publish_atomically(
    temp_location: TempLocation,
) -> None:
    """Atomically publish (rename) a temp location to its final location.

    Args:
        temp_location: TempLocation from create_temp_location().

    Raises:
        FileExistsError: If final_path already exists. Landed partitions are
            immutable: overwriting cold data is never permitted, so there is no
            opt-out. A FileExistsError here means something tried to violate the
            immutability contract and must be investigated.
        FileNotFoundError: If temp_path was removed before rename.

    Guarantees:
        - Atomic from reader perspective (uses os.replace).
        - If final_path exists, no changes are made (the write is refused).
        - On success, temp_path no longer exists.

    Example:
        >>> temp = create_temp_location("/data/partitions", "1M=1000000/10K=1000000")
        >>> write_data_to(temp.path)
        >>> publish_atomically(temp)  # Now visible at temp.final_path
    """
    if temp_location.final_path.exists():
        raise FileExistsError(
            f"refusing to overwrite immutable final location: {temp_location.final_path}"
        )

    if not temp_location.path.exists():
        raise FileNotFoundError(
            f"temp directory disappeared before publish: {temp_location.path}"
        )

    # Atomic rename using os.replace (works across filesystems on POSIX)
    os.replace(temp_location.path, temp_location.final_path)


@contextlib.contextmanager
def cleanup_on_failure(temp_location: TempLocation) -> Iterator[None]:
    """Context manager that ensures temp cleanup if an exception occurs.
    
    Usage:
        >>> temp = create_temp_location(...)
        >>> try:
        >>>     with cleanup_on_failure(temp):
        >>>         write_to(temp.path)
        >>>         publish_atomically(temp)
        >>> except Exception:
        >>>     # Temp is cleaned up automatically
        >>>     pass
    
    Or as a simpler alternative, always call cleanup_temp() on exception:
        >>> try:
        >>>     ...
        >>> except Exception:
        >>>     cleanup_temp(temp)
        >>>     raise
    """
    try:
        yield
    except Exception:
        cleanup_temp(temp_location)
        raise


def cleanup_temp(temp_location: TempLocation) -> None:
    """Manually clean up a temp location.
    
    Args:
        temp_location: TempLocation to clean up.
    
    Does nothing if temp_location.path doesn't exist.
    """
    if temp_location.path.exists():
        shutil.rmtree(temp_location.path, ignore_errors=True)


def cleanup_old_temp_artifacts(
    root_dir: str | Path,
    temp_suffix: str = ".tmp",
    pattern_check: Optional[callable] = None,
) -> int:
    """Clean up orphaned temp directories from previous interrupted runs.
    
    Args:
        root_dir: Root directory to scan for temp artifacts.
        temp_suffix: Suffix that marks a directory as temporary.
        pattern_check: Optional callback to validate whether a temp dir should be
                       removed. Receives the Path and should return True to remove.
                       If None, removes all directories ending with temp_suffix.
    
    Returns:
        Count of directories removed.
    
    Behavior:
        - Recursively scans root_dir for directories matching temp_suffix.
        - Uses shutil.rmtree with ignore_errors=True to remove each.
        - Continues on any errors, does not raise.
        - Idempotent: safe to call multiple times.
    
    Example:
        >>> # Remove all .tmp directories
        >>> count = cleanup_old_temp_artifacts("/data/partitions")
        >>> print(f"Removed {count} temp artifacts")
    """
    root_dir = Path(root_dir)
    if not root_dir.is_dir():
        return 0

    removed = 0
    for path in root_dir.rglob("*"):
        if not path.is_dir():
            continue
        if not path.name.endswith(temp_suffix):
            continue
        if pattern_check is not None and not pattern_check(path):
            continue
        shutil.rmtree(path, ignore_errors=True)
        removed += 1

    return removed
