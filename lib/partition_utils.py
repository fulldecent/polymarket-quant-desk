"""Shared helpers for partition math and canonical partition directory names."""

from __future__ import annotations


PARTITION_1M_LABEL = "1M"
_PARTITION_1M_SIZE = 1_000_000
PARTITION_10K_LABEL = "10K"
_PARTITION_10K_SIZE = 10_000


def partition_start(block: int) -> int:
    """Return the aligned 10K partition start that contains this block."""
    return (block // _PARTITION_10K_SIZE) * _PARTITION_10K_SIZE


def partition_end(block: int) -> int:
    """Return the inclusive end block of the 10K partition containing this block."""
    start = partition_start(block)
    return start + _PARTITION_10K_SIZE - 1


def partition_dir(block: int) -> str:
    """Return canonical partition directory path like "1M=33000000/10K=33000000".
    
    This is a relative path suitable for use with Path(root) / partition_dir(block).
    To get just the 1M parent, use: Path(partition_dir(block)).parent
    """
    start = partition_start(block)
    m_val = (start // _PARTITION_1M_SIZE) * _PARTITION_1M_SIZE
    return f"{PARTITION_1M_LABEL}={m_val}/{PARTITION_10K_LABEL}={start}"