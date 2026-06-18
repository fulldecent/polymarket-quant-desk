"""Shared helpers for partition math and canonical partition directory names.

Every dataset in this project (raw and derived) uses the same 1M/10K nested
partition scheme. These helpers are the single source of truth for that math so
producers never re-derive partition sizes or directory names by hand.
"""

from __future__ import annotations


PARTITION_1M_LABEL = "1M"
PARTITION_1M_SIZE = 1_000_000
PARTITION_10K_LABEL = "10K"
PARTITION_10K_SIZE = 10_000


def partition_start(block: int) -> int:
    """Return the aligned 10K partition start that contains this block."""
    return (block // PARTITION_10K_SIZE) * PARTITION_10K_SIZE


def partition_end(block: int) -> int:
    """Return the inclusive end block of the 10K partition containing this block."""
    start = partition_start(block)
    return start + PARTITION_10K_SIZE - 1


def partition_dir(block: int) -> str:
    """Return canonical partition directory path like "1M=33000000/10K=33600000".

    This is a relative path suitable for use with Path(root) / partition_dir(block).
    To get just the 1M parent, use: Path(partition_dir(block)).parent
    """
    start = partition_start(block)
    m_val = (start // PARTITION_1M_SIZE) * PARTITION_1M_SIZE
    return f"{PARTITION_1M_LABEL}={m_val}/{PARTITION_10K_LABEL}={start}"


def enumerate_partitions(start_block: int, frontier: int) -> list[tuple[int, int]]:
    """Every consecutive 10K partition from ``start_block`` up to the frontier.

    Returns ``(m_val, k_val)`` pairs — the 1M parent start and the 10K partition
    start — beginning at the 10K partition that contains ``start_block`` and
    continuing while the partition's inclusive end block is ``<= frontier`` (i.e.
    the partition is fully sunk upstream). The result is contiguous with no gaps.

    This is the canonical partition-planning primitive for derived producers:
    discover work by BLOCK RANGE, not by source-folder existence, so a 10K range
    with no source rows still yields a partition to materialize (as a zero-row
    output).
    """
    parts: list[tuple[int, int]] = []
    k = partition_start(start_block)
    while partition_end(k) <= frontier:
        m = (k // PARTITION_1M_SIZE) * PARTITION_1M_SIZE
        parts.append((m, k))
        k += PARTITION_10K_SIZE
    return parts


def _self_test() -> int:
    """Verify partition math against hand-computed values; return a process exit code."""
    failures = 0

    def check(label: str, got: object, want: object) -> None:
        nonlocal failures
        ok = got == want
        print(f"  {label}: {'PASS' if ok else f'FAIL (got {got!r}, want {want!r})'}")
        if not ok:
            failures += 1

    check("partition_start(33_605_403)", partition_start(33_605_403), 33_600_000)
    check("partition_end(33_605_403)", partition_end(33_605_403), 33_609_999)
    check("partition_dir(33_605_403)", partition_dir(33_605_403), "1M=33000000/10K=33600000")
    # A frontier on a partition's last block includes that partition; one block short excludes it.
    check(
        "enumerate_partitions(33_605_403, 33_629_999)",
        enumerate_partitions(33_605_403, 33_629_999),
        [(33_000_000, 33_600_000), (33_000_000, 33_610_000), (33_000_000, 33_620_000)],
    )
    check(
        "enumerate_partitions frontier one block short drops last",
        enumerate_partitions(33_605_403, 33_619_998),
        [(33_000_000, 33_600_000)],
    )
    check(
        "enumerate_partitions empty when frontier below first partition end",
        enumerate_partitions(33_605_403, 33_600_000),
        [],
    )

    return 1 if failures else 0


if __name__ == "__main__":
    raise SystemExit(_self_test())