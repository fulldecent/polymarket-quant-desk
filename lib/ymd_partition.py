"""UTC day partition paths for v4 raw and v2 derived datasets.

v1 and v3 stay on ``lib.partition_utils`` (``1M=`` / ``10K=``). This module
is the day scheme only. A day key is the integer ``YYYYMMDD`` in UTC.

Polygon PoS header time strictly increases. Bor ``ErrInvalidTimestamp``
(commit ``9c445ef``, ``consensus/bor/bor.go`` lines 131–133) rejects a
header whose timestamp is below ``parent.Time + period``, and mainnet
period is never 0. A UTC day folder is therefore a function of block
number. Hive keys stay out of the parquet files.
"""

from __future__ import annotations

import json
from collections.abc import Callable
from datetime import date, datetime, timedelta, timezone
from pathlib import Path


SECONDS_PER_DAY = 86400


def day_key_from_parts(year: int, month: int, day: int) -> int:
    """Return ``YYYYMMDD`` after checking the calendar date."""
    date(year, month, day)
    return year * 10000 + month * 100 + day


def day_key_from_timestamp(timestamp: int) -> int:
    """UTC day key of a Unix timestamp in seconds."""
    if timestamp < 0:
        raise ValueError(f"timestamp {timestamp} is before the Unix epoch")
    d = datetime.fromtimestamp(timestamp, timezone.utc).date()
    return day_key_from_parts(d.year, d.month, d.day)


def day_key_parts(day_key: int) -> tuple[int, int, int]:
    """Return ``(year, month, day)`` for a ``YYYYMMDD`` key."""
    if day_key < 10101:
        raise ValueError(f"day_key {day_key} is not YYYYMMDD")
    year, rest = divmod(day_key, 10000)
    month, day = divmod(rest, 100)
    date(year, month, day)
    return year, month, day


def day_dir(day_key: int) -> str:
    """Relative directory ``year=YYYY/month=MM/day=DD``."""
    year, month, day = day_key_parts(day_key)
    return f"year={year:04d}/month={month:02d}/day={day:02d}"


def day_label(day_key: int) -> str:
    """Screen form ``YYYY-MM-DD``."""
    year, month, day = day_key_parts(day_key)
    return f"{year:04d}-{month:02d}-{day:02d}"


def next_day_key(day_key: int) -> int:
    year, month, day = day_key_parts(day_key)
    nxt = date(year, month, day) + timedelta(days=1)
    return day_key_from_parts(nxt.year, nxt.month, nxt.day)


def prev_day_key(day_key: int) -> int:
    year, month, day = day_key_parts(day_key)
    prev = date(year, month, day) - timedelta(days=1)
    return day_key_from_parts(prev.year, prev.month, prev.day)


def utc_midnight(day_key: int) -> int:
    """Unix seconds of 00:00:00 UTC on ``day_key``."""
    year, month, day = day_key_parts(day_key)
    dt = datetime(year, month, day, tzinfo=timezone.utc)
    return int(dt.timestamp())


def midnights_crossed(ts_lo: int, ts_hi: int) -> list[int]:
    """UTC midnights ``M`` with ``ts_lo < M <= ts_hi``.

    A timestamp that lands exactly on midnight is already on that day, so
    that midnight is not a crossing. The returned values are the Unix
    starts of each new UTC day inside the span.
    """
    if ts_hi < ts_lo:
        raise ValueError(f"ts_hi {ts_hi} < ts_lo {ts_lo}")
    if ts_hi == ts_lo:
        return []
    first = utc_midnight(day_key_from_timestamp(ts_lo)) + SECONDS_PER_DAY
    out: list[int] = []
    midnight = first
    while midnight <= ts_hi:
        out.append(midnight)
        midnight += SECONDS_PER_DAY
    return out


def first_block_on_or_after(
    ts_at: Callable[[int], int],
    lo: int,
    hi: int,
    midnight: int,
) -> int:
    """Smallest block in ``(lo, hi]`` whose header time is ``>= midnight``.

    ``ts_at(lo)`` must be strictly before ``midnight``, and ``ts_at(hi)``
    must be at or after it. Header time only moves forward, so the cut
    is unique.
    """
    if lo >= hi:
        raise ValueError(f"need lo < hi, got {lo} >= {hi}")
    if ts_at(lo) >= midnight:
        raise ValueError(f"block {lo} is already at or after {midnight}")
    if ts_at(hi) < midnight:
        raise ValueError(f"block {hi} is still before {midnight}")
    while lo + 1 < hi:
        mid = (lo + hi) // 2
        if ts_at(mid) >= midnight:
            hi = mid
        else:
            lo = mid
    return hi


def day_key_from_hive_dir(path: Path) -> int:
    """Parse ``.../year=YYYY/month=MM/day=DD`` (a ``.tmp`` suffix is ignored)."""
    name = path.name[:-4] if path.name.endswith(".tmp") else path.name
    month_dir = path.parent
    year_dir = month_dir.parent
    year_name = year_dir.name[:-4] if year_dir.name.endswith(".tmp") else year_dir.name
    month_name = month_dir.name[:-4] if month_dir.name.endswith(".tmp") else month_dir.name
    try:
        year = int(year_name.split("=", 1)[1])
        month = int(month_name.split("=", 1)[1])
        day = int(name.split("=", 1)[1])
    except (IndexError, ValueError) as exc:
        raise ValueError(f"not a UTC day folder: {path}") from exc
    if not (
        year_name.startswith("year=")
        and month_name.startswith("month=")
        and name.startswith("day=")
    ):
        raise ValueError(f"not a UTC day folder: {path}")
    return day_key_from_parts(year, month, day)


def iter_manifest_day_keys(cold_root: str | Path) -> list[int]:
    """Sorted day keys that have ``manifests/year=/month=/day=/_SUCCESS``.

    ``.tmp`` folders are ignored. This does not check that the days are
    contiguous; callers that treat the list as a frontier must.
    """
    root = Path(cold_root) / "manifests"
    if not root.exists():
        return []
    if not root.is_dir():
        raise ValueError(f"manifests path is not a directory: {root}")
    keys: list[int] = []
    for success in root.glob("year=*/month=*/day=*/_SUCCESS"):
        day_folder = success.parent
        if any(part.endswith(".tmp") for part in day_folder.parts):
            continue
        if not success.is_file():
            continue
        keys.append(day_key_from_hive_dir(day_folder))
    return sorted(set(keys))


def assert_contiguous_day_keys(day_keys: list[int]) -> None:
    """Raise ``ValueError`` if ``day_keys`` is not a gap-free calendar run."""
    for prev, nxt in zip(day_keys, day_keys[1:]):
        expect = next_day_key(prev)
        if nxt != expect:
            raise ValueError(
                f"day keys skip {day_label(expect)} between "
                f"{day_label(prev)} and {day_label(nxt)}"
            )


def day_block_span(cold_root: str | Path, day_key: int) -> tuple[int, int]:
    """``(min_block, max_block)`` from one day's ``metadata.json`` parameters.

    Every metadata file for that day must agree. A sunk day with no
    metadata is an error.
    """
    rel = day_dir(day_key)
    spans: set[tuple[int, int]] = set()
    for meta in Path(cold_root).glob(f"*/*/{rel}/metadata.json"):
        if any(part.endswith(".tmp") for part in meta.parts):
            continue
        payload = json.loads(meta.read_text(encoding="utf-8"))
        params = payload.get("parameters")
        if not isinstance(params, dict):
            raise ValueError(f"{meta} has no parameters object")
        try:
            span = (int(params["min_block"]), int(params["max_block"]))
        except (KeyError, TypeError, ValueError) as exc:
            raise ValueError(f"{meta} parameters lack min_block/max_block") from exc
        spans.add(span)
    if not spans:
        raise ValueError(f"no metadata.json for {day_label(day_key)} under {cold_root}")
    if len(spans) != 1:
        raise ValueError(
            f"{day_label(day_key)} metadata block spans disagree: {sorted(spans)}"
        )
    return next(iter(spans))


def _self_test() -> int:
    """Check day math against hand-computed UTC values."""
    failures = 0

    def check(label: str, got: object, want: object) -> None:
        nonlocal failures
        ok = got == want
        print(f"  {label}: {'PASS' if ok else f'FAIL (got {got!r}, want {want!r})'}")
        if not ok:
            failures += 1

    # 2026-10-12 00:00:00 UTC and one second before the next midnight.
    midnight = int(datetime(2026, 10, 12, tzinfo=timezone.utc).timestamp())
    almost = midnight + SECONDS_PER_DAY - 1
    check("day_key_from_timestamp midnight", day_key_from_timestamp(midnight), 20261012)
    check("day_key_from_timestamp 23:59:59", day_key_from_timestamp(almost), 20261012)
    check("day_dir", day_dir(20261012), "year=2026/month=10/day=12")
    check("day_label", day_label(20261012), "2026-10-12")
    check("next_day_key year end", next_day_key(20261231), 20270101)
    check("prev_day_key year start", prev_day_key(20270101), 20261231)
    check("utc_midnight", utc_midnight(20261012), midnight)
    check("midnights same day", midnights_crossed(midnight, almost), [])
    tue = midnight + SECONDS_PER_DAY
    wed = tue + SECONDS_PER_DAY
    check(
        "midnights across two boundaries",
        midnights_crossed(midnight + 3600, wed + 60),
        [tue, wed],
    )
    # Exact midnight is already on that day, so it is not a crossing.
    check("midnight itself is not a crossing", midnights_crossed(midnight, midnight), [])
    check(
        "crossing starts at the next midnight",
        midnights_crossed(midnight, tue),
        [tue],
    )

    stamps = {10: 100, 11: 120, 12: 180, 13: 240}

    def ts_at(block: int) -> int:
        return stamps[block]

    check(
        "first_block_on_or_after",
        first_block_on_or_after(ts_at, 10, 13, 150),
        12,
    )
    check(
        "first_block_on_or_after adjacent",
        first_block_on_or_after(ts_at, 10, 11, 120),
        11,
    )
    return 1 if failures else 0


if __name__ == "__main__":
    raise SystemExit(_self_test())
