"""
Assert: every ``UmaCtfAdapter/question_initialized.request_timestamp``
value falls inside a reasonable Unix-epoch-seconds range.

The data dictionary describes ``request_timestamp`` as "Unix timestamp
(UTC) of the UMA request, in seconds". This test enforces that with a
generous range:

* lower bound: ``2020-01-01T00:00:00Z`` (1_577_836_800) — Polymarket
  launched mid-2020, so anything earlier is implausible.
* upper bound: ``2030-01-01T00:00:00Z`` (1_893_456_000) — comfortably
  after any plausible current scrape.

Any value outside that range is treated as a contract violation.

Scope: ``UmaCtfAdapter/question_initialized`` only.
"""
from helpers import glob_all

_LOWER = 1_577_836_800  # 2020-01-01T00:00:00Z
_UPPER = 1_893_456_000  # 2030-01-01T00:00:00Z


def test_request_timestamp_range(con):
    try:
        src = glob_all("question_initialized")
    except ValueError:
        # No UMA data on disk; the assertion has nothing to enforce.
        return

    bad = con.execute(f"""
        SELECT request_timestamp, COUNT(*) AS n
        FROM {src}
        WHERE request_timestamp < {_LOWER}
           OR request_timestamp > {_UPPER}
        GROUP BY request_timestamp
        ORDER BY n DESC
        LIMIT 5
    """).fetchall()

    assert not bad, (
        f"{len(bad)} distinct request_timestamp value(s) fall outside the "
        f"reasonable range [{_LOWER}, {_UPPER}] (2020-01-01 to 2030-01-01 "
        f"UTC); first offenders (request_timestamp, occurrences): "
        f"{bad[:5]}"
    )
