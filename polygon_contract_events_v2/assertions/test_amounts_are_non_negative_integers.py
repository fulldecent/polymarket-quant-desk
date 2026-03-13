"""
Assert: every amount column contains a valid non-negative integer string.
"""
import os
from helpers import RAW, glob_complete, glob_all, complete_1m_ranges_for


def _check_col(con, label, source, col):
    bad = con.execute(f"""
        SELECT {col}, COUNT(*) AS n
        FROM {source}
        WHERE TRY_CAST({col} AS HUGEINT) IS NULL
           OR TRY_CAST({col} AS HUGEINT) < 0
        GROUP BY {col}
        LIMIT 5
    """).fetchall()
    return [(label, col, val, n) for val, n in bad]


def _glob_event_ranged(event, ranges):
    """Try glob_complete for this event using the given ranges, falling back
    to glob_all if the event has no complete ranges."""
    try:
        return glob_complete(event, ranges)
    except ValueError:
        return glob_all(event)


def test_amounts_are_non_negative_integers(con, ranges):
    failures = []

    of = glob_complete("order_filled", ranges)
    om = glob_complete("orders_matched", ranges)

    for col in ("maker_amount_filled", "taker_amount_filled", "fee"):
        failures += _check_col(con, "order_filled", of, col)
    for col in ("maker_amount_filled", "taker_amount_filled"):
        failures += _check_col(con, "orders_matched", om, col)

    for table, col in [
        ("fee_charged", "amount"),
        ("payout_redemption", "payout"),
        ("position_split", "amount"),
        ("positions_merge", "amount"),
    ]:
        try:
            src = _glob_event_ranged(table, ranges)
            failures += _check_col(con, table, src, col)
        except ValueError:
            pass  # table not present

    assert len(failures) == 0, (
        f"{len(failures)} column(s) contain non-integer or negative values: "
        + "; ".join(f"{t}.{c}={v!r}" for t, c, v, _ in failures[:5])
    )
