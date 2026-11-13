"""
Assert: for v2 exchanges (CTFExchangeV2, NegRiskCtfExchangeV2),
SUM(order_filled.fee) per transaction equals SUM(fee_charged.amount) per
transaction.

v2 exchanges have no FeeModule; fee_charged and order_filled.fee represent the
same quantity from two different log events in the same transaction. A mismatch
would indicate a missing or duplicated log event.

Transactions where all fills carry fee == 0 are also covered: the sum on both
sides is 0, and COALESCE handles transactions with zero fee_charged events.
"""
from pathlib import Path

import pytest

from helpers import complete_1m_ranges_for_paths, event_day_parquet


def _mismatched_txs(con, of_path: Path, fc_path: Path) -> int:
    """Count transactions where SUM(order_filled.fee) != SUM(fee_charged.amount)."""
    return con.execute(f"""
        WITH of_fees AS (
            SELECT block_number, transaction_index,
                   SUM(CAST(fee AS HUGEINT)) AS total_fee
            FROM read_parquet('{of_path}')
            GROUP BY block_number, transaction_index
        ),
        fc_fees AS (
            SELECT block_number, transaction_index,
                   SUM(CAST(amount AS HUGEINT)) AS total_charged
            FROM read_parquet('{fc_path}')
            GROUP BY block_number, transaction_index
        )
        SELECT COUNT(*)
        FROM of_fees
        LEFT JOIN fc_fees
          ON of_fees.block_number = fc_fees.block_number
         AND of_fees.transaction_index = fc_fees.transaction_index
        WHERE of_fees.total_fee != COALESCE(fc_fees.total_charged, 0)
    """).fetchone()[0]


def _run(con, exchange: str) -> int:
    """Return total mismatch count across sampled partitions, or -1 to skip."""
    ranges_all = sorted(
        complete_1m_ranges_for_paths([f"{exchange}/order_filled"])
        & complete_1m_ranges_for_paths([f"{exchange}/fee_charged"])
    )
    if not ranges_all:
        return -1

    ranges = sorted(set(ranges_all[:3] + ranges_all[-3:]))
    bad_total = 0

    for day_key in ranges:
        of_p = event_day_parquet(exchange, "order_filled", day_key)
        fc_p = event_day_parquet(exchange, "fee_charged", day_key)
        if of_p.exists() and fc_p.exists():
            bad_total += _mismatched_txs(con, of_p, fc_p)

    return bad_total


def test_ctfv2_fee_per_tx(con):
    """CTFExchangeV2: SUM(order_filled.fee) == SUM(fee_charged.amount) per transaction."""
    result = _run(con, "CTFExchangeV2")
    if result == -1:
        pytest.skip("no UTC days present in both CTFExchangeV2/order_filled and CTFExchangeV2/fee_charged")
    assert result == 0, (
        f"{result} CTFExchangeV2 transaction(s) have mismatched "
        "order_filled.fee vs fee_charged.amount"
    )


def test_negriskv2_fee_per_tx(con):
    """NegRiskCtfExchangeV2: SUM(order_filled.fee) == SUM(fee_charged.amount) per transaction."""
    result = _run(con, "NegRiskCtfExchangeV2")
    if result == -1:
        pytest.skip(
            "no UTC days present in both NegRiskCtfExchangeV2/order_filled "
            "and NegRiskCtfExchangeV2/fee_charged"
        )
    assert result == 0, (
        f"{result} NegRiskCtfExchangeV2 transaction(s) have mismatched "
        "order_filled.fee vs fee_charged.amount"
    )
