"""
Assert: SUM(order_filled.fee) = SUM(fee_charged.amount) per transaction,
for UTC days present in both tables.

Processes one day at a time to avoid DuckDB memory pressure.
"""
from helpers import complete_1m_ranges_for_paths, event_day_parquet


def test_fee_reconciliation_per_tx(con):
    of_complete = complete_1m_ranges_for_paths(
        [
            "CTFExchange/order_filled",
            "NegRiskCtfExchange/order_filled",
        ]
    )
    fc_complete = complete_1m_ranges_for_paths(
        [
            "CTFExchange/fee_charged",
            "NegRiskCtfExchange/fee_charged",
        ]
    )
    # Sample first and last 3 ranges to keep runtime bounded
    ranges_all = sorted(of_complete & fc_complete)

    if not ranges_all:
        import pytest
        pytest.skip("no UTC days present in both order_filled and fee_charged")

    ranges = sorted(set(ranges_all[:3] + ranges_all[-3:]))

    bad_total = 0
    for day_key in ranges:
        of_paths = []
        fc_paths = []

        ctf_of = event_day_parquet("CTFExchange", "order_filled", day_key)
        ctf_fc = event_day_parquet("CTFExchange", "fee_charged", day_key)
        nr_of = event_day_parquet("NegRiskCtfExchange", "order_filled", day_key)
        nr_fc = event_day_parquet("NegRiskCtfExchange", "fee_charged", day_key)

        if ctf_of.exists():
            of_paths.append(f"'{ctf_of}'")
        if nr_of.exists():
            of_paths.append(f"'{nr_of}'")
        if ctf_fc.exists():
            fc_paths.append(f"'{ctf_fc}'")
        if nr_fc.exists():
            fc_paths.append(f"'{nr_fc}'")

        if not of_paths:
            continue

        of_src = (
            f"read_parquet([{', '.join(of_paths)}])"
            if len(of_paths) > 1
            else f"read_parquet({of_paths[0]})"
        )

        if not fc_paths:
            # If no fee_charged data, every non-zero fee is a mismatch
            bad = con.execute(f"""
                SELECT COUNT(*) FROM (
                    SELECT block_number, transaction_index
                    FROM {of_src}
                    GROUP BY block_number, transaction_index
                    HAVING SUM(CAST(fee AS HUGEINT)) != 0
                )
            """).fetchone()[0]
        else:
            fc_src = (
                f"read_parquet([{', '.join(fc_paths)}])"
                if len(fc_paths) > 1
                else f"read_parquet({fc_paths[0]})"
            )
            bad = con.execute(f"""
                SELECT COUNT(*) FROM (
                    WITH of_fees AS (
                        SELECT block_number, transaction_index,
                               SUM(CAST(fee AS HUGEINT)) AS total_of_fee
                        FROM {of_src}
                        GROUP BY block_number, transaction_index
                    ),
                    fc_fees AS (
                        SELECT block_number, transaction_index,
                               SUM(CAST(amount AS HUGEINT)) AS total_fc_fee
                        FROM {fc_src}
                        GROUP BY block_number, transaction_index
                    )
                    SELECT of_fees.block_number, of_fees.transaction_index
                    FROM of_fees
                    LEFT JOIN fc_fees
                        ON of_fees.block_number = fc_fees.block_number
                       AND of_fees.transaction_index = fc_fees.transaction_index
                    WHERE of_fees.total_of_fee != COALESCE(fc_fees.total_fc_fee, 0)
                )
            """).fetchone()[0]

        bad_total += bad

    assert bad_total == 0, (
        f"{bad_total} transactions have mismatched fee sums "
        f"between order_filled and fee_charged"
    )
