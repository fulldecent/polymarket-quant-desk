"""
Assert: SUM(order_filled.fee) = SUM(fee_charged.amount) per transaction,
for 1M ranges complete in both tables.

Processes one 10K partition at a time to avoid DuckDB memory pressure.
"""
import os
from pathlib import Path
from helpers import RAW, complete_1m_ranges_for


def _10k_dirs(contract, event, m_val):
    """Yield 10K partition values for a given contract/event/1M."""
    base = Path(RAW) / contract / event / f"1M={m_val}"
    if not base.exists():
        return
    for d in sorted(base.iterdir()):
        if d.is_dir() and d.name.startswith("10K="):
            yield int(d.name.split("=")[1])


def test_fee_reconciliation_per_tx(con):
    of_complete = complete_1m_ranges_for("order_filled")
    fc_complete = complete_1m_ranges_for("fee_charged")
    # Sample first and last 3 ranges to keep runtime bounded
    ranges_all = sorted(of_complete & fc_complete)

    if not ranges_all:
        import pytest
        pytest.skip("no 1M ranges complete in both order_filled and fee_charged")

    ranges = sorted(set(ranges_all[:3] + ranges_all[-3:]))

    bad_total = 0
    for r in ranges:
        # Get 10K partitions available in both order_filled and fee_charged
        ctf_of_10ks = set(_10k_dirs("CTFExchange", "order_filled", r))
        ctf_fc_10ks = set(_10k_dirs("CTFExchange", "fee_charged", r))
        nr_of_10ks = set(_10k_dirs("NegRiskCtfExchange", "order_filled", r))
        nr_fc_10ks = set(_10k_dirs("NegRiskCtfExchange", "fee_charged", r))

        all_10ks = sorted(ctf_of_10ks | nr_of_10ks)

        for k in all_10ks:
            of_paths = []
            fc_paths = []

            ctf_of = Path(RAW) / "CTFExchange" / "order_filled" / f"1M={r}" / f"10K={k}" / "data.parquet"
            ctf_fc = Path(RAW) / "CTFExchange" / "fee_charged" / f"1M={r}" / f"10K={k}" / "data.parquet"
            nr_of = Path(RAW) / "NegRiskCtfExchange" / "order_filled" / f"1M={r}" / f"10K={k}" / "data.parquet"
            nr_fc = Path(RAW) / "NegRiskCtfExchange" / "fee_charged" / f"1M={r}" / f"10K={k}" / "data.parquet"

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

            of_src = f"read_parquet([{', '.join(of_paths)}])" if len(of_paths) > 1 else f"read_parquet({of_paths[0]})"

            if not fc_paths:
                # If no fee_charged data, every non-zero fee is a mismatch
                bad = con.execute(f"""
                    SELECT COUNT(*) FROM (
                        SELECT transaction_hash
                        FROM {of_src}
                        GROUP BY transaction_hash
                        HAVING SUM(CAST(fee AS HUGEINT)) != 0
                    )
                """).fetchone()[0]
            else:
                fc_src = f"read_parquet([{', '.join(fc_paths)}])" if len(fc_paths) > 1 else f"read_parquet({fc_paths[0]})"
                bad = con.execute(f"""
                    SELECT COUNT(*) FROM (
                        WITH of_fees AS (
                            SELECT transaction_hash,
                                   SUM(CAST(fee AS HUGEINT)) AS total_of_fee
                            FROM {of_src}
                            GROUP BY transaction_hash
                        ),
                        fc_fees AS (
                            SELECT transaction_hash,
                                   SUM(CAST(amount AS HUGEINT)) AS total_fc_fee
                            FROM {fc_src}
                            GROUP BY transaction_hash
                        )
                        SELECT of_fees.transaction_hash
                        FROM of_fees
                        LEFT JOIN fc_fees
                            ON of_fees.transaction_hash = fc_fees.transaction_hash
                        WHERE of_fees.total_of_fee != COALESCE(fc_fees.total_fc_fee, 0)
                    )
                """).fetchone()[0]

            bad_total += bad

    assert bad_total == 0, (
        f"{bad_total} transactions have mismatched fee sums "
        f"between order_filled and fee_charged"
    )
