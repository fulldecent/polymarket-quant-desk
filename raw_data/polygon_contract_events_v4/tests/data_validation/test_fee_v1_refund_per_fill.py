"""
Assert: every v1 order_filled row in FeeModule-active partitions has exactly
one matching fee_refunded row by order_hash, and
    fee_refunded.refund + fee_refunded.fee_charged == order_filled.fee.

This tightens test_fee_reconciliation_per_tx.py from a per-transaction sum
check to a per-fill structural match (1:1 on order_hash, full decomposition).

FeeModuleCTF deployed at block 75,253,526 and FeeModuleNegRisk at 75,253,721.
Complete 1M ranges below those deployment blocks have no fee_refunded data, so
the intersection of complete ranges naturally excludes the pre-module period.
"""
from pathlib import Path

import pytest

from helpers import RAW, complete_1m_ranges_for_paths


def _available_10ks(path: Path) -> set[int]:
    if not path.exists():
        return set()
    return {
        int(d.name.split("=")[1])
        for d in path.iterdir()
        if d.is_dir() and d.name.startswith("10K=")
    }


def _bad_fills(con, of_path: Path, fr_path: Path) -> int:
    """Count order_filled rows that either lack a 1:1 fee_refunded match or
    fail the invariant refund + fee_charged == gross_fee."""
    return con.execute(f"""
        WITH of_fees AS (
            SELECT order_hash, CAST(fee AS HUGEINT) AS gross_fee
            FROM read_parquet('{of_path}')
        ),
        fr_agg AS (
            SELECT order_hash,
                   COUNT(*) AS cnt,
                   SUM(CAST(refund AS HUGEINT) + CAST(fee_charged AS HUGEINT)) AS total
            FROM read_parquet('{fr_path}')
            GROUP BY order_hash
        )
        SELECT COUNT(*)
        FROM of_fees
        LEFT JOIN fr_agg ON of_fees.order_hash = fr_agg.order_hash
        WHERE fr_agg.cnt IS NULL
           OR fr_agg.cnt != 1
           OR fr_agg.total != of_fees.gross_fee
    """).fetchone()[0]


def _run(con, exchange: str, fee_module: str) -> int:
    """Return total bad-fill count across sampled partitions, or -1 to skip."""
    ranges_all = sorted(
        complete_1m_ranges_for_paths([f"{exchange}/order_filled"])
        & complete_1m_ranges_for_paths([f"{fee_module}/fee_refunded"])
    )
    if not ranges_all:
        return -1

    ranges = sorted(set(ranges_all[:3] + ranges_all[-3:]))
    bad_total = 0

    for r in ranges:
        of_base = Path(RAW) / exchange / "order_filled" / f"1M={r}"
        fr_base = Path(RAW) / fee_module / "fee_refunded" / f"1M={r}"
        common = sorted(_available_10ks(of_base) & _available_10ks(fr_base))
        sampled = sorted(set(common[:2] + common[-2:]))

        for k in sampled:
            of_p = of_base / f"10K={k}" / "data.parquet"
            fr_p = fr_base / f"10K={k}" / "data.parquet"
            if of_p.exists() and fr_p.exists():
                bad_total += _bad_fills(con, of_p, fr_p)

    return bad_total


def test_ctf_fee_refunded_per_fill(con):
    """CTFExchange: every order_filled has exactly one fee_refunded with correct decomposition."""
    result = _run(con, "CTFExchange", "FeeModuleCTF")
    if result == -1:
        pytest.skip("no 1M ranges complete in both CTFExchange/order_filled and FeeModuleCTF/fee_refunded")
    assert result == 0, (
        f"{result} CTFExchange order_filled row(s) lack a 1:1 fee_refunded match "
        "or fail refund + fee_charged == gross fee"
    )


def test_negrisk_fee_refunded_per_fill(con):
    """NegRiskCtfExchange: every order_filled has exactly one fee_refunded with correct decomposition."""
    result = _run(con, "NegRiskCtfExchange", "FeeModuleNegRisk")
    if result == -1:
        pytest.skip("no 1M ranges complete in both NegRiskCtfExchange/order_filled and FeeModuleNegRisk/fee_refunded")
    assert result == 0, (
        f"{result} NegRiskCtfExchange order_filled row(s) lack a 1:1 fee_refunded match "
        "or fail refund + fee_charged == gross fee"
    )
