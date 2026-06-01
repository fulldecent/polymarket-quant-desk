"""
Assert: within every transaction, no maker order_hash is filled more than once.

Checks a bounded sample: first + last 2 complete 1M ranges, first + last 2
10K partitions per range.
"""
from pathlib import Path
from helpers import RAW


def _10k_dirs(contract, event, m_val):
    base = Path(RAW) / contract / event / f"1M={m_val}"
    if not base.exists():
        return []
    return sorted(int(d.name.split("=")[1]) for d in base.iterdir()
                  if d.is_dir() and d.name.startswith("10K="))


def test_orders_matched_bundle_consistency(con, ranges):
    # Sample first + last 2 ranges to keep runtime bounded.
    sampled = sorted(set(ranges[:2] + ranges[-2:]))
    bad_total = 0
    for r in sampled:
        for contract in ("CTFExchange", "NegRiskCtfExchange"):
            all_10ks = _10k_dirs(contract, "orders_matched", r)
            if not all_10ks:
                continue
            for k in sorted(set(all_10ks[:2] + all_10ks[-2:])):
                om = Path(RAW) / contract / "orders_matched" / f"1M={r}" / f"10K={k}" / "data.parquet"
                of = Path(RAW) / contract / "order_filled"    / f"1M={r}" / f"10K={k}" / "data.parquet"
                if not om.exists() or not of.exists():
                    continue
                dupes = con.execute(f"""
                    SELECT COUNT(*) FROM (
                        WITH maker_fills AS (
                            SELECT of.transaction_hash, of.order_hash
                            FROM read_parquet('{of}') of
                            JOIN read_parquet('{om}') om
                              ON om.transaction_hash = of.transaction_hash
                            WHERE of.order_hash != om.taker_order_hash
                        )
                        SELECT transaction_hash, order_hash
                        FROM maker_fills
                        GROUP BY transaction_hash, order_hash
                        HAVING COUNT(*) > 1
                    )
                """).fetchone()[0]
                bad_total += dupes

    assert bad_total == 0, (
        f"{bad_total} duplicate maker order_hash(es) within a bundle"
    )
