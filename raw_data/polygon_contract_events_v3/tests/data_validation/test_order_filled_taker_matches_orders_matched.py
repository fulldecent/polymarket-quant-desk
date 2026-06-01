"""
Assert: for every orders_matched row, the matching order_filled taker row
has identical maker_amount_filled and taker_amount_filled.

Checks a bounded sample: first + last 2 complete 1M ranges, first + last 2
10K partitions per range, to keep runtime O(seconds) rather than O(hours).
"""
import os
from pathlib import Path
from helpers import RAW


def _10k_dirs(contract, event, m_val):
    base = Path(RAW) / contract / event / f"1M={m_val}"
    if not base.exists():
        return []
    return sorted(int(d.name.split("=")[1]) for d in base.iterdir()
                  if d.is_dir() and d.name.startswith("10K="))


def test_order_filled_taker_matches_orders_matched(con, ranges):
    sampled = sorted(set(ranges[:2] + ranges[-2:]))
    mismatches = []
    for r in sampled:
        for contract in ("CTFExchange", "NegRiskCtfExchange"):
            all_10ks = _10k_dirs(contract, "orders_matched", r)
            if not all_10ks:
                continue
            # Sample first + last 2 10K partitions only
            for k in sorted(set(all_10ks[:2] + all_10ks[-2:])):
                om = Path(RAW) / contract / "orders_matched" / f"1M={r}" / f"10K={k}" / "data.parquet"
                of = Path(RAW) / contract / "order_filled"    / f"1M={r}" / f"10K={k}" / "data.parquet"
                if not om.exists() or not of.exists():
                    continue

                batch = con.execute(f"""
                    SELECT om.transaction_hash, om.taker_order_hash
                    FROM read_parquet('{om}') om
                    JOIN read_parquet('{of}') of
                      ON of.transaction_hash = om.transaction_hash
                     AND of.order_hash = om.taker_order_hash
                    WHERE om.maker_amount_filled != of.maker_amount_filled
                       OR om.taker_amount_filled != of.taker_amount_filled
                    LIMIT 10
                """).fetchall()
                mismatches.extend(batch)

        if len(mismatches) >= 50:
            break

    assert len(mismatches) == 0, (
        f"{len(mismatches)} taker order_filled rows have amounts "
        f"that differ from orders_matched"
    )
