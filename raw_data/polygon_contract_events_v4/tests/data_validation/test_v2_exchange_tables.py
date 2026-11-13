"""
Assertions for V2 exchange parquet tables.

These checks validate V2-specific schema and value invariants on the parquet
outputs under CTFExchangeV2/ and NegRiskCtfExchangeV2/.
"""

from pathlib import Path

from helpers import RAW


def _table_path(contract: str, event: str) -> str:
    return f"{RAW}/{contract}/{event}/**/*.parquet"


def _has_table(contract: str, event: str) -> bool:
    root = Path(RAW) / contract / event
    return root.exists() and any(root.rglob("*.parquet"))


def test_v2_order_filled_side_and_columns(con):
    for contract in ("CTFExchangeV2", "NegRiskCtfExchangeV2"):
        if not _has_table(contract, "order_filled"):
            continue

        p = _table_path(contract, "order_filled")
        cols = {row[0] for row in con.execute(f"DESCRIBE SELECT * FROM read_parquet('{p}')").fetchall()}

        assert "side" in cols
        assert "token_id" in cols
        assert "builder" in cols
        assert "metadata" in cols

        invalid_side_count = con.execute(f"""
            SELECT COUNT(*)
            FROM read_parquet('{p}')
            WHERE side NOT IN (0, 1)
        """).fetchone()[0]
        assert invalid_side_count == 0, f"{contract}/order_filled has invalid side values"


def test_v2_orders_matched_side_and_columns(con):
    for contract in ("CTFExchangeV2", "NegRiskCtfExchangeV2"):
        if not _has_table(contract, "orders_matched"):
            continue

        p = _table_path(contract, "orders_matched")
        cols = {row[0] for row in con.execute(f"DESCRIBE SELECT * FROM read_parquet('{p}')").fetchall()}

        assert "side" in cols
        assert "token_id" in cols

        invalid_side_count = con.execute(f"""
            SELECT COUNT(*)
            FROM read_parquet('{p}')
            WHERE side NOT IN (0, 1)
        """).fetchone()[0]
        assert invalid_side_count == 0, f"{contract}/orders_matched has invalid side values"


def test_v2_fee_charged_no_token_id(con):
    for contract in ("CTFExchangeV2", "NegRiskCtfExchangeV2"):
        if not _has_table(contract, "fee_charged"):
            continue

        p = _table_path(contract, "fee_charged")
        cols = {row[0] for row in con.execute(f"DESCRIBE SELECT * FROM read_parquet('{p}')").fetchall()}

        assert "token_id" not in cols, f"{contract}/fee_charged unexpectedly contains token_id"
        assert "receiver" in cols
        assert "amount" in cols
