"""
Assert: two known transactions have the expected FeeRefunded rows with correct
values, verified against Polygonscan.

TX 1 (NegRisk): 0x86F87CB694B82A63CDB70F604C9923E12210BBC8F7E36BA2118751B0D36D6115
  block 83,851,893 — BTC < $60K on March 13, MINT match type
  - Maker: gross fee 3,478,260, refund 3,478,260, feeCharged 0
  - Taker: gross fee 40,000,000, refund 39,458,310, feeCharged 541,690

TX 2 (CTF): 0x3DAB75C74D0752AFF1CA64430A1F23F089D14BAA40161BF43DC562DCE88F5A07
  block 83,750,000 — CTFExchange, 5 makers + 1 taker
  - All 5 makers: feeCharged 0 (100% refunded)
  - Taker: gross fee 4,200,000, refund 3,850,700, feeCharged 349,300
"""
from helpers import RAW


_TX1 = bytes.fromhex("86f87cb694b82a63cdb70f604c9923e12210bbc8f7e36ba2118751b0d36d6115")
_TX2 = bytes.fromhex("3dab75c74d0752aff1ca64430a1f23f089d14baa40161bf43dc562dce88f5a07")


def _partition_path(contract, event, block):
    m = (block // 1_000_000) * 1_000_000
    k = (block // 10_000) * 10_000
    return f"{RAW}/{contract}/{event}/1M={m}/10K={k}/data.parquet"


def _read_fee_refunded(con, contract, block, tx_hash):
    """Return list of (order_hash, refund, fee_charged) for a tx."""
    path = _partition_path(contract, "fee_refunded", block)
    return con.execute(f"""
        SELECT order_hash, refund, fee_charged
        FROM read_parquet('{path}')
        WHERE transaction_hash = $1
        ORDER BY log_index
    """, [tx_hash]).fetchall()


def _read_order_filled(con, contract, block, tx_hash):
    """Return list of (order_hash, fee) for a tx."""
    path = _partition_path(contract, "order_filled", block)
    return con.execute(f"""
        SELECT order_hash, fee
        FROM read_parquet('{path}')
        WHERE transaction_hash = $1
        ORDER BY log_index
    """, [tx_hash]).fetchall()


def test_tx1_negrisk_btc_fee_refunded(con):
    """TX1: NegRisk BTC market — 2 FeeRefunded rows (maker + taker)."""
    rows = _read_fee_refunded(con, "FeeModuleNegRisk", 83_851_893, _TX1)
    assert len(rows) == 2, f"expected 2 fee_refunded rows, got {len(rows)}"

    refunds = {(r[1], r[2]) for r in rows}
    assert ("3478260", "0") in refunds, "maker refund 3,478,260 / feeCharged 0 missing"
    assert ("39458310", "541690") in refunds, "taker refund 39,458,310 / feeCharged 541,690 missing"


def test_tx1_negrisk_btc_fee_reconciliation(con):
    """TX1: refund + feeCharged = gross fee from OrderFilled, joined on order_hash."""
    fr_rows = _read_fee_refunded(con, "FeeModuleNegRisk", 83_851_893, _TX1)
    of_rows = _read_order_filled(con, "NegRiskCtfExchange", 83_851_893, _TX1)

    assert len(fr_rows) == len(of_rows), (
        f"fee_refunded has {len(fr_rows)} rows but order_filled has {len(of_rows)}"
    )

    of_by_hash = {oh: fee for oh, fee in of_rows}
    for order_hash, refund, fee_charged in fr_rows:
        assert order_hash in of_by_hash, f"order_hash {order_hash.hex()} not in order_filled"
        gross_fee = of_by_hash[order_hash]
        assert int(refund) + int(fee_charged) == int(gross_fee), (
            f"order_hash {order_hash.hex()}: "
            f"refund {refund} + feeCharged {fee_charged} != gross fee {gross_fee}"
        )


def test_tx2_ctf_fee_refunded(con):
    """TX2: CTFExchange — 6 FeeRefunded rows (5 makers + 1 taker)."""
    rows = _read_fee_refunded(con, "FeeModuleCTF", 83_750_000, _TX2)
    assert len(rows) == 6, f"expected 6 fee_refunded rows, got {len(rows)}"

    maker_rows = [r for r in rows if r[2] == "0"]
    taker_rows = [r for r in rows if r[2] != "0"]

    assert len(maker_rows) == 5, f"expected 5 maker rows with feeCharged=0, got {len(maker_rows)}"
    assert len(taker_rows) == 1, f"expected 1 taker row, got {len(taker_rows)}"
    assert (taker_rows[0][1], taker_rows[0][2]) == ("3850700", "349300"), (
        f"taker row: expected refund=3850700 feeCharged=349300, got {taker_rows[0][1:]}"
    )


def test_tx2_ctf_fee_reconciliation(con):
    """TX2: refund + feeCharged = gross fee from OrderFilled, joined on order_hash."""
    fr_rows = _read_fee_refunded(con, "FeeModuleCTF", 83_750_000, _TX2)
    of_rows = _read_order_filled(con, "CTFExchange", 83_750_000, _TX2)

    assert len(fr_rows) == len(of_rows), (
        f"fee_refunded has {len(fr_rows)} rows but order_filled has {len(of_rows)}"
    )

    of_by_hash = {oh: fee for oh, fee in of_rows}
    for order_hash, refund, fee_charged in fr_rows:
        assert order_hash in of_by_hash, f"order_hash {order_hash.hex()} not in order_filled"
        gross_fee = of_by_hash[order_hash]
        assert int(refund) + int(fee_charged) == int(gross_fee), (
            f"order_hash {order_hash.hex()}: "
            f"refund {refund} + feeCharged {fee_charged} != gross fee {gross_fee}"
        )
