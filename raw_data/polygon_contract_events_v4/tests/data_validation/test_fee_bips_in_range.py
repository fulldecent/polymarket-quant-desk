"""
Assert: every ``NegRiskAdapter/market_prepared.fee_bips`` value lies in
``[0, 10_000]``.

Basis points run from 0 to 10_000 (= 100%). Any value outside that range
is a contract violation.

Scope: ``NegRiskAdapter/market_prepared`` only — the sole event that
carries ``fee_bips``.
"""
from helpers import glob_all


def test_fee_bips_in_range(con):
    src = glob_all("market_prepared")
    bad = con.execute(f"""
        SELECT fee_bips, COUNT(*) AS n
        FROM {src}
        WHERE fee_bips < 0 OR fee_bips > 10000
        GROUP BY fee_bips
        ORDER BY n DESC
        LIMIT 5
    """).fetchall()
    assert not bad, (
        f"{len(bad)} distinct fee_bips value(s) in market_prepared outside "
        f"[0, 10000]; first offenders (fee_bips, occurrences): {bad[:5]}"
    )
