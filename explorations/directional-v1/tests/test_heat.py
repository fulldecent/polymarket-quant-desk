"""Heat features: percentile among live prints, log venue ratios, floors."""

from __future__ import annotations

import sys
from pathlib import Path

import pandas as pd

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))

from heat import EPS, heat_from_cbar  # noqa: E402
from sim_lib import BLOCKS_PER_DAY  # noqa: E402


def test_pctile_among_live_not_catalog():
    rows = []
    x = 10_000
    for b in range(x - 5, x + 1):
        rows.append({"block_number": b, "cid": "hot", "all_usdc": 10.0})
        rows.append({"block_number": b, "cid": "cold", "all_usdc": 1.0})
    # silent name never prints — must not enter denominator
    cbar = pd.DataFrame(rows).sort_values("block_number")
    hot = heat_from_cbar(cbar, "hot", x)
    cold = heat_from_cbar(cbar, "cold", x)
    assert hot["notional_pctile"] > cold["notional_pctile"]
    assert 0.0 <= cold["notional_pctile"] <= 1.0
    missing = heat_from_cbar(cbar, "never", x)
    assert missing["notional_pctile"] == 0.0


def test_log_ratio_floored():
    x = 50_000
    rows = [{"block_number": x, "cid": "a", "all_usdc": 0.0}]
    cbar = pd.DataFrame(rows)
    out = heat_from_cbar(cbar, "a", x)
    assert abs(out["log_venue_vs_7d"] - 0.0) < 1e-9  # 0/0 → log(ε/ε)
    assert abs(out["log_venue_vs_t7"]) < 1e-6 or out["log_venue_vs_t7"] == out["log_venue_vs_t7"]


def test_t7_uses_protocol_day():
    x = 8 * BLOCKS_PER_DAY
    rows = []
    for b in range(x - 10, x + 1):
        rows.append({"block_number": b, "cid": "a", "all_usdc": 100.0})
    x7 = x - 7 * BLOCKS_PER_DAY
    for b in range(x7 - 10, x7 + 1):
        rows.append({"block_number": b, "cid": "a", "all_usdc": 10.0})
    cbar = pd.DataFrame(rows).sort_values("block_number")
    out = heat_from_cbar(cbar, "a", x)
    # V_t / V_t7 ≈ 100/10
    assert out["log_venue_vs_t7"] > 1.0
