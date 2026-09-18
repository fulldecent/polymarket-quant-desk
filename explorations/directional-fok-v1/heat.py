"""Causal heat features: which book, which session. Not calendar dummies."""

from __future__ import annotations

import numpy as np
import pandas as pd

from sim_lib import BLOCKS_PER_DAY, LOOKBACK_BLOCKS

HEAT_KEYS = ("notional_pctile", "log_venue_vs_7d", "log_venue_vs_t7")
EPS = 1e-6
HEAT_DAYS = 7


def heat_from_cbar(cbar: pd.DataFrame, cid: str, x: int) -> dict:
    """Percentile among tape-touched names + venue log-ratios. block ≤ X."""
    out = {k: 0.0 for k in HEAT_KEYS}
    if cbar is None or cbar.empty:
        return out
    bn = cbar["block_number"].to_numpy(dtype=np.int64)
    lo = int(np.searchsorted(bn, x - (LOOKBACK_BLOCKS - 1), side="left"))
    hi = int(np.searchsorted(bn, x, side="right"))
    sl = cbar.iloc[lo:hi]
    if sl.empty:
        return out
    by = sl.groupby("cid", sort=False)["all_usdc"].sum()
    live = by[by > 0]
    n_live = int(len(live))
    this = float(by.get(cid, 0.0))
    if n_live <= 1:
        out["notional_pctile"] = 1.0 if this > 0 else 0.0
    else:
        out["notional_pctile"] = float((live.to_numpy() <= this).sum() / n_live)
    v_t = float(by.sum())

    def venue_at(xx: int) -> float:
        i0 = int(np.searchsorted(bn, xx - (LOOKBACK_BLOCKS - 1), side="left"))
        i1 = int(np.searchsorted(bn, xx, side="right"))
        if i1 <= i0:
            return 0.0
        return float(cbar.iloc[i0:i1]["all_usdc"].sum())

    past = []
    for d in range(1, HEAT_DAYS + 1):
        past.append(venue_at(x - d * BLOCKS_PER_DAY))
    med = float(np.median(past)) if past else 0.0
    v_t7 = past[HEAT_DAYS - 1] if len(past) >= HEAT_DAYS else 0.0
    out["log_venue_vs_7d"] = float(np.log((v_t + EPS) / (med + EPS)))
    out["log_venue_vs_t7"] = float(np.log((v_t + EPS) / (v_t7 + EPS)))
    return out


def attach_heat(events: list[dict], cbar: pd.DataFrame) -> None:
    if cbar is None or cbar.empty or not events:
        return
    cbar = cbar.sort_values("block_number")
    cache: dict[tuple[int, str], dict] = {}
    for e in events:
        x, cid = int(e["x_block"]), e["cid"]
        key = (x, cid)
        if key not in cache:
            cache[key] = heat_from_cbar(cbar, cid, x)
        feat = e.get("feat")
        if feat is None:
            continue
        feat.update(cache[key])
