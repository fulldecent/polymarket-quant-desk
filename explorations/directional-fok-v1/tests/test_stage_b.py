"""Stage B families, window features, hard-subset definition."""

from __future__ import annotations

import sys
from pathlib import Path

import pandas as pd

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))

from sim_lib import BUCKETS, LOOKBACK_BLOCKS  # noqa: E402
from stage_b import (  # noqa: E402
    CBAR_COLS,
    FAMILIES,
    FEAT_KEYS,
    KEEP_ALWAYS,
    feat_keys,
    feat_window,
    hard_mask,
    stride_take,
)


def test_families_partition_feat_keys():
    keys = feat_keys()
    assert keys == FEAT_KEYS
    flat = [c for cols in FAMILIES.values() for c in cols]
    assert flat == keys
    assert len(set(keys)) == len(keys)
    assert set(KEEP_ALWAYS).issubset(FAMILIES)
    for b in BUCKETS:
        assert f"n_{b}" in keys
        assert f"range_ticks_{b}" in keys
        assert f"ret_{b}" in keys
        assert f"imbalance_{b}" in keys
        assert f"mt_abs_med_{b}" in keys


def test_feat_window_keys_and_empty():
    empty = feat_window(pd.DataFrame(columns=list(CBAR_COLS)), 100, 0.50, 0.01)
    assert set(empty) == set(FEAT_KEYS)
    assert empty["last"] == 0.50
    assert empty["tick"] == 0.01
    assert empty["min_p_1mp"] == 0.50
    assert empty["n_print_blocks"] == 0.0
    assert empty["blocks_since_floor"] == float(LOOKBACK_BLOCKS)


def test_feat_window_from_one_bar():
    sl = pd.DataFrame(
        {
            "block_number": [96, 100],
            "close_yes": [0.48, 0.50],
            "high_yes": [0.49, 0.51],
            "low_yes": [0.47, 0.49],
            "n_fills": [4, 6],
            "n_accounts": [2, 3],
            "n_makers": [1, 2],
            "up_usdc": [1.0, 2.0],
            "dn_usdc": [0.5, 0.0],
            "ask_usdc": [1.2, 3.0],
            "bid_usdc": [0.4, 0.8],
            "taker_usdc": [1.5, 2.0],
            "all_usdc": [3.0, 5.0],
            "max_taker_buy_usdc": [1.0, 2.0],
            "ask_shares": [5.0, 10.0],
            "ask_floor": [True, True],
            "vwap_yes": [0.48, 0.50],
            "fee_usdc": [0.0, 1.0],
            "neg_risk": [0, 1],
            "mt_abs_med": [0.01, 0.02],
        }
    )
    out = feat_window(sl, 100, 0.50, 0.01)
    assert out["fee_bit"] == 1.0
    assert out["neg_risk"] == 1.0
    assert out["ask_x"] == 3.0
    assert out["persist_floors_5"] == 2.0
    assert out["persist_8"] == 2.0
    assert out["n_print_blocks"] == 2.0
    # ret_b uses last close at block <= X-b. Bar 96 is <= 99 and not <= 95.
    assert out["ret_5"] == 0.0
    assert abs(out["ret_1"] - 0.02) < 1e-12
    assert out["range_100"] > 0
    assert out["dist_high_ticks"] == (0.51 - 0.50) / 0.01
    assert out["n_makers_x"] == 2.0
    assert out["depth_over_nmin"] == 10.0 / 5.0


def test_stride_take_is_not_a_prefix():
    idx = stride_take(1000, 5)
    assert list(idx) == [0, 249, 499, 749, 999]
    assert len(stride_take(10, 0)) == 10
    assert len(stride_take(10, 50)) == 10


def test_hard_mask_is_breakout_not_already_swung():
    import numpy as np

    range_100 = np.array([0.005, 0.02, 0.01])
    tick = np.array([0.01, 0.01, 0.01])
    m = hard_mask(range_100, tick, 1)
    assert list(m) == [True, False, False]
    m2 = hard_mask(range_100, tick, 2)
    assert list(m2) == [True, False, True]
