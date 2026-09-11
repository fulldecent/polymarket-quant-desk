"""USDC.e → pUSD wrap + v2 approval plan."""

from __future__ import annotations

import sys
from pathlib import Path
from unittest.mock import patch

_project_root = Path(__file__).resolve().parents[2]
sys.path.insert(0, str(_project_root))

from exchange_client.lib import trading_lib  # noqa: E402

OWNER = "0xf288f5d335976af2efcb4a13a50e4fea52be1f4f"


def _encode(sig: str, *args: str) -> bytes:
    return f"{sig}:{','.join(args)}".encode()


def test_plan_approve_then_wrap_then_missing_approvals():
    with patch.object(trading_lib, "_cast_calldata", side_effect=_encode):
        txs = trading_lib.plan_wrap_and_v2_approvals(
            OWNER,
            wrap_raw=2_000_000,
            usdce_onramp_allowance=0,
            pusd_allowances={s: 0 for s in trading_lib._PUSD_SPENDERS},
            ctf_approvals={o: True for o in trading_lib._CTF_OPERATORS},
        )
    targets = [to for to, _, _ in txs]
    labels = [label for _, _, label in txs]
    assert targets[0] == trading_lib._USDC_E
    assert targets[1] == trading_lib._COLLATERAL_ONRAMP
    assert labels[0] == "approve USDC.e → onramp"
    assert labels[1].startswith("wrap $2.00 USDC.e")
    assert targets.count(trading_lib._PUSD) == len(trading_lib._PUSD_SPENDERS)
    assert all(to != trading_lib._CTF_ADDRESS for to, _, _ in txs)


def test_plan_skips_wrap_when_amount_zero():
    with patch.object(trading_lib, "_cast_calldata", side_effect=_encode):
        txs = trading_lib.plan_wrap_and_v2_approvals(
            OWNER,
            wrap_raw=0,
            usdce_onramp_allowance=0,
            pusd_allowances={s: trading_lib._MAX_UINT256 for s in trading_lib._PUSD_SPENDERS},
            ctf_approvals={o: True for o in trading_lib._CTF_OPERATORS},
        )
    assert txs == []


def test_plan_skips_onramp_approve_when_allowance_covers_wrap():
    with patch.object(trading_lib, "_cast_calldata", side_effect=_encode):
        txs = trading_lib.plan_wrap_and_v2_approvals(
            OWNER,
            wrap_raw=1_000_000,
            usdce_onramp_allowance=1_000_000,
            pusd_allowances={s: trading_lib._MAX_UINT256 for s in trading_lib._PUSD_SPENDERS},
            ctf_approvals={o: True for o in trading_lib._CTF_OPERATORS},
        )
    assert len(txs) == 1
    assert txs[0][0] == trading_lib._COLLATERAL_ONRAMP


def test_plan_sets_missing_ctf_operator():
    approvals = {o: True for o in trading_lib._CTF_OPERATORS}
    missing = trading_lib._CTF_EXCHANGE_V2
    approvals[missing] = False
    with patch.object(trading_lib, "_cast_calldata", side_effect=_encode):
        txs = trading_lib.plan_wrap_and_v2_approvals(
            OWNER,
            wrap_raw=0,
            usdce_onramp_allowance=0,
            pusd_allowances={s: trading_lib._MAX_UINT256 for s in trading_lib._PUSD_SPENDERS},
            ctf_approvals=approvals,
        )
    assert len(txs) == 1
    assert txs[0][0] == trading_lib._CTF_ADDRESS
    assert txs[0][2].startswith("CTF setApprovalForAll")


def test_usd_to_raw_rounds_micros():
    assert trading_lib._usd_to_raw(2) == 2_000_000
    assert trading_lib._usd_to_raw(2.02) == 2_020_000
    assert trading_lib._usd_to_raw(166.760707) == 166_760_707


def test_plan_approves_negrisk_adapter_for_pusd():
    assert trading_lib._NEGRISK_ADAPTER in trading_lib._PUSD_SPENDERS
    with patch.object(trading_lib, "_cast_calldata", side_effect=_encode):
        txs = trading_lib.plan_wrap_and_v2_approvals(
            OWNER,
            wrap_raw=0,
            usdce_onramp_allowance=0,
            pusd_allowances={
                s: 0 if s == trading_lib._NEGRISK_ADAPTER else trading_lib._MAX_UINT256
                for s in trading_lib._PUSD_SPENDERS
            },
            ctf_approvals={o: True for o in trading_lib._CTF_OPERATORS},
        )
    assert len(txs) == 1
    assert txs[0][0] == trading_lib._PUSD
    spender = trading_lib._NEGRISK_ADAPTER
    assert txs[0][2] == f"approve pUSD → {spender[:6]}…{spender[-4:]}"
