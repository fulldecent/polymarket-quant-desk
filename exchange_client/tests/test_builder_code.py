"""Every v2 order we sign carries the builder profile code."""

from __future__ import annotations

import sys
from pathlib import Path

import pytest

_project_root = Path(__file__).resolve().parents[2]
sys.path.insert(0, str(_project_root))

from exchange_client.lib import trading_lib  # noqa: E402

CODE = "0x" + "ab" * 32


class _Recorder:
    def __init__(self) -> None:
        self.builder_codes: list[str] = []

    def build_market_order(self, args, options, version, fee_rate_bps):
        self.builder_codes.append(args.builder_code)
        return {"kind": "market"}

    def build_order(self, args, options, version, fee_rate_bps):
        self.builder_codes.append(args.builder_code)
        return {"kind": "limit"}


class _V2:
    def __init__(self) -> None:
        self.builder = _Recorder()


@pytest.fixture
def code(monkeypatch):
    monkeypatch.setenv("BUILDER_CODE", CODE.upper())
    trading_lib._builder_code = None
    yield CODE
    trading_lib._builder_code = None


def test_signers_stamp_builder_code(code):
    v2 = _V2()
    trading_lib._sign_v2_fok_buy(v2, "1", 2.0, max_price=0.5, neg_risk=False)
    trading_lib._sign_v2_fak_sell(
        v2, "1", 3.0, min_price=0.4, neg_risk=True, tick_size="0.01"
    )
    trading_lib._sign_v2_limit_sell(
        v2, "1", 3.0, 0.4, expiration=1_800_000_000, neg_risk=False, tick_size="0.01"
    )
    assert v2.builder.builder_codes == [code, code, code]


def test_builder_code_rejects_a_short_value(monkeypatch):
    monkeypatch.setenv("BUILDER_CODE", "0xabc")
    trading_lib._builder_code = None
    with pytest.raises(SystemExit):
        trading_lib.our_builder_code()


def test_builder_code_is_required(monkeypatch):
    monkeypatch.delenv("BUILDER_CODE", raising=False)
    trading_lib._builder_code = None
    with pytest.raises(SystemExit):
        trading_lib.our_builder_code()
