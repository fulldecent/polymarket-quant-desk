"""Header timestamps come from eth_getBlockByNumber, not interpolation."""

import sys
from pathlib import Path

import pytest

sys.path.insert(0, str(Path(__file__).resolve().parents[2]))

from _internal.rpc_client import JSONRPCError, RpcClient


def _client() -> RpcClient:
    return RpcClient("http://127.0.0.1:1")


def test_empty_input_makes_no_rpc_call():
    client = _client()
    calls = []
    client._rpc_batch = lambda payload: calls.append(payload) or []
    assert client.get_block_timestamps([]) == {}
    assert calls == []


def test_parses_unix_seconds_and_dedupes_blocks():
    client = _client()
    seen = []

    def fake_batch(calls):
        seen.append(calls)
        return [
            {"id": c["id"], "result": {"timestamp": hex(1_700_000_000 + c["id"])}}
            for c in calls
        ]

    client._rpc_batch = fake_batch
    got = client.get_block_timestamps([11, 10, 10])
    assert got == {10: 1_700_000_010, 11: 1_700_000_011}
    assert len(seen) == 1
    methods = {(c["method"], tuple(c["params"])) for c in seen[0]}
    assert methods == {
        ("eth_getBlockByNumber", ("0xa", False)),
        ("eth_getBlockByNumber", ("0xb", False)),
    }


def test_missing_header_is_fatal():
    client = _client()
    client._rpc_batch = lambda calls: [{"id": calls[0]["id"], "result": None}]
    with pytest.raises(JSONRPCError, match="missing header timestamp"):
        client.get_block_timestamps([10])
