"""Polynode listen hub: one WebSocket per API key."""

from __future__ import annotations

import sys
from pathlib import Path

_project_root = Path(__file__).resolve().parents[2]
sys.path.insert(0, str(_project_root))

from exchange_client.lib.polynode_event_stream import (  # noqa: E402
    _FATAL_WS_HTTP,
    _hub_for,
    _ws_http_status,
)


def test_ws_http_status_from_message():
    assert _ws_http_status(Exception("server rejected WebSocket connection: HTTP 403")) == 403
    assert _ws_http_status(Exception("HTTP 429")) == 429
    assert _ws_http_status(Exception("timeout")) is None


def test_403_is_fatal_429_is_not():
    assert 403 in _FATAL_WS_HTTP
    assert 401 in _FATAL_WS_HTTP
    assert 429 not in _FATAL_WS_HTTP


def test_hub_is_shared_for_same_key():
    a = _hub_for("k", "wss://example")
    b = _hub_for("k", "wss://example")
    c = _hub_for("other", "wss://example")
    assert a is b
    assert a is not c
