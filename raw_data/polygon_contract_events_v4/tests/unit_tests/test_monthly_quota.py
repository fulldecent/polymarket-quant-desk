"""A monthly request-unit quota is a dead lane, not a rate limit."""

import json
import sys
from pathlib import Path

import pytest

sys.path.insert(0, str(Path(__file__).resolve().parents[2]))
sys.path.insert(0, str(Path(__file__).resolve().parents[4]))

import main as scraper
from _internal.rpc_client import HTTPError, RpcClient

QUOTA = (
    "You've reached your monthly quota of Request Units (RUs). "
    "Enable Pay-as-you-go to continue beyond your quota."
)


class _Response:
    def __init__(self, status: int, body: bytes) -> None:
        self.status = status
        self._body = body

    def getheader(self, name: str, default: str = "") -> str:
        return default

    def read(self) -> bytes:
        return self._body


class _Conn:
    def __init__(self, response: _Response) -> None:
        self._response = response
        self.requests = 0

    def request(self, method, path, body=None, headers=None) -> None:
        self.requests += 1

    def getresponse(self) -> _Response:
        return self._response


def _client(response: _Response) -> tuple[RpcClient, _Conn]:
    client = RpcClient("https://polygon-mainnet.core.chainstack.com/example")
    conn = _Conn(response)
    client._get_connection = lambda timeout: conn
    client._backoff_wait = lambda attempt: None
    return client, conn


def test_quota_body_on_403_is_not_retried():
    body = json.dumps(
        {"jsonrpc": "2.0", "id": 1, "error": {"code": -32005, "message": QUOTA}}
    ).encode()
    client, conn = _client(_Response(403, body))
    with pytest.raises(HTTPError, match="monthly request-unit quota exhausted") as caught:
        client.get_block_number()
    assert caught.value.is_retryable is False
    assert caught.value.status_code == 403
    assert conn.requests == 1


def test_bare_403_is_still_retried():
    client, conn = _client(_Response(403, b"forbidden"))
    with pytest.raises(HTTPError, match="^HTTP 403$") as caught:
        client.get_block_number()
    assert caught.value.is_retryable is True
    assert conn.requests == client.config.max_retries


def test_rps_429_is_still_retried():
    body = json.dumps(
        {
            "error": {
                "code": -32005,
                "message": "You've exceeded the RPS limit available on the current plan",
            }
        }
    ).encode()
    client, conn = _client(_Response(429, body))
    with pytest.raises(HTTPError, match="^HTTP 429$"):
        client.get_block_number()
    assert conn.requests == client.config.max_retries


def test_fetch_treats_quota_as_fatal_and_a_bare_403_as_throttle():
    quota = HTTPError(403, provider_message=QUOTA)
    result = scraper._fetch_range(
        from_block=1_000,
        to_block=1_010,
        rpc_client=_Raising(quota),
        sleep_after_response_sec=5.0,
    )
    assert result["status"] == "fatal"
    assert result["error_message"] == "HTTP 403, monthly request-unit quota exhausted"

    bare = scraper._fetch_range(
        from_block=1_000,
        to_block=1_010,
        rpc_client=_Raising(HTTPError(403)),
        sleep_after_response_sec=0.0,
    )
    assert bare["status"] == "throttled"
    assert bare["http_status"] == 403


def test_chain_head_does_not_retry_a_quota_403():
    retries = []

    def get_block_number():
        raise HTTPError(403, provider_message=QUOTA)

    with pytest.raises(HTTPError, match="monthly request-unit quota exhausted"):
        scraper.fetch_block_number_with_retries(
            get_block_number,
            sleep_fn=lambda _s: None,
            on_retry=lambda *args: retries.append(args),
        )
    assert retries == []


class _Raising:
    def __init__(self, error: Exception) -> None:
        self._error = error

    def get_logs(self, **kwargs):
        raise self._error

    def get_block_timestamps(self, block_numbers):
        return {int(n): 1_700_000_000 for n in block_numbers}
