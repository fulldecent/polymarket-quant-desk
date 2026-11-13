"""Chain-head fetch is bounded and reports attempt N/M."""

import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parents[2]))
sys.path.insert(0, str(Path(__file__).resolve().parents[4]))

import main as scraper
from _internal.rpc_client import HTTPError, JSONRPCError

CHAIN_HEAD_ATTEMPTS = scraper.CHAIN_HEAD_ATTEMPTS
_chain_head_retry_wait_sec = scraper._chain_head_retry_wait_sec
fetch_block_number_with_retries = scraper.fetch_block_number_with_retries


def test_chain_head_attempts_is_four():
    assert CHAIN_HEAD_ATTEMPTS == 4


def test_retry_waits_are_2_4_8_then_capped():
    assert _chain_head_retry_wait_sec(1) == 2.0
    assert _chain_head_retry_wait_sec(2) == 4.0
    assert _chain_head_retry_wait_sec(3) == 8.0
    assert _chain_head_retry_wait_sec(4) == 8.0


def test_success_on_first_try_does_not_retry():
    retries = []
    sleeps = []

    def get_block_number():
        return 95_000_000

    n = fetch_block_number_with_retries(
        get_block_number,
        sleep_fn=sleeps.append,
        on_retry=lambda *args: retries.append(args),
    )
    assert n == 95_000_000
    assert retries == []
    assert sleeps == []


def test_retryable_403_then_success_reports_1_of_4():
    calls = {"n": 0}
    retries = []

    def get_block_number():
        calls["n"] += 1
        if calls["n"] == 1:
            raise HTTPError(403)
        return 95_000_000

    n = fetch_block_number_with_retries(
        get_block_number,
        sleep_fn=lambda s: None,
        on_retry=lambda attempt, attempts, status, wait: retries.append(
            (attempt, attempts, status, wait)
        ),
    )
    assert n == 95_000_000
    assert retries == [(1, 4, "HTTP 403", 2.0)]


def test_four_403s_stop_and_raise_http_403():
    retries = []

    def get_block_number():
        raise HTTPError(403)

    try:
        fetch_block_number_with_retries(
            get_block_number,
            sleep_fn=lambda s: None,
            on_retry=lambda attempt, attempts, status, wait: retries.append(
                (attempt, attempts, status)
            ),
        )
    except RuntimeError as e:
        assert str(e) == "HTTP 403"
    else:
        raise AssertionError("expected RuntimeError after 4 attempts")

    assert retries == [
        (1, 4, "HTTP 403"),
        (2, 4, "HTTP 403"),
        (3, 4, "HTTP 403"),
    ]


def test_http_401_fails_immediately():
    retries = []

    def get_block_number():
        raise HTTPError(401)

    try:
        fetch_block_number_with_retries(
            get_block_number,
            sleep_fn=lambda s: None,
            on_retry=lambda *args: retries.append(args),
        )
    except HTTPError as e:
        assert e.status_code == 401
    else:
        raise AssertionError("expected HTTPError 401")
    assert retries == []


def test_non_rate_limit_jsonrpc_fails_immediately():
    def get_block_number():
        raise JSONRPCError(-32000, "server busy")

    try:
        fetch_block_number_with_retries(
            get_block_number,
            sleep_fn=lambda s: None,
        )
    except JSONRPCError:
        pass
    else:
        raise AssertionError("expected JSONRPCError")
