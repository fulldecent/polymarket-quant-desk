"""How one fetch turns provider behaviour into an outcome the optimizer can act on."""

import sys
import time
from pathlib import Path

import pytest

sys.path.insert(0, str(Path(__file__).resolve().parents[2]))
sys.path.insert(0, str(Path(__file__).resolve().parents[4]))

import main as scraper
from _internal.errors import OperationCancelled
from _internal.rpc_client import HTTPError, JSONRPCError, RPCTimeout, TooManyResults


class FakeRpcClient:
    """Stands in for the provider: returns logs, or raises what a provider would."""

    def __init__(self, *, logs=None, error=None) -> None:
        self._logs = logs if logs is not None else []
        self._error = error
        self.calls = 0

    def get_logs(self, **kwargs):
        self.calls += 1
        if self._error is not None:
            raise self._error
        return list(self._logs)


def fetch(client, sleep_after_response_sec=0.0):
    return scraper._fetch_range(
        from_block=1_000,
        to_block=1_099,
        rpc_client=client,
        sleep_after_response_sec=sleep_after_response_sec,
    )


def test_successful_fetch_reports_ok():
    result = fetch(FakeRpcClient(logs=[]))

    assert result["status"] == "ok"
    assert result["raw_log_count"] == 0
    assert result["rows_by_target"] == {}


def test_oversized_range_asks_the_caller_to_split():
    assert fetch(FakeRpcClient(error=TooManyResults("too many"))) ["status"] == "split"


def test_timeout_is_its_own_outcome():
    assert fetch(FakeRpcClient(error=RPCTimeout("timed out")))["status"] == "timeout"


def test_rate_limit_is_throttling_and_carries_the_status_code():
    result = fetch(FakeRpcClient(error=HTTPError(429)))

    assert result["status"] == "throttled"
    assert result["http_status"] == 429


@pytest.mark.parametrize("status_code", [500, 502, 503, 504])
def test_server_errors_are_throttling_not_failures(status_code):
    result = fetch(FakeRpcClient(error=HTTPError(status_code)))

    assert result["status"] == "throttled"
    assert result["http_status"] == status_code


@pytest.mark.parametrize("status_code", [400, 401, 403, 404])
def test_permanent_http_errors_fail_fast(status_code):
    result = fetch(FakeRpcClient(error=HTTPError(status_code)))

    assert result["status"] == "fatal", "retrying bad credentials or a bad request never helps"
    assert str(status_code) in result["error_message"]


def test_jsonrpc_error_is_retryable_by_the_caller():
    result = fetch(FakeRpcClient(error=JSONRPCError(-32000, "server busy")))

    assert result["status"] == "error"
    assert "server busy" in result["error_message"]


def test_cancellation_is_not_reported_as_a_failure():
    assert fetch(FakeRpcClient(error=OperationCancelled("stopping")))["status"] == "cancelled"


def test_thread_sleeps_after_a_response_without_inflating_the_measured_latency():
    client = FakeRpcClient(logs=[])

    started = time.monotonic()
    result = fetch(client, sleep_after_response_sec=0.25)
    wall = time.monotonic() - started

    assert wall >= 0.25, "the throttle must actually hold the worker thread"
    assert result["elapsed_sec"] < 0.25, "the sleep is not part of the provider's response time"


def test_thread_also_sleeps_after_an_error_response():
    started = time.monotonic()
    fetch(FakeRpcClient(error=HTTPError(429)), sleep_after_response_sec=0.25)

    assert time.monotonic() - started >= 0.25, (
        "backing off matters most exactly when the provider is complaining"
    )


def test_stop_event_cuts_the_sleep_short():
    scraper._stop_event.set()
    try:
        started = time.monotonic()
        fetch(FakeRpcClient(logs=[]), sleep_after_response_sec=5.0)
        assert time.monotonic() - started < 1.0
    finally:
        scraper._stop_event.clear()
