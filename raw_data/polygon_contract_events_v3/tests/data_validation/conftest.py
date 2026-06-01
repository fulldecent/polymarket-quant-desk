"""Shared pytest fixtures and progress reporting for v3 data validation."""
import os
import signal
import sys

import duckdb
import pytest

# Ensure the assertions directory is on sys.path so helpers.py can be imported
sys.path.insert(0, os.path.dirname(__file__))

from helpers import TEMP_DIR, complete_1m_ranges_for_paths, set_progress_callback


_TOTAL_TESTS = 0
_COMPLETED_TESTS = 0
_TERMINAL_REPORTER = None
_CAPTURE_MANAGER = None


def _is_data_validation_nodeid(nodeid: str) -> bool:
    return "raw_data/polygon_contract_events_v3/tests/data_validation/" in nodeid


def _write_progress_line(message: str) -> None:
    line = f"[data_validation] {message}"
    if _CAPTURE_MANAGER is not None:
        with _CAPTURE_MANAGER.global_and_fixture_disabled():
            print(line, file=sys.stderr, flush=True)
        return
    if _TERMINAL_REPORTER is not None:
        _TERMINAL_REPORTER.write_line(line)
        return
    print(line, file=sys.stderr, flush=True)


def pytest_configure(config: pytest.Config) -> None:
    global _TERMINAL_REPORTER, _CAPTURE_MANAGER
    _TERMINAL_REPORTER = config.pluginmanager.getplugin("terminalreporter")
    _CAPTURE_MANAGER = config.pluginmanager.getplugin("capturemanager")


def pytest_collection_finish(session: pytest.Session) -> None:
    global _TOTAL_TESTS, _COMPLETED_TESTS
    _COMPLETED_TESTS = 0
    _TOTAL_TESTS = sum(1 for item in session.items if _is_data_validation_nodeid(item.nodeid))
    if _TOTAL_TESTS:
        _write_progress_line(f"collected {_TOTAL_TESTS} test(s)")


def pytest_runtest_logreport(report: pytest.TestReport) -> None:
    global _COMPLETED_TESTS
    if report.when != "call" or not _is_data_validation_nodeid(report.nodeid):
        return

    _COMPLETED_TESTS += 1
    outcome = "passed" if report.passed else "failed" if report.failed else "skipped"
    percent = (_COMPLETED_TESTS * 100) // _TOTAL_TESTS if _TOTAL_TESTS else 100
    _write_progress_line(
        f"{_COMPLETED_TESTS}/{_TOTAL_TESTS} ({percent}%) {outcome}: {report.nodeid}"
    )


@pytest.fixture(scope="session", autouse=True)
def _install_helper_progress_callback() -> None:
    set_progress_callback(_write_progress_line)
    yield
    set_progress_callback(None)


@pytest.fixture(scope="session")
def con():
    """Session-scoped DuckDB connection with temp directory configured.

    Installs a SIGINT handler that calls con.interrupt() so Ctrl-C aborts
    long-running DuckDB queries immediately instead of being ignored.
    """
    os.makedirs(TEMP_DIR, exist_ok=True)
    c = duckdb.connect()
    c.execute(f"SET temp_directory = '{TEMP_DIR}'")
    c.execute("SET memory_limit = '4GB'")
    c.execute("SET threads = 4")

    original = signal.getsignal(signal.SIGINT)

    def _handle(sig, frame):
        try:
            c.interrupt()
        except Exception:
            pass
        # Re-raise so pytest sees KeyboardInterrupt
        raise KeyboardInterrupt()

    signal.signal(signal.SIGINT, _handle)
    yield c
    signal.signal(signal.SIGINT, original)
    c.close()


@pytest.fixture(scope="session")
def ranges():
    """Sorted list of complete 1M ranges for V1 exchange order_filled.

    Most legacy assertion queries rely on V1 fields like maker_asset_id and
    taker_asset_id, so this fixture intentionally scopes to V1 exchange paths.
    """
    r = sorted(
        complete_1m_ranges_for_paths(
            [
                "CTFExchange/order_filled",
                "NegRiskCtfExchange/order_filled",
            ]
        )
    )
    if not r:
        pytest.skip("no complete 1M partitions found")
    return r

