"""
Shared pytest fixtures for polygon_contract_events_v2 assertions.
"""
import os
import signal
import sys

import duckdb
import pytest

# Ensure the assertions directory is on sys.path so helpers.py can be imported
sys.path.insert(0, os.path.dirname(__file__))

from helpers import TEMP_DIR, complete_1m_ranges


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
    """Sorted list of complete 1M ranges (based on order_filled)."""
    r = complete_1m_ranges("order_filled")
    if not r:
        pytest.skip("no complete 1M partitions found")
    return r

