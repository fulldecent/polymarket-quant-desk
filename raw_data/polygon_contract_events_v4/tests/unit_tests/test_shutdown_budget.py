"""One stop signal must force the process out before the hot DB lock outlives it."""

import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parents[2]))
sys.path.insert(0, str(Path(__file__).resolve().parents[4]))

import main as scraper


def test_the_first_signal_waits_out_the_budget():
    deadline, force = scraper._shutdown_action(
        signals_seen=1, now=10.0, deadline=None, budget=5.0
    )

    assert deadline == 15.0
    assert force is False

    _deadline, force = scraper._shutdown_action(
        signals_seen=1, now=15.0, deadline=deadline, budget=5.0
    )

    assert force is True


def test_a_second_signal_exits_without_waiting():
    _deadline, force = scraper._shutdown_action(
        signals_seen=2, now=10.0, deadline=15.0, budget=5.0
    )

    assert force is True


def test_no_signal_does_not_exit():
    deadline, force = scraper._shutdown_action(
        signals_seen=0, now=10.0, deadline=None, budget=5.0
    )

    assert deadline is None
    assert force is False
