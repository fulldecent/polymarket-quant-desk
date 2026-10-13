"""The current day is a seconds bar sitting immediately above total progress."""

import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parents[2]))
sys.path.insert(0, str(Path(__file__).resolve().parents[4]))

import main as scraper
from lib.ymd_partition import SECONDS_PER_DAY, utc_midnight


DAY = 20241019


def test_an_open_day_is_the_fraction_of_86400_seconds_reached():
    midnight = utc_midnight(DAY)

    assert scraper._day_bar_progress(DAY, None, closed=False) == (0, SECONDS_PER_DAY)
    assert scraper._day_bar_progress(DAY, midnight, closed=False) == (0, SECONDS_PER_DAY)
    assert scraper._day_bar_progress(DAY, midnight + 43_200, closed=False) == (
        43_200,
        SECONDS_PER_DAY,
    )
    assert scraper._day_bar_progress(DAY, midnight - 5, closed=False) == (0, SECONDS_PER_DAY)
    assert scraper._day_bar_progress(DAY, midnight + 90_000, closed=False) == (
        SECONDS_PER_DAY,
        SECONDS_PER_DAY,
    )


def test_a_closed_day_fills_the_bar():
    assert scraper._day_bar_progress(DAY, None, closed=True) == (
        SECONDS_PER_DAY,
        SECONDS_PER_DAY,
    )


def _column_text(progress, task) -> str:
    column = next(c for c in progress.columns if isinstance(c, scraper._DoneOfTotalColumn))
    return column.render(task).plain


def test_the_day_bar_is_the_row_immediately_above_total_progress():
    ui = scraper.RichRunStatus()
    ui.spinner("startup", "Opening hot database")
    ui.scrape("scrape:0", "Scraping host  1 blk/s")
    ui.bar("main", "Total progress", 10, 100)
    ui.bar("partition", "2024-10-19", 12 * 3600 + 34 * 60, SECONDS_PER_DAY, clock=True)

    rows = ui._row_groups()

    assert rows[-2] is ui._bars["partition"]
    assert rows[-1] is ui._bars["main"]
    assert "partition" not in ui._spinner_tasks
    assert _column_text(ui._bars["partition"], ui._bars["partition"].tasks[0]) == "12:34/24:00"
    assert _column_text(ui._bars["main"], ui._bars["main"].tasks[0]) == "10/100"


def test_day_clock_truncates_to_the_minute_and_a_full_day_is_24_00():
    assert scraper._format_day_clock(0) == "00:00"
    assert scraper._format_day_clock(59) == "00:00"
    assert scraper._format_day_clock(60) == "00:01"
    assert scraper._format_day_clock(SECONDS_PER_DAY) == "24:00"
