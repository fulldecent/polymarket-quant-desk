"""Several POLYGON_RPC_URL values become independent scrape lanes."""

import sys
from dataclasses import replace
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parents[2]))
sys.path.insert(0, str(Path(__file__).resolve().parents[4]))

import main as scraper

DRPC = "https://lb.drpc.live/polygon/AmPj4tH4dkWWiCQVmCT_2I7_z4nrBGsR8bQ0ehXRfUMv"
INFURA = "https://polygon-mainnet.infura.io/v3/782ecce393224d4db12cd4f40221d1e9"
CHAINSTACK = "https://polygon-mainnet.core.chainstack.com/12f954879f8b2c10acd1c28736fc7b18"


def _lane(*, index: int = 0, url: str = DRPC, label: str | None = None) -> scraper._RpcLane:
    return scraper._RpcLane(
        index=index,
        url=url,
        label=label or scraper._describe_rpc_endpoint(url),
        client=object(),  # type: ignore[arg-type]
    )


def test_a_single_url_is_one_entry():
    assert scraper._parse_rpc_urls(DRPC) == [DRPC]


def test_comma_separated_urls():
    raw = f"{CHAINSTACK},{DRPC}"
    assert scraper._parse_rpc_urls(raw) == [CHAINSTACK, DRPC]


def test_comma_and_space_separated_urls():
    raw = f"{CHAINSTACK}, {DRPC} {INFURA}"
    assert scraper._parse_rpc_urls(raw) == [CHAINSTACK, DRPC, INFURA]


def test_whitespace_only_is_also_a_delimiter():
    raw = f"{DRPC}  {INFURA}"
    assert scraper._parse_rpc_urls(raw) == [DRPC, INFURA]


def test_duplicate_urls_collapse_in_order():
    raw = f"{DRPC}, {CHAINSTACK}, {DRPC}"
    assert scraper._parse_rpc_urls(raw) == [DRPC, CHAINSTACK]


def test_empty_tokens_are_dropped():
    assert scraper._parse_rpc_urls(f" , {DRPC}, , ") == [DRPC]
    assert scraper._parse_rpc_urls("  ,  ") == []


def test_quoted_tokens_are_unquoted():
    assert scraper._parse_rpc_urls(f'"{DRPC}", {INFURA}') == [DRPC, INFURA]


def test_http_and_https_look_like_rpc_urls():
    assert scraper._looks_like_rpc_url("https://example.com/key")
    assert scraper._looks_like_rpc_url("http://127.0.0.1:8545")
    assert not scraper._looks_like_rpc_url("example.com/key")
    assert not scraper._looks_like_rpc_url("ws://example.com")


def test_labels_stay_distinct_when_two_urls_mask_the_same_way():
    a = "https://rpc.example.com/abcdef123456def"
    b = "https://rpc.example.com/abcdef789012def"
    assert scraper._describe_rpc_endpoint(a) == scraper._describe_rpc_endpoint(b)
    labels = scraper._unique_endpoint_labels([a, b])
    assert labels[0] != labels[1]
    assert labels[0].endswith("(1)")
    assert labels[1].endswith("(2)")


def test_typical_providers_keep_their_masked_labels():
    labels = scraper._unique_endpoint_labels([DRPC, INFURA, CHAINSTACK])
    assert labels == [
        scraper._describe_rpc_endpoint(DRPC),
        scraper._describe_rpc_endpoint(INFURA),
        scraper._describe_rpc_endpoint(CHAINSTACK),
    ]


def test_http_401_is_one_key_dying_not_the_process():
    assert not scraper._fetch_fatal_is_process_wide(
        {"error_message": "HTTP 401 from the RPC provider"}
    )
    assert not scraper._fetch_fatal_is_process_wide(
        {"error_message": "header fetch: HTTP 401 from the RPC provider"}
    )


def test_decode_and_missing_header_still_stop_the_process():
    assert scraper._fetch_fatal_is_process_wide(
        {"error_message": "decode failure: ValueError: bad log"}
    )
    assert scraper._fetch_fatal_is_process_wide(
        {"error_message": "missing header timestamp for blocks [1, 2]"}
    )


def test_one_dead_key_does_not_make_the_run_fatal():
    live = _lane(index=0, url=DRPC)
    dead = _lane(index=1, url=CHAINSTACK)
    dead.fatal_reason = "HTTP 401 from the RPC provider"
    assert not scraper._all_rpc_lanes_fatal([live, dead])
    live.fatal_reason = "HTTP 403"
    assert scraper._all_rpc_lanes_fatal([live, dead])
    assert scraper._all_lanes_fatal_message([live, dead]).startswith(
        "all RPC endpoints failed:"
    )


def test_footer_line_spins_while_working_and_says_failed_when_dead():
    lane = _lane(label="lb.drpc.live AmP...UMv")
    lane.blocks_done = 1_000
    lane.optimizer._active = replace(lane.optimizer.params, workers=3)
    working = scraper._lane_footer_text(lane, run_seconds=10.0)
    assert working.startswith("3× Scraping lb.drpc.live AmP...UMv  100 blk/s")
    assert "threads" not in working
    assert "max blks/req" in working

    lane.fatal_reason = "HTTP 401 from the RPC provider"
    failed = scraper._lane_footer_text(lane, run_seconds=10.0)
    assert failed == "3× Failed lb.drpc.live AmP...UMv  HTTP 401 from the RPC provider"


def test_failed_scrape_row_uses_the_stopped_glyph_and_stays_failed():
    ui = scraper.RichRunStatus()
    ui.scrape("scrape:0", "Scraping host key  0 blk/s")
    ui.scrape("scrape:1", "Failed host key  HTTP 401", failed=True)

    live_task, dead_task = ui._scrape.tasks
    glyph = ui._scrape.columns[0]
    assert not live_task.fields.get("failed")
    assert dead_task.fields.get("failed")
    assert glyph.render(live_task) != scraper._SCRAPE_FAILED_GLYPH
    assert glyph.render(dead_task) == scraper._SCRAPE_FAILED_GLYPH

    ui.scrape("scrape:1", "Scraping host key  12 blk/s")
    dead_task = next(t for t in ui._scrape.tasks if t.id == dead_task.id)
    assert dead_task.fields.get("failed")
    assert dead_task.description.startswith("Failed")
