"""Naming the RPC endpoint on screen without putting the credential there."""

import sys
from pathlib import Path

import pytest

sys.path.insert(0, str(Path(__file__).resolve().parents[2]))
sys.path.insert(0, str(Path(__file__).resolve().parents[4]))

import main as scraper

DRPC = "https://lb.drpc.live/polygon/AmPj4tH4dkWWiCQVmCT_2I7_z4nrBGsR8bQ0ehXRfUMv"
INFURA = "https://polygon-mainnet.infura.io/v3/782ecce393224d4db12cd4f40221d1e9"
CHAINSTACK = "https://polygon-mainnet.core.chainstack.com/12f954879f8b2c10acd1c28736fc7b18"


def test_drpc_url_shows_the_host_and_a_masked_key():
    assert scraper._describe_rpc_endpoint(DRPC) == "lb.drpc.live AmP...UMv"


def test_the_word_in_the_path_is_not_mistaken_for_the_key():
    """``polygon`` sits in the same path as the key and must not win."""
    assert "polygon " not in scraper._describe_rpc_endpoint(DRPC)


def test_infura_style_url_skips_the_version_segment():
    assert scraper._describe_rpc_endpoint(INFURA) == "polygon-mainnet.infura.io 782...1e9"


def test_key_as_the_only_path_segment():
    assert scraper._describe_rpc_endpoint(CHAINSTACK) == (
        "polygon-mainnet.core.chainstack.com 12f...b18"
    )


def test_key_in_the_query_string():
    url = "https://rpc.example.com/polygon?apikey=Zx8Qw2Lm4Tn7Vb1Rk9Hs3Yd6Pf0Ju5C"

    assert scraper._describe_rpc_endpoint(url) == "rpc.example.com Zx8...u5C"


def test_url_with_no_key_shows_just_the_host():
    assert scraper._describe_rpc_endpoint("https://rpc.example.com/") == "rpc.example.com"


@pytest.mark.parametrize("url", [DRPC, INFURA, CHAINSTACK])
def test_the_full_key_never_appears(url):
    """The label lands in terminals, screenshots and screen recordings."""
    label = scraper._describe_rpc_endpoint(url)
    key = url.rsplit("/", 1)[-1]

    assert key not in label
    assert len(label) < len(url)


def test_higher_entropy_wins_between_two_long_segments():
    url = "https://rpc.example.com/verylongwordsegment/aB9x2Kq7Lm4Tn8Vb1Rk"

    assert scraper._describe_rpc_endpoint(url) == "rpc.example.com aB9...1Rk"
