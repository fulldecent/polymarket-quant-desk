#!/usr/bin/env python3
"""
Show Polymarket positions and open orders (read-only).

This script intentionally does not perform write actions.
Use traders/liquidate/trader.py for cancel/sell/redeem/merge operations.

Usage:
    python3 positions_and_orders.py --execution-client clob
    python3 positions_and_orders.py --execution-client polynode

Environment variables (from .env):
    DATA_API_URL             data API base URL
    CLOB_API_URL             CLOB API base URL
    EOA_PRIVATE_KEY          signer private key
    CLOB_API_KEY             API key
    CLOB_SECRET              API secret
    CLOB_PASS_PHRASE         API passphrase
    POLYMARKET_PROXY_WALLET  account address
"""

from __future__ import annotations

import argparse
import sys

import requests

from lib.market_api import Position
from lib.polynode_provider import PolynodeProvider
from lib.proxy_provider import ProxyProvider
from lib.trading_lib import (
    build_client,
    explain_api_error,
    fetch_open_orders,
    find_mergeable_pairs_from_positions,
    fmt_usd,
    get_clob_api_url,
    get_data_api_url,
    get_funder_address,
    get_pol_balance,
    get_usdc_balance,
    get_usdc_balance_via_rpc,
    load_env,
    require_env,
    print_merge_opportunities,
    print_orders,
    print_positions,
)

load_env()
_POLYNODE_API_URL = "https://api.polynode.dev"


def _print_section_unavailable(label: str, reason: str) -> None:
    print(f"{label}: unavailable")
    print(f"  {reason}\n")


def _build_read_client(execution_client: str):
    """Return a client used for CLOB-backed read-only queries when applicable."""
    if execution_client == "clob":
        return build_client()
    return None


def _build_positions_provider(execution_client: str):
    """Return the positions provider, endpoint name, and endpoint URL."""
    if execution_client == "clob":
        return (
            ProxyProvider(get_data_api_url(), get_clob_api_url()),
            "data API",
            get_data_api_url(),
        )
    if execution_client == "polynode":
        return (
            PolynodeProvider(require_env("POLYNODE_API_KEY"), _POLYNODE_API_URL),
            "Polynode API",
            _POLYNODE_API_URL,
        )
    raise ValueError(f"unknown execution client: {execution_client}")


def _position_to_dict(position: Position) -> dict:
    return {
        "asset": position.asset,
        "conditionId": position.condition_id,
        "title": position.title,
        "outcome": position.outcome,
        "outcomeIndex": position.outcome_index,
        "size": position.size,
        "avgPrice": position.avg_price,
        "curPrice": position.cur_price,
        "initialValue": position.initial_value,
        "currentValue": position.current_value,
        "cashPnl": position.cash_pnl,
        "redeemable": position.redeemable,
        "negativeRisk": position.negative_risk,
        "oppositeAsset": position.opposite_asset,
    }


def _fetch_positions(provider, account: str, *, redeemable: bool | None = None) -> list[dict]:
    return [_position_to_dict(p) for p in provider.fetch_positions(account, redeemable=redeemable)]


def parse_args(argv: list[str] | None = None) -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description="Show positions, winners, mergeable pairs, and open orders.",
    )
    parser.add_argument(
        "--execution-client",
        required=True,
        choices=["clob", "polynode"],
        help="backend for account queries (required)",
    )
    return parser.parse_args(argv)


# ── Main ─────────────────────────────────────────────────────────────────────

def main(argv: list[str] | None = None) -> None:
    args = parse_args(argv)
    account = get_funder_address()
    clob_api_url = get_clob_api_url()
    min_value = 0.005
    provider, positions_endpoint_name, positions_endpoint_url = _build_positions_provider(
        args.execution_client
    )

    print(f"Account: {account}\n")

    client = _build_read_client(args.execution_client)

    if args.execution_client == "polynode":
        print(
            "execution-client=polynode selected; "
            "using the Polynode API for positions and Polygon RPC for balances.\n"
        )

    try:
        if client is not None:
            usdc = get_usdc_balance(client)
        else:
            usdc = get_usdc_balance_via_rpc(account)
        print(f"USDC balance: {fmt_usd(usdc)}")
    except Exception as exc:
        if client is not None:
            message = explain_api_error("CLOB API", clob_api_url, exc)
        else:
            message = str(exc)
        print(f"Could not fetch USDC balance: {message}", file=sys.stderr)

    try:
        pol = get_pol_balance()
        print(f"POL balance:  {pol:.4f} (signer)")
    except Exception as exc:
        print(f"Could not fetch POL balance: {exc}", file=sys.stderr)

    print()

    # ── Positions ────────────────────────────────────────────────────────

    open_pos: list[dict] | None = None
    try:
        open_pos = _fetch_positions(provider, account, redeemable=False)
        visible_pos = [
            p for p in open_pos
            if float(p.get("currentValue", 0)) >= min_value
        ]
        hidden = len(open_pos) - len(visible_pos)
        label = "Active positions (unresolved)"
        if hidden > 0:
            label += f" — {hidden} below {fmt_usd(min_value)} hidden"
        print_positions(visible_pos, label)
    except Exception as exc:
        if isinstance(exc, requests.exceptions.RequestException):
            reason = explain_api_error(
                positions_endpoint_name,
                positions_endpoint_url,
                exc,
            )
        else:
            reason = str(exc)
        _print_section_unavailable("Active positions (unresolved)", reason)

    try:
        redeemable_pos = [
            p for p in _fetch_positions(provider, account, redeemable=True)
            if float(p.get("curPrice", 0)) > 0
        ]
        print_positions(redeemable_pos, "Resolved winners (redeem for USDC)")
    except Exception as exc:
        if isinstance(exc, requests.exceptions.RequestException):
            reason = explain_api_error(
                positions_endpoint_name,
                positions_endpoint_url,
                exc,
            )
        else:
            reason = str(exc)
        _print_section_unavailable("Resolved winners (redeem for USDC)", reason)

    # ── Mergeable pairs ──────────────────────────────────────────────────

    if open_pos is None:
        _print_section_unavailable(
            "Mergeable pairs (YES+NO → USDC)",
            "skipped because active positions are unavailable",
        )
    else:
        mergeable = find_mergeable_pairs_from_positions(open_pos)
        print_merge_opportunities(mergeable)

    # ── Open orders ──────────────────────────────────────────────────────

    if client is None:
        _print_section_unavailable(
            "Open orders",
            "not implemented for the polynode backend in this CLI",
        )
        return

    try:
        orders = fetch_open_orders(client)
        print_orders(orders, "Open orders")
    except Exception as exc:
        message = explain_api_error("CLOB API", clob_api_url, exc)
        _print_section_unavailable("Open orders", message)


if __name__ == "__main__":
    main()
