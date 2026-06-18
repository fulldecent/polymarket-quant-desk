"""Proxy implementation of MarketDataProvider.

Uses the existing Polymarket data API (through the local proxy) and the
CLOB API for order book data.  This is the original backend — it wraps
the same endpoints that trading_lib.py calls directly.
"""

from __future__ import annotations

import requests

from .market_api import MarketDataProvider, OrderBook, Position, PriceLevel


class ProxyProvider:
    """MarketDataProvider backed by the local Polymarket proxy."""

    def __init__(self, data_api_url: str, clob_api_url: str):
        self._data_api_url = data_api_url
        self._clob_api_url = clob_api_url

    # ── positions ────────────────────────────────────────────────────────

    def fetch_positions(
        self, wallet: str, *, redeemable: bool | None = None
    ) -> list[Position]:
        params: dict = {
            "user": wallet,
            "sizeThreshold": "0",
            "limit": "500",
            "sortBy": "CURRENT",
            "sortDirection": "DESC",
        }
        if redeemable is not None:
            params["redeemable"] = str(redeemable).lower()

        resp = requests.get(f"{self._data_api_url}/positions", params=params)
        resp.raise_for_status()

        return [
            Position(
                asset=p["asset"],
                condition_id=p["conditionId"],
                title=p.get("title", ""),
                outcome=p.get("outcome", ""),
                outcome_index=p.get("outcomeIndex", 0),
                size=float(p.get("size", 0)),
                avg_price=float(p.get("avgPrice", 0)),
                cur_price=float(p.get("curPrice", 0)),
                initial_value=float(p.get("initialValue", 0)),
                current_value=float(p.get("currentValue", 0)),
                cash_pnl=float(p.get("cashPnl", 0)),
                redeemable=bool(p.get("redeemable", False)),
                negative_risk=bool(p.get("negativeRisk", False)),
                opposite_asset=p.get("oppositeAsset", ""),
            )
            for p in resp.json()
        ]

    # ── order book ───────────────────────────────────────────────────────

    def get_order_book(self, token_id: str) -> OrderBook:
        resp = requests.get(f"{self._clob_api_url}/book", params={"token_id": token_id})
        resp.raise_for_status()
        data = resp.json()

        # The CLOB API returns bids best-first (descending) and asks
        # best-first (ascending), which matches our convention already.
        bids = [
            PriceLevel(price=float(b["price"]), size=float(b["size"]))
            for b in data.get("bids", [])
        ]
        asks = [
            PriceLevel(price=float(a["price"]), size=float(a["size"]))
            for a in data.get("asks", [])
        ]

        return OrderBook(
            bids=bids,
            asks=asks,
            min_order_size=float(data.get("min_order_size", 0)),
            tick_size=float(data.get("tick_size", "0.01")),
        )
