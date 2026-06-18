"""Polynode implementation of MarketDataProvider.

Uses the Polynode REST API (https://api.polynode.dev) for positions and
order book data.  Requires a POLYNODE_API_KEY.
"""

from __future__ import annotations

import requests

from .market_api import MarketDataProvider, OrderBook, Position, PriceLevel


class PolynodeProvider:
    """MarketDataProvider backed by the Polynode REST API."""

    def __init__(self, api_key: str, base_url: str = "https://api.polynode.dev"):
        self._base_url = base_url
        self._session = requests.Session()
        self._session.headers["x-api-key"] = api_key

    # ── positions ────────────────────────────────────────────────────────

    def fetch_positions(
        self, wallet: str, *, redeemable: bool | None = None
    ) -> list[Position]:
        params: dict = {
            "limit": 500,
            "sortBy": "CURRENT",
            "sortDirection": "DESC",
            "sizeThreshold": 0,
        }
        if redeemable is not None:
            params["redeemable"] = str(redeemable).lower()

        resp = self._session.get(
            f"{self._base_url}/v1/wallets/{wallet}/positions",
            params=params,
            timeout=30,
        )
        resp.raise_for_status()
        data = resp.json()

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
            for p in data.get("positions", [])
        ]

    # ── order book ───────────────────────────────────────────────────────

    def get_order_book(self, token_id: str) -> OrderBook:
        resp = self._session.get(
            f"{self._base_url}/v1/orderbook/{token_id}",
            timeout=30,
        )
        resp.raise_for_status()
        data = resp.json()

        # Polynode sorts bids ascending (best bid last) and asks descending
        # (best ask last).  Normalize to best-first for both.
        bids = [
            PriceLevel(price=float(b["price"]), size=float(b["size"]))
            for b in reversed(data.get("bids", []))
        ]
        asks = [
            PriceLevel(price=float(a["price"]), size=float(a["size"]))
            for a in reversed(data.get("asks", []))
        ]

        return OrderBook(
            bids=bids,
            asks=asks,
            min_order_size=float(data.get("min_order_size", 0)),
            tick_size=float(data.get("tick_size", "0.01")),
        )
