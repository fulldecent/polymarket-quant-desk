"""Market data protocol — lowest common denominator between the Polymarket
data/CLOB APIs and the Polynode API.

New scripts should code against MarketDataProvider rather than calling
specific API endpoints directly.  This allows switching backends without
changing business logic.

Dataclasses
-----------
Position      — a wallet's position in one outcome token
OrderBook     — bid/ask snapshot for one token
PriceLevel    — single price level in an order book

Protocol
--------
MarketDataProvider — any backend that can serve positions and order books

Helper
------
estimate_fill_price(book, side, amount) — walk a book to estimate fill price
"""

from __future__ import annotations

from dataclasses import dataclass
from typing import Protocol


# ── Data classes ─────────────────────────────────────────────────────────────


@dataclass(frozen=True)
class PriceLevel:
    price: float
    size: float


@dataclass(frozen=True)
class OrderBook:
    """Order book snapshot.

    bids: best (highest price) first.
    asks: best (lowest price) first.
    """

    bids: list[PriceLevel]
    asks: list[PriceLevel]
    min_order_size: float
    tick_size: float


@dataclass
class Position:
    """A wallet's position in one outcome token.

    Field names match the Polymarket data API response (camelCase keys
    are mapped to snake_case here).  Both the proxy-based data API and
    the Polynode wallet-positions endpoint provide all of these fields.
    """

    asset: str  # token ID (decimal string)
    condition_id: str
    title: str
    outcome: str
    outcome_index: int
    size: float
    avg_price: float
    cur_price: float
    initial_value: float
    current_value: float
    cash_pnl: float
    redeemable: bool
    negative_risk: bool
    opposite_asset: str = ""


# ── Protocol ─────────────────────────────────────────────────────────────────


class MarketDataProvider(Protocol):
    def fetch_positions(
        self, wallet: str, *, redeemable: bool | None = None
    ) -> list[Position]: ...

    def get_order_book(self, token_id: str) -> OrderBook: ...


# ── Helpers ──────────────────────────────────────────────────────────────────


def estimate_fill_price(
    book: OrderBook, side: str, amount: float
) -> float | None:
    """Walk the order book to estimate the average fill price.

    Args:
        book: order book snapshot (from any MarketDataProvider)
        side: "BUY" or "SELL"
        amount: USDC to spend (BUY) or shares to sell (SELL)

    Returns:
        weighted-average fill price, or None if the book has
        insufficient liquidity to fill the full amount.
    """
    if amount <= 0:
        return None

    if side == "BUY":
        remaining_usd = amount
        total_shares = 0.0
        for level in book.asks:
            cost_at_level = level.price * level.size
            if cost_at_level >= remaining_usd:
                total_shares += remaining_usd / level.price
                remaining_usd = 0.0
                break
            total_shares += level.size
            remaining_usd -= cost_at_level
        if remaining_usd > 0:
            return None
        return amount / total_shares if total_shares > 0 else None

    if side == "SELL":
        remaining_shares = amount
        total_proceeds = 0.0
        for level in book.bids:
            if level.size >= remaining_shares:
                total_proceeds += remaining_shares * level.price
                remaining_shares = 0.0
                break
            total_proceeds += level.size * level.price
            remaining_shares -= level.size
        if remaining_shares > 0:
            return None
        return total_proceeds / amount

    return None
