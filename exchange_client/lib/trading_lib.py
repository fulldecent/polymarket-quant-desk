"""
Shared trading library for Polymarket CLOB operations.

All private key access and CLOB API interactions are confined to this module
and the scripts in the exchange_client/ folder. External callers (e.g. trading
bots in purple-moose/) import these functions instead of handling keys directly.
"""

import json
import os
import re
import subprocess
import sys
import time
from datetime import datetime, timezone
from pathlib import Path
from urllib.parse import urlparse

import requests
from dotenv import load_dotenv
from eth_account import Account
from Crypto.Hash import keccak
from py_clob_client import (
    ApiCreds,
    AssetType,
    BalanceAllowanceParams,
    ClobClient,
    MarketOrderArgs,
    OpenOrderParams,
    OrderArgs,
    OrderType,
)

CHAIN_POLYGON = 137
TRADING_LOGS_DIR = Path(__file__).resolve().parent.parent / "trading-logs"
_LOCAL_PROXY_PORTS = {
    9430: "httpbin proxy",
    9431: "CLOB API proxy",
    9432: "gamma API proxy",
    9433: "data API proxy",
    9434: "relayer API proxy",
}

_env_loaded = False


def load_env() -> None:
    """Load .env files (idempotent).

    Loads both the project root .env and the exchange_client .env.
    The exchange_client .env is loaded second so it can override root values.
    """
    global _env_loaded
    if _env_loaded:
        return
    _api_client_dir = Path(__file__).resolve().parent.parent
    _project_root = _api_client_dir.parent
    load_dotenv(_project_root / ".env", override=False)
    load_dotenv(_api_client_dir / ".env", override=True)
    _env_loaded = True


def require_env(name: str) -> str:
    """Read a required environment variable or exit."""
    load_env()
    val = os.environ.get(name)
    if not val:
        sys.exit(f"Error: {name} environment variable is required")
    return val


# ── Logging ──────────────────────────────────────────────────────────────────


def _log_path() -> Path:
    now = datetime.now(timezone.utc)
    return TRADING_LOGS_DIR / f"{now.strftime('%Y-%m-%dT%H')}Z.jsonl"


def log_event(event: dict) -> None:
    """Append a JSON line to the current trading log file."""
    event["timestamp"] = datetime.now(timezone.utc).isoformat()
    TRADING_LOGS_DIR.mkdir(parents=True, exist_ok=True)
    with open(_log_path(), "a") as f:
        f.write(json.dumps(event) + "\n")


# ── Display helpers ──────────────────────────────────────────────────────────


def fmt_usd(n: float) -> str:
    return f"${n:,.2f}"


def fmt_pct(n: float) -> str:
    sign = "+" if n >= 0 else ""
    return f"{sign}{n:.1f}%"


def print_positions(positions: list[dict], label: str) -> None:
    if not positions:
        print(f"{label}: none\n")
        return

    total_initial = 0.0
    total_current = 0.0
    total_pnl = 0.0

    for p in positions:
        total_initial += float(p.get("initialValue", 0))
        total_current += float(p.get("currentValue", 0))
        total_pnl += float(p.get("cashPnl", 0))

    print(f"{label}: {len(positions)} ({fmt_usd(total_current)})")

    for p in positions:
        current = float(p.get("currentValue", 0))

        resolved = p.get("redeemable")
        status = " RESOLVED" if resolved else ""
        outcome = p.get("outcome", "?")
        # Truncate long outcomes (e.g. player names)
        if len(outcome) > 6:
            outcome = outcome[:5] + "…"
        title = p.get("title", "?")
        # Truncate title if too long
        max_title = 50
        if len(title) > max_title:
            title = title[:max_title - 1] + "…"

        print(f"  {fmt_usd(current):>8}  {outcome:<6}{status:<10} {title}")

    print(f"  {fmt_usd(total_current):>8}  total (cost {fmt_usd(total_initial)}, P&L {fmt_usd(total_pnl)})")
    print()


# ── CLOB client ──────────────────────────────────────────────────────────────


def build_client() -> ClobClient:
    """Construct an authenticated ClobClient from environment variables."""
    load_env()
    host = get_clob_api_url()
    pk = require_env("EOA_PRIVATE_KEY")
    funder = require_env("POLYMARKET_PROXY_WALLET")
    chain_id = CHAIN_POLYGON

    creds = ApiCreds(
        api_key=require_env("CLOB_API_KEY"),
        api_secret=require_env("CLOB_SECRET"),
        api_passphrase=require_env("CLOB_PASS_PHRASE"),
    )

    return ClobClient(
        host=host,
        chain_id=chain_id,
        key=pk,
        creds=creds,
        signature_type=2,
        funder=funder,
    )


def get_funder_address() -> str:
    """Return the proxy wallet (funder) address."""
    return require_env("POLYMARKET_PROXY_WALLET")


def get_clob_api_url() -> str:
    """Return the CLOB API base URL."""
    return require_env("CLOB_API_URL")


def get_data_api_url() -> str:
    """Return the data API base URL."""
    return require_env("DATA_API_URL")


def get_polygon_rpc_url() -> str:
    """Return the Polygon RPC URL."""
    return require_env("POLYGON_RPC_URL")


def _local_proxy_hint(endpoint_url: str) -> str | None:
    parsed = urlparse(endpoint_url)
    if parsed.hostname not in {"localhost", "127.0.0.1"}:
        return None
    label = _LOCAL_PROXY_PORTS.get(parsed.port)
    if label is None:
        return None
    return (
        f"{endpoint_url} is the local {label}. "
        "Start `python3 exchange_client/start_proxy.py` if the proxy stack is not running."
    )


def explain_api_error(endpoint_name: str, endpoint_url: str, exc: Exception) -> str:
    """Return a concise, actionable message for API failures."""
    message = str(exc).strip() or exc.__class__.__name__
    proxy_hint = _local_proxy_hint(endpoint_url)

    response = None
    if isinstance(exc, requests.exceptions.RequestException):
        response = exc.response

    if isinstance(exc, requests.exceptions.HTTPError):
        status = response.status_code if response is not None else "unknown"
        body = ""
        if response is not None and response.text:
            body = response.text.strip().replace("\n", " ")
        detail = body or message
        if status == 401:
            return (
                f"{endpoint_name} returned HTTP 401 at {endpoint_url}. "
                "Check that the endpoint and credentials are valid. "
                f"Response: {detail}"
            )
        return f"{endpoint_name} returned HTTP {status} at {endpoint_url}: {detail}"

    if isinstance(exc, requests.exceptions.Timeout):
        return f"{endpoint_name} timed out at {endpoint_url}: {message}"

    if isinstance(exc, requests.exceptions.ConnectionError):
        if proxy_hint:
            return f"could not reach {endpoint_name} at {endpoint_url}. {proxy_hint}"
        return f"could not reach {endpoint_name} at {endpoint_url}: {message}"

    status_match = re.search(r"status_code=(\d+)", message)
    if status_match:
        status = status_match.group(1)
        if status == "401":
            return (
                f"{endpoint_name} returned HTTP 401 at {endpoint_url}. "
                "Check that the endpoint and credentials are valid."
            )
        return f"{endpoint_name} returned HTTP {status} at {endpoint_url}: {message}"

    lowered = message.lower()
    if proxy_hint and (
        "request exception" in lowered
        or "connection refused" in lowered
        or "failed to establish a new connection" in lowered
    ):
        return f"could not reach {endpoint_name} at {endpoint_url}. {proxy_hint}"

    return f"{endpoint_name} error at {endpoint_url}: {message}"


def explain_rpc_error(rpc_url: str, detail: str) -> str:
    """Return a concise, actionable message for Polygon RPC failures."""
    detail = detail.strip() or "unknown error"
    lowered = detail.lower()

    if "http error 401" in lowered or "access token missing or invalid" in lowered:
        return (
            f"POLYGON_RPC_URL returned HTTP 401 at {rpc_url}. "
            "Use an authenticated Polygon RPC endpoint."
        )

    if "connection refused" in lowered:
        return f"could not reach Polygon RPC at {rpc_url}: connection refused"

    if "timed out" in lowered or "timeout" in lowered:
        return f"Polygon RPC timed out at {rpc_url}: {detail}"

    return f"Polygon RPC error at {rpc_url}: {detail}"


# ── Balance ──────────────────────────────────────────────────────────────────


def get_eoa_address() -> str:
    """Derive the EOA (signer) address from the private key."""
    return Account.from_key(require_env("EOA_PRIVATE_KEY")).address


def get_pol_balance() -> float:
    """Return native POL balance of the EOA signer in whole units (18 decimals)."""
    rpc = get_polygon_rpc_url()
    address = get_eoa_address()
    cmd = ["cast", "balance", address, "--rpc-url", rpc]
    r = subprocess.run(cmd, capture_output=True, text=True, timeout=30)
    if r.returncode != 0:
        raise RuntimeError(explain_rpc_error(rpc, r.stderr))
    return int(r.stdout.strip()) / 1e18


def get_usdc_balance(client: ClobClient) -> float:
    """Return spendable USDC balance in dollars."""
    result = client.get_balance_allowance(
        BalanceAllowanceParams(asset_type=AssetType.COLLATERAL)
    )
    return float(result.get("balance", "0")) / 1e6


def get_usdc_balance_via_rpc(address: str | None = None) -> float:
    """Return the proxy wallet's USDC.e balance in dollars via Polygon RPC."""
    rpc = get_polygon_rpc_url()
    wallet = address or get_funder_address()
    cmd = [
        "cast",
        "call",
        _USDC_E,
        "balanceOf(address)(uint256)",
        wallet,
        "--rpc-url",
        rpc,
    ]
    r = subprocess.run(cmd, capture_output=True, text=True, timeout=30)
    if r.returncode != 0:
        raise RuntimeError(explain_rpc_error(rpc, r.stderr))
    return int(r.stdout.strip(), 0) / 1e6


def get_usdc_balance_raw(client: ClobClient) -> str:
    """Return raw USDC balance string (6-decimal integer)."""
    result = client.get_balance_allowance(
        BalanceAllowanceParams(asset_type=AssetType.COLLATERAL)
    )
    return result.get("balance", "0")


# ── Order book / spread ──────────────────────────────────────────────────────


def get_book_spread(client: ClobClient, token_id: str) -> dict:
    """Fetch best bid/ask from the live order book.

    Returns a dict with keys:
      best_bid    – best bid price (float) or None
      best_ask    – best ask price (float) or None
    """
    try:
        ob = client.get_order_book(token_id)
        best_bid = float(ob.bids[0].price) if ob.bids else None
        best_ask = float(ob.asks[0].price) if ob.asks else None

        return {
            "best_bid": best_bid,
            "best_ask": best_ask,
        }
    except Exception as exc:
        return {
            "best_bid": None,
            "best_ask": None,
            "error": str(exc),
        }


def effective_spread(
    client: ClobClient,
    token_id: str,
    buy_usd: float,
    sell_shares: float | None = None,
) -> dict:
    """Compute the effective spread from simulated fill prices.

    The CLOB API's get_spread()/get_midpoint() return values derived from
    recent trades, not the live order book. On illiquid markets, top-of-book
    can show $0.01/$0.99 while real fills happen at much tighter prices
    deeper in the book.

    This function uses calculate_market_price to walk the order book and
    find the actual prices a market order would fill at, then computes the
    spread from those. All three queries (buy fill, sell fill, order book)
    run in parallel via a thread pool.

    Args:
        client: authenticated ClobClient
        token_id: conditional token ID
        buy_usd: USDC amount for the buy side
        sell_shares: share count for the sell side. If None, defaults to
            buy_usd * 2 as a conservative estimate (covers prices down
            to $0.50 per share).

    Returns a dict with keys:
        buy_price   – estimated buy fill price (float) or None
        sell_price  – estimated sell fill price (float) or None
        spread_pct  – effective spread as % of midpoint (float) or None
        tradeable   – True if both prices exist and spread is finite
        best_bid    – top-of-book bid (float) or None
        best_ask    – top-of-book ask (float) or None
    """
    import concurrent.futures

    if sell_shares is None:
        sell_shares = buy_usd * 2

    with concurrent.futures.ThreadPoolExecutor(max_workers=3) as pool:
        buy_fut = pool.submit(estimate_fill_price, client, token_id, "BUY", buy_usd)
        sell_fut = pool.submit(estimate_fill_price, client, token_id, "SELL", sell_shares)
        book_fut = pool.submit(get_book_spread, client, token_id)
        buy_price = buy_fut.result()
        sell_price = sell_fut.result()
        book_info = book_fut.result()

    base = {
        "buy_price": buy_price,
        "sell_price": sell_price,
        "best_bid": book_info.get("best_bid"),
        "best_ask": book_info.get("best_ask"),
    }

    if buy_price is None or sell_price is None or buy_price <= 0 or sell_price <= 0:
        base["spread_pct"] = None
        base["tradeable"] = False
        return base

    mid = (buy_price + sell_price) / 2
    base["spread_pct"] = (buy_price - sell_price) / mid * 100
    base["tradeable"] = True
    return base


def estimate_fill_price(
    client: ClobClient, token_id: str, side: str, amount: float
) -> float | None:
    """Estimate the fill price for a market order without submitting it.

    Uses the live order book to simulate walking through resting orders.

    Args:
        client: authenticated ClobClient
        token_id: conditional token ID
        side: "BUY" or "SELL"
        amount: USDC amount (for BUY) or share count (for SELL)

    Returns:
        estimated average fill price, or None if the book is empty
    """
    try:
        return client.calculate_market_price(
            token_id, side, amount, OrderType.FOK
        )
    except Exception:
        return None


# ── Positions ────────────────────────────────────────────────────────────────


def fetch_positions(user: str, *, redeemable: bool | None = None) -> list[dict]:
    """Fetch positions from the data API."""
    load_env()
    data_api = get_data_api_url()
    params = {
        "user": user,
        "sizeThreshold": "0",
        "limit": "500",
        "sortBy": "CURRENT",
        "sortDirection": "DESC",
    }
    if redeemable is not None:
        params["redeemable"] = str(redeemable).lower()

    try:
        resp = requests.get(f"{data_api}/positions", params=params, timeout=30)
        resp.raise_for_status()
        return resp.json()
    except requests.exceptions.RequestException as exc:
        raise RuntimeError(explain_api_error("data API", data_api, exc)) from exc


# ── Open orders ──────────────────────────────────────────────────────────────


def fetch_open_orders(client: ClobClient) -> list[dict]:
    """Fetch all open orders from the CLOB API."""
    return client.get_orders(OpenOrderParams())


def print_orders(orders: list[dict], label: str) -> None:
    if not orders:
        print(f"{label}: none\n")
        return

    print(f"{label}: {len(orders)}")

    for o in orders:
        side = o.get("side", "?")
        price = float(o.get("price", 0))
        original = float(o.get("original_size", 0) or o.get("size", 0))
        matched = float(
            o.get("size_matched", 0) or o.get("sizeMatched", 0) or 0
        )
        remaining = original - matched
        order_id = o.get("id", "?")[:8]

        print(f"  {side:4} {remaining:>8,.2f} @ {fmt_usd(price)}  (filled {matched:,.0f}/{original:,.0f})  {order_id}…")

    print()


# ── Trading ──────────────────────────────────────────────────────────────────


def buy_token(
    client: ClobClient, token_id: str, amount_usd: float,
    *, max_price: float = 0,
) -> dict:
    """Place a fill-or-kill market buy order. Returns the API response.

    Args:
        max_price: worst acceptable fill price per share. If nonzero,
            the CLOB order's price field is set to this value instead
            of auto-walking the full order book. This prevents
            slippage beyond the expected entry price.
    """
    log_event({
        "action": "buy",
        "phase": "submitted",
        "token_id": token_id,
        "amount_usd": amount_usd,
        "max_price": max_price,
        "side": "BUY",
        "order_type": "FOK",
    })

    try:
        order = client.create_market_order(
            MarketOrderArgs(
                token_id=token_id,
                amount=amount_usd,
                price=max_price,
                side="BUY",
                order_type=OrderType.FOK,
            ),
        )
        resp = client.post_order(order, orderType=OrderType.FOK)

        log_event({
            "action": "buy",
            "phase": "completed",
            "token_id": token_id,
            "amount_usd": amount_usd,
            "max_price": max_price,
            "side": "BUY",
            "order_type": "FOK",
            "response": resp,
        })
        return resp
    except Exception as exc:
        log_event({
            "action": "buy",
            "phase": "failed",
            "token_id": token_id,
            "amount_usd": amount_usd,
            "max_price": max_price,
            "side": "BUY",
            "order_type": "FOK",
            "error": str(exc),
        })
        raise


def buy_limit_order(
    client: ClobClient, token_id: str, amount: float,
    price: float, ttl_seconds: int = 30,
) -> dict:
    """Place a GTD limit buy order. Returns the API response.

    Args:
        client: authenticated ClobClient
        token_id: conditional token ID to buy
        amount: USDC amount to spend
        price: limit price per share
        ttl_seconds: order lifetime in seconds (default 30)
    """
    size = amount / price if price > 0 else 0
    # CLOB requires expiration >= now + 60s (security threshold)
    expiration = int(time.time()) + 60 + ttl_seconds

    log_event({
        "action": "buy_limit",
        "phase": "submitted",
        "token_id": token_id,
        "amount": amount,
        "price": price,
        "size": size,
        "ttl_seconds": ttl_seconds,
        "side": "BUY",
        "order_type": "GTD",
    })

    try:
        order = client.create_order(
            OrderArgs(
                token_id=token_id,
                price=price,
                size=size,
                side="BUY",
                expiration=expiration,
            )
        )
        resp = client.post_order(order, orderType=OrderType.GTD)

        log_event({
            "action": "buy_limit",
            "phase": "completed",
            "token_id": token_id,
            "amount": amount,
            "price": price,
            "size": size,
            "side": "BUY",
            "order_type": "GTD",
            "response": resp,
        })
        return resp
    except Exception as exc:
        log_event({
            "action": "buy_limit",
            "phase": "failed",
            "token_id": token_id,
            "amount": amount,
            "price": price,
            "size": size,
            "side": "BUY",
            "order_type": "GTD",
            "error": str(exc),
        })
        raise


def cancel_all_orders(client: ClobClient, *, dry_run: bool = False,
                      strict: bool = False) -> None:
    """Cancel all open orders on the CLOB.

    Args:
        client: authenticated ClobClient
        dry_run: if True, show what would happen without cancelling
        strict: if True, raise on any failure instead of printing
    """
    try:
        orders = fetch_open_orders(client)
    except Exception as exc:
        if strict:
            raise
        print(f"Could not fetch open orders: {exc}", file=sys.stderr)
        return

    if not orders:
        print("No open orders to cancel.\n")
        return

    if dry_run:
        print(f"[dry run] would cancel {len(orders)} open order(s)\n")
        return

    print(f"Cancelling {len(orders)} open order(s) …", end=" ", flush=True)

    log_event({
        "action": "cancel_all_orders",
        "phase": "submitted",
        "order_count": len(orders),
    })

    try:
        resp = client.cancel_all()
        print("done")
        log_event({
            "action": "cancel_all_orders",
            "phase": "completed",
            "order_count": len(orders),
            "response": resp,
        })
    except Exception as exc:
        print("FAILED")
        print(f"  {exc}", file=sys.stderr)
        log_event({
            "action": "cancel_all_orders",
            "phase": "failed",
            "error": str(exc),
        })
        if strict:
            raise

    print()


def sell_token(client: ClobClient, token_id: str, size: float) -> dict:
    """Place a fill-and-kill market sell order. Returns the API response."""
    log_event({
        "action": "sell",
        "phase": "submitted",
        "token_id": token_id,
        "size": size,
        "side": "SELL",
        "order_type": "FAK",
    })

    try:
        order = client.create_market_order(
            MarketOrderArgs(
                token_id=token_id,
                amount=size,
                side="SELL",
                order_type=OrderType.FAK,
            ),
        )
        resp = client.post_order(order, orderType=OrderType.FAK)

        log_event({
            "action": "sell",
            "phase": "completed",
            "token_id": token_id,
            "size": size,
            "side": "SELL",
            "order_type": "FAK",
            "response": resp,
        })
        return resp
    except Exception as exc:
        log_event({
            "action": "sell",
            "phase": "failed",
            "token_id": token_id,
            "size": size,
            "side": "SELL",
            "order_type": "FAK",
            "error": str(exc),
        })
        raise


def sell_token_limit(
    client: ClobClient, token_id: str, size: float,
    price: float, ttl_seconds: float,
) -> dict:
    """Place a GTD limit sell order. Returns the API response."""
    expiration = str(int(time.time() + ttl_seconds))
    log_event({
        "action": "sell_limit",
        "phase": "submitted",
        "token_id": token_id,
        "size": size,
        "price": price,
        "ttl_seconds": ttl_seconds,
        "side": "SELL",
        "order_type": "GTD",
    })
    try:
        order = client.create_order(
            OrderArgs(
                token_id=token_id,
                price=price,
                size=size,
                side="SELL",
                expiration=expiration,
            )
        )
        resp = client.post_order(order, orderType=OrderType.GTD)
        log_event({
            "action": "sell_limit",
            "phase": "completed",
            "token_id": token_id,
            "size": size,
            "price": price,
            "side": "SELL",
            "order_type": "GTD",
            "response": resp,
        })
        return resp
    except Exception as exc:
        log_event({
            "action": "sell_limit",
            "phase": "failed",
            "token_id": token_id,
            "size": size,
            "price": price,
            "side": "SELL",
            "order_type": "GTD",
            "error": str(exc),
        })
        raise


def parse_limit_sell_offset(value: str) -> float:
    """Argparse type function: parse a --limit-sell offset to a float percentage.

    Accepted formats:
        0, 0.0, 0.00, +0, -0, 0%, +0%  → 0.0  (match ask)
        +3.4%                           → 3.4  (premium above ask)
        -0.3%                           → -0.3 (undercut below ask)

    Non-zero values must include an explicit sign and a trailing %.
    """
    import argparse
    s = value.strip()

    # Zero: optional sign, zero with optional decimals, optional %
    if re.fullmatch(r'[+-]?0(\.0+)?%?', s):
        return 0.0

    # Non-zero: mandatory sign, number, mandatory %
    m = re.fullmatch(r'([+-])(\d+(?:\.\d+)?)%', s)
    if not m:
        raise argparse.ArgumentTypeError(
            f"invalid offset: {value!r} "
            f"(expected 0, +3.4%, or -0.3% — non-zero values need sign and %)")
    return float(m.group(1) + m.group(2))


_TTL_MULTIPLIERS = {"s": 1, "m": 60, "h": 3600, "d": 86400}


def parse_ttl(value: str) -> int:
    """Argparse type function: parse a duration string into seconds.

    Accepted formats: 30s, 45m, 24h, 1.5h, 3d (number + unit suffix).
    Returns integer seconds.
    """
    import argparse
    s = value.strip()
    if not s or s[-1] not in _TTL_MULTIPLIERS:
        raise argparse.ArgumentTypeError(
            f"invalid duration: {value!r} "
            f"— use a suffix: s, m, h, d (e.g. 30s, 24h, 1.5h)")
    suffix = s[-1]
    try:
        num = float(s[:-1])
    except ValueError:
        raise argparse.ArgumentTypeError(f"invalid duration: {value!r}")
    if num < 0:
        raise argparse.ArgumentTypeError(
            f"negative duration not allowed: {value!r}")
    return round(num * _TTL_MULTIPLIERS[suffix])


def create_limit_sell_orders(
    client: ClobClient,
    user: str,
    *,
    offset_pct: float = 0.0,
    ttl_seconds: float = 72 * 3600,
    dry_run: bool = False,
) -> list[dict]:
    """Create GTD limit sell orders for all active positions.

    Each order is priced relative to the current best ask:
        sell_price = best_ask * (1 + offset_pct / 100)

    Examples:
        offset_pct =  0  → match the existing ask
        offset_pct =  5  → 5% above ask (patient, premium sell)
        offset_pct = -2  → 2% below ask (aggressive undercut)

    Args:
        client: authenticated ClobClient
        user: wallet address to fetch positions for
        offset_pct: percentage offset from best ask (positive = premium,
            negative = undercut, 0 = match ask)
        ttl_seconds: order lifetime in seconds (default 72h)
        dry_run: if True, print what would happen without placing orders

    Returns:
        list of result dicts, one per position attempted
    """
    positions = fetch_positions(user, redeemable=False)

    if not positions:
        print("No active positions to sell.")
        return []

    expiration = int(time.time()) + int(ttl_seconds)
    results = []
    skipped = 0

    for p in positions:
        size = float(p.get("size", 0))
        if size < 0.01:
            skipped += 1
            continue

        title = p.get("title", "?")
        outcome = p.get("outcome", "?")
        token_id = p.get("asset", "")

        # Fetch order book for best ask and tick size
        try:
            ob = client.get_order_book(token_id)
        except Exception as exc:
            print(f"  skipping \"{title}\" ({outcome}) — order book error: {exc}")
            results.append({"token_id": token_id, "status": "skipped", "reason": str(exc)})
            continue

        if not ob.asks:
            print(f"  skipping \"{title}\" ({outcome}) — no asks on book")
            results.append({"token_id": token_id, "status": "skipped", "reason": "no asks"})
            continue

        # The CLOB enforces a per-market minimum order size, returned as
        # min_order_size in the order book response.  Orders below this
        # are rejected with INVALID_ORDER_MIN_SIZE.
        # Source: https://docs.polymarket.com/trading/orderbook
        #         https://docs.polymarket.com/trading/orders/overview
        min_size = float(ob.min_order_size) if ob.min_order_size else 0
        if size < min_size:
            print(f"  skipping \"{title}\" ({outcome})"
                  f" — size {size:,.2f} below minimum {min_size:,.0f}")
            results.append({
                "token_id": token_id, "status": "skipped",
                "reason": f"size {size} < min {min_size}",
            })
            continue

        best_ask = float(ob.asks[0].price)
        tick = float(ob.tick_size)

        # Compute offset price, snap to tick, clamp to valid range
        raw_price = best_ask * (1 + offset_pct / 100)
        sell_price = round(round(raw_price / tick) * tick, 4)
        sell_price = max(sell_price, tick)
        sell_price = min(sell_price, 1 - tick)

        if offset_pct > 0:
            direction = f"+{offset_pct}%"
        elif offset_pct < 0:
            direction = f"{offset_pct}%"
        else:
            direction = "0%"

        value_est = size * sell_price
        label = "(DRY RUN) " if dry_run else ""
        print(f"  {label}{title} ({outcome})")
        print(f"    {size:,.2f} shares @ {fmt_usd(sell_price)}  (ask {fmt_usd(best_ask)},"
              f" offset {direction}, ~{fmt_usd(value_est)})")

        if dry_run:
            results.append({
                "token_id": token_id, "status": "dry_run",
                "price": sell_price, "size": size,
            })
            continue

        log_event({
            "action": "limit_sell",
            "phase": "submitted",
            "token_id": token_id,
            "size": size,
            "price": sell_price,
            "best_ask": best_ask,
            "offset_pct": offset_pct,
            "ttl_seconds": ttl_seconds,
        })

        try:
            order = client.create_order(
                OrderArgs(
                    token_id=token_id,
                    price=sell_price,
                    size=size,
                    side="SELL",
                    expiration=expiration,
                )
            )
            resp = client.post_order(order, orderType=OrderType.GTD)

            # post_order returns a dict with success/errorMsg on HTTP 200.
            # Only non-200 responses raise PolyApiException.
            if isinstance(resp, dict) and not resp.get("success", True):
                error_msg = resp.get("errorMsg", "unknown error")
                print(f"    REJECTED: {error_msg}", file=sys.stderr)
                log_event({
                    "action": "limit_sell",
                    "phase": "rejected",
                    "token_id": token_id,
                    "size": size,
                    "price": sell_price,
                    "error": error_msg,
                    "response": resp,
                })
                results.append({"token_id": token_id, "status": "failed", "error": error_msg})
            else:
                print(f"    order placed: {resp}")
                log_event({
                    "action": "limit_sell",
                    "phase": "completed",
                    "token_id": token_id,
                    "size": size,
                    "price": sell_price,
                    "response": resp,
                })
                results.append({"token_id": token_id, "status": "ok", "response": resp})
        except Exception as exc:
            print(f"    FAILED: {exc}", file=sys.stderr)
            log_event({
                "action": "limit_sell",
                "phase": "failed",
                "token_id": token_id,
                "size": size,
                "price": sell_price,
                "error": str(exc),
            })
            results.append({"token_id": token_id, "status": "failed", "error": str(exc)})

        time.sleep(0.5)

    placed = sum(1 for r in results if r["status"] == "ok")
    failed = sum(1 for r in results if r["status"] == "failed")
    dry = sum(1 for r in results if r["status"] == "dry_run")
    skipped_total = skipped + sum(1 for r in results if r["status"] == "skipped")

    parts = []
    if placed:
        parts.append(f"{placed} placed")
    if dry:
        parts.append(f"{dry} previewed")
    if failed:
        parts.append(f"{failed} failed")
    if skipped_total:
        parts.append(f"{skipped_total} skipped")
    print(f"\nLimit sell summary: {', '.join(parts)}\n")

    return results


def dump_all_positions(client: ClobClient, user: str) -> list[dict]:
    """Sell all open positions at market price. Returns list of result dicts."""
    data_api = require_env("DATA_API_URL")
    positions = fetch_positions(user, redeemable=False)

    if not positions:
        print("No open positions to dump.")
        return []

    print_positions(positions, "Positions to dump")

    log_event({
        "action": "dump_positions",
        "phase": "submitted",
        "position_count": len(positions),
        "positions": [
            {
                "title": p.get("title", "?"),
                "outcome": p.get("outcome", "?"),
                "token_id": p.get("asset", ""),
                "size": float(p.get("size", 0)),
            }
            for p in positions if float(p.get("size", 0)) > 0
        ],
    })

    balance_before = get_usdc_balance_raw(client)
    results = []

    for p in positions:
        size = float(p.get("size", 0))
        if size < 0.01:
            continue

        title = p.get("title", "?")
        outcome = p.get("outcome", "?")
        token_id = p.get("asset", "")

        print(f'  selling {size:,.2f} shares of "{title}" ({outcome}) …')

        try:
            resp = sell_token(client, token_id, size)
            print(f"    result: {resp}")
            results.append({"token_id": token_id, "status": "ok", "response": resp})
        except Exception as exc:
            print(f"    FAILED: {exc}", file=sys.stderr)
            results.append({"token_id": token_id, "status": "failed", "error": str(exc)})

        time.sleep(0.5)

    print("\nWaiting 10 seconds for settlement …\n")
    time.sleep(10)

    balance_after = get_usdc_balance_raw(client)
    print(f"USDC balance: {fmt_usd(float(balance_after) / 1e6)}\n")

    remaining = fetch_positions(user, redeemable=False)
    print_positions(remaining, "Remaining positions")

    gained = (float(balance_after) - float(balance_before)) / 1e6
    print(f"USDC change: {fmt_usd(gained)}")

    log_event({
        "action": "dump_positions",
        "phase": "completed",
        "usdc_before": float(balance_before) / 1e6,
        "usdc_after": float(balance_after) / 1e6,
        "usdc_change": gained,
        "remaining_positions": len(remaining),
        "results": results,
    })

    return results


# ── Builder relayer client ───────────────────────────────────────────────────


def build_relayer_client():
    """Construct an authenticated RelayClient from environment variables.

    Requires BUILDER_API_KEY, BUILDER_SECRET, BUILDER_PASSPHRASE,
    and EOA_PRIVATE_KEY.
    """
    from py_builder_relayer_client.client import RelayClient
    from py_builder_signing_sdk.config import BuilderConfig
    from py_builder_signing_sdk.sdk_types import BuilderApiKeyCreds

    load_env()
    private_key = require_env("EOA_PRIVATE_KEY")

    builder_config = BuilderConfig(
        local_builder_creds=BuilderApiKeyCreds(
            key=require_env("BUILDER_API_KEY"),
            secret=require_env("BUILDER_SECRET"),
            passphrase=require_env("BUILDER_PASSPHRASE"),
        ),
    )

    return RelayClient(
        relayer_url=require_env("RELAYER_API_URL"),
        chain_id=CHAIN_POLYGON,
        private_key=private_key,
        builder_config=builder_config,
    )


# ── Redeem resolved positions ───────────────────────────────────────────────
#
# Redeems winning outcome tokens for USDC via on-chain calls to
# ConditionalTokens.redeemPositions() or NegRiskAdapter.redeemPositions().
#
# Two code paths:
#   - "relayer" (default): gasless via the builder relayer API
#   - "rpc": direct on-chain via Gnosis Safe + cast
#
# The RPC path requires: cast (foundry) on PATH, POLYGON_RPC_URL, EOA_PRIVATE_KEY.

_CTF_ADDRESS = "0x4D97DCd97eC945f40cF65F87097ACe5EA0476045"
_NEGRISK_ADAPTER = "0xd91E80cF2E7be2e162c6513ceD06f1dD0dA35296"
_USDC_E = "0x2791Bca1f2de4661ED88A30C99A7a9449Aa84174"
_MULTISEND_ADDRESS = "0xA238CBeb142c10Ef7Ad8442C6D1f9E89e07e7761"
_ZERO_ADDRESS = "0x0000000000000000000000000000000000000000"
_CALL_OPERATION = 0
_DELEGATECALL_OPERATION = 1


def _keccak256(data: bytes) -> bytes:
    return keccak.new(digest_bits=256, data=data).digest()


def _get_safe_nonce(proxy: str, rpc: str) -> int:
    cmd = ["cast", "call", proxy, "nonce()(uint256)", "--rpc-url", rpc]
    r = subprocess.run(cmd, capture_output=True, text=True, timeout=30)
    if r.returncode != 0:
        sys.exit(f"Failed to get Safe nonce: {r.stderr.strip()}")
    return int(r.stdout.strip())


def _cast_calldata(sig: str, *args: str) -> bytes:
    """Use cast to ABI-encode a function call, return raw bytes."""
    cmd = ["cast", "calldata", sig] + list(args)
    r = subprocess.run(cmd, capture_output=True, text=True, timeout=10)
    if r.returncode != 0:
        sys.exit(f"cast calldata failed: {r.stderr.strip()}")
    hex_str = r.stdout.strip()
    return bytes.fromhex(hex_str[2:] if hex_str.startswith("0x") else hex_str)


def _encode_ct_redeem(condition_id: str) -> bytes:
    """Encode ConditionalTokens.redeemPositions(USDC.e, 0x0, conditionId, [1,2])."""
    return _cast_calldata(
        "redeemPositions(address,bytes32,bytes32,uint256[])",
        _USDC_E,
        "0x0000000000000000000000000000000000000000000000000000000000000000",
        condition_id,
        "[1,2]",
    )


def _encode_nr_redeem(condition_id: str, amounts: list[int]) -> bytes:
    """Encode NegRiskAdapter.redeemPositions(conditionId, amounts)."""
    amounts_str = "[" + ",".join(str(a) for a in amounts) + "]"
    return _cast_calldata(
        "redeemPositions(bytes32,uint256[])",
        condition_id,
        amounts_str,
    )


def _pack_multisend_tx(operation: int, to: str, value: int, data: bytes) -> bytes:
    """Pack one sub-transaction for Gnosis Safe MultiSend.

    Format: operation (1 byte) | to (20 bytes) | value (32 bytes) |
            dataLength (32 bytes) | data (variable)
    """
    return (
        operation.to_bytes(1, "big")
        + bytes.fromhex(to[2:].lower())
        + value.to_bytes(32, "big")
        + len(data).to_bytes(32, "big")
        + data
    )


def _encode_multisend(sub_txs: list[tuple[str, bytes]]) -> bytes:
    """Encode multiSend(bytes transactions) for a list of (to, calldata) pairs."""
    packed = b""
    for to_addr, calldata in sub_txs:
        packed += _pack_multisend_tx(_CALL_OPERATION, to_addr, 0, calldata)

    padded_len = ((len(packed) + 31) // 32) * 32
    padded_data = packed + b"\x00" * (padded_len - len(packed))

    return (
        bytes.fromhex("8d80ff0a")
        + (32).to_bytes(32, "big")
        + len(packed).to_bytes(32, "big")
        + padded_data
    )


def _safe_tx_hash(
    proxy: str, to: str, value: int, data: bytes,
    operation: int, nonce: int, chain_id: int,
) -> bytes:
    """Compute the Gnosis Safe transaction hash for EIP-712 signing."""
    domain_type_hash = _keccak256(
        b"EIP712Domain(uint256 chainId,address verifyingContract)"
    )
    domain_separator = _keccak256(
        domain_type_hash
        + chain_id.to_bytes(32, "big")
        + bytes(12) + bytes.fromhex(proxy[2:].lower())
    )

    safe_tx_type_hash = _keccak256(
        b"SafeTx(address to,uint256 value,bytes data,uint8 operation,"
        b"uint256 safeTxGas,uint256 baseGas,uint256 gasPrice,"
        b"address gasToken,address refundReceiver,uint256 nonce)"
    )

    data_hash = _keccak256(data)
    zero32 = b"\x00" * 32

    encoded_tx = (
        safe_tx_type_hash
        + bytes(12) + bytes.fromhex(to[2:].lower())
        + value.to_bytes(32, "big")
        + data_hash
        + operation.to_bytes(32, "big")
        + zero32  # safeTxGas
        + zero32  # baseGas
        + zero32  # gasPrice
        + zero32  # gasToken
        + zero32  # refundReceiver
        + nonce.to_bytes(32, "big")
    )

    struct_hash = _keccak256(encoded_tx)
    return _keccak256(b"\x19\x01" + domain_separator + struct_hash)


def _build_redeem_plan(
    user: str,
    positions: list[dict] | None = None,
) -> tuple[list[dict], list[dict], float, dict[str, dict]]:
    """Shared logic: fetch positions and build the redemption plan.

    Returns:
        (winners, losers, total_value, condition_ids)
    """
    if positions is None:
        positions = fetch_positions(user, redeemable=True)
    winners = [p for p in positions if float(p.get("curPrice", 0)) > 0]
    losers = [p for p in positions if float(p.get("curPrice", 0)) == 0]
    total_value = sum(float(p.get("currentValue", 0)) for p in winners)

    condition_ids: dict[str, dict] = {}
    for p in winners:
        cid = p.get("conditionId", "")
        neg_risk = p.get("negativeRisk", False)
        if cid not in condition_ids:
            condition_ids[cid] = {
                "negativeRisk": neg_risk,
                "title": p.get("title", "?"),
                "total_value": 0.0,
                "positions": [],
            }
        condition_ids[cid]["total_value"] += float(p.get("currentValue", 0))
        condition_ids[cid]["positions"].append(p)

    return winners, losers, total_value, condition_ids


def _build_sub_txs(condition_ids: dict[str, dict]) -> list[tuple[str, bytes, str]]:
    """Build (target, calldata, label) tuples for each condition to redeem."""
    result = []
    for cid, info in sorted(condition_ids.items(), key=lambda x: -x[1]["total_value"]):
        label = f"{info['title'][:60]} ({fmt_usd(info['total_value'])})"
        if info["negativeRisk"]:
            amounts = [0, 0]
            for p in info["positions"]:
                idx = int(p.get("outcomeIndex", 0))
                size_raw = int(float(p.get("size", 0)) * 1_000_000)
                if idx < 2:
                    amounts[idx] = size_raw
            calldata = _encode_nr_redeem(cid, amounts)
            target = _NEGRISK_ADAPTER
        else:
            calldata = _encode_ct_redeem(cid)
            target = _CTF_ADDRESS
        result.append((target, calldata, label))
    return result


def redeem_positions_relayer(
    user: str, *, dry_run: bool = False,
    positions: list[dict] | None = None,
) -> list[str]:
    """Redeem resolved winners via the builder relayer (gasless).

    Uses the builder relayer API to submit redemption transactions
    without paying gas. Requires BUILDER_API_KEY, BUILDER_SECRET,
    BUILDER_PASSPHRASE, and EOA_PRIVATE_KEY.
    """
    from py_builder_relayer_client.models import SafeTransaction, OperationType

    winners, losers, total_value, condition_ids = _build_redeem_plan(
        user, positions=positions)

    if not winners:
        print("No winning positions to redeem.\n")
        return []

    print_positions(winners, "Winning positions to redeem")
    print(f"Total redeemable: {fmt_usd(total_value)} across {len(winners)} positions")
    print(f"Losing positions (will also clear): {len(losers)}")
    print("Method: builder relayer (gasless)")
    print()

    sub_txs = _build_sub_txs(condition_ids)

    print(f"Batching {len(sub_txs)} redemptions via relayer\n")
    for i, (_, _, label) in enumerate(sub_txs, 1):
        print(f"  {i:3d}. {label}")
    print()

    if dry_run:
        print(f"[dry run] would redeem {len(sub_txs)} conditions "
              f"({fmt_usd(total_value)} total) via relayer\n")
        return []

    relayer = build_relayer_client()

    safe_txs = [
        SafeTransaction(
            to=target,
            operation=OperationType.Call,
            data="0x" + calldata.hex(),
            value="0",
        )
        for target, calldata, _ in sub_txs
    ]

    log_event({
        "action": "redeem_positions",
        "phase": "submitted",
        "method": "relayer",
        "condition_count": len(sub_txs),
        "total_value": total_value,
    })

    print(f"Submitting {len(sub_txs)} redemptions to relayer...",
          end=" ", flush=True)

    try:
        response = relayer.execute(safe_txs, "Redeem resolved positions")
        print(f"submitted (id: {response.transaction_id})")
        print("Waiting for confirmation...", end=" ", flush=True)
        result = response.wait()

        if result is not None:
            state = result.get("state", "unknown")
            tx_hash = result.get("transactionHash")
            print(f"{state}" + (f" (tx: {tx_hash})" if tx_hash else ""))
            print(f"\nRedeemed {len(sub_txs)} conditions "
                  f"({fmt_usd(total_value)} total)\n")
            log_event({
                "action": "redeem_positions",
                "phase": "completed",
                "method": "relayer",
                "condition_count": len(sub_txs),
                "total_value": total_value,
                "tx_hash": tx_hash,
                "state": state,
            })
            if tx_hash:
                return [tx_hash]
            return []
        else:
            print("timed out waiting for confirmation")
            log_event({
                "action": "redeem_positions",
                "phase": "timeout",
                "method": "relayer",
                "condition_count": len(sub_txs),
                "total_value": total_value,
            })
            return []
    except Exception as exc:
        print(f"FAILED: {exc}")
        log_event({
            "action": "redeem_positions",
            "phase": "failed",
            "method": "relayer",
            "error": str(exc),
        })
        return []


def redeem_positions_rpc(user: str, *, dry_run: bool = False) -> list[str]:
    """Redeem resolved winners via direct RPC (pays gas).

    Requires: POLYGON_RPC_URL, EOA_PRIVATE_KEY, and `cast` (foundry) on PATH.
    """
    winners, losers, total_value, condition_ids = _build_redeem_plan(user)

    if not winners:
        print("No winning positions to redeem.\n")
        return []

    print_positions(winners, "Winning positions to redeem")
    print(f"Total redeemable: {fmt_usd(total_value)} across {len(winners)} positions")
    print(f"Losing positions (will also clear): {len(losers)}")
    print("Method: direct RPC (pays gas)")
    print()

    sub_txs_labeled = _build_sub_txs(condition_ids)
    sub_txs: list[tuple[str, bytes]] = [(t, c) for t, c, _ in sub_txs_labeled]
    labels = [lbl for _, _, lbl in sub_txs_labeled]

    print(f"Batching {len(sub_txs)} redemptions into 1 MultiSend transaction\n")

    for i, label in enumerate(labels, 1):
        print(f"  {i:3d}. {label}")
    print()

    if dry_run:
        print(f"[dry run] would redeem {len(sub_txs)} conditions "
              f"({fmt_usd(total_value)} total) in 1 transaction\n")
        return []

    rpc = require_env("POLYGON_RPC_URL")
    private_key = require_env("EOA_PRIVATE_KEY")

    eoa = Account.from_key(private_key)
    print(f"EOA: {eoa.address}")
    print(f"Safe: {user}\n")

    multisend_data = _encode_multisend(sub_txs)
    multisend_hex = "0x" + multisend_data.hex()

    nonce = _get_safe_nonce(user, rpc)
    tx_hash = _safe_tx_hash(
        proxy=user,
        to=_MULTISEND_ADDRESS,
        value=0,
        data=multisend_data,
        operation=_DELEGATECALL_OPERATION,
        nonce=nonce,
        chain_id=137,
    )

    sig = eoa.unsafe_sign_hash(tx_hash)
    sig_hex = "0x" + (
        sig.r.to_bytes(32, "big").hex()
        + sig.s.to_bytes(32, "big").hex()
        + sig.v.to_bytes(1, "big").hex()
    )

    print(f"Sending MultiSend (nonce={nonce}, {len(sub_txs)} calls)...",
          end=" ", flush=True)

    log_event({
        "action": "redeem_positions",
        "phase": "submitted",
        "method": "rpc",
        "condition_count": len(sub_txs),
        "total_value": total_value,
        "nonce": nonce,
    })

    cmd = [
        "cast", "send", user,
        "execTransaction(address,uint256,bytes,uint8,uint256,uint256,uint256,address,address,bytes)(bool)",
        _MULTISEND_ADDRESS,
        "0",
        multisend_hex,
        str(_DELEGATECALL_OPERATION),
        "0", "0", "0",
        _ZERO_ADDRESS,
        _ZERO_ADDRESS,
        sig_hex,
        "--private-key", private_key,
        "--rpc-url", rpc,
    ]

    r = subprocess.run(cmd, capture_output=True, text=True, timeout=120)
    if r.returncode != 0:
        print("FAILED")
        print(r.stderr.strip()[:500], file=sys.stderr)
        log_event({
            "action": "redeem_positions",
            "phase": "failed",
            "method": "rpc",
            "error": r.stderr.strip()[:500],
        })
        return []

    tx_hash_hex = None
    for line in r.stdout.splitlines():
        if "transactionHash" in line:
            tx_hash_hex = line.split()[-1]
            print(f"OK (tx: {tx_hash_hex})")
            break
    else:
        print("OK")

    print(f"\nRedeemed {len(sub_txs)} conditions ({fmt_usd(total_value)} total)\n")

    log_event({
        "action": "redeem_positions",
        "phase": "completed",
        "method": "rpc",
        "condition_count": len(sub_txs),
        "total_value": total_value,
        "tx_hash": tx_hash_hex,
    })
    if tx_hash_hex:
        return [tx_hash_hex]
    return []


def redeem_positions(
    user: str, *, dry_run: bool = False, method: str = "relayer",
    positions: list[dict] | None = None,
) -> list[str]:
    """Redeem all resolved winning positions for USDC.

    Args:
        user: proxy wallet address
        dry_run: if True, preview without sending the transaction
        method: "relayer" for gasless via builder API, "rpc" for direct on-chain
        positions: pre-fetched redeemable positions (skips fetch if provided)
    """
    if method == "relayer":
        return redeem_positions_relayer(
            user, dry_run=dry_run, positions=positions)
    elif method == "rpc":
        return redeem_positions_rpc(user, dry_run=dry_run)
    else:
        sys.exit(f"Unknown redeem method: {method!r} (use 'relayer' or 'rpc')")


# ── Merge paired positions ──────────────────────────────────────────────────
#
# Merges paired YES+NO positions back into USDC via on-chain calls to
# ConditionalTokens.mergePositions() or NegRiskAdapter.mergePositions().
#
# Two code paths (same as redeem):
#   - "relayer" (default): gasless via the builder relayer API
#   - "rpc": direct on-chain via Gnosis Safe + cast


def _encode_ct_merge(condition_id: str, amount: int) -> bytes:
    """Encode ConditionalTokens.mergePositions(USDC.e, 0x0, conditionId, [1,2], amount)."""
    return _cast_calldata(
        "mergePositions(address,bytes32,bytes32,uint256[],uint256)",
        _USDC_E,
        "0x0000000000000000000000000000000000000000000000000000000000000000",
        condition_id,
        "[1,2]",
        str(amount),
    )


def _encode_nr_merge(condition_id: str, amount: int) -> bytes:
    """Encode NegRiskAdapter.mergePositions(conditionId, amount)."""
    return _cast_calldata(
        "mergePositions(bytes32,uint256)",
        condition_id,
        str(amount),
    )


def find_mergeable_pairs_from_positions(positions: list[dict]) -> list[dict]:
    """Find mergeable YES/NO pairs inside an already-fetched positions list.

    Returns a list of dicts with keys:
        conditionId, negativeRisk, title, merge_amount, recover_usd,
        positions (list of the two position dicts)
    """
    by_asset = {}
    for p in positions:
        by_asset[p["asset"]] = p

    seen = set()
    pairs = []
    for p in positions:
        asset = p["asset"]
        opp = p.get("oppositeAsset", "")
        if asset in seen or opp in seen:
            continue
        if opp and opp in by_asset:
            seen.add(asset)
            seen.add(opp)
            p2 = by_asset[opp]
            s1 = float(p["size"])
            s2 = float(p2["size"])
            merge_qty = min(s1, s2)
            if merge_qty < 1e-8:
                continue
            pairs.append({
                "conditionId": p.get("conditionId", ""),
                "negativeRisk": p.get("negativeRisk", False),
                "title": p.get("title", "?"),
                "merge_amount": merge_qty,
                "recover_usd": merge_qty,
                "positions": [p, p2],
            })

    return pairs


def find_mergeable_pairs(user: str) -> list[dict]:
    """Find positions where we hold both YES and NO of the same market.

    Returns a list of dicts with keys:
        conditionId, negativeRisk, title, merge_amount, recover_usd,
        positions (list of the two position dicts)
    """
    positions = fetch_positions(user, redeemable=False)
    return find_mergeable_pairs_from_positions(positions)


def filter_mergeable_pairs(
    pairs: list[dict],
    condition_id: str | None = None,
) -> list[dict]:
    """Optionally restrict mergeable pairs to a specific condition."""
    if not condition_id:
        return pairs
    return [pair for pair in pairs if pair.get("conditionId", "") == condition_id]


def print_merge_opportunities(pairs: list[dict]) -> None:
    """Print mergeable pairs in a format matching print_positions."""
    if not pairs:
        print("Mergeable pairs (YES+NO → USDC): none\n")
        return

    total_recover = sum(m["recover_usd"] for m in pairs)
    print(f"Mergeable pairs (YES+NO → USDC): "
          f"{len(pairs)} ({fmt_usd(total_recover)})")

    for m in pairs:
        title = m['title']
        max_title = 50
        if len(title) > max_title:
            title = title[:max_title - 1] + "…"
        print(f"  {fmt_usd(m['recover_usd']):>8}  merge {m['merge_amount']:.2f} shares  {title}")

    print()


def _build_merge_sub_txs(
    pairs: list[dict],
) -> list[tuple[str, bytes, str]]:
    """Build (target, calldata, label) tuples for each merge."""
    result = []
    for m in sorted(pairs, key=lambda x: -x["recover_usd"]):
        cid = m["conditionId"]
        amount_raw = int(m["merge_amount"] * 1_000_000)
        if amount_raw <= 0:
            continue
        label = f"{m['title'][:60]} ({fmt_usd(m['recover_usd'])})"
        if m["negativeRisk"]:
            calldata = _encode_nr_merge(cid, amount_raw)
            target = _NEGRISK_ADAPTER
        else:
            calldata = _encode_ct_merge(cid, amount_raw)
            target = _CTF_ADDRESS
        result.append((target, calldata, label))
    return result


def merge_positions_relayer(
    user: str,
    *,
    dry_run: bool = False,
    condition_id: str | None = None,
) -> list[str]:
    """Merge paired YES+NO positions via the builder relayer (gasless)."""
    from py_builder_relayer_client.models import SafeTransaction, OperationType

    pairs = filter_mergeable_pairs(find_mergeable_pairs(user), condition_id)
    if not pairs:
        if condition_id:
            print(f"No mergeable pair found for condition {condition_id}.\n")
        else:
            print("No mergeable pairs found.\n")
        return []

    total_recover = sum(m["recover_usd"] for m in pairs)
    print_merge_opportunities(pairs)

    sub_txs = _build_merge_sub_txs(pairs)
    if not sub_txs:
        print("All merge amounts too small to execute.\n")
        return []

    print(f"Batching {len(sub_txs)} merges via relayer\n")
    for i, (_, _, label) in enumerate(sub_txs, 1):
        print(f"  {i:3d}. {label}")
    print()

    if dry_run:
        print(f"[dry run] would merge {len(sub_txs)} conditions "
              f"({fmt_usd(total_recover)} total) via relayer\n")
        return []

    relayer = build_relayer_client()

    safe_txs = [
        SafeTransaction(
            to=target,
            operation=OperationType.Call,
            data="0x" + calldata.hex(),
            value="0",
        )
        for target, calldata, _ in sub_txs
    ]

    log_event({
        "action": "merge_positions",
        "phase": "submitted",
        "method": "relayer",
        "condition_count": len(sub_txs),
        "total_recover": total_recover,
    })

    print(f"Submitting {len(sub_txs)} merges to relayer...",
          end=" ", flush=True)

    try:
        response = relayer.execute(safe_txs, "Merge paired positions")
        print(f"submitted (id: {response.transaction_id})")
        print("Waiting for confirmation...", end=" ", flush=True)
        result = response.wait()

        if result is not None:
            state = result.get("state", "unknown")
            tx_hash = result.get("transactionHash")
            print(f"{state}" + (f" (tx: {tx_hash})" if tx_hash else ""))
            print(f"\nMerged {len(sub_txs)} conditions "
                  f"({fmt_usd(total_recover)} total)\n")
            log_event({
                "action": "merge_positions",
                "phase": "completed",
                "method": "relayer",
                "condition_count": len(sub_txs),
                "total_recover": total_recover,
                "tx_hash": tx_hash,
                "state": state,
            })
            if tx_hash:
                return [tx_hash]
            return []
        else:
            print("timed out waiting for confirmation")
            log_event({
                "action": "merge_positions",
                "phase": "timeout",
                "method": "relayer",
                "condition_count": len(sub_txs),
                "total_recover": total_recover,
            })
            return []
    except Exception as exc:
        print(f"FAILED: {exc}")
        log_event({
            "action": "merge_positions",
            "phase": "failed",
            "method": "relayer",
            "error": str(exc),
        })
        return []


def merge_positions_rpc(
    user: str,
    *,
    dry_run: bool = False,
    condition_id: str | None = None,
) -> list[str]:
    """Merge paired YES+NO positions via direct RPC (pays gas)."""
    pairs = filter_mergeable_pairs(find_mergeable_pairs(user), condition_id)
    if not pairs:
        if condition_id:
            print(f"No mergeable pair found for condition {condition_id}.\n")
        else:
            print("No mergeable pairs found.\n")
        return []

    total_recover = sum(m["recover_usd"] for m in pairs)
    print_merge_opportunities(pairs)

    sub_txs_labeled = _build_merge_sub_txs(pairs)
    if not sub_txs_labeled:
        print("All merge amounts too small to execute.\n")
        return []

    sub_txs: list[tuple[str, bytes]] = [
        (t, c) for t, c, _ in sub_txs_labeled
    ]
    labels = [lbl for _, _, lbl in sub_txs_labeled]

    print(f"Batching {len(sub_txs)} merges into 1 MultiSend transaction\n")
    for i, label in enumerate(labels, 1):
        print(f"  {i:3d}. {label}")
    print()

    if dry_run:
        print(f"[dry run] would merge {len(sub_txs)} conditions "
              f"({fmt_usd(total_recover)} total) in 1 transaction\n")
        return []

    rpc = require_env("POLYGON_RPC_URL")
    private_key = require_env("EOA_PRIVATE_KEY")

    eoa = Account.from_key(private_key)
    print(f"EOA: {eoa.address}")
    print(f"Safe: {user}\n")

    multisend_data = _encode_multisend(sub_txs)
    multisend_hex = "0x" + multisend_data.hex()

    nonce = _get_safe_nonce(user, rpc)
    tx_hash = _safe_tx_hash(
        proxy=user,
        to=_MULTISEND_ADDRESS,
        value=0,
        data=multisend_data,
        operation=_DELEGATECALL_OPERATION,
        nonce=nonce,
        chain_id=137,
    )

    sig = eoa.unsafe_sign_hash(tx_hash)
    sig_hex = "0x" + (
        sig.r.to_bytes(32, "big").hex()
        + sig.s.to_bytes(32, "big").hex()
        + sig.v.to_bytes(1, "big").hex()
    )

    print(f"Sending MultiSend (nonce={nonce}, {len(sub_txs)} calls)...",
          end=" ", flush=True)

    log_event({
        "action": "merge_positions",
        "phase": "submitted",
        "method": "rpc",
        "condition_count": len(sub_txs),
        "total_recover": total_recover,
        "nonce": nonce,
    })

    cmd = [
        "cast", "send", user,
        "execTransaction(address,uint256,bytes,uint8,uint256,uint256,uint256,address,address,bytes)(bool)",
        _MULTISEND_ADDRESS,
        "0",
        multisend_hex,
        str(_DELEGATECALL_OPERATION),
        "0", "0", "0",
        _ZERO_ADDRESS,
        _ZERO_ADDRESS,
        sig_hex,
        "--private-key", private_key,
        "--rpc-url", rpc,
    ]

    r = subprocess.run(cmd, capture_output=True, text=True, timeout=120)
    if r.returncode != 0:
        print("FAILED")
        print(r.stderr.strip()[:500], file=sys.stderr)
        log_event({
            "action": "merge_positions",
            "phase": "failed",
            "method": "rpc",
            "error": r.stderr.strip()[:500],
        })
        return []

    tx_hash_hex = None
    for line in r.stdout.splitlines():
        if "transactionHash" in line:
            tx_hash_hex = line.split()[-1]
            print(f"OK (tx: {tx_hash_hex})")
            break
    else:
        print("OK")

    print(f"\nMerged {len(sub_txs)} conditions "
          f"({fmt_usd(total_recover)} total)\n")

    log_event({
        "action": "merge_positions",
        "phase": "completed",
        "method": "rpc",
        "condition_count": len(sub_txs),
        "total_recover": total_recover,
        "tx_hash": tx_hash_hex,
    })
    if tx_hash_hex:
        return [tx_hash_hex]
    return []


def merge_positions(
    user: str,
    *,
    dry_run: bool = False,
    method: str = "relayer",
    condition_id: str | None = None,
) -> list[str]:
    """Merge all paired YES+NO positions back to USDC.

    Args:
        user: proxy wallet address
        dry_run: if True, preview without sending the transaction
        method: "relayer" for gasless via builder API, "rpc" for direct on-chain
        condition_id: if set, merge only this condition instead of every pair
    """
    if method == "relayer":
        return merge_positions_relayer(user, dry_run=dry_run, condition_id=condition_id)
    elif method == "rpc":
        return merge_positions_rpc(user, dry_run=dry_run, condition_id=condition_id)
    else:
        sys.exit(f"Unknown merge method: {method!r} (use 'relayer' or 'rpc')")
