#!/usr/bin/env python3
"""One fill-or-kill market buy, then wait on --listen for the fill block."""

from __future__ import annotations

import argparse
import asyncio
import sys
import time
from pathlib import Path

_project_root = Path(__file__).resolve().parents[2]
sys.path.insert(0, str(_project_root))

from exchange_client.lib import trading_lib  # noqa: E402
from exchange_client.lib.event_stream import TradeEvent  # noqa: E402
from traders.lib.streams import (  # noqa: E402
    EXEC_CHOICES,
    LISTEN_CHOICES,
    SETTLEMENT_TIMEOUT_SECONDS,
    cleared_price,
    execution_client,
    listen_host,
    settled_stream,
)
from traders.lib.ui import TraderUI, format_hms, shorten  # noqa: E402


async def run(args: argparse.Namespace) -> int:
    trading_lib.load_env()
    ui = TraderUI("buy_token", __file__)
    account = trading_lib.get_funder_address()
    worst = "none" if args.worst_price is None else f"{args.worst_price:g}"
    ui.opening(account=account)
    ui.log_only(
        f"exec={args.exec}  listen={args.listen}  amount={args.amount:.2f}  "
        f"worst_price={worst}"
    )

    client = execution_client(args.exec)
    started = time.monotonic()
    stream = settled_stream(args.listen, on_status=ui.print)
    ui.print(f"connecting listen={args.listen}  host={listen_host(args.listen)}")
    try:
        await stream.connect()
    except Exception as exc:
        ui.closing(
            f"status=failed  error={type(exc).__name__}: {exc}",
            time.monotonic() - started,
        )
        return 1

    try:
        ui.print(f"warmup token={shorten(args.token_id, head=4, tail=4)}")
        await client.warmup(args.token_id)

        max_price = 0.0 if args.worst_price is None else args.worst_price
        try:
            result = await client.buy_market(
                args.token_id, args.amount, max_price=max_price
            )
        except Exception as exc:
            ui.closing(
                f"status=failed  error={exc}",
                time.monotonic() - started,
            )
            return 1

        tx = result.tx_hashes[0] if result.tx_hashes else ""
        order = result.order_id or ""
        ui.print(f"submitted order={shorten(order)}  tx={shorten(tx)}")
        ui.log_only(f"order_response {result.raw_response!r}")

        if not result.success:
            ui.closing(
                f"status=failed  error=order_rejected  order={shorten(order)}",
                time.monotonic() - started,
            )
            return 1
        if not result.tx_hashes:
            ui.closing(
                f"status=failed  error=no_tx_hash  order={shorten(order)}",
                time.monotonic() - started,
            )
            return 1

        settlement = await _wait_for_fill(
            ui,
            stream=stream,
            listen=args.listen,
            tx_hashes=result.tx_hashes,
            token_id=args.token_id,
        )
        if settlement is None:
            ui.closing(
                f"status=failed  timeout={SETTLEMENT_TIMEOUT_SECONDS}s  "
                f"order={shorten(order)}  tx={shorten(tx)}",
                time.monotonic() - started,
            )
            return 1

        price = settlement.get("cleared_price")
        price_s = f"{price:.6f}" if price is not None else "none"
        ui.closing(
            f"status=ok  order={shorten(order)}  block={settlement['block']:,}  "
            f"cleared={price_s}  tx={shorten(settlement['tx_hash'])}",
            time.monotonic() - started,
        )
        return 0
    finally:
        await stream.disconnect()


async def _wait_for_fill(
    ui: TraderUI,
    *,
    stream,
    listen: str,
    tx_hashes: list[str],
    token_id: str,
) -> dict | None:
    wanted = {x.lower() for x in tx_hashes if x}
    tx_show = shorten(next(iter(tx_hashes)))
    wait_started = time.monotonic()
    ui.start_footer(
        f"waiting  tx={tx_show}  listen={listen}  {format_hms(0)}"
    )

    async def _match() -> dict:
        async for event in stream:
            if not isinstance(event, TradeEvent):
                continue
            if event.tx_hash.lower() not in wanted:
                continue
            price = cleared_price(event, token_id)
            return {
                "tx_hash": event.tx_hash,
                "block": event.block_number,
                "cleared_price": price,
            }
        raise RuntimeError("listen stream ended")

    async def _tick() -> None:
        while True:
            elapsed = time.monotonic() - wait_started
            ui.update_footer(
                f"waiting  tx={tx_show}  listen={listen}  {format_hms(elapsed)}"
            )
            await asyncio.sleep(0.25)

    ticker = asyncio.create_task(_tick())
    try:
        return await asyncio.wait_for(_match(), timeout=SETTLEMENT_TIMEOUT_SECONDS)
    except TimeoutError:
        return None
    finally:
        ticker.cancel()
        ui.stop_footer()


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description="Fill-or-kill market buy of one outcome token.",
        epilog="Look up TOKEN_ID: python explorations/token-search/main.py <query>",
        formatter_class=argparse.RawDescriptionHelpFormatter,
    )
    parser.add_argument(
        "token_id",
        metavar="TOKEN_ID",
        help="decimal clobTokenId (python explorations/token-search/main.py <query>)",
    )
    parser.add_argument("--exec", required=True, choices=EXEC_CHOICES)
    parser.add_argument("--listen", required=True, choices=LISTEN_CHOICES)
    parser.add_argument("--amount", type=float, required=True, metavar="USD")
    parser.add_argument(
        "--worst-price",
        type=float,
        default=None,
        metavar="P",
        help="worst acceptable fill price per share (default: no cap)",
    )
    args = parser.parse_args()
    if args.amount < 2:
        parser.error("--amount must be >= 2")
    return args


def main() -> None:
    args = parse_args()
    raise SystemExit(asyncio.run(run(args)))


if __name__ == "__main__":
    main()
