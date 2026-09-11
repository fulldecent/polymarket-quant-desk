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

from rich.text import Text  # noqa: E402

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
from traders.lib.ui import COLOR_DONE, TraderUI, format_hms, shorten  # noqa: E402

# CLOB v2 FOK buy of $2 posted as 2.005990 pUSD (taker fee). Keep a 1% buffer
# when wrapping just enough USDC.e to cover the order.
_WRAP_FEE_BUFFER = 1.01


def _print_collateral(ui: TraderUI, account: str) -> tuple[float, float]:
    pusd = trading_lib.get_pusd_balance_via_rpc(account)
    usdce = trading_lib.get_usdc_balance_via_rpc(account)
    line = Text("pUSD        ")
    line.append(trading_lib.fmt_usd(pusd), style=COLOR_DONE)
    ui.print(line)
    line = Text("usdc.e      ")
    line.append(trading_lib.fmt_usd(usdce), style=COLOR_DONE)
    ui.print(line)
    return pusd, usdce


async def run(args: argparse.Namespace) -> int:
    trading_lib.load_env()
    ui = TraderUI("buy_token", __file__)
    account = trading_lib.get_funder_address()
    worst = "none" if args.worst_price is None else f"{args.worst_price:g}"
    ui.opening(account=account)
    ui.log_only(
        f"exec={args.exec}  listen={args.listen}  amount={args.amount:.2f}  "
        f"worst_price={worst}  wrap={str(args.wrap).lower()}"
    )

    started = time.monotonic()
    try:
        pusd, usdce = _print_collateral(ui, account)
    except Exception as exc:
        ui.closing(
            f"status=failed  error={exc}",
            time.monotonic() - started,
        )
        return 1

    needed = args.amount
    wrap_usd: float | None = None
    if pusd + 1e-9 < needed:
        wrap_usd = round((needed - pusd) * _WRAP_FEE_BUFFER, 6)
        if wrap_usd < 0.01:
            wrap_usd = 0.01
        if not args.wrap:
            ui.closing(
                f"status=failed  error=pUSD {trading_lib.fmt_usd(pusd)} "
                f"need {trading_lib.fmt_usd(needed)}; "
                f"USDC.e {trading_lib.fmt_usd(usdce)} is not CLOB v2 collateral. "
                "Wrap: python traders/liquidate/main.py --exec clob --listen rpc --wrap",
                time.monotonic() - started,
            )
            return 1
        if usdce + 1e-9 < wrap_usd:
            ui.closing(
                f"status=failed  error=need {trading_lib.fmt_usd(wrap_usd)} pUSD, "
                f"have pUSD {trading_lib.fmt_usd(pusd)} and "
                f"USDC.e {trading_lib.fmt_usd(usdce)}",
                time.monotonic() - started,
            )
            return 1
    elif args.wrap:
        wrap_usd = 0.0
    else:
        try:
            missing = trading_lib.missing_pusd_spenders(account)
        except Exception as exc:
            ui.closing(
                f"status=failed  error={exc}",
                time.monotonic() - started,
            )
            return 1
        if missing:
            spenders = " ".join(shorten(s) for s in missing)
            ui.closing(
                f"status=failed  error=pUSD allowance 0 for {spenders}. "
                "Wrap: python traders/liquidate/main.py --exec clob --listen rpc --wrap",
                time.monotonic() - started,
            )
            return 1

    if wrap_usd is not None:
        amount_s = (
            "approvals"
            if wrap_usd == 0
            else f"{trading_lib.fmt_usd(wrap_usd)} USDC.e → pUSD"
        )
        ui.print(f"wrap         {amount_s}")
        ui.start_footer("wrapping  usdc.e → pUSD")
        try:
            result = await asyncio.to_thread(
                trading_lib.wrap_usdce_to_pusd,
                account,
                amount_usd=wrap_usd,
                dry_run=False,
            )
        except Exception as exc:
            ui.closing(
                f"status=failed  error={exc}",
                time.monotonic() - started,
            )
            return 1
        finally:
            ui.stop_footer()
        for label in result.labels:
            ui.print(f"  {label}")
        tx = result.tx_hashes[0] if result.tx_hashes else ""
        if result.tx_hashes:
            ui.print(
                f"wrap mined   tx={shorten(tx)}  "
                f"pUSD={trading_lib.fmt_usd(result.pusd_after)}"
            )
        elif result.labels:
            ui.print("wrap none")
        pusd = result.pusd_after
        if pusd + 1e-9 < needed:
            ui.closing(
                f"status=failed  error=pUSD {trading_lib.fmt_usd(pusd)} "
                f"after wrap, need {trading_lib.fmt_usd(needed)}",
                time.monotonic() - started,
            )
            return 1

    client = execution_client(args.exec)
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
        epilog=(
            "TOKEN_ID is a CLOB Yes/No token. Look one up with:\n"
            "  python explorations/token-search/main.py <query>\n"
            "then:\n"
            "  python traders/buy_token/main.py TOKEN_ID --exec clob --listen rpc --amount 2"
        ),
        formatter_class=argparse.RawDescriptionHelpFormatter,
    )
    parser.add_argument(
        "token_id",
        nargs="?",
        metavar="TOKEN_ID",
        help="decimal clobTokenId",
    )
    parser.add_argument("--exec", choices=EXEC_CHOICES)
    parser.add_argument("--listen", choices=LISTEN_CHOICES)
    parser.add_argument("--amount", type=float, metavar="USD")
    parser.add_argument(
        "--worst-price",
        type=float,
        default=None,
        metavar="P",
        help="worst acceptable fill price per share (default: no cap)",
    )
    parser.add_argument(
        "--wrap",
        action="store_true",
        help="wrap just enough USDC.e → pUSD before the buy (CLOB v2 collateral)",
    )
    args = parser.parse_args()
    if not args.token_id:
        parser.print_help()
        raise SystemExit(2)
    missing = [
        name
        for name, ok in (
            ("--exec", args.exec),
            ("--listen", args.listen),
            ("--amount", args.amount is not None),
        )
        if not ok
    ]
    if missing:
        parser.error("the following arguments are required: " + ", ".join(missing))
    if args.amount < 2:
        parser.error("--amount must be >= 2")
    return args


def main() -> None:
    args = parse_args()
    raise SystemExit(asyncio.run(run(args)))


if __name__ == "__main__":
    main()
