#!/usr/bin/env python3
"""Reduce exposure on our account. Book actions are CLOB; redeem/merge follow --exec."""

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
    execution_client,
    listen_host,
    settled_stream,
)
from traders.lib.ui import TraderUI, format_hms, shorten  # noqa: E402


def _extract_tx_hashes(raw: object) -> list[str]:
    if not isinstance(raw, dict):
        return []
    out: list[str] = []
    for key in ("transactionsHashes", "transactionHashes"):
        vals = raw.get(key, [])
        if isinstance(vals, list):
            for x in vals:
                if isinstance(x, str) and x:
                    out.append(x)
    return out


async def run(args: argparse.Namespace) -> int:
    trading_lib.load_env()
    ui = TraderUI("liquidate", __file__)
    account = trading_lib.get_funder_address()
    actions = []
    if args.cancel_orders:
        actions.append("cancel-orders")
    if args.limit_sell is not None:
        actions.append(f"limit-sell={args.limit_sell:g}")
    if args.market_sell:
        actions.append("market-sell")
    if args.redeem:
        actions.append("redeem")
    if args.merge:
        actions.append("merge")
    if args.dry_run:
        actions.append("dry-run")
    ui.opening(
        f"liquidate  exec={args.exec}  listen={args.listen}  "
        f"order-book=clob  redeem/merge={args.exec}  actions={','.join(actions)}",
        account=account,
    )
    ui.print(f"order-book: clob")
    ui.print(f"redeem/merge: {args.exec}")

    clob = trading_lib.build_client()
    exec_client = execution_client(args.exec)
    started = time.monotonic()
    stream = None
    if not args.dry_run:
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
        try:
            usdc = trading_lib.get_usdc_balance(clob)
            ui.print(f"usdc={trading_lib.fmt_usd(usdc)}")
        except Exception as exc:
            ui.print(f"usdc=unavailable  error={exc}")

        _print_snapshot(ui, clob, account)

        expected: list[str] = []

        if args.cancel_orders:
            ui.print("cancel_orders submitted")
            trading_lib.cancel_all_orders(clob, dry_run=args.dry_run)

        if args.limit_sell is not None:
            trading_lib.cancel_all_orders(clob, dry_run=args.dry_run)
            ui.print(f"limit_sell offset={args.limit_sell:g} ttl={args.limit_sell_ttl}s")
            trading_lib.create_limit_sell_orders(
                clob,
                account,
                offset_pct=args.limit_sell,
                ttl_seconds=args.limit_sell_ttl,
                dry_run=args.dry_run,
            )

        if args.market_sell:
            if args.dry_run:
                ui.print("market_sell dry-run")
            else:
                ui.print("market_sell submitted")
                results = trading_lib.dump_all_positions(clob, account)
                for row in results:
                    if row.get("status") == "ok":
                        expected.extend(_extract_tx_hashes(row.get("response")))

        if args.redeem:
            hashes = await exec_client.redeem_positions(account, dry_run=args.dry_run)
            expected.extend(hashes)
            ui.print(f"redeem submitted  txs={len(hashes)}")

        if args.merge:
            hashes = await exec_client.merge_positions(account, dry_run=args.dry_run)
            expected.extend(hashes)
            ui.print(f"merge submitted  txs={len(hashes)}")

        if args.dry_run or not expected:
            ui.closing(
                f"status=ok  txs=0  dry_run={str(args.dry_run).lower()}",
                time.monotonic() - started,
            )
            return 0

        assert stream is not None
        landed = await _wait_hashes(ui, stream, args.listen, expected)
        missing = len(expected) - landed
        if missing:
            ui.closing(
                f"status=failed  timeout={SETTLEMENT_TIMEOUT_SECONDS}s  "
                f"landed={landed}  missing={missing}",
                time.monotonic() - started,
            )
            return 1
        ui.closing(
            f"status=ok  landed={landed}  txs={len(expected)}",
            time.monotonic() - started,
        )
        return 0
    finally:
        if stream is not None:
            await stream.disconnect()


def _print_snapshot(ui: TraderUI, client, account: str) -> None:
    try:
        open_positions = trading_lib.fetch_positions(account, redeemable=False)
        redeemable = [
            p
            for p in trading_lib.fetch_positions(account, redeemable=True)
            if float(p.get("curPrice", 0)) > 0
        ]
        orders = trading_lib.fetch_open_orders(client)
        mergeable = trading_lib.find_mergeable_pairs(account)
        ui.print(
            f"snapshot  positions={len(open_positions)}  "
            f"redeemable={len(redeemable)}  mergeable={len(mergeable)}  "
            f"open_orders={len(orders)}"
        )
    except Exception as exc:
        ui.print(f"snapshot unavailable  error={exc}")


async def _wait_hashes(
    ui: TraderUI,
    stream,
    listen: str,
    tx_hashes: list[str],
) -> int:
    wanted = {x.lower() for x in tx_hashes if x}
    landed: set[str] = set()
    wait_started = time.monotonic()
    ui.start_footer(
        f"waiting  txs={len(wanted)}  listen={listen}  {format_hms(0)}"
    )

    async def _consume() -> None:
        async for event in stream:
            if not isinstance(event, TradeEvent):
                continue
            tx = event.tx_hash.lower()
            if tx in wanted and tx not in landed:
                landed.add(tx)
                ui.print(f"settled  tx={shorten(event.tx_hash)}  block={event.block_number:,}")
            if landed == wanted:
                return

    async def _tick() -> None:
        while True:
            elapsed = time.monotonic() - wait_started
            ui.update_footer(
                f"waiting  txs={len(wanted) - len(landed)}/{len(wanted)}  "
                f"listen={listen}  {format_hms(elapsed)}"
            )
            await asyncio.sleep(0.25)

    ticker = asyncio.create_task(_tick())
    try:
        await asyncio.wait_for(_consume(), timeout=SETTLEMENT_TIMEOUT_SECONDS)
    except TimeoutError:
        pass
    finally:
        ticker.cancel()
        ui.stop_footer()
    return len(landed)


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description="Cancel, sell, redeem, or merge our exposure."
    )
    parser.add_argument("--exec", required=True, choices=EXEC_CHOICES)
    parser.add_argument("--listen", required=True, choices=LISTEN_CHOICES)
    parser.add_argument("--cancel-orders", action="store_true")
    parser.add_argument(
        "--limit-sell",
        metavar="PCT",
        type=trading_lib.parse_limit_sell_offset,
        default=None,
    )
    parser.add_argument(
        "--limit-sell-ttl",
        default="72h",
        metavar="DUR",
        type=trading_lib.parse_ttl,
    )
    parser.add_argument("--market-sell", action="store_true")
    parser.add_argument("--redeem", action="store_true")
    parser.add_argument("--merge", action="store_true")
    parser.add_argument("--dry-run", action="store_true")
    args = parser.parse_args()
    if not any(
        [
            args.cancel_orders,
            args.limit_sell is not None,
            args.market_sell,
            args.redeem,
            args.merge,
        ]
    ):
        parser.error(
            "at least one of --cancel-orders --limit-sell --market-sell --redeem --merge"
        )
    return args


def main() -> None:
    args = parse_args()
    raise SystemExit(asyncio.run(run(args)))


if __name__ == "__main__":
    main()
