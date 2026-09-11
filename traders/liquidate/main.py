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
    has_write = bool(actions)
    ui.opening(
        f"liquidate  exec={args.exec}  listen={args.listen}  "
        f"order-book=clob  redeem/merge={args.exec}  "
        f"actions={','.join(actions) if actions else 'snapshot'}",
        account=account,
    )
    ui.print(f"order-book: clob")
    ui.print(f"redeem/merge: {args.exec}")

    clob = trading_lib.build_client()
    exec_client = execution_client(args.exec)
    started = time.monotonic()
    stream = None

    try:
        _print_usdc(ui, clob, account)
        _print_snapshot(ui, clob, account)

        if not has_write:
            ui.closing("status=ok  snapshot", time.monotonic() - started)
            return 0

        expected: list[str] = []
        fill_hashes: list[str] = []

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
                        hashes = _extract_tx_hashes(row.get("response"))
                        expected.extend(hashes)
                        fill_hashes.extend(hashes)

        if args.redeem:
            try:
                hashes = await exec_client.redeem_positions(account, dry_run=args.dry_run)
            except Exception as exc:
                ui.print(f"redeem failed  error={exc}")
                ui.closing(
                    f"status=failed  error={exc}",
                    time.monotonic() - started,
                )
                return 1
            expected.extend(hashes)
            ui.print(f"redeem submitted  txs={len(hashes)}")

        if args.merge:
            hashes = await exec_client.merge_positions(account, dry_run=args.dry_run)
            expected.extend(hashes)
            ui.print(f"merge submitted  txs={len(hashes)}")

        if args.dry_run or not expected:
            ui.closing(
                f"status=ok  txs={len(expected)}  dry_run={str(args.dry_run).lower()}",
                time.monotonic() - started,
            )
            return 0

        # Relayer redeem/merge already waited until STATE_MINED. Those txs are
        # ConditionalTokens, not OrderFilled — --listen would never see them.
        if not fill_hashes:
            ui.closing(
                f"status=ok  txs={len(expected)}",
                time.monotonic() - started,
            )
            return 0

        stream = settled_stream(args.listen, on_status=ui.print)
        ui.print(f"connecting listen={args.listen}  host={listen_host(args.listen)}")
        try:
            await stream.connect()
        except Exception as exc:
            ui.closing(
                f"status=ok  txs={len(expected)}  listen_failed={type(exc).__name__}: {exc}",
                time.monotonic() - started,
            )
            return 0

        landed = await _wait_hashes(ui, stream, args.listen, fill_hashes)
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


def _print_usdc(ui: TraderUI, client, account: str) -> None:
    try:
        usdc = trading_lib.get_usdc_balance_via_rpc(account)
        ui.print(f"usdc={trading_lib.fmt_usd(usdc)}")
        return
    except Exception as rpc_exc:
        ui.log_only(f"usdc via rpc failed: {rpc_exc}")
    try:
        usdc = trading_lib.get_usdc_balance(client)
        ui.print(f"usdc={trading_lib.fmt_usd(usdc)}")
    except Exception as exc:
        ui.print(
            "usdc=unavailable  "
            f"error={trading_lib.explain_api_error('CLOB API', trading_lib.get_clob_api_url(), exc)}"
        )


def _print_snapshot(ui: TraderUI, client, account: str) -> None:
    try:
        open_positions = trading_lib.fetch_positions(account, redeemable=False)
        redeemable = [
            p
            for p in trading_lib.fetch_positions(account, redeemable=True)
            if float(p.get("curPrice", 0)) > 0
        ]
    except Exception as exc:
        ui.print(f"positions unavailable  error={exc}")
        open_positions, redeemable = [], []

    mergeable = trading_lib.find_mergeable_pairs_from_positions(open_positions)

    total = sum(float(p.get("currentValue", 0)) for p in open_positions)
    ui.print(f"positions: {len(open_positions)} ({trading_lib.fmt_usd(total)})")
    for p in open_positions:
        outcome = str(p.get("outcome", "?"))
        if len(outcome) > 6:
            outcome = outcome[:5] + "…"
        title = str(p.get("title", "?"))
        if len(title) > 50:
            title = title[:49] + "…"
        ui.print(
            f"  {trading_lib.fmt_usd(float(p.get('currentValue', 0))):>8}  "
            f"{outcome:6}  {title}"
        )
    if not open_positions:
        ui.print("  none")

    ui.print(f"redeemable: {len(redeemable)}")
    for p in redeemable:
        title = str(p.get("title", "?"))
        if len(title) > 50:
            title = title[:49] + "…"
        ui.print(
            f"  {trading_lib.fmt_usd(float(p.get('currentValue', 0))):>8}  {title}"
        )

    recover = sum(float(m.get("recover_usd", 0)) for m in mergeable)
    ui.print(f"mergeable: {len(mergeable)} ({trading_lib.fmt_usd(recover)})")
    for m in mergeable:
        title = str(m.get("title", "?"))
        if len(title) > 50:
            title = title[:49] + "…"
        ui.print(f"  {trading_lib.fmt_usd(float(m.get('recover_usd', 0))):>8}  {title}")

    try:
        orders = trading_lib.fetch_open_orders(client)
    except Exception as exc:
        if "401" in str(exc):
            try:
                trading_lib.derive_clob_api_creds(client)
                orders = trading_lib.fetch_open_orders(client)
            except Exception as exc2:
                ui.print(
                    "open_orders unavailable  "
                    f"error={trading_lib.explain_api_error('CLOB API', client.host, exc2)}"
                )
                return
        else:
            ui.print(
                "open_orders unavailable  "
                f"error={trading_lib.explain_api_error('CLOB API', client.host, exc)}"
            )
            return

    ui.print(f"open_orders: {len(orders)}")
    for o in orders:
        side = o.get("side", "?")
        price = float(o.get("price", 0))
        original = float(o.get("original_size", 0) or o.get("size", 0))
        matched = float(o.get("size_matched", 0) or o.get("sizeMatched", 0) or 0)
        remaining = original - matched
        ui.print(
            f"  {side:4} {remaining:>8,.2f} @ {trading_lib.fmt_usd(price)}"
        )


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
        description="Cancel, sell, redeem, or merge our exposure. "
        "With no action flags, print a snapshot of current positions."
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
    return parser.parse_args()


def main() -> None:
    args = parse_args()
    raise SystemExit(asyncio.run(run(args)))


if __name__ == "__main__":
    main()
