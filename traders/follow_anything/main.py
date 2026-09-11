#!/usr/bin/env python3
"""Copy the next N settled buy fills as FOK market buys. Measure block lag."""

from __future__ import annotations

import argparse
import asyncio
import sys
import time
from pathlib import Path

_project_root = Path(__file__).resolve().parents[2]
sys.path.insert(0, str(_project_root))

from exchange_client.lib import trading_lib  # noqa: E402
from exchange_client.lib.event_stream import MempoolTradeEvent, TradeEvent  # noqa: E402
from traders.lib.streams import (  # noqa: E402
    EXEC_CHOICES,
    LISTEN_CHOICES,
    SETTLEMENT_TIMEOUT_SECONDS,
    execution_client,
    extract_buy_trigger,
    listen_host,
    mempool_stream,
    settled_stream,
)
from traders.lib.ui import TraderUI, format_hms, shorten, shorten_addr  # noqa: E402


async def run(args: argparse.Namespace) -> int:
    trading_lib.load_env()
    ui = TraderUI("follow_anything", __file__)
    funder = trading_lib.get_funder_address()
    eoa = trading_lib.get_eoa_address()
    skip = {funder.lower(), eoa.lower()} - {""}
    mempool_flag = "on" if args.trigger_polynode_mempool else "off"
    ui.opening(
        f"follow_anything  exec={args.exec}  listen={args.listen}  "
        f"mempool={mempool_flag}  amount={args.amount:.2f}  count={args.count}  "
        f"warmup={args.warmup_seconds:g}s",
        account=funder,
    )

    client = execution_client(args.exec)
    started = time.monotonic()
    listen = settled_stream(args.listen, on_status=ui.print)
    mempool = (
        mempool_stream(on_status=ui.print) if args.trigger_polynode_mempool else None
    )
    ui.print(f"connecting listen={args.listen}  host={listen_host(args.listen)}")
    try:
        await listen.connect()
        if mempool is not None:
            await mempool.connect()
    except Exception as exc:
        ui.closing(
            f"status=failed  error={type(exc).__name__}: {exc}",
            time.monotonic() - started,
        )
        return 1

    next_ready = time.monotonic() + args.warmup_seconds
    settled_n = 0
    lags: list[int | None] = []
    last_lag = "none"
    pending: dict | None = None
    trigger_blocks: dict[str, int] = {}
    seen_triggers: set[str] = set()

    listen_q: asyncio.Queue[TradeEvent] = asyncio.Queue()
    trigger_q: asyncio.Queue[TradeEvent | MempoolTradeEvent] = asyncio.Queue()

    async def _pump_listen() -> None:
        async for event in listen:
            await listen_q.put(event)
            if mempool is None:
                await trigger_q.put(event)

    async def _pump_mempool() -> None:
        assert mempool is not None
        async for event in mempool:
            await trigger_q.put(event)

    pumps = [asyncio.create_task(_pump_listen())]
    if mempool is not None:
        pumps.append(asyncio.create_task(_pump_mempool()))
    ui.start_footer(f"copied 0/{args.count}  last_lag=none  waiting")

    async def _drain() -> None:
        nonlocal pending, settled_n, last_lag
        while True:
            try:
                event = listen_q.get_nowait()
            except asyncio.QueueEmpty:
                return
            tx = event.tx_hash.lower()
            if tx in trigger_blocks and trigger_blocks[tx] == 0:
                trigger_blocks[tx] = event.block_number
            if pending is None:
                continue
            if tx not in pending["expected"]:
                continue
            pending["landed"].add(tx)
            pending["copy_block"] = event.block_number
            trig_block = trigger_blocks.get(pending["trigger_tx"])
            lag = None
            if trig_block:
                lag = event.block_number - trig_block
            pending["lag"] = lag
            if pending["landed"] == pending["expected"]:
                lags.append(lag)
                last_lag = "none" if lag is None else str(lag)
                settled_n += 1
                ui.print(
                    f"settled  seq={settled_n}  copy_tx={shorten(event.tx_hash)}  "
                    f"copy_block={event.block_number:,}  "
                    f"trigger_block={trig_block}  lag={last_lag}"
                )
                pending = None

    try:
        while settled_n < args.count:
            await _drain()
            elapsed = format_hms(time.monotonic() - started)
            ui.update_footer(
                f"copied {settled_n}/{args.count}  last_lag={last_lag}  waiting  {elapsed}"
            )

            if pending is not None:
                if time.monotonic() - pending["submitted_at"] > SETTLEMENT_TIMEOUT_SECONDS:
                    ui.print(
                        f"timeout  seq={settled_n + 1}  "
                        f"tx={shorten(next(iter(pending['expected'])))}  "
                        f"timeout={SETTLEMENT_TIMEOUT_SECONDS}s"
                    )
                    pending = None
                    next_ready = time.monotonic() + args.warmup_seconds
                else:
                    await asyncio.sleep(0.1)
                    continue

            now = time.monotonic()
            if now < next_ready:
                await asyncio.sleep(min(0.25, next_ready - now))
                continue

            try:
                event = await asyncio.wait_for(trigger_q.get(), timeout=0.5)
            except asyncio.TimeoutError:
                continue

            trigger = extract_buy_trigger(event)
            if trigger is None:
                continue
            tx = event.tx_hash.lower()
            if tx in seen_triggers:
                continue
            seen_triggers.add(tx)

            parties = {trigger["buyer"].lower()}
            if getattr(event, "maker", ""):
                parties.add(event.maker.lower())
            if getattr(event, "taker", ""):
                parties.add(event.taker.lower())
            if parties & skip:
                ui.log_only(f"skip own account trigger {tx}")
                continue

            if isinstance(event, TradeEvent):
                trigger_blocks[tx] = event.block_number
            else:
                trigger_blocks[tx] = 0

            ui.print(
                f"trigger  token={shorten(trigger['buy_token_id'], head=4, tail=4)}  "
                f"buyer={shorten_addr(trigger['buyer'])}  "
                f"value=${trigger['trade_value_usd']:.2f}  tx={shorten(event.tx_hash)}"
            )

            try:
                await client.warmup(trigger["buy_token_id"])
                result = await client.buy_market(trigger["buy_token_id"], args.amount)
            except Exception as exc:
                ui.print(f"copy_failed  error={exc}")
                next_ready = time.monotonic() + args.warmup_seconds
                continue

            if not result.success or not result.tx_hashes:
                ui.print("copy_failed  error=order_rejected")
                next_ready = time.monotonic() + args.warmup_seconds
                continue

            expected = {x.lower() for x in result.tx_hashes if x}
            pending = {
                "trigger_tx": tx,
                "expected": expected,
                "landed": set(),
                "submitted_at": time.monotonic(),
                "copy_block": None,
                "lag": None,
            }
            ui.print(
                f"submitted  order={shorten(result.order_id or '')}  "
                f"tx={shorten(result.tx_hashes[0])}"
            )
            next_ready = time.monotonic() + args.warmup_seconds

        await _drain()
        ok = settled_n >= args.count
        lag_s = ",".join("none" if x is None else str(x) for x in lags)
        ui.closing(
            f"status={'ok' if ok else 'failed'}  settled={settled_n}/{args.count}  lags={lag_s}",
            time.monotonic() - started,
        )
        return 0 if ok else 1
    finally:
        for task in pumps:
            task.cancel()
        await listen.disconnect()
        if mempool is not None:
            await mempool.disconnect()


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description="Copy N buy fills and measure settlement lag."
    )
    parser.add_argument("--exec", required=True, choices=EXEC_CHOICES)
    parser.add_argument("--listen", required=True, choices=LISTEN_CHOICES)
    parser.add_argument("--trigger-polynode-mempool", action="store_true")
    parser.add_argument(
        "--warmup",
        default="10s",
        metavar="DUR",
        type=trading_lib.parse_ttl,
    )
    parser.add_argument("--amount", default=2.0, metavar="USD", type=float)
    parser.add_argument("--count", default=5, type=int, metavar="N")
    args = parser.parse_args()
    if args.amount < 2:
        parser.error("--amount must be >= 2")
    if args.count < 1:
        parser.error("--count must be >= 1")
    args.warmup_seconds = args.warmup
    return args


def main() -> None:
    args = parse_args()
    raise SystemExit(asyncio.run(run(args)))


if __name__ == "__main__":
    main()
