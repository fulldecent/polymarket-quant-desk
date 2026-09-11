#!/usr/bin/env python3
"""Watch settled (and optional mempool) fills. No orders."""

from __future__ import annotations

import argparse
import asyncio
import signal
import sys
import time
from collections import defaultdict
from pathlib import Path

_project_root = Path(__file__).resolve().parents[2]
sys.path.insert(0, str(_project_root))

from exchange_client.lib import trading_lib  # noqa: E402
from traders.lib.streams import (  # noqa: E402
    LISTEN_CHOICES,
    mempool_stream,
    outcome_token_id,
    settled_stream,
    usdc_notional,
)
from traders.lib.ui import TraderUI, format_hms  # noqa: E402


async def run(args: argparse.Namespace) -> int:
    trading_lib.load_env()
    ui = TraderUI("watch_only", __file__)
    account = os_account()
    mempool_flag = "  mempool" if args.trigger_polynode_mempool else ""
    ui.opening(f"watch_only  listen={args.listen}{mempool_flag}", account=account)

    listen = settled_stream(args.listen)
    trigger = mempool_stream() if args.trigger_polynode_mempool else None
    await listen.connect()
    if trigger is not None:
        await trigger.connect()

    started = time.monotonic()
    fills = 0
    last_block = 0
    block_fills: dict[int, list] = defaultdict(list)
    mempool_seen = 0
    stop = asyncio.Event()

    async def _listen_loop() -> None:
        nonlocal fills, last_block
        async for event in listen:
            if stop.is_set():
                return
            fills += 1
            last_block = event.block_number
            bucket = block_fills[event.block_number]
            bucket.append(event)
            # emit when we leave a block (next block arrives)
            prior = sorted(b for b in block_fills if b < event.block_number)
            for b in prior:
                _emit_block(ui, b, block_fills.pop(b))

    async def _mempool_loop() -> None:
        nonlocal mempool_seen
        assert trigger is not None
        async for _event in trigger:
            if stop.is_set():
                return
            mempool_seen += 1

    async def _footer_loop() -> None:
        while not stop.is_set():
            elapsed = format_hms(time.monotonic() - started)
            extra = f"  mempool={mempool_seen:,}" if trigger is not None else ""
            ui.update_footer(
                f"waiting  last_block={last_block:,}  fills={fills:,}{extra}  {elapsed}"
            )
            try:
                await asyncio.wait_for(stop.wait(), timeout=0.25)
            except asyncio.TimeoutError:
                continue

    loop = asyncio.get_running_loop()
    for sig in (signal.SIGINT, signal.SIGTERM):
        loop.add_signal_handler(sig, stop.set)

    ui.start_footer("waiting  last_block=none  fills=0  0:00:00")
    tasks = [
        asyncio.create_task(_listen_loop()),
        asyncio.create_task(_footer_loop()),
    ]
    if trigger is not None:
        tasks.append(asyncio.create_task(_mempool_loop()))

    try:
        await stop.wait()
    finally:
        stop.set()
        for t in tasks:
            t.cancel()
        await listen.disconnect()
        if trigger is not None:
            await trigger.disconnect()
        for b in sorted(block_fills):
            _emit_block(ui, b, block_fills[b])
        extra = f"  mempool={mempool_seen:,}" if args.trigger_polynode_mempool else ""
        ui.closing(
            f"status=ok  fills={fills:,}  last_block={last_block:,}{extra}",
            time.monotonic() - started,
        )
    return 0


def _emit_block(ui: TraderUI, block: int, events: list) -> None:
    accounts = {e.maker for e in events if e.maker} | {e.taker for e in events if e.taker}
    tokens = {outcome_token_id(e) for e in events if outcome_token_id(e)}
    usdc = sum(usdc_notional(e) for e in events)
    ui.print(
        f"block={block:,}  fills={len(events):,}  accounts={len(accounts):,}  "
        f"usdc={usdc:,.2f}  tokens={len(tokens):,}"
    )


def os_account() -> str:
    try:
        return trading_lib.get_funder_address()
    except SystemExit:
        return ""
    except Exception:
        return ""


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Watch live fills. No orders.")
    parser.add_argument("--listen", required=True, choices=LISTEN_CHOICES)
    parser.add_argument(
        "--trigger-polynode-mempool",
        action="store_true",
        help="also print Polynode mempool trigger events",
    )
    return parser.parse_args()


def main() -> None:
    args = parse_args()
    raise SystemExit(asyncio.run(run(args)))


if __name__ == "__main__":
    main()
