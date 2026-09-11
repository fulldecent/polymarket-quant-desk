#!/usr/bin/env python3
"""Warm up, then copy N buy fills from the next new block. Measure block lag."""

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
    exchange_is_neg_risk,
    execution_client,
    extract_buy_trigger,
    is_live_trigger,
    listen_host,
    mempool_stream,
    settled_stream,
)
from traders.lib.ui import TraderUI, format_hms, shorten, shorten_addr  # noqa: E402


def _is_our_copy_fill(event: TradeEvent, token_id: str, ours: set[str]) -> bool:
    parties = {event.maker.lower(), event.taker.lower()} - {""}
    if not parties & ours:
        return False
    return event.maker_asset_id == token_id or event.taker_asset_id == token_id


def _drop(queue: asyncio.Queue) -> int:
    n = 0
    while True:
        try:
            queue.get_nowait()
            n += 1
        except asyncio.QueueEmpty:
            return n


async def run(args: argparse.Namespace) -> int:
    trading_lib.load_env()
    ui = TraderUI("follow_anything", __file__)
    funder = trading_lib.get_funder_address()
    eoa = trading_lib.get_eoa_address()
    skip = {funder.lower(), eoa.lower()} - {""}
    mempool_flag = "on" if args.trigger_polynode_mempool else "off"
    ui.opening(account=funder)
    ui.log_only(
        f"exec={args.exec}  listen={args.listen}  mempool={mempool_flag}  "
        f"amount={args.amount:.2f}  count={args.count}  warmup={args.warmup_seconds:g}s"
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

    warmup_until = time.monotonic() + args.warmup_seconds
    after_block = 0
    high_water = 0
    warmup_done = False
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
    ui.start_footer(f"warmup  {format_hms(args.warmup_seconds)}")

    async def _drain() -> None:
        nonlocal pending, settled_n, last_lag, high_water
        while True:
            try:
                event = listen_q.get_nowait()
            except asyncio.QueueEmpty:
                return
            if event.block_number > high_water:
                high_water = event.block_number
            tx = event.tx_hash.lower()
            if tx in trigger_blocks and trigger_blocks[tx] == 0:
                trigger_blocks[tx] = event.block_number
            if pending is None:
                continue
            expected = pending["expected"]
            if expected:
                if tx not in expected:
                    continue
            elif not _is_our_copy_fill(event, pending["token_id"], skip):
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
                trig_s = f"{trig_block:,}" if trig_block else "none"
                ui.print(
                    f"settled  seq={settled_n}  copy_tx={shorten(event.tx_hash)}  "
                    f"copy_block={event.block_number:,}  "
                    f"trigger_block={trig_s}  lag={last_lag}"
                )
                pending = None

    try:
        while settled_n < args.count:
            await _drain()
            elapsed = format_hms(time.monotonic() - started)
            remaining = warmup_until - time.monotonic()

            if remaining > 0:
                _drop(trigger_q)
                ui.update_footer(
                    f"warmup  {format_hms(remaining)}  block={high_water:,}  {elapsed}"
                )
                await asyncio.sleep(0.1)
                continue

            if not warmup_done:
                after_block = high_water
                dropped = _drop(trigger_q)
                ui.print(f"warmup done  after_block={after_block:,}  dropped={dropped}")
                warmup_done = True

            if settled_n >= args.count:
                break

            if pending is not None:
                _drop(trigger_q)
                if time.monotonic() - pending["submitted_at"] > SETTLEMENT_TIMEOUT_SECONDS:
                    ui.print(
                        f"timeout  seq={settled_n + 1}  "
                        f"tx={shorten(next(iter(pending['expected'])))}  "
                        f"timeout={SETTLEMENT_TIMEOUT_SECONDS}s"
                    )
                    pending = None
                else:
                    ui.update_footer(
                        f"copied {settled_n}/{args.count}  last_lag={last_lag}  "
                        f"waiting  {elapsed}"
                    )
                    await asyncio.sleep(0.1)
                    continue

            ui.update_footer(
                f"copied {settled_n}/{args.count}  last_lag={last_lag}  "
                f"wait_block>{after_block:,}  {elapsed}"
            )
            try:
                event = await asyncio.wait_for(trigger_q.get(), timeout=0.5)
            except asyncio.TimeoutError:
                continue

            if not is_live_trigger(event, after_block=after_block):
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

            block = getattr(event, "block_number", 0) or 0
            block_s = "mempool" if not block else f"{block:,}"
            ui.print(
                f"trigger  token={shorten(trigger['buy_token_id'], head=4, tail=4)}  "
                f"buyer={shorten_addr(trigger['buyer'])}  "
                f"value=${trigger['trade_value_usd']:.2f}  "
                f"block={block_s}  tx={shorten(event.tx_hash)}"
            )

            try:
                result = await client.buy_market(
                    trigger["buy_token_id"],
                    args.amount,
                    neg_risk=exchange_is_neg_risk(event.contract_address),
                )
            except Exception as exc:
                ui.print(f"copy_failed  error={exc}")
                _drop(trigger_q)
                continue

            if not result.success:
                ui.print("copy_failed  error=order_rejected")
                _drop(trigger_q)
                continue

            expected = {x.lower() for x in result.tx_hashes if x}
            pending = {
                "trigger_tx": tx,
                "token_id": trigger["buy_token_id"],
                "expected": expected,
                "landed": set(),
                "submitted_at": time.monotonic(),
                "copy_block": None,
                "lag": None,
            }
            tx_s = shorten(result.tx_hashes[0]) if result.tx_hashes else "none"
            delayed = "" if result.tx_hashes else "  delayed"
            ui.print(
                f"submitted  order={shorten(result.order_id or '')}  "
                f"tx={tx_s}{delayed}"
            )
            _drop(trigger_q)

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
        description="Warm up, then copy N buy fills from the next new block. "
        "Measure copy_settled_block - trigger_settled_block."
    )
    parser.add_argument("--exec", required=True, choices=EXEC_CHOICES)
    parser.add_argument("--listen", required=True, choices=LISTEN_CHOICES)
    parser.add_argument("--trigger-polynode-mempool", action="store_true")
    parser.add_argument(
        "--warmup",
        default="10s",
        metavar="DUR",
        type=trading_lib.parse_ttl,
        help="observe only, then copy from the next new block (once)",
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
