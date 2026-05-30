#!/usr/bin/env python3
"""Polynode inclusion test v2.

Same goal as polynode_inclusion_test.py, but implemented on top of shared
EventStream classes from exchange_client/lib instead of direct websocket code.

Compares:
- PolynodeMempoolEventStream (early/low-latency feed)
- RpcSettledEventStream (confirmed on-chain trades)
"""

from __future__ import annotations

import asyncio
import os
import statistics
import sys
import time
from collections import defaultdict
from pathlib import Path

from dotenv import load_dotenv

# Allow imports from exchange_client/lib as package `lib`.
_PROJECT_ROOT = Path(__file__).resolve().parent.parent
_EXCHANGE_CLIENT_DIR = _PROJECT_ROOT / "exchange_client"
sys.path.insert(0, str(_EXCHANGE_CLIENT_DIR))

from lib.event_stream import TradeEvent, ws_url_from_env  # noqa: E402
from lib.polynode_event_stream import PolynodeMempoolEventStream  # noqa: E402
from lib.rpc_event_stream import RpcSettledEventStream  # noqa: E402


def _load_env() -> None:
    load_dotenv(_PROJECT_ROOT / ".env", override=False)
    load_dotenv(_EXCHANGE_CLIENT_DIR / ".env", override=True)


class State:
    def __init__(self) -> None:
        self.start_mono = time.monotonic()

        self.polynode_event_count = 0
        self.rpc_event_count = 0

        self.polynode_seen: dict[str, float] = {}
        self.polynode_pending: dict[str, float] = {}
        self.rpc_seen: dict[str, float] = {}
        self.delay_ms_by_tx: dict[str, float] = {}

        self.rpc_by_block: dict[int, set[str]] = defaultdict(set)
        self.rpc_max_block = 0

        self.finalized: list[dict] = []

        self.lock = asyncio.Lock()
        self.footer_lines = 0

    def uptime(self) -> str:
        elapsed = int(time.monotonic() - self.start_mono)
        h, rem = divmod(elapsed, 3600)
        m, s = divmod(rem, 60)
        return f"{h:02d}:{m:02d}:{s:02d}"


state = State()


def _finalize_block(block_number: int) -> None:
    txs = state.rpc_by_block.get(block_number, set())
    not_seen = sum(1 for tx in txs if tx not in state.polynode_seen)
    delays = [state.delay_ms_by_tx[tx] for tx in txs if tx in state.delay_ms_by_tx]
    confirmed = set(state.rpc_seen)
    now = time.monotonic()
    in_flight = 0
    in_flight_gt_10s = 0
    for tx, t0 in state.polynode_pending.items():
        if tx not in confirmed:
            in_flight += 1
            if now - t0 > 10:
                in_flight_gt_10s += 1
    state.finalized.append({
        "block": block_number,
        "trades": len(txs),
        "not_seen": not_seen,
        "delays": delays,
        "in_flight": in_flight,
        "in_flight_gt_10s": in_flight_gt_10s,
    })


def _format_delay_stats(delays_ms: list[float]) -> str:
    if not delays_ms:
        return "delay_ms n/a"

    min_ms = min(delays_ms)
    max_ms = max(delays_ms)
    avg_ms = sum(delays_ms) / len(delays_ms)
    med_ms = statistics.median(delays_ms)
    return (
        f"delay_ms min {min_ms:.1f} max {max_ms:.1f} "
        f"avg {avg_ms:.1f} median {med_ms:.1f}"
    )


def _emit_line(text: str) -> None:
    if state.footer_lines > 0:
        sys.stdout.write(f"\033[{state.footer_lines}F")
        sys.stdout.write("\033[J")
    sys.stdout.write(text + "\n")
    _draw_footer()
    sys.stdout.flush()


def _draw_footer() -> None:
    matched_delays = list(state.delay_ms_by_tx.values())
    matched_tx = len(state.delay_ms_by_tx)
    sys.stdout.write(f"tx {matched_tx} | {_format_delay_stats(matched_delays)}\n")
    state.footer_lines = 1


async def handle_polynode(event: TradeEvent) -> None:
    tx = event.tx_hash
    if not tx:
        return
    async with state.lock:
        state.polynode_event_count += 1
        now = time.monotonic()
        if tx not in state.polynode_seen:
            state.polynode_seen[tx] = now
        if tx not in state.polynode_pending:
            state.polynode_pending[tx] = now


async def handle_rpc(event: TradeEvent) -> None:
    tx = event.tx_hash
    block = event.block_number
    if not tx or block is None:
        return

    async with state.lock:
        state.rpc_event_count += 1
        now = time.monotonic()
        if tx not in state.rpc_seen:
            state.rpc_seen[tx] = now
            if tx in state.polynode_seen and tx not in state.delay_ms_by_tx:
                state.delay_ms_by_tx[tx] = (now - state.polynode_seen[tx]) * 1000.0
        state.rpc_by_block[block].add(tx)
        state.polynode_pending.pop(tx, None)

        if block > state.rpc_max_block:
            if state.rpc_max_block > 0:
                existing = {x["block"] for x in state.finalized}
                for b in sorted(state.rpc_by_block.keys()):
                    if b < block and b not in existing:
                        _finalize_block(b)
            state.rpc_max_block = block


async def rpc_consumer(stream: RpcSettledEventStream) -> None:
    await stream.connect()
    _emit_line("RPC stream: connected")
    try:
        async for event in stream:
            await handle_rpc(event)
    finally:
        await stream.disconnect()


async def polynode_consumer(stream: PolynodeMempoolEventStream) -> None:
    await stream.connect()
    _emit_line("Polynode stream: connected")
    try:
        async for event in stream:
            await handle_polynode(event)
    finally:
        await stream.disconnect()


async def printer() -> None:
    emitted = 0
    while True:
        await asyncio.sleep(1)
        async with state.lock:
            while emitted < len(state.finalized):
                s = state.finalized[emitted]
                _emit_line(
                    f"block {s['block']}: tx {s['trades']} not_seen_in_polynode {s['not_seen']} "
                    f"| polynode in_flight {s['in_flight']} in_flight_gt_10s {s['in_flight_gt_10s']}"
                )
                emitted += 1

            if state.footer_lines > 0:
                sys.stdout.write(f"\033[{state.footer_lines}F")
                sys.stdout.write("\033[J")
            _draw_footer()
            sys.stdout.flush()


async def main() -> None:
    _load_env()

    polynode_key = os.environ.get("POLYNODE_API_KEY", "")
    if not polynode_key:
        sys.exit("POLYNODE_API_KEY not set")

    rpc_ws = ws_url_from_env(
        os.environ.get("POLYGON_WS_URL", ""),
        os.environ.get("POLYGON_RPC_URL", ""),
    )
    polynode_ws = os.environ.get("POLYNODE_WS_URL", "wss://ws.polynode.dev/ws")

    rpc_stream = RpcSettledEventStream(rpc_ws)
    polynode_stream = PolynodeMempoolEventStream(polynode_key, ws_url=polynode_ws)

    tasks = [
        asyncio.create_task(rpc_consumer(rpc_stream)),
        asyncio.create_task(polynode_consumer(polynode_stream)),
        asyncio.create_task(printer()),
    ]

    try:
        await asyncio.gather(*tasks)
    except asyncio.CancelledError:
        pass


if __name__ == "__main__":
    try:
        asyncio.run(main())
    except KeyboardInterrupt:
        print("\nexiting")
