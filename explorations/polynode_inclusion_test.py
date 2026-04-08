#!/usr/bin/env python3
"""
Polynode inclusion test — compare Polynode settlement feed (early detection)
against RPC-confirmed on-chain trades.

Subscribes to:
  1. Polynode WebSocket (settlements only) — early feed
  2. Polygon RPC WebSocket (eth_subscribe for OrderFilled/OrdersMatched) —
     confirmed on-chain trades with block numbers

Prints per-block stats showing how many confirmed trades were pre-seen
in the settlements feed, with a sticky aggregate footer.
"""

import argparse
import asyncio
import json
import os
import sys
import time
import zlib
from collections import defaultdict
from pathlib import Path

from dotenv import load_dotenv

parser = argparse.ArgumentParser(
    description="""\
Subscribe to two independent feeds and compare them in real time.

Feed 1 — Polynode settlements: trades that have been signed by the CLOB
but have not yet landed on chain. This is the early-detection feed.

Feed 2 — Polygon RPC (eth_subscribe): confirmed on-chain OrderFilled and
OrdersMatched events with block numbers. This is the ground-truth feed.

The first few settled blocks are expected to contain transactions that were
not pre-announced by Polynode (the settlement feed hadn't started yet).
After that warm-up period, every block should contain exclusively
transactions that were pre-announced. Any confirmed trade missing from the
settlements feed is flagged red as "not seen in settlements feed".

Conversely, every settlement from Polynode is expected to land in an
upcoming block. If a settlement has not appeared in a confirmed block
within 10 seconds it is flagged red as "in-flight (> 10s)".

Additional invariants tested:
  - The RPC feed must deliver blocks in strictly increasing order.
    Out-of-order blocks are flagged as a violation.
""",
    formatter_class=argparse.RawDescriptionHelpFormatter,
)
parser.parse_args()

_project_root = Path(__file__).resolve().parent.parent
load_dotenv(_project_root / ".env")

POLYNODE_API_KEY = os.environ.get("POLYNODE_API_KEY", "")
if not POLYNODE_API_KEY:
    sys.exit("POLYNODE_API_KEY not set. Add it to .env.")

# RPC WebSocket URL (same pattern as green_lynx)
_ws_url = os.environ.get("POLYGON_WS_URL", "")
if not _ws_url:
    _http_url = os.environ.get("POLYGON_RPC_URL", "")
    if _http_url:
        if "infura.io" in _http_url:
            _ws_url = _http_url.replace("https://", "wss://").replace(
                "http://", "ws://").replace("/v3/", "/ws/v3/")
        else:
            _ws_url = _http_url.replace("https://", "wss://").replace(
                "http://", "ws://")
if not _ws_url:
    sys.exit("POLYGON_WS_URL or POLYGON_RPC_URL not set. Add it to .env.")

RPC_WS_URL = _ws_url

try:
    import websockets
except ImportError:
    sys.exit("websockets package required: pip install websockets")

# ── Contract addresses and topics ─────────────────────────────────────────

EXCHANGE_ADDRESSES = [
    "0x4bFb41d5B3570DeFd03C39a9A4D8dE6Bd8B8982E",  # CTFExchange
    "0xC5d563A36AE78145C45a50134d48A1215220f80a",  # NegRiskCTFExchange
]

WANTED_TOPICS = [
    "0xd0a08e8c493f9c94f29311604c9de1b4e8c8d4c06bd0c789af57f2d65bfec0f6",  # OrderFilled
    "0x63bf4d16b7fa898ef4c4b2b6d90fd201e9c56313b65638af6088d149d2ce956c",  # OrdersMatched
]

# ── ANSI helpers ──────────────────────────────────────────────────────────

RED = "\033[31m"
RESET = "\033[0m"
CLEAR_TO_END = "\033[J"


def _red_if(val, text):
    """Wrap text in red ANSI if val > 0."""
    if val > 0:
        return f"{RED}{text}{RESET}"
    return text


# ── Shared state ──────────────────────────────────────────────────────────

class State:
    def __init__(self):
        self.start_time = time.monotonic()

        # Confirmed trades from the trades feed: block_number -> set of tx_hash
        self.trades_by_block: dict[int, set[str]] = defaultdict(set)
        # Settlements seen (pending or confirmed): tx_hash -> mono arrival time
        self.settlement_seen: dict[str, float] = {}
        # Pending settlements not yet confirmed: tx_hash -> mono arrival time
        self.settlement_pending: dict[str, float] = {}

        # The highest block seen in the trades feed
        self.trades_max_block = 0
        # Finalized block stats
        self.finalized_stats: list[dict] = []

        # Totals
        self.total_settlements = 0
        self.total_trades = 0

        # Lock for async safety
        self.lock = asyncio.Lock()

        # Number of sticky footer lines (for cursor control)
        self.footer_lines = 0

    def uptime_str(self) -> str:
        elapsed = int(time.monotonic() - self.start_time)
        h, rem = divmod(elapsed, 3600)
        m, s = divmod(rem, 60)
        return f"{h:02d}:{m:02d}:{s:02d}"


state = State()


def _finalize_block(block_number: int):
    """Mark a block as finalized and compute stats (called under lock)."""
    tx_hashes = state.trades_by_block.get(block_number, set())
    trade_count = len(tx_hashes)
    not_seen = sum(
        1 for tx in tx_hashes if tx not in state.settlement_seen)
    state.finalized_stats.append({
        "block": block_number,
        "trades": trade_count,
        "not_seen": not_seen,
    })


def _emit_line(line: str):
    """Print a scrolling line above the sticky footer."""
    if state.footer_lines > 0:
        sys.stdout.write(f"\033[{state.footer_lines}F")
        sys.stdout.write(CLEAR_TO_END)
    sys.stdout.write(line + "\n")
    _draw_footer()
    sys.stdout.flush()


def _draw_footer():
    """Draw the sticky footer."""
    now = time.monotonic()

    # In-flight = pending settlements whose tx_hash hasn't appeared in
    # the trades feed yet
    in_flight_count = 0
    in_flight_old = 0
    confirmed_txs = set()
    for txs in state.trades_by_block.values():
        confirmed_txs |= txs
    for tx_hash, mono_t in state.settlement_pending.items():
        if tx_hash not in confirmed_txs:
            in_flight_count += 1
            if now - mono_t > 10.0:
                in_flight_old += 1

    # Current (in-progress) block stats
    current_block = state.trades_max_block
    current_txs = state.trades_by_block.get(current_block, set())
    current_trades = len(current_txs)
    current_not_seen = sum(
        1 for tx in current_txs if tx not in state.settlement_seen)

    lines = []
    lines.append("─" * 60)
    lines.append(f"uptime {state.uptime_str()}")
    in_flight_old_str = _red_if(
        in_flight_old,
        f"in-flight (> 10s) {in_flight_old}")
    lines.append(
        f"settlements {state.total_settlements}:  "
        f"in-flight {in_flight_count}  "
        f"{in_flight_old_str}")
    current_not_seen_str = _red_if(
        current_not_seen,
        f"not seen in settlements feed {current_not_seen}")
    lines.append(
        f"block {current_block} (in progress):  "
        f"trades {current_trades}  "
        f"{current_not_seen_str}")
    lines.append(
        f"RPC trades total: {state.total_trades}")
    lines.append("press CTRL-C to exit")

    for line in lines:
        sys.stdout.write(line + "\n")

    state.footer_lines = len(lines)


# ── Event handlers ────────────────────────────────────────────────────────

async def handle_rpc_log(block_number: int, tx_hash: str):
    """Handle a confirmed on-chain trade from the RPC feed."""
    async with state.lock:
        state.total_trades += 1
        state.trades_by_block[block_number].add(tx_hash)

        # Remove from pending if it was there
        state.settlement_pending.pop(tx_hash, None)

        # Check for block regression
        if block_number < state.trades_max_block:
            line = (f"{RED}VIOLATION: RPC emitted block "
                    f"{block_number} after block "
                    f"{state.trades_max_block} — out-of-order!{RESET}")
            _emit_line(line)
        elif block_number > state.trades_max_block:
            # Finalize all blocks < block_number
            if state.trades_max_block > 0:
                for b in sorted(state.trades_by_block.keys()):
                    if b < block_number and b not in [
                            s["block"] for s in state.finalized_stats]:
                        _finalize_block(b)
            state.trades_max_block = block_number


async def handle_settlement(data: dict):
    """Handle a settlement event (pending or confirmed)."""
    tx_hash = data.get("tx_hash", "")
    status = data.get("status", "")
    if not tx_hash:
        return

    async with state.lock:
        state.total_settlements += 1
        now = time.monotonic()
        if tx_hash not in state.settlement_seen:
            state.settlement_seen[tx_hash] = now
        if status == "pending" and tx_hash not in state.settlement_pending:
            state.settlement_pending[tx_hash] = now


# ── Display tasks ─────────────────────────────────────────────────────────

async def redraw_display():
    """Periodically redraw the sticky footer."""
    while True:
        await asyncio.sleep(1.0)
        async with state.lock:
            if state.footer_lines > 0:
                sys.stdout.write(f"\033[{state.footer_lines}F")
                sys.stdout.write(CLEAR_TO_END)
            _draw_footer()
            sys.stdout.flush()


async def emit_finalized_blocks():
    """Watch for newly finalized blocks and emit their lines."""
    emitted = 0
    while True:
        await asyncio.sleep(0.5)
        async with state.lock:
            while emitted < len(state.finalized_stats):
                s = state.finalized_stats[emitted]
                not_seen_str = _red_if(
                    s["not_seen"],
                    f"not seen in settlements feed {s['not_seen']}")
                line = (f"block {s['block']}: trades {s['trades']}  "
                        f"{not_seen_str}")
                _emit_line(line)
                emitted += 1


# ── Polynode WebSocket (settlements only) ─────────────────────────────────

async def polynode_stream():
    """Connect to Polynode WebSocket with zlib compression, subscribe to
    settlements only (early detection feed)."""
    url = (f"wss://ws.polynode.dev/ws"
           f"?key={POLYNODE_API_KEY}&compress=zlib")
    reconnect_delay = 1.0

    while True:
        try:
            async with websockets.connect(
                url,
                ping_interval=None,
                ping_timeout=None,
            ) as ws:
                reconnect_delay = 1.0
                _emit_line_safe("Polynode: connected")

                await ws.send(json.dumps({
                    "action": "subscribe",
                    "type": "settlements",
                }))

                async def keepalive():
                    while True:
                        await asyncio.sleep(30)
                        await ws.send(json.dumps({"action": "ping"}))

                ping_task = asyncio.create_task(keepalive())
                try:
                    async for raw_msg in ws:
                        try:
                            if isinstance(raw_msg, bytes):
                                text = zlib.decompress(
                                    raw_msg, -zlib.MAX_WBITS).decode()
                            else:
                                text = raw_msg
                            msg = json.loads(text)
                        except (zlib.error, json.JSONDecodeError):
                            continue

                        msg_type = msg.get("type", "")

                        if msg_type in ("pong", "heartbeat"):
                            continue

                        if msg_type == "subscribed":
                            sub_id = msg.get("subscription_id", "")
                            _emit_line_safe(
                                f"Polynode: subscribed ({sub_id})")
                            continue

                        if msg_type == "snapshot":
                            for evt in msg.get("events", []):
                                await handle_settlement(evt)
                            continue

                        if msg_type == "settlement":
                            await handle_settlement(msg.get("data", {}))
                        elif msg_type == "status_update":
                            data = msg.get("data", {})
                            data.setdefault("status", "confirmed")
                            await handle_settlement(data)
                        elif msg_type == "error":
                            _emit_line_safe(
                                f"{RED}Polynode error: "
                                f"{msg.get('message', msg)}{RESET}")
                finally:
                    ping_task.cancel()

        except (websockets.ConnectionClosed, OSError,
                asyncio.CancelledError) as e:
            if isinstance(e, asyncio.CancelledError):
                return
            _emit_line_safe(
                f"Polynode: disconnected ({e}), "
                f"reconnecting in {reconnect_delay:.0f}s…")
            await asyncio.sleep(reconnect_delay)
            reconnect_delay = min(reconnect_delay * 2, 30.0)


# ── RPC WebSocket (confirmed on-chain trades) ─────────────────────────────

async def rpc_stream():
    """Connect to Polygon RPC WebSocket, subscribe to OrderFilled and
    OrdersMatched events on both exchange contracts."""
    reconnect_delay = 1.0

    while True:
        try:
            async with websockets.connect(
                RPC_WS_URL,
                ping_interval=30,
                ping_timeout=10,
                close_timeout=2,
            ) as ws:
                reconnect_delay = 1.0
                _emit_line_safe("RPC: connected")

                subscribe_msg = {
                    "jsonrpc": "2.0",
                    "id": 1,
                    "method": "eth_subscribe",
                    "params": [
                        "logs",
                        {
                            "address": EXCHANGE_ADDRESSES,
                            "topics": [WANTED_TOPICS],
                        },
                    ],
                }
                await ws.send(json.dumps(subscribe_msg))
                response = await ws.recv()
                data = json.loads(response)

                if "error" in data:
                    _emit_line_safe(
                        f"{RED}RPC: subscription error: "
                        f"{data['error']}{RESET}")
                    sys.exit(1)

                _emit_line_safe("RPC: subscribed to exchange events")

                async for message in ws:
                    try:
                        data = json.loads(message)
                    except json.JSONDecodeError:
                        continue

                    if data.get("method") != "eth_subscription":
                        continue

                    log = data.get("params", {}).get("result", {})
                    block_hex = log.get("blockNumber", "")
                    tx_hash = log.get("transactionHash", "")

                    if not block_hex or not tx_hash:
                        continue

                    block_number = int(block_hex, 16)
                    await handle_rpc_log(block_number, tx_hash)

        except (websockets.ConnectionClosed, OSError,
                asyncio.CancelledError) as e:
            if isinstance(e, asyncio.CancelledError):
                return
            _emit_line_safe(
                f"RPC: disconnected ({e}), "
                f"reconnecting in {reconnect_delay:.0f}s…")
            await asyncio.sleep(reconnect_delay)
            reconnect_delay = min(reconnect_delay * 2, 30.0)


def _emit_line_safe(text: str):
    """Emit a scrolling line above the sticky footer (outside lock)."""
    if state.footer_lines > 0:
        sys.stdout.write(f"\033[{state.footer_lines}F")
        sys.stdout.write(CLEAR_TO_END)
    sys.stdout.write(text + "\n")
    _draw_footer()
    sys.stdout.flush()


# ── Main ──────────────────────────────────────────────────────────────────

async def main():
    _draw_footer()
    sys.stdout.flush()

    tasks = [
        asyncio.create_task(polynode_stream()),
        asyncio.create_task(rpc_stream()),
        asyncio.create_task(redraw_display()),
        asyncio.create_task(emit_finalized_blocks()),
    ]

    try:
        await asyncio.gather(*tasks)
    except asyncio.CancelledError:
        pass


if __name__ == "__main__":
    try:
        asyncio.run(main())
    except KeyboardInterrupt:
        print(f"\n\nexiting after {state.uptime_str()}")
