#!/usr/bin/env python3
"""Live directional FOK/GTC from the frozen 6-of-8 + Kris-10+ formula.

Warm up 100 blocks (progress bar), then hot. Max 3 outstanding bets.
FOK buy then GTC sell in the same turn — no wait on exposure.
"""

from __future__ import annotations

import argparse
import asyncio
import json
import math
import sys
import time
from pathlib import Path

_HERE = Path(__file__).resolve().parent
_ROOT = _HERE.parents[1]
sys.path.insert(0, str(_ROOT))
sys.path.insert(0, str(_ROOT / "explorations" / "directional-fok-v1"))

import joblib  # noqa: E402

from exchange_client.lib import trading_lib  # noqa: E402
from exchange_client.lib.event_stream import TradeEvent  # noqa: E402
from stage_b import FEAT_KEYS  # noqa: E402
from stage_c import attach_stage_b, pick_ticket  # noqa: E402
from book import LiveBook  # noqa: E402
from traders.lib.streams import (  # noqa: E402
    EXEC_CHOICES,
    LISTEN_CHOICES,
    event_is_neg_risk,
    execution_client,
    listen_host,
    parse_units,
    settled_stream,
    usdc_notional,
)
from traders.lib.ui import COLOR_DONE, TraderUI, format_hms, shorten  # noqa: E402

FORMULA_PATH = _HERE / "formula.json"


def load_formula() -> dict:
    return json.loads(FORMULA_PATH.read_text())


def _lot2(n: float) -> float:
    """CLOB sell maker size: max 2 decimal places."""
    return math.floor(max(0.0, float(n)) * 100.0 + 1e-9) / 100.0


def _px4(p: float) -> float:
    """CLOB sell taker amount: max 4 decimal places on price."""
    return math.floor(max(0.0, float(p)) * 10000.0 + 1e-9) / 10000.0


def _shares_from_resp(taking: str | None, making: str | None, p_in: float, spent: float) -> float:
    for raw in (taking, making):
        if not raw:
            continue
        try:
            v = float(raw)
        except (TypeError, ValueError):
            continue
        if v > 1000:
            v = v / 1e6
        if v > 0:
            return v
    if p_in > 0:
        return spent / p_in
    return 0.0


def _yes_px(event: TradeEvent) -> tuple[str, float, float, float, bool, bool, str, str] | None:
    """cid, yes_px, usdc, shares, is_taker, is_buy_yes, yes_token, no_token."""
    usdc = usdc_notional(event)
    if event.maker_asset_id == "0" and event.taker_asset_id not in {"", "0"}:
        token = event.taker_asset_id
        shares = parse_units(event.taker_amount)
        is_taker = False
        maker_buys_outcome = True
    elif event.taker_asset_id == "0" and event.maker_asset_id not in {"", "0"}:
        token = event.maker_asset_id
        shares = parse_units(event.maker_amount)
        is_taker = True
        maker_buys_outcome = False
    else:
        return None
    if usdc <= 0 or shares <= 0:
        return None
    px = usdc / shares
    if not (0 < px < 1):
        return None
    cid = (event.condition_id or "").replace("0x", "").lower()
    if not cid:
        cid = f"tok:{token}"
    outcome = (event.outcome or "").strip().lower()
    tokens = [t for t in (event.token_ids_csv or "").split(",") if t]
    yes_token = no_token = ""
    is_yes = True
    if outcome in {"no", "down", "0"}:
        is_yes = False
        no_token = token
        if len(tokens) == 2:
            yes_token = tokens[0] if tokens[0] != token else tokens[1]
    elif outcome in {"yes", "up", "1"}:
        yes_token = token
        if len(tokens) == 2:
            no_token = tokens[1] if tokens[0] == token else tokens[0]
    else:
        yes_token = token
        if len(tokens) == 2:
            yes_token, no_token = tokens[0], tokens[1]
            is_yes = token == yes_token
    yes_px = px if is_yes else 1.0 - px
    is_buy_yes = (maker_buys_outcome and is_yes) or (not maker_buys_outcome and not is_yes)
    return cid, yes_px, usdc, shares, is_taker, is_buy_yes, yes_token, no_token


async def run(args: argparse.Namespace) -> int:
    trading_lib.load_env()
    formula = load_formula()
    ui = TraderUI("directional_fok", __file__)
    funder = trading_lib.get_funder_address()
    eoa = trading_lib.get_eoa_address()
    skip = {funder.lower(), eoa.lower()} - {""}
    ui.opening(account=funder)

    a = formula["stage_a"]
    c = formula["stage_c"]
    port = formula["portfolio"]
    job_path = _HERE / formula["stage_b"]["joblib"]
    if not job_path.exists():
        ui.closing(f"status=failed  error=missing {job_path}", 0.0)
        return 1
    blob = joblib.load(job_path)
    est = blob["hl"]
    b_keys = list(blob.get("feat_keys") or FEAT_KEYS)
    ui.log_only(
        f"exec={args.exec}  listen={args.listen}  "
        f"gate={a['need']}-of-{a['window']} kris={a['kris_min']}+  "
        f"in={c['in_frac']} out={c['out_frac']} dbl={c['double_k']}  "
        f"max_open={port['max_open']}  warmup={port['warmup_blocks']}"
    )
    ui.print(
        f"formula    {a['need']}-of-{a['window']}  kris {a['kris_min']}+  "
        f"in={c['in_frac']}  out={c['out_frac']}  dbl_k={c['double_k']}"
    )
    ui.print(f"portfolio  max_open={port['max_open']}  warmup={port['warmup_blocks']} blocks")
    try:
        pusd = trading_lib.get_pusd_balance_via_rpc(funder)
        ui.print(f"pUSD       {trading_lib.fmt_usd(pusd)}")
    except Exception as exc:
        ui.print(f"pUSD       error={exc}")
    if args.dry_run:
        ui.print("mode       dry-run  (no orders)")
    else:
        ui.print("mode       live")

    client = execution_client(args.exec)
    started = time.monotonic()
    def _on_status(msg: str) -> None:
        if msg.startswith("listen error"):
            ui.print(msg)
        else:
            ui.log_only(msg)

    listen = settled_stream(args.listen, on_status=_on_status)
    ui.print(f"connecting listen={args.listen}  host={listen_host(args.listen)}")
    try:
        await listen.connect()
    except Exception as exc:
        ui.closing(
            f"status=failed  error={type(exc).__name__}: {exc}",
            time.monotonic() - started,
        )
        return 1

    book = LiveBook()
    warmup_n = int(port["warmup_blocks"])
    need = int(a["need"])
    kris_min = int(a["kris_min"])
    max_open = int(port["max_open"])
    cooldown_blocks = int(port["cooldown_blocks"])
    min_notional = float(c.get("min_notional_usd", 2.0))
    size_mult = float(c.get("size_mult", 1.1))
    gtc_blocks = int(c.get("exit_end", 30))
    gtc_ttl = 240.0

    seen_blocks: set[int] = set()
    high_water = 0
    finalized = 0
    hot = False
    open_pos: list[dict] = []
    cooldown: dict[str, int] = {}
    n_fok = n_gtc = n_dump = n_skip_full = 0
    n_fills = n_drop = 0
    parse_fail_logged = 0
    stop = asyncio.Event()
    pending_fill: dict[str, asyncio.Event] = {}
    pending_size: dict[str, float] = {}
    deadline = (
        started + float(args.run_seconds) if args.run_seconds and args.run_seconds > 0 else 0.0
    )

    listen_q: asyncio.Queue[TradeEvent] = asyncio.Queue()

    async def _pump() -> None:
        async for event in listen:
            if stop.is_set():
                return
            for tok in (event.maker_asset_id, event.taker_asset_id):
                if tok and tok in pending_fill:
                    parsed = _yes_px(event)
                    if parsed is not None:
                        pending_size[tok] = parsed[3]
                    pending_fill[tok].set()
                    break
            await listen_q.put(event)

    pump = asyncio.create_task(_pump())
    ui.start_bar(f"warmup  blocks 0/{warmup_n}", warmup_n)

    def _token_for(cid: str, side: str) -> str:
        if side == "yes":
            return book.yes_id.get(cid, "")
        return book.no_id.get(cid, "")

    async def _enter(cid: str, x: int) -> None:
        nonlocal n_fok, n_gtc, n_dump, n_skip_full
        if len(open_pos) >= max_open:
            n_skip_full += 1
            return
        if any(p["cid"] == cid for p in open_pos):
            return
        if x < cooldown.get(cid, -1):
            return
        if not book.gate(cid, x, need=need, kris_min=kris_min):
            return
        feat, last, tick = book.features(cid, x)
        if feat is None or last is None:
            return
        ev = {
            "last": last,
            "tick": tick,
            "feat": feat,
            "x_block": x,
            "cid": cid,
        }
        attach_stage_b([ev], est, b_keys)
        ticket = pick_ticket(
            ev,
            in_frac=c["in_frac"],
            out_frac=c["out_frac"],
            double_k=c["double_k"],
            size_mult=size_mult,
            exit_end=gtc_blocks,
        )
        if ticket is None:
            return
        token = _token_for(cid, ticket["side"])
        if not token:
            return
        p_in = _px4(float(ticket["p_in"]))
        p_out = _px4(float(ticket["p_out"]))
        if p_out <= p_in:
            p_out = _px4(p_in + max(tick, 0.0001))
        n = _lot2(float(ticket["n"]))
        if n < 5:
            n = 5.0
        spent = max(min_notional, n * p_in)
        neg = bool(book.neg_risk.get(cid, False))
        cooldown[cid] = x + cooldown_blocks
        t_fok = time.monotonic()
        if args.dry_run:
            n_fok += 1
            n_gtc += 1
            open_pos.append(
                {
                    "cid": cid,
                    "token": token,
                    "shares": n,
                    "p_in": p_in,
                    "p_out": p_out,
                    "tick": tick,
                    "neg_risk": neg,
                    "entry_block": x,
                    "exit_end": x + max(gtc_blocks, int(gtc_ttl / 1.5) + 10),
                    "order_id": "dry",
                }
            )
            ui.print(
                f"  -> dry_bet  cid={shorten(cid)}  {ticket['side']}  "
                f"n={n:.2f}  p_in={p_in:.4f}  p_out={p_out:.4f}  "
                f"open={len(open_pos)}/{max_open}"
            )
            return
        try:
            bought = await client.buy_market(token, spent, max_price=p_in, neg_risk=neg)
        except Exception as exc:
            msg = str(exc)
            if "fully filled" in msg.lower() or "killed" in msg.lower():
                ui.print(f"  -> fok_miss  cid={shorten(cid)}  p_in={p_in:.4f}")
            else:
                ui.print(f"  -> fok_fail  cid={shorten(cid)}  error={exc}")
            return
        if not bought.success:
            ui.print(f"  -> fok_miss  cid={shorten(cid)}  p_in={p_in:.4f}")
            return
        n_fok += 1
        shares = _lot2(
            _shares_from_resp(bought.taking_amount, bought.making_amount, p_in, spent)
        )
        if shares < 5:
            shares = n
        landed = asyncio.Event()
        pending_fill[token] = landed
        try:
            await asyncio.wait_for(landed.wait(), timeout=8.0)
            shares = _lot2(pending_size.get(token, shares))
        except asyncio.TimeoutError:
            ui.print(f"  -> fok_unseen  cid={shorten(cid)}  token={shorten(token)}")
            pending_fill.pop(token, None)
            return
        pending_fill.pop(token, None)
        if shares < 5:
            ui.print(f"  -> fok_dust  cid={shorten(cid)}  shares={shares:.2f}")
            return
        try:
            sold = await client.sell_limit(
                token,
                shares,
                p_out,
                ttl_seconds=gtc_ttl,
                neg_risk=neg,
                tick_size=tick,
            )
        except Exception as exc:
            ui.print(
                f"  -> gtc_fail  cid={shorten(cid)}  shares={shares:.2f}  "
                f"error={exc}  dumping"
            )
            try:
                await client.sell_fak(
                    token,
                    shares,
                    min_price=_px4(max(tick, 0.01)),
                    neg_risk=neg,
                    tick_size=tick if tick >= 0.01 else 0.01,
                )
                n_dump += 1
            except Exception as dump_exc:
                ui.print(f"  -> dump_fail  error={dump_exc}")
            return
        n_gtc += 1
        open_pos.append(
            {
                "cid": cid,
                "token": token,
                "shares": shares,
                "p_in": p_in,
                "p_out": p_out,
                "tick": tick,
                "neg_risk": neg,
                "entry_block": x,
                "exit_end": x + gtc_blocks,
                "order_id": sold.order_id or bought.order_id or "",
            }
        )
        ms = int((time.monotonic() - t_fok) * 1000)
        ui.print(
            f"  -> bet  cid={shorten(cid)}  {ticket['side']}  "
            f"n={shares:.2f}  p_in={p_in:.4f}  p_out={p_out:.4f}  "
            f"open={len(open_pos)}/{max_open}  {ms}ms"
        )

    async def _age_out(now_block: int) -> None:
        nonlocal n_dump
        keep = []
        for pos in open_pos:
            if now_block < pos["exit_end"]:
                keep.append(pos)
                continue
            if args.dry_run:
                n_dump += 1
                ui.print(
                    f"  -> dry_dump  cid={shorten(pos['cid'])}  "
                    f"shares={pos['shares']:.2f}  block={now_block:,}"
                )
                continue
            nxt = int(pos.get("dump_next", 0))
            if nxt and now_block < nxt:
                keep.append(pos)
                continue
            try:
                tick_s = float(pos.get("tick") or 0.01)
                if tick_s < 0.01:
                    tick_s = 0.01
                await client.sell_fak(
                    pos["token"],
                    _lot2(pos["shares"]),
                    min_price=_px4(tick_s),
                    neg_risk=bool(pos.get("neg_risk")),
                    tick_size=tick_s,
                )
                n_dump += 1
                ui.print(
                    f"  -> dump  cid={shorten(pos['cid'])}  "
                    f"shares={pos['shares']:.2f}  block={now_block:,}"
                )
            except Exception as exc:
                pos["dump_next"] = now_block + 20
                ui.print(f"  -> dump_fail  cid={shorten(pos['cid'])}  error={exc}")
                keep.append(pos)
        open_pos[:] = keep

    async def _finalize(x: int) -> None:
        if not hot:
            return
        await _age_out(x)
        cids = sorted(book.printed.get(x, ()), key=lambda c: -book.last.get(c, 0))
        for cid in cids:
            if len(open_pos) >= max_open:
                break
            await _enter(cid, x)

    try:
        while not stop.is_set():
            if deadline and time.monotonic() >= deadline:
                ui.print(
                    f"  -> stop  run_seconds={args.run_seconds:g}  "
                    f"blocks={len(seen_blocks):,}  fills={n_fills:,}"
                )
                break
            try:
                event = await asyncio.wait_for(listen_q.get(), timeout=0.25)
            except asyncio.TimeoutError:
                elapsed = format_hms(time.monotonic() - started)
                if hot:
                    ui.update_footer(
                        f"hot  open={len(open_pos)}/{max_open}  "
                        f"fok={n_fok}  gtc={n_gtc}  dump={n_dump}  "
                        f"block={high_water:,}  fills={n_fills:,}  {elapsed}"
                    )
                else:
                    ui.update_bar(
                        min(len(seen_blocks), warmup_n),
                        f"warmup  blocks {min(len(seen_blocks), warmup_n)}/{warmup_n}  "
                        f"fills={n_fills:,}  drop={n_drop:,}",
                    )
                continue

            n_fills += 1
            blk = int(event.block_number or 0)
            if blk > 0 and blk not in seen_blocks:
                seen_blocks.add(blk)
                if not hot and len(seen_blocks) % 10 == 0:
                    ui.print(
                        f"  -> tape  blocks: {len(seen_blocks):,}  "
                        f"fills: {n_fills:,}  drop: {n_drop:,}  last: {blk:,}"
                    )
                if not hot:
                    ui.update_bar(
                        min(len(seen_blocks), warmup_n),
                        f"warmup  blocks {min(len(seen_blocks), warmup_n)}/{warmup_n}  "
                        f"fills={n_fills:,}  drop={n_drop:,}",
                    )
                    if len(seen_blocks) >= warmup_n:
                        ui.stop_footer()
                        ui.print(
                            f"warmup done  after_block={blk:,}  fills={n_fills:,}"
                        )
                        hot = True
                        ui.start_spinner(
                            f"hot  open=0/{max_open}  block={blk:,}"
                        )
            if blk > high_water:
                prev = high_water
                high_water = blk
                if prev and hot:
                    await _finalize(prev)

            parties = {(event.maker or "").lower(), (event.taker or "").lower()} - {""}
            ours = bool(parties & skip)
            parsed = _yes_px(event)
            if parsed is None:
                n_drop += 1
                if parse_fail_logged < 5:
                    parse_fail_logged += 1
                    ui.log_only(
                        f"drop fill  block={blk}  maker_asset={event.maker_asset_id}  "
                        f"taker_asset={event.taker_asset_id}  "
                        f"maker_amt={event.maker_amount}  taker_amt={event.taker_amount}"
                    )
                continue
            cid, yes_px, usdc, shares, is_taker, is_buy_yes, yes_tok, no_tok = parsed
            fee = parse_units(event.fee or 0)
            try:
                mkt_tick = float(event.tick_size) if event.tick_size else 0.0
            except (TypeError, ValueError):
                mkt_tick = 0.0
            book.ingest(
                cid=cid,
                block=blk,
                yes_px=yes_px,
                usdc=usdc,
                shares=shares,
                is_taker=is_taker,
                is_buy_yes=is_buy_yes,
                is_maker=not is_taker,
                account=(event.maker or event.taker or "").lower(),
                fee=fee,
                neg_risk=event_is_neg_risk(event),
                yes_token=yes_tok,
                no_token=no_tok,
                tick_size=mkt_tick,
            )
            if ours:
                for tok in (event.maker_asset_id, event.taker_asset_id, yes_tok, no_tok):
                    if tok and tok in pending_fill:
                        pending_size[tok] = shares
                        pending_fill[tok].set()
                        break
                # our GTC fill frees a slot
                keep = []
                for pos in open_pos:
                    if pos["token"] in {event.maker_asset_id, event.taker_asset_id} and (
                        not is_buy_yes
                    ):
                        ui.print(
                            f"  -> filled_exit  cid={shorten(pos['cid'])}  "
                            f"block={blk:,}"
                        )
                        continue
                    keep.append(pos)
                open_pos[:] = keep
    except asyncio.CancelledError:
        pass
    finally:
        stop.set()
        pump.cancel()
        await listen.disconnect()

    ui.closing(
        f"status=ok  fok={n_fok}  gtc={n_gtc}  dump={n_dump}  open={len(open_pos)}",
        time.monotonic() - started,
    )
    return 0


def parse_args() -> argparse.Namespace:
    p = argparse.ArgumentParser(
        description="Directional FOK/GTC from the frozen formula. "
        "Warm up 100 blocks, then trade with at most 3 outstanding bets."
    )
    p.add_argument("--exec", required=True, choices=EXEC_CHOICES)
    p.add_argument("--listen", required=True, choices=LISTEN_CHOICES)
    p.add_argument(
        "--dry-run",
        action="store_true",
        help="score tickets and print them; do not post orders",
    )
    p.add_argument(
        "--run-seconds",
        type=float,
        default=0.0,
        metavar="SEC",
        help="stop after this many seconds (0 = until CTRL-C)",
    )
    return p.parse_args()


def main() -> None:
    args = parse_args()
    try:
        raise SystemExit(asyncio.run(run(args)))
    except KeyboardInterrupt:
        raise SystemExit(130)


if __name__ == "__main__":
    main()
