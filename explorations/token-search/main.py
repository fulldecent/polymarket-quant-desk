#!/usr/bin/env python3
"""Look up Polymarket CLOB token IDs and Gamma stats."""

from __future__ import annotations

import argparse
import json
import os
import re
import sys
import time
from datetime import datetime, timezone
from pathlib import Path
from urllib.parse import urlparse

import requests
from dotenv import load_dotenv
from rich.console import Console
from rich.text import Text
from rich.theme import Theme

_project_root = Path(__file__).resolve().parents[2]
sys.path.insert(0, str(_project_root))

from exchange_client.lib.trading_lib import fmt_usd  # noqa: E402
from lib.run_logging import format_duration  # noqa: E402

COLOR_DONE = "green"
COLOR_TODO = "magenta"
_SLUG_RE = re.compile(r"^[a-z0-9]+(?:-[a-z0-9]+)+$")
_CONDITION_RE = re.compile(r"^0x[0-9a-fA-F]{64}$")
_TOKEN_RE = re.compile(r"^[0-9]{20,}$")


def _gamma_base() -> str:
    load_dotenv(_project_root / ".env")
    env = (os.environ.get("GAMMA_API_URL") or "").rstrip("/")
    if env:
        return env
    try:
        r = requests.get("http://127.0.0.1:9432/events", params={"limit": 1}, timeout=1.5)
        if r.ok:
            return "http://127.0.0.1:9432"
    except requests.RequestException:
        pass
    return "https://gamma-api.polymarket.com"


def _get(base: str, path: str, params: dict | None = None) -> object:
    r = requests.get(f"{base}{path}", params=params, timeout=20)
    r.raise_for_status()
    return r.json()


def _as_list(val: object) -> list:
    if val is None:
        return []
    if isinstance(val, list):
        return val
    if isinstance(val, str):
        try:
            parsed = json.loads(val)
            if isinstance(parsed, list):
                return parsed
        except json.JSONDecodeError:
            pass
        return [val]
    return [val]


def _num(val: object) -> float:
    if val is None or val == "":
        return 0.0
    try:
        return float(val)
    except (TypeError, ValueError):
        return 0.0


def _sort_key(obj: dict) -> tuple[float, float, float]:
    return (
        _num(obj.get("volume24hr") or obj.get("volume_24hr")),
        _num(obj.get("volume")),
        _num(obj.get("liquidity") or obj.get("liquidityNum")),
    )


def _slug_from_query(raw: str) -> str | None:
    text = raw.strip()
    if "polymarket.com" in text:
        path = urlparse(text).path.strip("/")
        parts = path.split("/")
        if len(parts) >= 2 and parts[0] in {"event", "market", "events", "markets"}:
            return parts[1]
        if parts:
            return parts[-1]
    if _SLUG_RE.match(text):
        return text
    return None


def _events_from_payload(payload: object) -> list[dict]:
    if isinstance(payload, list):
        return [x for x in payload if isinstance(x, dict)]
    if not isinstance(payload, dict):
        return []
    if "markets" in payload and "slug" in payload:
        return [payload]
    events = payload.get("events")
    if isinstance(events, list):
        return [x for x in events if isinstance(x, dict)]
    return []


def _markets_from_payload(payload: object) -> list[dict]:
    if isinstance(payload, list):
        return [x for x in payload if isinstance(x, dict)]
    if isinstance(payload, dict) and (payload.get("conditionId") or payload.get("clobTokenIds")):
        return [payload]
    return []


def resolve(base: str, query: str, *, include_closed: bool, limit: int) -> list[dict]:
    query = query.strip()
    if not query:
        return []

    if _CONDITION_RE.match(query):
        markets = _markets_from_payload(
            _get(base, "/markets", {"condition_ids": query, "limit": limit})
        )
        return _events_wrapping_markets(markets)

    if _TOKEN_RE.match(query):
        markets = _markets_from_payload(
            _get(base, "/markets", {"clob_token_ids": query, "limit": limit})
        )
        return _events_wrapping_markets(markets)

    slug = _slug_from_query(query)
    if slug:
        events = _events_from_payload(_get(base, "/events", {"slug": slug}))
        if not events:
            markets = _markets_from_payload(_get(base, "/markets", {"slug": slug}))
            events = _events_wrapping_markets(markets)
        if events:
            return _prepare(events, include_closed=include_closed, limit=limit)

    payload = _get(base, "/public-search", {"q": query})
    events = _events_from_payload(payload)
    return _prepare(events, include_closed=include_closed, limit=limit)


def _events_wrapping_markets(markets: list[dict]) -> list[dict]:
    if not markets:
        return []
    return [
        {
            "title": m.get("question") or m.get("title") or "",
            "slug": m.get("slug") or "",
            "endDate": m.get("endDate") or m.get("end_date_iso"),
            "volume24hr": m.get("volume24hr"),
            "volume": m.get("volume"),
            "liquidity": m.get("liquidity") or m.get("liquidityNum"),
            "markets": [m],
        }
        for m in markets
    ]


def _prepare(events: list[dict], *, include_closed: bool, limit: int) -> list[dict]:
    out: list[dict] = []
    for event in events:
        markets = [m for m in (event.get("markets") or []) if isinstance(m, dict)]
        if not include_closed:
            if event.get("closed") is True:
                continue
            markets = [m for m in markets if m.get("closed") is not True]
        markets.sort(key=_sort_key, reverse=True)
        event = dict(event)
        event["markets"] = markets
        if markets or include_closed:
            out.append(event)
    out.sort(key=_sort_key, reverse=True)
    return out[:limit]


def _outcome_style(name: str) -> str | None:
    key = name.strip().lower()
    if key in {"yes", "up"}:
        return COLOR_DONE
    if key in {"no", "down"}:
        return COLOR_TODO
    return None


def _end_date(val: object) -> str:
    if not val:
        return "—"
    text = str(val)
    if "T" in text:
        return text.split("T", 1)[0]
    return text[:10]


def _print_event(console: Console, event: dict) -> None:
    title = str(event.get("title") or event.get("ticker") or "untitled")
    slug = str(event.get("slug") or "")
    console.print("")
    console.print(Text(title, style="bold"))
    if slug:
        console.print(f"  slug:   {slug}")
    console.print(f"  end:    {_end_date(event.get('endDate'))}")
    vol = _num(event.get("volume"))
    vol24 = _num(event.get("volume24hr") or event.get("volume_24hr"))
    liq = _num(event.get("liquidity") or event.get("liquidityNum"))
    stats = Text("  vol:    ")
    stats.append(fmt_usd(vol), style=COLOR_DONE)
    stats.append("    24h: ")
    stats.append(fmt_usd(vol24), style=COLOR_DONE)
    stats.append("    liq: ")
    stats.append(fmt_usd(liq), style=COLOR_DONE)
    console.print(stats)

    for market in event.get("markets") or []:
        _print_market(console, market, event_title=title)


def _print_market(console: Console, market: dict, *, event_title: str = "") -> None:
    question = str(market.get("question") or "")
    if question and question != event_title:
        console.print(f"  {question}")
    fees = "yes" if market.get("feesEnabled") else "no"
    neg = "yes" if market.get("negRisk") else "no"
    cond = str(market.get("conditionId") or "")
    meta = Text("  fees:   ")
    meta.append(fees, style=COLOR_TODO if fees == "yes" else COLOR_DONE)
    meta.append("    neg-risk: ")
    meta.append(neg, style=COLOR_TODO if neg == "yes" else "dim")
    console.print(meta)
    if cond:
        console.print(f"  condition: {cond}")

    outcomes = [str(x) for x in _as_list(market.get("outcomes"))]
    prices = [_num(x) for x in _as_list(market.get("outcomePrices"))]
    tokens = [str(x) for x in _as_list(market.get("clobTokenIds"))]
    n = max(len(outcomes), len(prices), len(tokens))
    for i in range(n):
        name = outcomes[i] if i < len(outcomes) else f"outcome {i}"
        price = prices[i] if i < len(prices) else 0.0
        token = tokens[i] if i < len(tokens) else ""
        line = Text("  ")
        line.append(f"{name:<6}  ", style=_outcome_style(name))
        line.append(fmt_usd(price), style=COLOR_DONE)
        console.print(line)
        if token:
            console.print(f"          {token}")


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description="Look up Polymarket CLOB token IDs via Gamma.",
        epilog="Example: python explorations/token-search/main.py invade iran 2027",
        formatter_class=argparse.RawDescriptionHelpFormatter,
    )
    parser.add_argument(
        "query",
        nargs="*",
        help="URL, slug, keywords, condition id, or token id",
    )
    parser.add_argument("--limit", type=int, default=8, metavar="N")
    parser.add_argument("--closed", action="store_true", help="include resolved events")
    parser.add_argument("--json", action="store_true", dest="as_json")
    args = parser.parse_args()
    if not args.query:
        parser.print_help()
        raise SystemExit(2)
    return args


def main() -> None:
    args = parse_args()
    query = " ".join(args.query).strip()
    started = time.monotonic()
    console = Console(
        theme=Theme({"progress.elapsed": COLOR_DONE, "progress.remaining": COLOR_TODO})
    )
    log_dir = Path(__file__).resolve().parent / "logs"
    log_dir.mkdir(parents=True, exist_ok=True)
    ts = datetime.now(timezone.utc).strftime("%Y-%m-%dT%H%M%SZ")
    log_path = log_dir / f"main-{ts}.log"

    try:
        base = _gamma_base()
        console.print(Text(f"gamma:  {base}", style="bold"))
        console.print(f"log:    {log_path}")
        console.print("")
        events = resolve(base, query, include_closed=args.closed, limit=max(1, args.limit))
        if args.as_json:
            console.print_json(data=events)
        elif not events:
            console.print("no matches")
        else:
            for event in events:
                _print_event(console, event)
        console.print("")
        console.print(Text("run complete", style=COLOR_DONE))
        console.print(f"   time: {format_duration(time.monotonic() - started)}")
        console.print("")
        log_path.write_text(
            f"gamma: {base}\nquery: {query}\nevents: {len(events)}\n",
            encoding="utf-8",
        )
    except requests.HTTPError as exc:
        console.print(Text("run failed", style=COLOR_TODO))
        console.print(f"   error: {exc}")
        raise SystemExit(1) from exc
    except requests.RequestException as exc:
        console.print(Text("run failed", style=COLOR_TODO))
        console.print(f"   error: {exc}")
        raise SystemExit(1) from exc


if __name__ == "__main__":
    main()
