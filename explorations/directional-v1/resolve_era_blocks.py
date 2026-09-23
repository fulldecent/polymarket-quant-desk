#!/usr/bin/env python3
"""Map Stage B era UTC dates to Polygon block numbers via RPC binary search."""

from __future__ import annotations

import json
import os
import sys
import time
import urllib.request
from datetime import datetime, timezone
from pathlib import Path

from dotenv import load_dotenv

_HERE = Path(__file__).resolve().parent
_ROOT = _HERE.parents[1]
load_dotenv(_ROOT / ".env")

from eras import ERAS  # noqa: E402

OUT = _HERE / "era-blocks.json"
LO = 33_600_000
HI = 94_000_000


def _rpc(url: str, method: str, params: list):
    body = json.dumps({"jsonrpc": "2.0", "id": 1, "method": method, "params": params}).encode()
    req = urllib.request.Request(url, body, {"Content-Type": "application/json"})
    with urllib.request.urlopen(req, timeout=30) as resp:
        payload = json.loads(resp.read())
    if payload.get("error"):
        raise RuntimeError(payload["error"])
    return payload["result"]


def _ts(url: str, n: int) -> int:
    h = _rpc(url, "eth_getBlockByNumber", [hex(n), False])
    if h is None:
        raise RuntimeError(f"no block {n}")
    return int(h["timestamp"], 16)


def _first_ge(url: str, target: int, lo: int, hi: int) -> int:
    while lo < hi:
        mid = (lo + hi) // 2
        t = _ts(url, mid)
        if t < target:
            lo = mid + 1
        else:
            hi = mid
    return lo


def main() -> int:
    url = os.environ.get("POLYGON_RPC_URL", "")
    if not url:
        sys.exit("POLYGON_RPC_URL missing")
    t0 = time.time()
    head_hex = _rpc(url, "eth_blockNumber", [])
    head = int(head_hex, 16)
    hi = min(HI, head)
    print(f"rpc head {head:,}  search [{LO:,}, {hi:,}]", flush=True)
    out = []
    for era in ERAS:
        start = datetime.fromisoformat(era["start"] + "T00:00:00+00:00")
        end = datetime.fromisoformat(era["end"] + "T00:00:00+00:00")
        ts0 = int(start.timestamp())
        ts1 = int(end.timestamp())
        b0 = _first_ge(url, ts0, LO, hi)
        b1 = _first_ge(url, ts1, b0, hi)
        row = {
            **era,
            "start_block": b0,
            "end_block": b1 - 1,  # inclusive last block before exclusive end day
            "start_block_ts": datetime.fromtimestamp(_ts(url, b0), timezone.utc).isoformat(),
            "end_block_ts": datetime.fromtimestamp(_ts(url, b1 - 1), timezone.utc).isoformat(),
        }
        out.append(row)
        print(
            f"  {era['id']:<22} {era['start']}–{era['end']}  "
            f"{b0:,}–{b1-1:,}  {time.time()-t0:.1f}s",
            flush=True,
        )
    OUT.write_text(json.dumps(out, indent=2))
    print(f"wrote {OUT}  {time.time()-t0:.1f}s", flush=True)
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
