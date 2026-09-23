#!/usr/bin/env python3
"""Collect Stage B events across the last 9 months of fills, not one weekend.

Random 6-of-8 + Kris-10+ triggers from equal block bins in that span, plus extra
draws from special eras that overlap it (Super Bowl 2026, Iran war,
World Cup 2026, freeze weekend). Then freeze via
`stage_b.py --from-events`.
"""

from __future__ import annotations

import json
import os
import sys
import time
from datetime import datetime, timezone
from pathlib import Path

import joblib
import numpy as np
from dotenv import load_dotenv

_HERE = Path(__file__).resolve().parent
_ROOT = _HERE.parents[1]
sys.path.insert(0, str(_HERE))
sys.path.insert(0, str(_ROOT))
load_dotenv(_ROOT / ".env")

import run as R  # noqa: E402
from sim_lib import BLOCKS_PER_DAY, LIQ_END, LOOKBACK_BLOCKS  # noqa: E402
from stage_b import LOG_DIR, collect_window  # noqa: E402

ERA_BLOCKS = _HERE / "era-blocks.json"
OUT = LOG_DIR / "stage_b_span_events.joblib"
MAX_CHUNK = 120_000  # ~2 days at 1.5 s; avoids OOM on 2026 density
N_BINS = 9
PER_BIN = 20_000
PER_ERA = 5_000
SEED = 0
# 9 calendar months before the frontier (Sep 2026 → Dec 2025).
SPAN_START = "2025-12-14"


def _block_at_or_after(ts: int, lo: int, hi: int, url: str) -> int:
    import urllib.request

    def _rpc(method, params):
        body = json.dumps({"jsonrpc": "2.0", "id": 1, "method": method, "params": params}).encode()
        req = urllib.request.Request(url, body, {"Content-Type": "application/json"})
        with urllib.request.urlopen(req, timeout=30) as resp:
            payload = json.loads(resp.read())
        if payload.get("error"):
            raise RuntimeError(payload["error"])
        return payload["result"]

    def _ts(n: int) -> int:
        h = _rpc("eth_getBlockByNumber", [hex(n), False])
        return int(h["timestamp"], 16)

    while lo < hi:
        mid = (lo + hi) // 2
        if _ts(mid) < ts:
            lo = mid + 1
        else:
            hi = mid
    return lo


def _collect_maybe_chunked(x_lo: int, x_hi: int, keep: int, rng: np.random.RandomState):
    span = x_hi - x_lo + 1
    if span <= MAX_CHUNK:
        return collect_window(x_lo, x_hi, keep, rng)
    n = min(6, int(np.ceil(span / MAX_CHUNK)))
    width = min(MAX_CHUNK, span)
    starts = np.linspace(x_lo, x_hi - width + 1, n).astype(int)
    per = max(1, keep // n) if keep else 0
    all_evs = []
    stats = {"n_cand": 0, "n_alive": 0, "n_kept": 0, "n_res_h1": 0, "n_res_h60": 0, "n_skip": 0}
    for s in starts:
        evs, st = collect_window(int(s), int(s) + width - 1, per, rng)
        all_evs.extend(evs)
        for k in stats:
            stats[k] += st.get(k, 0)
    if keep > 0 and len(all_evs) > keep:
        idx = np.sort(rng.choice(len(all_evs), keep, replace=False))
        all_evs = [all_evs[i] for i in idx]
        stats["n_kept"] = len(all_evs)
    return all_evs, stats


def main() -> int:
    if not R.FILLS_DIR or not Path(R.FILLS_DIR).exists():
        R._fail("FILLS_V1_DIR missing")
    LOG_DIR.mkdir(parents=True, exist_ok=True)
    fills_root = Path(R.FILLS_DIR)
    frontier = R._frontier(fills_root)
    x_hi = frontier - LIQ_END
    url = os.environ.get("POLYGON_RPC_URL", "")
    start = datetime.fromisoformat(SPAN_START + "T00:00:00+00:00")
    if url:
        x_lo = _block_at_or_after(int(start.timestamp()), 70_000_000, x_hi, url)
    else:
        x_lo = x_hi - int(9 * 30.44 * BLOCKS_PER_DAY)
    x_lo = max(x_lo, LOOKBACK_BLOCKS)
    rng = np.random.RandomState(SEED)
    t0 = time.time()
    started = datetime.now(timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ")
    span = x_hi - x_lo + 1
    bin_w = max(1, span // N_BINS)
    print(
        f"stage B 9-month sample  {SPAN_START}→frontier  {x_lo:,}–{x_hi:,}  "
        f"bins {N_BINS}  per-bin {PER_BIN}  per-era {PER_ERA}",
        flush=True,
    )

    seen: set[tuple[int, str]] = set()
    events: list[dict] = []
    bin_counts = []
    for i in range(N_BINS):
        b_lo = x_lo + i * bin_w
        b_hi = x_lo + (i + 1) * bin_w - 1 if i < N_BINS - 1 else x_hi
        day = 3 * BLOCKS_PER_DAY
        if b_hi - b_lo + 1 > day:
            start = int(rng.randint(b_lo, b_hi - day + 2))
            w_lo, w_hi = start, start + day - 1
        else:
            w_lo, w_hi = b_lo, b_hi
        print(f"  bin {i+1:02d}/{N_BINS}  {w_lo:,}–{w_hi:,}", flush=True)
        try:
            evs, st = _collect_maybe_chunked(w_lo, w_hi, PER_BIN, rng)
        except Exception as exc:
            print(f"    FAILED: {exc}", flush=True)
            evs, st = [], {}
        n_new = 0
        for e in evs:
            key = (e["x_block"], e["cid"])
            if key in seen:
                continue
            seen.add(key)
            events.append(e)
            n_new += 1
        bin_counts.append((i, n_new, st.get("n_alive", 0)))
        print(
            f"    kept +{n_new:,}  alive {st.get('n_alive', 0):,}  "
            f"pool {len(events):,}  {time.time()-t0:.1f}s",
            flush=True,
        )

    era_counts = []
    if ERA_BLOCKS.exists():
        eras = json.loads(ERA_BLOCKS.read_text())
        for era in eras:
            elo, ehi = int(era["start_block"]), int(era["end_block"])
            if ehi < x_lo or elo > x_hi:
                print(f"  era {era['id']} skipped (outside 9 months)", flush=True)
                continue
            elo, ehi = max(elo, x_lo), min(ehi, x_hi)
            print(f"  era {era['id']}  {elo:,}–{ehi:,}", flush=True)
            try:
                evs, st = _collect_maybe_chunked(elo, ehi, PER_ERA, rng)
            except Exception as exc:
                print(f"    FAILED: {exc}", flush=True)
                evs, st = [], {}
            n_new = 0
            for e in evs:
                key = (e["x_block"], e["cid"])
                if key in seen:
                    continue
                seen.add(key)
                events.append(e)
                n_new += 1
            era_counts.append((era["id"], n_new, st.get("n_alive", 0)))
            print(
                f"    +{n_new:,} new  alive {st.get('n_alive', 0):,}  "
                f"pool {len(events):,}  {time.time()-t0:.1f}s",
                flush=True,
            )

    events.sort(key=lambda e: (e["x_block"], e["cid"]))
    blob = {
        "events": events,
        "x_lo": x_lo,
        "x_hi": x_hi,
        "n_skip_gate": 0,
        "started": started,
        "n_bins": N_BINS,
        "bin_counts": bin_counts,
        "era_counts": era_counts,
        "seed": SEED,
    }
    joblib.dump(blob, OUT)
    print(f"wrote {OUT}  n={len(events):,}  {time.time()-t0:.1f}s", flush=True)
    print("next: python explorations/directional-v1/stage_b.py --from-events", OUT)
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
