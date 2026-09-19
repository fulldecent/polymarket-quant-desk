"""Live last-100-block tape for Stage A/B. Updates are O(1) per fill."""

from __future__ import annotations

from collections import defaultdict, deque
from dataclasses import dataclass, field

import pandas as pd

from sim_lib import LOOKBACK_BLOCKS, PREFILTER_WINDOW, infer_tick, kris_count
from stage_b import feat_window


@dataclass
class Bar:
    block: int
    high: float
    low: float
    close: float
    n_fills: int = 0
    n_makers: int = 0
    n_accounts: int = 0
    up_usdc: float = 0.0
    dn_usdc: float = 0.0
    ask_usdc: float = 0.0
    bid_usdc: float = 0.0
    taker_usdc: float = 0.0
    all_usdc: float = 0.0
    max_taker_buy: float = 0.0
    ask_shares: float = 0.0
    ask_floor: bool = False
    vwap_num: float = 0.0
    fee_usdc: float = 0.0
    neg_risk: float = 0.0
    mt_abs: float = 0.0
    accounts: set = field(default_factory=set)
    makers: set = field(default_factory=set)


class LiveBook:
    def __init__(self) -> None:
        self.bars: dict[str, dict[int, Bar]] = defaultdict(dict)
        self.order: dict[str, deque[int]] = defaultdict(deque)
        self.makers: dict[str, deque[tuple[int, float]]] = defaultdict(deque)
        self.last: dict[str, float] = {}
        self.yes_id: dict[str, str] = {}
        self.no_id: dict[str, str] = {}
        self.neg_risk: dict[str, bool] = {}
        self.tick_size: dict[str, float] = {}
        self.printed: dict[int, set[str]] = defaultdict(set)

    def ingest(
        self,
        *,
        cid: str,
        block: int,
        yes_px: float,
        usdc: float,
        shares: float,
        is_taker: bool,
        is_buy_yes: bool,
        is_maker: bool,
        account: str,
        fee: float,
        neg_risk: bool,
        yes_token: str = "",
        no_token: str = "",
        tick_size: float = 0.0,
    ) -> None:
        if not cid or not (0 < yes_px < 1) or block <= 0:
            return
        if yes_token:
            self.yes_id[cid] = yes_token
        if no_token:
            self.no_id[cid] = no_token
        if neg_risk:
            self.neg_risk[cid] = True
        if tick_size and tick_size > 0:
            self.tick_size[cid] = float(tick_size)
        bars = self.bars[cid]
        bar = bars.get(block)
        if bar is None:
            bar = Bar(block=block, high=yes_px, low=yes_px, close=yes_px)
            bars[block] = bar
            self.order[cid].append(block)
            self.printed[block].add(cid)
            self._trim(cid, block)
        bar.high = max(bar.high, yes_px)
        bar.low = min(bar.low, yes_px)
        bar.close = yes_px
        bar.n_fills += 1
        bar.all_usdc += usdc
        bar.fee_usdc += fee
        bar.vwap_num += yes_px * usdc
        bar.neg_risk = max(bar.neg_risk, 1.0 if neg_risk else 0.0)
        if account:
            bar.accounts.add(account)
            bar.n_accounts = len(bar.accounts)
        if is_maker and account:
            bar.makers.add(account)
            bar.n_makers = len(bar.makers)
        if is_taker:
            bar.taker_usdc += usdc
            if is_buy_yes:
                bar.max_taker_buy = max(bar.max_taker_buy, usdc)
        if is_buy_yes:
            bar.up_usdc += usdc
            bar.ask_usdc += usdc
            bar.ask_shares += shares
            bar.ask_floor = True
        else:
            bar.dn_usdc += usdc
            bar.bid_usdc += usdc
        if is_maker:
            self.makers[cid].append((block, yes_px))
            mq = self.makers[cid]
            lo = block - (PREFILTER_WINDOW - 1)
            while mq and mq[0][0] < lo - LOOKBACK_BLOCKS:
                mq.popleft()
        self.last[cid] = yes_px

    def _trim(self, cid: str, now: int) -> None:
        q = self.order[cid]
        floor = now - LOOKBACK_BLOCKS
        bars = self.bars[cid]
        while q and q[0] < floor:
            old = q.popleft()
            bars.pop(old, None)

    def persist_count(self, cid: str, x: int) -> int:
        bars = self.bars.get(cid) or {}
        n = 0
        for d in range(PREFILTER_WINDOW):
            if (x - d) in bars:
                n += 1
        return n

    def kris(self, cid: str, x: int) -> int:
        lo = x - (PREFILTER_WINDOW - 1)
        px = [p for b, p in self.makers.get(cid, ()) if lo <= b <= x]
        return kris_count(px)

    def gate(self, cid: str, x: int, *, need: int, kris_min: int) -> bool:
        return self.persist_count(cid, x) >= need and self.kris(cid, x) >= kris_min

    def features(self, cid: str, x: int):
        last = self.last.get(cid)
        if last is None:
            return None, None, 0.01
        bars = self.bars.get(cid) or {}
        rows = []
        for blk in self.order.get(cid, ()):
            if blk < x - (LOOKBACK_BLOCKS - 1) or blk > x:
                continue
            bar = bars[blk]
            vwap = (bar.vwap_num / bar.all_usdc) if bar.all_usdc > 0 else last
            rows.append(
                {
                    "block_number": bar.block,
                    "close_yes": bar.close,
                    "high_yes": bar.high,
                    "low_yes": bar.low,
                    "n_fills": bar.n_fills,
                    "n_accounts": bar.n_accounts,
                    "n_makers": bar.n_makers,
                    "up_usdc": bar.up_usdc,
                    "dn_usdc": bar.dn_usdc,
                    "ask_usdc": bar.ask_usdc,
                    "bid_usdc": bar.bid_usdc,
                    "taker_usdc": bar.taker_usdc,
                    "all_usdc": bar.all_usdc,
                    "max_taker_buy_usdc": bar.max_taker_buy,
                    "ask_shares": bar.ask_shares,
                    "ask_floor": bar.ask_floor,
                    "vwap_yes": vwap,
                    "fee_usdc": bar.fee_usdc,
                    "neg_risk": bar.neg_risk,
                    "mt_abs_med": abs(bar.high - bar.low) / 2.0,
                }
            )
        if not rows:
            return None, last, 0.01
        sl = pd.DataFrame(rows)
        prices = sl["close_yes"].tolist()
        tick = self.tick_size.get(cid)
        if not tick:
            tick = infer_tick(prices)
            if tick < 0.01:
                tick = 0.01
        if tick <= 0:
            tick = 0.01
        feat = feat_window(sl, x, last, tick)
        feat["kris_count_8"] = float(self.kris(cid, x))
        feat["notional_pctile"] = 0.0
        feat["log_venue_vs_7d"] = 0.0
        feat["log_venue_vs_t7"] = 0.0
        return feat, last, tick
