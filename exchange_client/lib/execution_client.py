"""Execution client protocol and implementations.

Execution clients submit our copy-trade orders. They are intentionally
independent from trigger event streams.
"""

from __future__ import annotations

import asyncio
import json
import os
from dataclasses import dataclass
from typing import Protocol

import requests

from . import trading_lib


@dataclass(frozen=True)
class ExecutionResult:
    success: bool
    order_id: str | None
    making_amount: str | None
    taking_amount: str | None
    tx_hashes: list[str]
    raw_response: dict
    fee_rate_bps: int = 0


class ExecutionClient(Protocol):
    async def buy_market(
        self,
        token_id: str,
        amount_usd: float,
        *,
        max_price: float = 0,
        neg_risk: bool = False,
    ) -> ExecutionResult: ...
    async def sell_limit(
        self,
        token_id: str,
        size: float,
        price: float,
        *,
        ttl_seconds: float = 60,
        neg_risk: bool = False,
        tick_size: str | float | None = None,
    ) -> ExecutionResult: ...
    async def sell_fak(
        self,
        token_id: str,
        size: float,
        *,
        min_price: float = 0.01,
        neg_risk: bool = False,
        tick_size: str | float | None = None,
    ) -> ExecutionResult: ...
    async def warmup(self, token_id: str) -> None: ...
    def cached_fee_rate_bps(self, token_id: str) -> int | None: ...
    async def redeem_positions(self, user: str, dry_run: bool = False) -> list[str]: ...
    async def merge_positions(self, user: str, dry_run: bool = False, condition_id: str | None = None) -> list[str]: ...


def _result_from_resp(resp: dict, client, token_id: str) -> ExecutionResult:
    tx_hashes: list[str] = []
    if isinstance(resp, dict):
        for x in resp.get("transactionsHashes", []) or []:
            if isinstance(x, str) and x:
                tx_hashes.append(x)
        for x in resp.get("transactionHashes", []) or []:
            if isinstance(x, str) and x:
                tx_hashes.append(x)
        return ExecutionResult(
            success=bool(resp.get("success", True)),
            order_id=resp.get("orderID") or resp.get("orderId"),
            making_amount=resp.get("makingAmount"),
            taking_amount=resp.get("takingAmount"),
            tx_hashes=tx_hashes,
            raw_response=resp,
            fee_rate_bps=_cached_fee_rate_bps(client, token_id) or 0,
        )
    return ExecutionResult(
        success=False,
        order_id=None,
        making_amount=None,
        taking_amount=None,
        tx_hashes=[],
        raw_response={"raw": resp},
    )


def _cached_fee_rate_bps(client, token_id: str) -> int | None:
    """Return the fee rate for `token_id` from cache, or None if unknown.

    Reads a private cache attribute written by py_clob_client, so no I/O.
    """
    cache = getattr(client, "_ClobClient__fee_rates", None)
    if cache is None:
        return None
    value = cache.get(token_id)
    if value is None:
        return None
    try:
        return int(value)
    except (TypeError, ValueError):
        return None


class ClobExecutionClient:
    """ExecutionClient using py_clob_client through CLOB_API_URL."""

    def __init__(self) -> None:
        trading_lib.load_env()
        self._client = trading_lib.build_client()
        self._v2 = trading_lib.get_v2_client()
        self._neg_risk: dict[str, bool] = {}

    async def buy_market(
        self,
        token_id: str,
        amount_usd: float,
        *,
        max_price: float = 0,
        neg_risk: bool = False,
    ) -> ExecutionResult:
        if token_id in self._neg_risk:
            neg_risk = self._neg_risk[token_id]
        resp = await asyncio.to_thread(
            trading_lib.buy_token,
            self._client,
            token_id,
            amount_usd,
            max_price=max_price,
            neg_risk=neg_risk,
        )

        tx_hashes: list[str] = []
        for x in resp.get("transactionsHashes", []) or []:
            if isinstance(x, str) and x:
                tx_hashes.append(x)
        for x in resp.get("transactionHashes", []) or []:
            if isinstance(x, str) and x:
                tx_hashes.append(x)

        return ExecutionResult(
            success=bool(resp.get("success", True)),
            order_id=resp.get("orderID") or resp.get("orderId"),
            making_amount=resp.get("makingAmount"),
            taking_amount=resp.get("takingAmount"),
            tx_hashes=tx_hashes,
            raw_response=resp,
            fee_rate_bps=_cached_fee_rate_bps(self._client, token_id) or 0,
        )

    async def sell_limit(
        self,
        token_id: str,
        size: float,
        price: float,
        *,
        ttl_seconds: float = 60,
        neg_risk: bool = False,
        tick_size: str | float | None = None,
    ) -> ExecutionResult:
        resp = await asyncio.to_thread(
            trading_lib.sell_token_limit,
            self._client,
            token_id,
            size,
            price,
            ttl_seconds,
            neg_risk=neg_risk,
            tick_size=tick_size,
        )
        return _result_from_resp(resp, self._client, token_id)

    async def sell_fak(
        self,
        token_id: str,
        size: float,
        *,
        min_price: float = 0.01,
        neg_risk: bool = False,
        tick_size: str | float | None = None,
    ) -> ExecutionResult:
        resp = await asyncio.to_thread(
            trading_lib.sell_token,
            self._client,
            token_id,
            size,
            min_price=min_price,
            neg_risk=neg_risk,
            tick_size=tick_size,
        )
        return _result_from_resp(resp, self._client, token_id)

    async def warmup(self, token_id: str) -> None:
        def _warm() -> None:
            self._neg_risk[token_id] = bool(self._v2.get_neg_risk(token_id))

        await asyncio.to_thread(_warm)

    def cached_fee_rate_bps(self, token_id: str) -> int | None:
        return _cached_fee_rate_bps(self._client, token_id)

    async def redeem_positions(self, user: str, dry_run: bool = False) -> list[str]:
        # CLOB execution path uses sponsored builder relayer flow.
        return await asyncio.to_thread(
            trading_lib.redeem_positions,
            user,
            dry_run=dry_run,
            method="relayer",
        )

    async def merge_positions(self, user: str, dry_run: bool = False, condition_id: str | None = None) -> list[str]:
        # CLOB execution path uses sponsored builder relayer flow.
        return await asyncio.to_thread(
            trading_lib.merge_positions,
            user,
            dry_run=dry_run,
            method="relayer",
            condition_id=condition_id,
        )


class PolynodeExecutionClient:
    """ExecutionClient routed through Polynode co-signer.

    Uses py_clob_client for order construction + L2 header signing, then submits
    the request via Polynode's co-signer endpoint instead of direct CLOB POST.
    """

    def __init__(self) -> None:
        trading_lib.load_env()
        self._polynode_key = trading_lib.require_env("POLYNODE_API_KEY")
        self._cosigner_url = (
            os.environ["POLYNODE_COSIGNER_URL"]
            if "POLYNODE_COSIGNER_URL" in os.environ
            else "https://trade.polynode.dev"
        )
        self._client = trading_lib.build_client()
        self._v2 = trading_lib.get_v2_client()
        self._neg_risk: dict[str, bool] = {}

    async def buy_market(
        self,
        token_id: str,
        amount_usd: float,
        *,
        max_price: float = 0,
        neg_risk: bool = False,
    ) -> ExecutionResult:
        if token_id in self._neg_risk:
            neg_risk = self._neg_risk[token_id]

        def _sign() -> tuple[str, dict]:
            from py_clob_client_v2 import OrderType as OrderTypeV2
            from py_clob_client_v2.client import order_to_json_v2
            from py_clob_client_v2.headers.headers import create_level_2_headers as l2
            from py_clob_client_v2.clob_types import RequestArgs as RequestArgsV2

            order = trading_lib._sign_v2_fok_buy(
                self._v2, token_id, amount_usd, max_price=max_price, neg_risk=neg_risk
            )
            owner = self._v2.creds.api_key or ""
            body = order_to_json_v2(order, owner, OrderTypeV2.FOK, False, False)
            serialized = json.dumps(body, separators=(",", ":"), ensure_ascii=False)
            request_args = RequestArgsV2(
                method="POST",
                request_path="/order",
                body=body,
                serialized_body=serialized,
            )
            headers = l2(self._v2.signer, self._v2.creds, request_args)
            return serialized, headers

        serialized, headers = await asyncio.to_thread(_sign)
        payload = {
            "method": "POST",
            "path": "/order",
            "body": serialized,
            "headers": headers,
        }

        response = await asyncio.to_thread(
            requests.post,
            f"{self._cosigner_url}/submit",
            headers={
                "Content-Type": "application/json",
                "X-PolyNode-Key": self._polynode_key,
            },
            json=payload,
            timeout=30,
        )
        if response.status_code != 200:
            detail = (response.text or "")[:300]
            raise RuntimeError(
                f"polynode submit HTTP {response.status_code}: {detail}"
            )
        raw = response.json()

        tx_hashes: list[str] = []
        for x in raw.get("transactionsHashes", []) or []:
            if isinstance(x, str) and x:
                tx_hashes.append(x)
        for x in raw.get("transactionHashes", []) or []:
            if isinstance(x, str) and x:
                tx_hashes.append(x)

        return ExecutionResult(
            success=bool(raw.get("success", raw.get("orderID") or raw.get("orderId"))),
            order_id=raw.get("orderID") or raw.get("orderId"),
            making_amount=raw.get("makingAmount"),
            taking_amount=raw.get("takingAmount"),
            tx_hashes=tx_hashes,
            raw_response=raw,
            fee_rate_bps=_cached_fee_rate_bps(self._client, token_id) or 0,
        )

    async def sell_limit(
        self,
        token_id: str,
        size: float,
        price: float,
        *,
        ttl_seconds: float = 60,
        neg_risk: bool = False,
        tick_size: str | float | None = None,
    ) -> ExecutionResult:
        resp = await asyncio.to_thread(
            trading_lib.sell_token_limit,
            self._client,
            token_id,
            size,
            price,
            ttl_seconds,
            neg_risk=neg_risk,
            tick_size=tick_size,
        )
        return _result_from_resp(resp, self._client, token_id)

    async def sell_fak(
        self,
        token_id: str,
        size: float,
        *,
        min_price: float = 0.01,
        neg_risk: bool = False,
        tick_size: str | float | None = None,
    ) -> ExecutionResult:
        resp = await asyncio.to_thread(
            trading_lib.sell_token,
            self._client,
            token_id,
            size,
            min_price=min_price,
            neg_risk=neg_risk,
            tick_size=tick_size,
        )
        return _result_from_resp(resp, self._client, token_id)

    async def warmup(self, token_id: str) -> None:
        def _warm() -> None:
            self._neg_risk[token_id] = bool(self._v2.get_neg_risk(token_id))

        await asyncio.to_thread(_warm)

    def cached_fee_rate_bps(self, token_id: str) -> int | None:
        return _cached_fee_rate_bps(self._client, token_id)

    async def redeem_positions(self, user: str, dry_run: bool = False) -> list[str]:
        # Polynode execution path uses signed Safe transaction submission flow.
        return await asyncio.to_thread(
            trading_lib.redeem_positions,
            user,
            dry_run=dry_run,
            method="rpc",
        )

    async def merge_positions(self, user: str, dry_run: bool = False, condition_id: str | None = None) -> list[str]:
        # Polynode execution path uses signed Safe transaction submission flow.
        return await asyncio.to_thread(
            trading_lib.merge_positions,
            user,
            dry_run=dry_run,
            method="rpc",
            condition_id=condition_id,
        )
