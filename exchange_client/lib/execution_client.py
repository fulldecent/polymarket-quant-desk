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
from py_clob_client.clob_types import MarketOrderArgs, OrderType, RequestArgs
from py_clob_client.headers.headers import create_level_2_headers
from py_clob_client.utilities import order_to_json

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
    ) -> ExecutionResult: ...
    async def warmup(self, token_id: str) -> None: ...
    def cached_fee_rate_bps(self, token_id: str) -> int | None: ...
    async def redeem_positions(self, user: str, dry_run: bool = False) -> list[str]: ...
    async def merge_positions(self, user: str, dry_run: bool = False, condition_id: str | None = None) -> list[str]: ...


def _warmup_clob_client(client, token_id: str) -> None:
    """Populate py_clob_client in-process caches for `token_id`.

    After this runs, `create_market_order` for that token makes zero GET
    requests (only the signed POST remains).
    """
    client.get_tick_size(token_id)
    client.get_neg_risk(token_id)
    client.get_fee_rate_bps(token_id)


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

    async def buy_market(
        self,
        token_id: str,
        amount_usd: float,
        *,
        max_price: float = 0,
    ) -> ExecutionResult:
        resp = await asyncio.to_thread(
            trading_lib.buy_token,
            self._client,
            token_id,
            amount_usd,
            max_price=max_price,
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

    async def warmup(self, token_id: str) -> None:
        await asyncio.to_thread(_warmup_clob_client, self._client, token_id)

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

    async def buy_market(
        self,
        token_id: str,
        amount_usd: float,
        *,
        max_price: float = 0,
    ) -> ExecutionResult:
        order = await asyncio.to_thread(
            self._client.create_market_order,
            MarketOrderArgs(
                token_id=token_id,
                amount=amount_usd,
                price=max_price,
                side="BUY",
                order_type=OrderType.FOK,
            ),
        )

        body = order_to_json(order, self._client.creds.api_key, OrderType.FOK, False)
        serialized = json.dumps(body, separators=(",", ":"), ensure_ascii=False)

        request_args = RequestArgs(
            method="POST",
            request_path="/order",
            body=body,
            serialized_body=serialized,
        )
        signer = self._client.signer
        if signer is None:
            raise RuntimeError("client signer is not initialized")
        headers = create_level_2_headers(signer, self._client.creds, request_args)

        payload = {
            "method": "POST",
            "path": "/order",
            "body": serialized,
            "headers": headers,
        }

        try:
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
            response.raise_for_status()
            raw = response.json()
        except Exception as exc:
            response_body = ""
            response_status = ""
            try:
                response_status = str(response.status_code)  # type: ignore[name-defined]
                response_body = response.text  # type: ignore[name-defined]
            except Exception:
                pass
            print(f"[PolynodeExecutionClient] buy_market request failed: {exc}")
            if response_status:
                print(f"[PolynodeExecutionClient] Response status: {response_status}")
            if response_body:
                print(f"[PolynodeExecutionClient] Response body: {response_body[:2000]}")
            print(f"[PolynodeExecutionClient] Signed order: {serialized}")
            raise

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

    async def warmup(self, token_id: str) -> None:
        await asyncio.to_thread(_warmup_clob_client, self._client, token_id)

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
