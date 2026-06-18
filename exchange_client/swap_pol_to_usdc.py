#!/usr/bin/env python3
"""Swap native POL for USDC.e on Polygon and forward proceeds to the trading wallet.

This script uses the Odos SOR API to build a swap transaction from the signer EOA,
then transfers the received USDC.e to the configured trading wallet.

Usage:
    python3 exchange_client/swap_pol_to_usdc.py --amount-pol 1000 --dry-run
    python3 exchange_client/swap_pol_to_usdc.py --amount-pol 1000 --execute

Required environment variables:
    POLYGON_RPC_URL
    EOA_PRIVATE_KEY
    POLYMARKET_PROXY_WALLET
"""

from __future__ import annotations

import argparse
import json
import subprocess
import sys
import time
from decimal import Decimal, InvalidOperation

import requests
from eth_account import Account

from lib import trading_lib


CHAIN_ID = 137
NATIVE_TOKEN_ADDRESS = "0x0000000000000000000000000000000000000000"
USDC_E = "0x2791Bca1f2de4661ED88A30C99A7a9449Aa84174"
ODOS_QUOTE_URL = "https://api.odos.xyz/sor/quote/v2"
ODOS_ASSEMBLE_URL = "https://api.odos.xyz/sor/assemble"
USDC_DECIMALS = 6
POL_DECIMALS = 18
TX_POLL_SECONDS = 2.0
TX_TIMEOUT_SECONDS = 180.0


def parse_args(argv: list[str] | None = None) -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description="Swap POL to USDC.e and send it to the trading wallet",
    )
    parser.add_argument(
        "--amount-pol",
        type=parse_positive_decimal,
        required=True,
        metavar="POL",
        help="amount of native POL to swap, e.g. 1000",
    )
    parser.add_argument(
        "--slippage-pct",
        type=parse_positive_decimal,
        default=Decimal("0.5"),
        metavar="PCT",
        help="max swap slippage percent, e.g. 0.5 (default: 0.5)",
    )
    parser.add_argument(
        "--recipient",
        default=None,
        metavar="ADDRESS",
        help="recipient for the final USDC.e transfer (default: POLYMARKET_PROXY_WALLET)",
    )
    mode = parser.add_mutually_exclusive_group()
    mode.add_argument(
        "--execute",
        action="store_true",
        help="submit the swap and transfer transactions",
    )
    mode.add_argument(
        "--dry-run",
        action="store_true",
        help="quote only; do not submit transactions",
    )
    return parser.parse_args(argv)


def parse_positive_decimal(value: str) -> Decimal:
    try:
        parsed = Decimal(value.strip())
    except InvalidOperation as exc:
        raise argparse.ArgumentTypeError(f"Invalid decimal value: {value!r}") from exc
    if parsed <= 0:
        raise argparse.ArgumentTypeError("value must be positive")
    return parsed


def to_base_units(amount: Decimal, decimals: int) -> int:
    scale = Decimal(10) ** decimals
    return int((amount * scale).to_integral_value())


def from_base_units(amount: int, decimals: int) -> Decimal:
    scale = Decimal(10) ** decimals
    return Decimal(amount) / scale


def rpc_call(rpc_url: str, method: str, params: list[object]) -> object:
    payload = {
        "jsonrpc": "2.0",
        "id": 1,
        "method": method,
        "params": params,
    }
    response = requests.post(rpc_url, json=payload, timeout=30)
    response.raise_for_status()
    body = response.json()
    if "error" in body:
        raise RuntimeError(json.dumps(body["error"]))
    return body["result"]


def get_native_balance_wei(rpc_url: str, address: str) -> int:
    result = rpc_call(rpc_url, "eth_getBalance", [address, "latest"])
    return int(str(result), 16)


def get_token_balance_raw(rpc_url: str, token: str, address: str) -> int:
    calldata = cast_calldata("balanceOf(address)(uint256)", address)
    result = rpc_call(
        rpc_url,
        "eth_call",
        [
            {"to": token, "data": calldata},
            "latest",
        ],
    )
    return int(str(result), 16)


def cast_calldata(signature: str, *args: str) -> str:
    command = ["cast", "calldata", signature, *args]
    result = subprocess.run(command, capture_output=True, text=True, timeout=15)
    if result.returncode != 0:
        raise RuntimeError(result.stderr.strip() or "cast calldata failed")
    return result.stdout.strip()


def sign_and_send_transaction(private_key: str, tx: dict, rpc_url: str) -> str:
    signed = Account.sign_transaction(tx, private_key)
    raw_hex = signed.raw_transaction.hex()
    if not raw_hex.startswith("0x"):
        raw_hex = f"0x{raw_hex}"
    tx_hash = rpc_call(rpc_url, "eth_sendRawTransaction", [raw_hex])
    return str(tx_hash)


def wait_for_receipt(rpc_url: str, tx_hash: str) -> dict:
    deadline = time.monotonic() + TX_TIMEOUT_SECONDS
    while time.monotonic() < deadline:
        receipt = rpc_call(rpc_url, "eth_getTransactionReceipt", [tx_hash])
        if receipt is not None:
            if int(str(receipt.get("status", "0x0")), 16) != 1:
                raise RuntimeError(f"transaction failed: {tx_hash}")
            return receipt
        time.sleep(TX_POLL_SECONDS)
    raise RuntimeError(f"timed out waiting for receipt: {tx_hash}")


def get_nonce(rpc_url: str, address: str) -> int:
    result = rpc_call(rpc_url, "eth_getTransactionCount", [address, "pending"])
    return int(str(result), 16)


def estimate_gas(rpc_url: str, tx: dict) -> int:
    result = rpc_call(rpc_url, "eth_estimateGas", [tx])
    return int(str(result), 16)


def build_transfer_tx(
    *,
    rpc_url: str,
    from_address: str,
    to_address: str,
    amount_raw: int,
    nonce: int,
) -> dict:
    calldata = cast_calldata("transfer(address,uint256)", to_address, str(amount_raw))
    gas_price = int(str(rpc_call(rpc_url, "eth_gasPrice", [])), 16)
    estimate = estimate_gas(
        rpc_url,
        {
            "from": from_address,
            "to": USDC_E,
            "data": calldata,
            "value": hex(0),
        },
    )
    return {
        "chainId": CHAIN_ID,
        "nonce": nonce,
        "to": USDC_E,
        "data": calldata,
        "value": 0,
        "gas": int(estimate * 1.2),
        "gasPrice": gas_price,
    }


def get_odos_quote(
    *,
    amount_raw: int,
    slippage_pct: Decimal,
    user_addr: str,
) -> dict:
    payload = {
        "chainId": CHAIN_ID,
        "inputTokens": [
            {
                "tokenAddress": NATIVE_TOKEN_ADDRESS,
                "amount": str(amount_raw),
            }
        ],
        "outputTokens": [
            {
                "tokenAddress": USDC_E,
                "proportion": 1,
            }
        ],
        "slippageLimitPercent": float(slippage_pct),
        "userAddr": user_addr,
        "referralCode": 0,
        "disableRFQs": True,
        "compact": True,
    }
    response = requests.post(ODOS_QUOTE_URL, json=payload, timeout=30)
    response.raise_for_status()
    return response.json()


def assemble_odos_swap(*, path_id: str, user_addr: str) -> dict:
    payload = {
        "userAddr": user_addr,
        "pathId": path_id,
        "simulate": False,
    }
    response = requests.post(ODOS_ASSEMBLE_URL, json=payload, timeout=30)
    response.raise_for_status()
    return response.json()


def print_quote_summary(
    *,
    amount_pol: Decimal,
    quote: dict,
    recipient: str,
    signer: str,
) -> None:
    out_amount_raw = int(quote["outAmounts"][0])
    out_amount = from_base_units(out_amount_raw, USDC_DECIMALS)
    gas_pol = Decimal(str(quote.get("gasEstimateValue", 0)))
    in_value = Decimal(str((quote.get("inValues") or [0])[0]))
    out_value = Decimal(str((quote.get("outValues") or [0])[0]))
    net_out_value = Decimal(str(quote.get("netOutValue", 0)))
    gross_diff_pct = Decimal(0)
    net_diff_pct = Decimal(0)
    if in_value > 0:
        gross_diff_pct = (out_value / in_value - Decimal(1)) * Decimal(100)
        net_diff_pct = (net_out_value / in_value - Decimal(1)) * Decimal(100)
    odos_price_impact_pct = Decimal(str(quote.get("priceImpact", 0))) * Decimal(100)
    print("swap quote")
    print(f"  signer:          {signer}")
    print(f"  recipient:       {recipient}")
    print(f"  sell:            {amount_pol} POL")
    print(f"  expected buy:    {out_amount:.6f} USDC.e")
    print(f"  est gas:         {gas_pol:.6f} POL")
    print(f"  gross value diff:{gross_diff_pct:+.4f}%")
    print(f"  net value diff:  {net_diff_pct:+.4f}%")
    print(f"  odos impact:     {odos_price_impact_pct:.4f}%")
    print(f"  path id:         {quote['pathId']}")
    print()


def main(argv: list[str] | None = None) -> None:
    args = parse_args(argv)
    trading_lib.load_env()

    rpc_url = trading_lib.get_polygon_rpc_url()
    private_key = trading_lib.require_env("EOA_PRIVATE_KEY")
    signer = trading_lib.get_eoa_address()
    recipient = args.recipient or trading_lib.get_funder_address()

    amount_raw = to_base_units(args.amount_pol, POL_DECIMALS)
    signer_pol_raw = get_native_balance_wei(rpc_url, signer)
    if signer_pol_raw < amount_raw:
        sys.exit(
            f"Signer POL balance is too low. Have {from_base_units(signer_pol_raw, POL_DECIMALS):.6f} POL, need {args.amount_pol}."
        )

    quote = get_odos_quote(
        amount_raw=amount_raw,
        slippage_pct=args.slippage_pct,
        user_addr=signer,
    )
    print_quote_summary(
        amount_pol=args.amount_pol,
        quote=quote,
        recipient=recipient,
        signer=signer,
    )

    if not args.execute:
        print("dry run only; no transaction submitted")
        return

    assemble = assemble_odos_swap(path_id=quote["pathId"], user_addr=signer)
    swap_tx = assemble["transaction"]

    signer_usdc_before = get_token_balance_raw(rpc_url, USDC_E, signer)
    recipient_usdc_before = get_token_balance_raw(rpc_url, USDC_E, recipient)

    normalized_swap_tx = {
        "chainId": int(swap_tx["chainId"]),
        "nonce": int(swap_tx["nonce"]),
        "to": swap_tx["to"],
        "data": swap_tx["data"],
        "value": int(str(swap_tx["value"])),
        "gas": int(swap_tx["gas"]),
        "gasPrice": int(swap_tx["gasPrice"]),
    }

    print("submitting swap transaction...")
    swap_hash = sign_and_send_transaction(private_key, normalized_swap_tx, rpc_url)
    print(f"  swap tx: {swap_hash}")
    wait_for_receipt(rpc_url, swap_hash)

    signer_usdc_after_swap = get_token_balance_raw(rpc_url, USDC_E, signer)
    usdc_received = signer_usdc_after_swap - signer_usdc_before
    if usdc_received <= 0:
        sys.exit("Swap mined, but signer USDC.e balance did not increase.")

    transfer_nonce = get_nonce(rpc_url, signer)
    transfer_tx = build_transfer_tx(
        rpc_url=rpc_url,
        from_address=signer,
        to_address=recipient,
        amount_raw=usdc_received,
        nonce=transfer_nonce,
    )

    print("submitting transfer transaction...")
    transfer_hash = sign_and_send_transaction(private_key, transfer_tx, rpc_url)
    print(f"  transfer tx: {transfer_hash}")
    wait_for_receipt(rpc_url, transfer_hash)

    recipient_usdc_after = get_token_balance_raw(rpc_url, USDC_E, recipient)
    delivered = recipient_usdc_after - recipient_usdc_before
    print()
    print("completed")
    print(f"  signer:         {signer}")
    print(f"  recipient:      {recipient}")
    print(f"  swapped:        {args.amount_pol} POL")
    print(f"  received:       {from_base_units(usdc_received, USDC_DECIMALS):.6f} USDC.e at signer")
    print(f"  delivered:      {from_base_units(delivered, USDC_DECIMALS):.6f} USDC.e to trading wallet")
    print(f"  swap tx hash:   {swap_hash}")
    print(f"  transfer hash:  {transfer_hash}")


if __name__ == "__main__":
    main()