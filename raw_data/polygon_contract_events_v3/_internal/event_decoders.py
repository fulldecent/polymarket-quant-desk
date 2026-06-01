"""
Solidity event decoders for Polymarket contract logs.

Pure library: takes one raw ``eth_getLogs`` response entry and returns a
``(contract_name, event_name, row_dict)`` triple whose row keys match the
canonical column list in ``tables.all_columns(contract, event)`` exactly,
and whose values are already the final on-disk Python types expected by
``HotStore.persist``.

Type contract for emitted row values:

  * ``BLOB`` columns → ``bytes`` of the exact expected length
    (32 for hashes / token IDs / condition IDs / order hashes; 20 for
    addresses).
  * ``UINTEGER`` columns → ``int`` in ``[0, 2**32)``.
  * ``STRING`` columns that store uint256 → decimal ``str`` (no leading
    zeros, no ``0x`` prefix).
  * ``STRING`` columns that store JSON arrays → ``str``, the result of
    ``json.dumps([str(x) for x in ...])`` so the empty array is exactly
    ``"[]"``.
  * ``STRING`` columns that store free-form data (NegRiskAdapter
    ``data``, UmaCtfAdapter ``ancillary_data``) → hex with ``0x``
    prefix.

Routing:

The contract name is derived from ``log["address"]``; it disambiguates
events that share a Solidity signature across multiple contracts
(``CTFExchange`` and ``NegRiskCtfExchange`` share five events;
``CTFExchangeV2`` and ``NegRiskCtfExchangeV2`` share five events;
``FeeModuleCTF`` and ``FeeModuleNegRisk`` share one event).
``ConditionalTokens`` and ``NegRiskAdapter`` each emit a ``PositionSplit``
/ ``PositionsMerge`` / ``PayoutRedemption`` event but with DIFFERENT
Solidity signatures (different topic0 hashes), so they decode through
separate paths and need no address-based routing.

Entry point:
    decode_log(log) -> (contract, event, row) | None

Returns ``None`` when:
  * The log's ``topics[0]`` is not in ``TOPIC0_MAP`` (unknown event).
  * The log's ``address`` is not in our registry (event from a contract
    we do not track — defensive only, since the RPC filter restricts to
    our addresses).
  * Decoding raises an exception. The library swallows decode errors so
    one malformed log does not drop a batch. Use ``decode_log_strict``
    if you want exceptions to propagate.

This module does NOT:
  * Read environment variables.
  * Hold a DuckDB connection or write to disk.
  * Configure logging.

At import time, the module asserts that:
  * every ``topics[0]`` key in ``TOPIC0_MAP`` has a matching entry in
    the internal dispatch table, and
  * every ``(contract, event)`` pair produced by the decoders exists in
    the v3 schema, and every v3 schema pair is produced by some decoder.

Verifying that each decoder produces the exact column set declared by
``tables.all_columns(contract, event)`` requires invoking the decoder
with real ABI-encoded data, which is the job of the test suite (see
``tests/test_event_decoders.py``), not of this module's import-time
checks.
"""

from __future__ import annotations

import json
from dataclasses import dataclass
from typing import Callable

from eth_abi import decode as abi_decode

from .tables import CONTRACTS, EVENT_COLUMNS


# ---------------------------------------------------------------------------
# topic0 → internal event tag
# ---------------------------------------------------------------------------
#
# ``TOPIC0_MAP`` is exposed for diagnostics (mostly: log messages and
# tests). It is NOT used by the decoder itself, which dispatches off
# ``_DISPATCH`` below. The internal tag is a human-readable name that
# distinguishes between events sharing a base Solidity name but
# different topic0 hashes (e.g. ``PositionSplit_CT`` for
# ``ConditionalTokens.PositionSplit`` versus ``PositionSplit_NR`` for
# ``NegRiskAdapter.PositionSplit``). Events that share BOTH the
# Solidity signature AND topic0 across multiple contracts (e.g. v1
# exchange ``OrderFilled`` on ``CTFExchange`` and ``NegRiskCtfExchange``)
# share one tag and are routed to the correct ``(contract, event)``
# pair via ``log["address"]``.

TOPIC0_MAP: dict[str, str] = {
    # ConditionalTokens
    "0xab3760c3bd2bb38b5bcf54dc79802ed67338b4cf29f3054ded67ed24661e4177": "ConditionPreparation",
    "0xb44d84d3289691f71497564b85d4233648d9dbae8cbdbb4329f301c3a0185894": "ConditionResolution",
    "0x2e6bb91f8cbcda0c93623c54d0403a43514fabc40084ec96b6d5379a74786298": "PositionSplit_CT",
    "0x6f13ca62553fcc2bcd2372180a43949c1e4cebba603901ede2f4e14f36b282ca": "PositionsMerge_CT",
    "0x2682012a4a4f1973119f1c9b90745d1bd91fa2bab387344f044cb3586864d18d": "PayoutRedemption_CT",
    # CTFExchange / NegRiskCtfExchange (v1 exchanges)
    "0xd0a08e8c493f9c94f29311604c9de1b4e8c8d4c06bd0c789af57f2d65bfec0f6": "OrderFilled_V1",
    "0x63bf4d16b7fa898ef4c4b2b6d90fd201e9c56313b65638af6088d149d2ce956c": "OrdersMatched_V1",
    "0xacffcc86834d0f1a64b0d5a675798deed6ff0bcfc2231edd3480e7288dba7ff4": "FeeCharged_V1",
    "0x5152abf959f6564662358c2e52b702259b78bac5ee7842a0f01937e670efcc7d": "OrderCancelled",
    "0xbc9a2432e8aeb48327246cddd6e872ef452812b4243c04e6bfb786a2cd8faf0d": "TokenRegistered",
    # CTFExchangeV2 / NegRiskCtfExchangeV2 (next-gen exchanges)
    "0xd543adfd945773f1a62f74f0ee55a5e3b9b1a28262980ba90b1a89f2ea84d8ee": "OrderFilled_V2",
    "0x174b3811690657c217184f89418266767c87e4805d09680c39fc9c031c0cab7c": "OrdersMatched_V2",
    "0x55bb3cade9d43b798a4fe5ffdd05024b2d7870df53920673bfc7e68047cd0ab1": "FeeCharged_V2",
    "0xe92c22722d9c284034b6c9f5aaec018edb3e593c0e084900b6b9d390a1182a0b": "OrderPreapproved",
    "0xb766aa470f20b094f26a9a14ea5bf63a60af51703c15776e2e739b6a0428adf6": "OrderPreapprovalInvalidated",
    # NegRiskAdapter
    "0xf059ab16d1ca60e123eab60e3c02b68faf060347c701a5d14885a8e1def7b3a8": "MarketPrepared",
    "0xaac410f87d423a922a7b226ac68f0c2eaf5bf6d15e644ac0758c7f96e2c253f7": "QuestionPrepared",
    "0x9e9fa7fd355160bd4cd3f22d4333519354beff1f5689bde87f2c5e63d8d484b2": "OutcomeReported",
    "0xbbed930dbfb7907ae2d60ddf78345610214f26419a0128df39b6cc3d9e5df9b0": "PositionSplit_NR",
    "0xba33ac50d8894676597e6e35dc09cff59854708b642cd069d21eb9c7ca072a04": "PositionsMerge_NR",
    "0xb03d19dddbc72a87e735ff0ea3b57bef133ebe44e1894284916a84044deb367e": "PositionsConverted",
    "0x9140a6a270ef945260c03894b3c6b3b2695e9d5101feef0ff24fec960cfd3224": "PayoutRedemption_NR",
    # UmaCtfAdapter
    "0xeee0897acd6893adcaf2ba5158191b3601098ab6bece35c5d57874340b64c5b7": "QuestionInitialized",
    "0x6ded7250a9d5f79aef5add44600fc20a74a0af6f4730baa4fc4ab87bf484b812": "QuestionPaused",
    "0x92d28918c5574e7fc0f4f948c39502682c81cfb4089b07b83f95b3264e5e5e06": "QuestionUnpaused",
    "0x566c3fbdd12dd86bb341787f6d531f79fd7ad4ce7e3ae2d15ac0ca1b601af9df": "QuestionResolved",
    "0x7981b5832932948db4e32a4a16a0f44b2ce7ff088574afb9364b313f70f82e8f": "QuestionReset",
    "0x2435a0347185933b12027c6f394a5fd9c03646dba233e956f50658719dfc0b35": "QuestionFlagged",
    "0x052435bc04fc49113a7bfd9198a92c0852ca622a621800f6da66d4b29b786c05": "QuestionUnflagged",
    "0x6edb5841a476c9c29c34a652d1a44f785fe71a6157a3da9a6a6a589a1bd2945a": "QuestionEmergencyResolved",
    # FeeModuleCTF / FeeModuleNegRisk
    "0xb608d2bf25d8b4b744ba23ce2ea9802ea955e216c064a62f42152fbf98958d24": "FeeRefunded",
}

# Flat list of all topic0 values — pass directly to the eth_getLogs topics filter.
WANTED_TOPICS: list[str] = list(TOPIC0_MAP.keys())


# Lowercased ``log["address"]`` → contract name. Lowercase because the
# JSON-RPC response is not normalized to EIP-55 checksum.
_CONTRACT_BY_ADDRESS: dict[str, str] = {
    c.address.lower(): c.name for c in CONTRACTS
}

# Flat list of all addresses — pass directly to the eth_getLogs address
# filter (the JSON-RPC accepts lowercase, mixed-case, or EIP-55; lowercase
# is safest).
WANTED_ADDRESSES: list[str] = [c.address for c in CONTRACTS]


# ---------------------------------------------------------------------------
# Low-level helpers
# ---------------------------------------------------------------------------

def _parse_hex(val: str | None) -> int:
    """Parse a ``0x``-prefixed hex string from the JSON-RPC into an int."""
    if not val or val == "0x":
        return 0
    return int(val, 16)


def _hex_to_bytes(val: str) -> bytes:
    """Convert a hex string (with or without ``0x``) to ``bytes``."""
    if val.startswith("0x"):
        val = val[2:]
    if len(val) % 2:
        val = "0" + val
    return bytes.fromhex(val)


def _addr_from_topic(topic: str) -> bytes:
    """Extract a 20-byte address from a 32-byte topic string.

    Topics are 32-byte values; the address occupies the low 20 bytes
    (right-padded with zero bytes on the high side).
    """
    if topic.startswith("0x"):
        topic = topic[2:]
    return bytes.fromhex(topic[-40:])


def _addr_to_bytes(addr: str) -> bytes:
    """Convert an ``eth_abi`` decoded address string to 20 raw bytes."""
    if addr.startswith("0x"):
        addr = addr[2:]
    return bytes.fromhex(addr)


def _u256_str(val: int) -> str:
    """Return the canonical decimal string for a uint256 value."""
    return str(val)


def _decode_data(types: list[str], data_hex: str) -> tuple:
    """ABI-decode the ``log["data"]`` payload using ``eth_abi.decode``.

    Returns a tuple of zeros if ``data`` is empty. Some on-chain logs
    have empty data when the call reverted but the contract still
    emitted; this handling is defensive and rarely triggered.
    """
    raw = _hex_to_bytes(data_hex)
    if not raw:
        return tuple(0 for _ in types)
    return abi_decode(types, raw)


def _common(log: dict) -> dict:
    """Return the four common columns extracted from the JSON-RPC log
    entry, in the exact form required by the v3 schema.

    Block, transaction-index, and log-index are ``int`` in
    ``[0, 2**32)``. Transaction hash is ``bytes`` of length 32.
    """
    return {
        "block_number":      _parse_hex(log["blockNumber"]),
        "transaction_index": _parse_hex(log["transactionIndex"]),
        "transaction_hash":  _hex_to_bytes(log["transactionHash"]),
        "log_index":         _parse_hex(log["logIndex"]),
    }


# ---------------------------------------------------------------------------
# Per-event decoders
# ---------------------------------------------------------------------------
#
# Each decoder returns (event_name, row_dict). The row_dict already
# includes the four common columns. ``decode_log`` adds the routing
# step (resolve ``log["address"]`` to ``contract_name``) on top.
#
# Each decoder MUST emit exactly the columns named by
# ``all_columns(contract, event)``, in any order (the row is a dict).
# The ``_validate_row`` self-check at module import time enforces this.

# --- ConditionalTokens ---------------------------------------------------

def _decode_condition_preparation(log: dict, topics: list[str]) -> tuple[str, dict]:
    row = _common(log)
    row["condition_id"] = _hex_to_bytes(topics[1])
    row["oracle"] = _addr_from_topic(topics[2])
    row["question_id"] = _hex_to_bytes(topics[3])
    (outcome_slot_count,) = _decode_data(["uint256"], log["data"])
    row["outcome_slot_count"] = int(outcome_slot_count)
    return ("condition_preparation", row)


def _decode_condition_resolution(log: dict, topics: list[str]) -> tuple[str, dict]:
    row = _common(log)
    row["condition_id"] = _hex_to_bytes(topics[1])
    row["oracle"] = _addr_from_topic(topics[2])
    row["question_id"] = _hex_to_bytes(topics[3])
    outcome_slot_count, payout_numerators = _decode_data(
        ["uint256", "uint256[]"], log["data"]
    )
    row["outcome_slot_count"] = int(outcome_slot_count)
    row["payout_numerators"] = json.dumps([str(x) for x in payout_numerators])
    return ("condition_resolution", row)


def _decode_position_split_ct(log: dict, topics: list[str]) -> tuple[str, dict]:
    row = _common(log)
    row["stakeholder"] = _addr_from_topic(topics[1])
    row["parent_collection_id"] = _hex_to_bytes(topics[2])
    row["condition_id"] = _hex_to_bytes(topics[3])
    collateral_token, partition, amount = _decode_data(
        ["address", "uint256[]", "uint256"], log["data"]
    )
    row["collateral_token"] = _addr_to_bytes(collateral_token)
    row["partition"] = json.dumps([str(x) for x in partition])
    row["amount"] = _u256_str(amount)
    return ("position_split", row)


def _decode_positions_merge_ct(log: dict, topics: list[str]) -> tuple[str, dict]:
    row = _common(log)
    row["stakeholder"] = _addr_from_topic(topics[1])
    row["parent_collection_id"] = _hex_to_bytes(topics[2])
    row["condition_id"] = _hex_to_bytes(topics[3])
    collateral_token, partition, amount = _decode_data(
        ["address", "uint256[]", "uint256"], log["data"]
    )
    row["collateral_token"] = _addr_to_bytes(collateral_token)
    row["partition"] = json.dumps([str(x) for x in partition])
    row["amount"] = _u256_str(amount)
    return ("positions_merge", row)


def _decode_payout_redemption_ct(log: dict, topics: list[str]) -> tuple[str, dict]:
    row = _common(log)
    row["redeemer"] = _addr_from_topic(topics[1])
    row["collateral_token"] = _addr_from_topic(topics[2])
    row["parent_collection_id"] = _hex_to_bytes(topics[3])
    condition_id, index_sets, payout = _decode_data(
        ["bytes32", "uint256[]", "uint256"], log["data"]
    )
    row["condition_id"] = condition_id  # bytes(32) from eth_abi
    row["index_sets"] = json.dumps([str(x) for x in index_sets])
    row["payout"] = _u256_str(payout)
    return ("payout_redemption", row)


# --- CTFExchange / NegRiskCtfExchange (v1 exchange contracts) ------------

def _decode_order_filled_v1(log: dict, topics: list[str]) -> tuple[str, dict]:
    row = _common(log)
    row["order_hash"] = _hex_to_bytes(topics[1])
    row["maker"] = _addr_from_topic(topics[2])
    row["taker"] = _addr_from_topic(topics[3])
    maker_asset_id, taker_asset_id, maker_amount, taker_amount, fee = _decode_data(
        ["uint256", "uint256", "uint256", "uint256", "uint256"], log["data"]
    )
    # Asset IDs are declared as BLOB(32) in the schema; emit 32-byte
    # values directly so the producer and the schema agree without
    # any coercion at insert time.
    row["maker_asset_id"] = maker_asset_id.to_bytes(32, "big")
    row["taker_asset_id"] = taker_asset_id.to_bytes(32, "big")
    row["maker_amount_filled"] = _u256_str(maker_amount)
    row["taker_amount_filled"] = _u256_str(taker_amount)
    row["fee"] = _u256_str(fee)
    return ("order_filled", row)


def _decode_orders_matched_v1(log: dict, topics: list[str]) -> tuple[str, dict]:
    row = _common(log)
    row["taker_order_hash"] = _hex_to_bytes(topics[1])
    row["taker_order_maker"] = _addr_from_topic(topics[2])
    maker_asset_id, taker_asset_id, maker_amount, taker_amount = _decode_data(
        ["uint256", "uint256", "uint256", "uint256"], log["data"]
    )
    row["maker_asset_id"] = maker_asset_id.to_bytes(32, "big")
    row["taker_asset_id"] = taker_asset_id.to_bytes(32, "big")
    row["maker_amount_filled"] = _u256_str(maker_amount)
    row["taker_amount_filled"] = _u256_str(taker_amount)
    return ("orders_matched", row)


def _decode_fee_charged_v1(log: dict, topics: list[str]) -> tuple[str, dict]:
    row = _common(log)
    row["receiver"] = _addr_from_topic(topics[1])
    token_id, amount = _decode_data(["uint256", "uint256"], log["data"])
    row["token_id"] = token_id.to_bytes(32, "big")
    row["amount"] = _u256_str(amount)
    return ("fee_charged", row)


def _decode_order_cancelled(log: dict, topics: list[str]) -> tuple[str, dict]:
    row = _common(log)
    row["order_hash"] = _hex_to_bytes(topics[1])
    return ("order_cancelled", row)


def _decode_token_registered(log: dict, topics: list[str]) -> tuple[str, dict]:
    row = _common(log)
    # token0 and token1 are uint256 indexed parameters; each topic is 32
    # bytes (the integer encoded big-endian). The v3 schema stores them
    # as BLOB(32).
    row["token0"] = _hex_to_bytes(topics[1])
    row["token1"] = _hex_to_bytes(topics[2])
    row["condition_id"] = _hex_to_bytes(topics[3])
    return ("token_registered", row)


# --- CTFExchangeV2 / NegRiskCtfExchangeV2 (next-gen exchanges) ----------

def _decode_order_filled_v2(log: dict, topics: list[str]) -> tuple[str, dict]:
    row = _common(log)
    row["order_hash"] = _hex_to_bytes(topics[1])
    row["maker"] = _addr_from_topic(topics[2])
    row["taker"] = _addr_from_topic(topics[3])
    side, token_id, maker_amount, taker_amount, fee, builder, metadata = _decode_data(
        ["uint8", "uint256", "uint256", "uint256", "uint256", "bytes32", "bytes32"],
        log["data"],
    )
    row["side"] = int(side)
    row["token_id"] = token_id.to_bytes(32, "big")
    row["maker_amount_filled"] = _u256_str(maker_amount)
    row["taker_amount_filled"] = _u256_str(taker_amount)
    row["fee"] = _u256_str(fee)
    row["builder"] = builder  # bytes(32) from eth_abi
    row["metadata"] = metadata  # bytes(32) from eth_abi
    return ("order_filled", row)


def _decode_orders_matched_v2(log: dict, topics: list[str]) -> tuple[str, dict]:
    row = _common(log)
    row["taker_order_hash"] = _hex_to_bytes(topics[1])
    row["taker_order_maker"] = _addr_from_topic(topics[2])
    side, token_id, maker_amount, taker_amount = _decode_data(
        ["uint8", "uint256", "uint256", "uint256"], log["data"]
    )
    row["side"] = int(side)
    row["token_id"] = token_id.to_bytes(32, "big")
    row["maker_amount_filled"] = _u256_str(maker_amount)
    row["taker_amount_filled"] = _u256_str(taker_amount)
    return ("orders_matched", row)


def _decode_fee_charged_v2(log: dict, topics: list[str]) -> tuple[str, dict]:
    row = _common(log)
    row["receiver"] = _addr_from_topic(topics[1])
    (amount,) = _decode_data(["uint256"], log["data"])
    row["amount"] = _u256_str(amount)
    return ("fee_charged", row)


def _decode_order_preapproved(log: dict, topics: list[str]) -> tuple[str, dict]:
    row = _common(log)
    row["order_hash"] = _hex_to_bytes(topics[1])
    return ("order_preapproved", row)


def _decode_order_preapproval_invalidated(log: dict, topics: list[str]) -> tuple[str, dict]:
    row = _common(log)
    row["order_hash"] = _hex_to_bytes(topics[1])
    return ("order_preapproval_invalidated", row)


# --- NegRiskAdapter ------------------------------------------------------

def _decode_market_prepared(log: dict, topics: list[str]) -> tuple[str, dict]:
    row = _common(log)
    row["market_id"] = _hex_to_bytes(topics[1])
    row["oracle"] = _addr_from_topic(topics[2])
    fee_bips, data_bytes = _decode_data(["uint256", "bytes"], log["data"])
    row["fee_bips"] = int(fee_bips)
    row["data"] = "0x" + data_bytes.hex()
    return ("market_prepared", row)


def _decode_question_prepared(log: dict, topics: list[str]) -> tuple[str, dict]:
    row = _common(log)
    row["market_id"] = _hex_to_bytes(topics[1])
    row["question_id"] = _hex_to_bytes(topics[2])
    index_val, data_bytes = _decode_data(["uint256", "bytes"], log["data"])
    row["index_val"] = int(index_val)
    row["data"] = "0x" + data_bytes.hex()
    return ("question_prepared", row)


def _decode_outcome_reported(log: dict, topics: list[str]) -> tuple[str, dict]:
    row = _common(log)
    row["market_id"] = _hex_to_bytes(topics[1])
    row["question_id"] = _hex_to_bytes(topics[2])
    (outcome,) = _decode_data(["bool"], log["data"])
    row["outcome"] = 1 if outcome else 0
    return ("outcome_reported", row)


def _decode_position_split_nr(log: dict, topics: list[str]) -> tuple[str, dict]:
    row = _common(log)
    row["stakeholder"] = _addr_from_topic(topics[1])
    row["condition_id"] = _hex_to_bytes(topics[2])
    (amount,) = _decode_data(["uint256"], log["data"])
    row["amount"] = _u256_str(amount)
    return ("position_split", row)


def _decode_positions_merge_nr(log: dict, topics: list[str]) -> tuple[str, dict]:
    row = _common(log)
    row["stakeholder"] = _addr_from_topic(topics[1])
    row["condition_id"] = _hex_to_bytes(topics[2])
    (amount,) = _decode_data(["uint256"], log["data"])
    row["amount"] = _u256_str(amount)
    return ("positions_merge", row)


def _decode_positions_converted(log: dict, topics: list[str]) -> tuple[str, dict]:
    row = _common(log)
    row["stakeholder"] = _addr_from_topic(topics[1])
    row["market_id"] = _hex_to_bytes(topics[2])
    row["index_set"] = _u256_str(_parse_hex(topics[3]))
    (amount,) = _decode_data(["uint256"], log["data"])
    row["amount"] = _u256_str(amount)
    return ("positions_converted", row)


def _decode_payout_redemption_nr(log: dict, topics: list[str]) -> tuple[str, dict]:
    row = _common(log)
    row["redeemer"] = _addr_from_topic(topics[1])
    row["condition_id"] = _hex_to_bytes(topics[2])
    amounts, payout = _decode_data(["uint256[]", "uint256"], log["data"])
    row["amounts"] = json.dumps([str(x) for x in amounts])
    row["payout"] = _u256_str(payout)
    return ("payout_redemption", row)


# --- UmaCtfAdapter ------------------------------------------------------

def _decode_question_initialized(log: dict, topics: list[str]) -> tuple[str, dict]:
    row = _common(log)
    row["question_id"] = _hex_to_bytes(topics[1])
    row["request_timestamp"] = _parse_hex(topics[2])
    row["creator"] = _addr_from_topic(topics[3])
    ancillary_data, reward_token, reward, proposal_bond = _decode_data(
        ["bytes", "address", "uint256", "uint256"], log["data"]
    )
    row["ancillary_data"] = "0x" + ancillary_data.hex()
    row["reward_token"] = _addr_to_bytes(reward_token)
    row["reward"] = _u256_str(reward)
    row["proposal_bond"] = _u256_str(proposal_bond)
    return ("question_initialized", row)


def _decode_question_paused(log: dict, topics: list[str]) -> tuple[str, dict]:
    row = _common(log)
    row["question_id"] = _hex_to_bytes(topics[1])
    return ("question_paused", row)


def _decode_question_unpaused(log: dict, topics: list[str]) -> tuple[str, dict]:
    row = _common(log)
    row["question_id"] = _hex_to_bytes(topics[1])
    return ("question_unpaused", row)


def _decode_question_resolved(log: dict, topics: list[str]) -> tuple[str, dict]:
    row = _common(log)
    row["question_id"] = _hex_to_bytes(topics[1])
    # ``settledPrice`` is ``int256 indexed``; topic encodes it as a
    # 32-byte two's complement integer. Convert to a signed Python int
    # and store as a decimal string (possibly negative).
    raw = _parse_hex(topics[2])
    row["settled_price"] = str(raw - 2**256 if raw >= 2**255 else raw)
    (payouts,) = _decode_data(["uint256[]"], log["data"])
    row["payouts"] = json.dumps([str(x) for x in payouts])
    return ("question_resolved", row)


def _decode_question_reset(log: dict, topics: list[str]) -> tuple[str, dict]:
    row = _common(log)
    row["question_id"] = _hex_to_bytes(topics[1])
    return ("question_reset", row)


def _decode_question_flagged(log: dict, topics: list[str]) -> tuple[str, dict]:
    row = _common(log)
    row["question_id"] = _hex_to_bytes(topics[1])
    return ("question_flagged", row)


def _decode_question_unflagged(log: dict, topics: list[str]) -> tuple[str, dict]:
    row = _common(log)
    row["question_id"] = _hex_to_bytes(topics[1])
    return ("question_unflagged", row)


def _decode_question_emergency_resolved(log: dict, topics: list[str]) -> tuple[str, dict]:
    row = _common(log)
    row["question_id"] = _hex_to_bytes(topics[1])
    (payouts,) = _decode_data(["uint256[]"], log["data"])
    row["payouts"] = json.dumps([str(x) for x in payouts])
    return ("question_emergency_resolved", row)


# --- FeeModuleCTF / FeeModuleNegRisk ------------------------------------

def _decode_fee_refunded(log: dict, topics: list[str]) -> tuple[str, dict]:
    row = _common(log)
    row["order_hash"] = _hex_to_bytes(topics[1])
    row["receiver"] = _addr_from_topic(topics[2])
    # ``feeCharged`` is the third indexed parameter (uint256). Encoded
    # as a 32-byte topic, stored as BLOB(32).
    row["fee_charged"] = _hex_to_bytes(topics[3])
    token_id, refund = _decode_data(["uint256", "uint256"], log["data"])
    row["token_id"] = token_id.to_bytes(32, "big")
    row["refund"] = _u256_str(refund)
    return ("fee_refunded", row)


# ---------------------------------------------------------------------------
# Dispatch table
# ---------------------------------------------------------------------------
#
# One row per topic0 hash. Each row pins down everything needed to
# decode a log and route it to the right ``(contract, event)`` pair:
#
#   * ``decoder``: the function that turns the log into a row dict and
#     returns the canonical event name (snake_case, matches the v3
#     schema). For events shared across contracts, the decoder is the
#     same function for every legitimate emitter — the row contents are
#     identical, only the routing differs.
#   * ``event``: the snake_case event name produced by ``decoder``. This
#     is the same string the decoder returns; we duplicate it here so
#     the import-time consistency check is a pure data comparison
#     rather than requiring synthetic decoder invocations.
#   * ``contracts``: the set of v3 contract names that are legitimate
#     emitters of this event. A log from any other contract is dropped
#     (unknown / wrong-address).
#
# Where two events share a topic0 (v1 exchange ``OrderFilled``, v2
# exchange ``OrderFilled``, FeeModule ``FeeRefunded``), the ``contracts``
# set names every contract that emits it; routing uses
# ``_CONTRACT_BY_ADDRESS`` to pick the right one.

_DecoderFn = Callable[[dict, list[str]], tuple[str, dict]]


@dataclass(frozen=True)
class _DispatchEntry:
    decoder: _DecoderFn
    event: str
    contracts: frozenset[str]


# Keyed by topic0 (lowercase hex string with ``0x`` prefix), in the same
# order as ``TOPIC0_MAP``. Use ``_DISPATCH`` for decode, ``TOPIC0_MAP``
# for human-readable internal names, and ``WANTED_TOPICS`` /
# ``WANTED_ADDRESSES`` for RPC filter construction.
_DISPATCH: dict[str, _DispatchEntry] = {
    # ConditionalTokens
    "0xab3760c3bd2bb38b5bcf54dc79802ed67338b4cf29f3054ded67ed24661e4177": _DispatchEntry(
        _decode_condition_preparation, "condition_preparation",
        frozenset({"ConditionalTokens"}),
    ),
    "0xb44d84d3289691f71497564b85d4233648d9dbae8cbdbb4329f301c3a0185894": _DispatchEntry(
        _decode_condition_resolution, "condition_resolution",
        frozenset({"ConditionalTokens"}),
    ),
    "0x2e6bb91f8cbcda0c93623c54d0403a43514fabc40084ec96b6d5379a74786298": _DispatchEntry(
        _decode_position_split_ct, "position_split",
        frozenset({"ConditionalTokens"}),
    ),
    "0x6f13ca62553fcc2bcd2372180a43949c1e4cebba603901ede2f4e14f36b282ca": _DispatchEntry(
        _decode_positions_merge_ct, "positions_merge",
        frozenset({"ConditionalTokens"}),
    ),
    "0x2682012a4a4f1973119f1c9b90745d1bd91fa2bab387344f044cb3586864d18d": _DispatchEntry(
        _decode_payout_redemption_ct, "payout_redemption",
        frozenset({"ConditionalTokens"}),
    ),
    # CTFExchange / NegRiskCtfExchange (v1 exchanges)
    "0xd0a08e8c493f9c94f29311604c9de1b4e8c8d4c06bd0c789af57f2d65bfec0f6": _DispatchEntry(
        _decode_order_filled_v1, "order_filled",
        frozenset({"CTFExchange", "NegRiskCtfExchange"}),
    ),
    "0x63bf4d16b7fa898ef4c4b2b6d90fd201e9c56313b65638af6088d149d2ce956c": _DispatchEntry(
        _decode_orders_matched_v1, "orders_matched",
        frozenset({"CTFExchange", "NegRiskCtfExchange"}),
    ),
    "0xacffcc86834d0f1a64b0d5a675798deed6ff0bcfc2231edd3480e7288dba7ff4": _DispatchEntry(
        _decode_fee_charged_v1, "fee_charged",
        frozenset({"CTFExchange", "NegRiskCtfExchange"}),
    ),
    "0x5152abf959f6564662358c2e52b702259b78bac5ee7842a0f01937e670efcc7d": _DispatchEntry(
        _decode_order_cancelled, "order_cancelled",
        frozenset({"CTFExchange", "NegRiskCtfExchange"}),
    ),
    "0xbc9a2432e8aeb48327246cddd6e872ef452812b4243c04e6bfb786a2cd8faf0d": _DispatchEntry(
        _decode_token_registered, "token_registered",
        frozenset({"CTFExchange", "NegRiskCtfExchange"}),
    ),
    # CTFExchangeV2 / NegRiskCtfExchangeV2 (next-gen exchanges)
    "0xd543adfd945773f1a62f74f0ee55a5e3b9b1a28262980ba90b1a89f2ea84d8ee": _DispatchEntry(
        _decode_order_filled_v2, "order_filled",
        frozenset({"CTFExchangeV2", "NegRiskCtfExchangeV2"}),
    ),
    "0x174b3811690657c217184f89418266767c87e4805d09680c39fc9c031c0cab7c": _DispatchEntry(
        _decode_orders_matched_v2, "orders_matched",
        frozenset({"CTFExchangeV2", "NegRiskCtfExchangeV2"}),
    ),
    "0x55bb3cade9d43b798a4fe5ffdd05024b2d7870df53920673bfc7e68047cd0ab1": _DispatchEntry(
        _decode_fee_charged_v2, "fee_charged",
        frozenset({"CTFExchangeV2", "NegRiskCtfExchangeV2"}),
    ),
    "0xe92c22722d9c284034b6c9f5aaec018edb3e593c0e084900b6b9d390a1182a0b": _DispatchEntry(
        _decode_order_preapproved, "order_preapproved",
        frozenset({"CTFExchangeV2", "NegRiskCtfExchangeV2"}),
    ),
    "0xb766aa470f20b094f26a9a14ea5bf63a60af51703c15776e2e739b6a0428adf6": _DispatchEntry(
        _decode_order_preapproval_invalidated, "order_preapproval_invalidated",
        frozenset({"CTFExchangeV2", "NegRiskCtfExchangeV2"}),
    ),
    # NegRiskAdapter
    "0xf059ab16d1ca60e123eab60e3c02b68faf060347c701a5d14885a8e1def7b3a8": _DispatchEntry(
        _decode_market_prepared, "market_prepared",
        frozenset({"NegRiskAdapter"}),
    ),
    "0xaac410f87d423a922a7b226ac68f0c2eaf5bf6d15e644ac0758c7f96e2c253f7": _DispatchEntry(
        _decode_question_prepared, "question_prepared",
        frozenset({"NegRiskAdapter"}),
    ),
    "0x9e9fa7fd355160bd4cd3f22d4333519354beff1f5689bde87f2c5e63d8d484b2": _DispatchEntry(
        _decode_outcome_reported, "outcome_reported",
        frozenset({"NegRiskAdapter"}),
    ),
    "0xbbed930dbfb7907ae2d60ddf78345610214f26419a0128df39b6cc3d9e5df9b0": _DispatchEntry(
        _decode_position_split_nr, "position_split",
        frozenset({"NegRiskAdapter"}),
    ),
    "0xba33ac50d8894676597e6e35dc09cff59854708b642cd069d21eb9c7ca072a04": _DispatchEntry(
        _decode_positions_merge_nr, "positions_merge",
        frozenset({"NegRiskAdapter"}),
    ),
    "0xb03d19dddbc72a87e735ff0ea3b57bef133ebe44e1894284916a84044deb367e": _DispatchEntry(
        _decode_positions_converted, "positions_converted",
        frozenset({"NegRiskAdapter"}),
    ),
    "0x9140a6a270ef945260c03894b3c6b3b2695e9d5101feef0ff24fec960cfd3224": _DispatchEntry(
        _decode_payout_redemption_nr, "payout_redemption",
        frozenset({"NegRiskAdapter"}),
    ),
    # UmaCtfAdapter
    "0xeee0897acd6893adcaf2ba5158191b3601098ab6bece35c5d57874340b64c5b7": _DispatchEntry(
        _decode_question_initialized, "question_initialized",
        frozenset({"UmaCtfAdapter"}),
    ),
    "0x6ded7250a9d5f79aef5add44600fc20a74a0af6f4730baa4fc4ab87bf484b812": _DispatchEntry(
        _decode_question_paused, "question_paused",
        frozenset({"UmaCtfAdapter"}),
    ),
    "0x92d28918c5574e7fc0f4f948c39502682c81cfb4089b07b83f95b3264e5e5e06": _DispatchEntry(
        _decode_question_unpaused, "question_unpaused",
        frozenset({"UmaCtfAdapter"}),
    ),
    "0x566c3fbdd12dd86bb341787f6d531f79fd7ad4ce7e3ae2d15ac0ca1b601af9df": _DispatchEntry(
        _decode_question_resolved, "question_resolved",
        frozenset({"UmaCtfAdapter"}),
    ),
    "0x7981b5832932948db4e32a4a16a0f44b2ce7ff088574afb9364b313f70f82e8f": _DispatchEntry(
        _decode_question_reset, "question_reset",
        frozenset({"UmaCtfAdapter"}),
    ),
    "0x2435a0347185933b12027c6f394a5fd9c03646dba233e956f50658719dfc0b35": _DispatchEntry(
        _decode_question_flagged, "question_flagged",
        frozenset({"UmaCtfAdapter"}),
    ),
    "0x052435bc04fc49113a7bfd9198a92c0852ca622a621800f6da66d4b29b786c05": _DispatchEntry(
        _decode_question_unflagged, "question_unflagged",
        frozenset({"UmaCtfAdapter"}),
    ),
    "0x6edb5841a476c9c29c34a652d1a44f785fe71a6157a3da9a6a6a589a1bd2945a": _DispatchEntry(
        _decode_question_emergency_resolved, "question_emergency_resolved",
        frozenset({"UmaCtfAdapter"}),
    ),
    # FeeModuleCTF / FeeModuleNegRisk
    "0xb608d2bf25d8b4b744ba23ce2ea9802ea955e216c064a62f42152fbf98958d24": _DispatchEntry(
        _decode_fee_refunded, "fee_refunded",
        frozenset({"FeeModuleCTF", "FeeModuleNegRisk"}),
    ),
}


# Import-time consistency checks. These run once at module load and
# fail loudly if the dispatch table drifts from ``TOPIC0_MAP`` or from
# the v3 schema.
assert set(_DISPATCH.keys()) == set(TOPIC0_MAP.keys()), (
    "TOPIC0_MAP and _DISPATCH disagree on topic0 keys"
)

_produced_pairs: set[tuple[str, str]] = {
    (contract, entry.event)
    for entry in _DISPATCH.values()
    for contract in entry.contracts
}
_unknown_pairs = _produced_pairs - set(EVENT_COLUMNS.keys())
assert not _unknown_pairs, (
    f"decoder produces (contract, event) pairs not in v3 schema: "
    f"{sorted(_unknown_pairs)}"
)
_missing_pairs = set(EVENT_COLUMNS.keys()) - _produced_pairs
assert not _missing_pairs, (
    f"v3 schema pairs not produced by any decoder: {sorted(_missing_pairs)}"
)


# ---------------------------------------------------------------------------
# Public API
# ---------------------------------------------------------------------------

def decode_log(log: dict) -> tuple[str, str, dict] | None:
    """Decode one ``eth_getLogs`` entry into ``(contract, event, row)``.

    Returns ``None`` in three distinct cases:

      * ``topics[0]`` is not in ``TOPIC0_MAP`` (unknown event).
      * ``log["address"]`` is not in our contract registry, or does not
        match any legitimate emitter of this event.
      * The decoder raised an exception (malformed log data).

    The row dict's keys are exactly the columns named by
    ``tables.all_columns(contract, event)``.

    For strict-error behavior (propagate ABI-decode exceptions instead
    of returning ``None``), see :func:`decode_log_strict`.
    """
    return _decode_one(log, strict=False)


def decode_log_strict(log: dict) -> tuple[str, str, dict] | None:
    """Decode one ``eth_getLogs`` entry; propagate ABI-decode failures.

    Same as :func:`decode_log` except that an exception during ABI
    decoding propagates to the caller instead of being swallowed.
    Unknown events and unknown contracts still return ``None`` because
    they are routing decisions, not decode failures.
    """
    return _decode_one(log, strict=True)


def decode_batch(logs: list[dict]) -> list[tuple[str, str, dict]]:
    """Decode a batch of logs, skipping unknowns and decode failures.

    Returns the decoded triples in input order. Use
    :func:`decode_batch_strict` if an ABI-decode failure on any single
    log should abort the whole batch.
    """
    out: list[tuple[str, str, dict]] = []
    for log in logs:
        triple = decode_log(log)
        if triple is not None:
            out.append(triple)
    return out


def decode_batch_strict(logs: list[dict]) -> list[tuple[str, str, dict]]:
    """Decode a batch of logs; propagate ABI-decode failures.

    Same as :func:`decode_batch` except that an ABI-decode exception
    raised by any single log aborts the entire batch. Unknown events
    and unknown contracts are still silently skipped — they are
    routing decisions, not decode failures.
    """
    out: list[tuple[str, str, dict]] = []
    for log in logs:
        triple = decode_log_strict(log)
        if triple is not None:
            out.append(triple)
    return out


def _decode_one(log: dict, *, strict: bool) -> tuple[str, str, dict] | None:
    topics = log.get("topics", [])
    if not topics:
        return None
    entry = _DISPATCH.get(topics[0].lower())
    if entry is None:
        return None
    contract = _CONTRACT_BY_ADDRESS.get(log.get("address", "").lower())
    if contract is None or contract not in entry.contracts:
        # Either the log is from a contract we do not track, or the
        # (event, contract) pair is impossible (e.g. someone deployed a
        # contract with our event signature at an unrelated address).
        # Either way, skip — but never raise.
        return None
    if strict:
        event, row = entry.decoder(log, topics)
    else:
        try:
            event, row = entry.decoder(log, topics)
        except Exception:
            return None
    return contract, event, row
