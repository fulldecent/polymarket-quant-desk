"""
Canonical v3 schema metadata as Python data.

This module is the single source of truth for every (contract, event) pair
in the v3 dataset, the canonical column order within each table, and the
contract deployment blocks. The on-disk ``schema.sql`` is the executable
form of the same information for DuckDB; this module is the form used by
Python code that needs to enumerate tables, validate inputs, or build
SQL strings dynamically.

If a new event is added in v4, this is one of two places (along with
``schema.sql``) that must change.

Conventions:
    contract_name   — the directory name on disk, e.g. "CTFExchange",
                      "NegRiskCtfExchangeV2". CamelCase.
    event_name      — the Solidity event name in snake_case, e.g.
                      "order_filled", "condition_preparation".
    table_name      — the DuckDB hot-table name, formed as
                      ``snake_case(contract_name) + "__" + event_name``,
                      e.g. "ctf_exchange__order_filled".

Every event table's columns follow this order:
    1. block_number       UINTEGER
    2. transaction_index  UINTEGER
    3. transaction_hash   BLOB
    4. log_index          UINTEGER
    5..N. event-specific columns in the dictionary order

The first four are exposed as ``COMMON_COLUMNS`` so callers can construct
``SELECT`` and ``INSERT`` column lists without re-stating them.

SCRAPE_START_BLOCK is the earliest block from which any partition is
materialized. Partitions whose end block falls below the contract's
deployment block are never written; partitions at or above it are
expected.
"""

from __future__ import annotations

from dataclasses import dataclass




# ---------------------------------------------------------------------------
# Global constants
# ---------------------------------------------------------------------------

SCRAPE_START_BLOCK: int = 33_605_403
PARTITION_SIZE_10K: int = 10_000
PARTITION_SIZE_1M: int = 1_000_000

# Common column order for every event table.
COMMON_COLUMNS: tuple[str, ...] = (
    "block_number",
    "transaction_index",
    "transaction_hash",
    "log_index",
)


# ---------------------------------------------------------------------------
# Contract registry
# ---------------------------------------------------------------------------

@dataclass(frozen=True)
class Contract:
    """One contract emitting events into the v3 dataset."""

    name: str
    """Directory name in the cold tier (e.g. ``CTFExchange``)."""

    address: str
    """Hex-encoded 20-byte EVM address with the ``0x`` prefix."""

    deployment_block: int
    """Earliest block at which any event from this contract may appear."""


CONTRACTS: tuple[Contract, ...] = (
    Contract("ConditionalTokens",     "0x4D97DCd97eC945f40cF65F87097ACe5EA0476045", 33_605_403),
    Contract("CTFExchange",           "0x4bFb41d5B3570DeFd03C39a9A4D8dE6Bd8B8982E", 33_605_743),
    Contract("NegRiskCtfExchange",    "0xC5d563A36AE78145C45a50134d48A1215220f80a", 45_169_177),
    Contract("CTFExchangeV2",         "0xE111180000d2663C0091e4f400237545B87B996B", 84_902_353),
    Contract("NegRiskCtfExchangeV2",  "0xe2222d279d744050d28e00520010520000310F59", 85_058_176),
    Contract("NegRiskAdapter",        "0xd91E80cF2E7be2e162c6513ceD06f1dD0dA35296", 45_169_177),
    Contract("UmaCtfAdapter",         "0x157Ce2d672854c848c9b79C49a8Cc6cc89176a49", 33_605_574),
    Contract("FeeModuleCTF",          "0xE3f18aCc55091e2c48d883fc8C8413319d4Ab7b0", 75_253_526),
    Contract("FeeModuleNegRisk",      "0xB768891e3130F6dF18214Ac804d4DB76c2C37730", 75_253_721),
)

CONTRACTS_BY_NAME: dict[str, Contract] = {c.name: c for c in CONTRACTS}


# ---------------------------------------------------------------------------
# Per-(contract, event) column orderings
# ---------------------------------------------------------------------------
# Each value is the tuple of event-specific column names (NOT including
# the four common columns) in dictionary order.

_EVENT_COLUMNS_RAW: dict[tuple[str, str], tuple[str, ...]] = {
    # ConditionalTokens
    ("ConditionalTokens", "condition_preparation"): (
        "condition_id", "oracle", "question_id", "outcome_slot_count",
    ),
    ("ConditionalTokens", "condition_resolution"): (
        "condition_id", "oracle", "question_id", "outcome_slot_count",
        "payout_numerators",
    ),
    ("ConditionalTokens", "position_split"): (
        "stakeholder", "collateral_token", "parent_collection_id",
        "condition_id", "partition", "amount",
    ),
    ("ConditionalTokens", "positions_merge"): (
        "stakeholder", "collateral_token", "parent_collection_id",
        "condition_id", "partition", "amount",
    ),
    ("ConditionalTokens", "payout_redemption"): (
        "redeemer", "collateral_token", "parent_collection_id",
        "condition_id", "index_sets", "payout",
    ),
    # First-generation exchanges (CTFExchange + NegRiskCtfExchange share a
    # schema and emit the same five events).
    **{
        (contract, event): cols
        for contract in ("CTFExchange", "NegRiskCtfExchange")
        for event, cols in {
            "order_filled": (
                "order_hash", "maker", "taker",
                "maker_asset_id", "taker_asset_id",
                "maker_amount_filled", "taker_amount_filled", "fee",
            ),
            "orders_matched": (
                "taker_order_hash", "taker_order_maker",
                "maker_asset_id", "taker_asset_id",
                "maker_amount_filled", "taker_amount_filled",
            ),
            "fee_charged": ("receiver", "token_id", "amount"),
            "token_registered": ("token0", "token1", "condition_id"),
            "order_cancelled": ("order_hash",),
        }.items()
    },
    # Next-generation exchanges (CTFExchangeV2 + NegRiskCtfExchangeV2 share
    # a schema, distinct from the first-generation exchanges above: order
    # fills carry ``side`` + ``token_id`` instead of maker/taker asset IDs,
    # fee_charged has no token_id, no token_registered / order_cancelled,
    # and a pair of pre-approval events is added). "V2" here is part of the
    # Solidity contract name; it is unrelated to this dataset's schema
    # versioning.
    **{
        (contract, event): cols
        for contract in ("CTFExchangeV2", "NegRiskCtfExchangeV2")
        for event, cols in {
            "order_filled": (
                "order_hash", "maker", "taker", "side", "token_id",
                "maker_amount_filled", "taker_amount_filled", "fee",
                "builder", "metadata",
            ),
            "orders_matched": (
                "taker_order_hash", "taker_order_maker", "side", "token_id",
                "maker_amount_filled", "taker_amount_filled",
            ),
            "fee_charged": ("receiver", "amount"),
            "order_preapproved": ("order_hash",),
            "order_preapproval_invalidated": ("order_hash",),
        }.items()
    },
    # Fee modules
    **{
        (contract, "fee_refunded"): (
            "order_hash", "receiver", "token_id", "refund", "fee_charged",
        )
        for contract in ("FeeModuleCTF", "FeeModuleNegRisk")
    },
    # NegRiskAdapter
    ("NegRiskAdapter", "market_prepared"): (
        "market_id", "oracle", "fee_bips", "data",
    ),
    ("NegRiskAdapter", "question_prepared"): (
        "market_id", "question_id", "index_val", "data",
    ),
    ("NegRiskAdapter", "outcome_reported"): (
        "market_id", "question_id", "outcome",
    ),
    ("NegRiskAdapter", "position_split"): (
        "stakeholder", "condition_id", "amount",
    ),
    ("NegRiskAdapter", "positions_merge"): (
        "stakeholder", "condition_id", "amount",
    ),
    ("NegRiskAdapter", "positions_converted"): (
        "stakeholder", "market_id", "index_set", "amount",
    ),
    ("NegRiskAdapter", "payout_redemption"): (
        "redeemer", "condition_id", "amounts", "payout",
    ),
    # UmaCtfAdapter
    ("UmaCtfAdapter", "question_initialized"): (
        "question_id", "request_timestamp", "creator",
        "ancillary_data", "reward_token", "reward", "proposal_bond",
    ),
    ("UmaCtfAdapter", "question_resolved"): (
        "question_id", "settled_price", "payouts",
    ),
    ("UmaCtfAdapter", "question_emergency_resolved"): (
        "question_id", "payouts",
    ),
    **{
        ("UmaCtfAdapter", event): ("question_id",)
        for event in (
            "question_reset", "question_flagged", "question_unflagged",
            "question_paused", "question_unpaused",
        )
    },
}


# Public read-only mapping.
EVENT_COLUMNS: dict[tuple[str, str], tuple[str, ...]] = dict(_EVENT_COLUMNS_RAW)


# ---------------------------------------------------------------------------
# Table-name conversion
# ---------------------------------------------------------------------------

def _camel_to_snake(camel: str) -> str:
    """Convert ``CamelCase`` → ``camel_case`` for table-name composition.

    ``CTFExchange`` → ``ctf_exchange``
    ``NegRiskCtfExchangeV2`` → ``neg_risk_ctf_exchange_v2``
    """
    out: list[str] = []
    for i, ch in enumerate(camel):
        if ch.isupper() and i > 0:
            prev = camel[i - 1]
            nxt = camel[i + 1] if i + 1 < len(camel) else ""
            # Insert underscore before an upper-case letter unless it is
            # mid-acronym (previous char upper AND next char upper or end).
            if not prev.isupper() or (nxt and nxt.islower()):
                out.append("_")
        out.append(ch.lower())
    return "".join(out)


def table_name(contract: str, event: str) -> str:
    """Return the hot DuckDB table name for one ``(contract, event)`` pair.

    Raises ``KeyError`` if the pair is not in the v3 schema.
    """
    if (contract, event) not in EVENT_COLUMNS:
        raise KeyError(f"unknown (contract, event): ({contract!r}, {event!r})")
    return f"{_camel_to_snake(contract)}__{event}"


def all_table_names() -> list[str]:
    """Return every event table name, in ascending lexicographic order."""
    return sorted(table_name(c, e) for c, e in EVENT_COLUMNS)


def all_targets() -> list[tuple[str, str]]:
    """Return every ``(contract, event)`` pair, in deterministic order."""
    return sorted(EVENT_COLUMNS)


def all_columns(contract: str, event: str) -> tuple[str, ...]:
    """Return the full canonical column order for one event table.

    The first four entries are always ``COMMON_COLUMNS`` in fixed order,
    followed by the event-specific columns from ``EVENT_COLUMNS``.
    """
    if (contract, event) not in EVENT_COLUMNS:
        raise KeyError(f"unknown (contract, event): ({contract!r}, {event!r})")
    return COMMON_COLUMNS + EVENT_COLUMNS[(contract, event)]


def targets_active_at(end_block: int) -> list[tuple[str, str]]:
    """Return every ``(contract, event)`` whose contract was deployed at or
    before ``end_block``.

    Use this to determine which Parquet files must be written for one 10K
    partition: the v3 frontier contract requires all such files to land
    together.
    """
    return [
        (c, e) for c, e in all_targets()
        if CONTRACTS_BY_NAME[c].deployment_block <= end_block
    ]
