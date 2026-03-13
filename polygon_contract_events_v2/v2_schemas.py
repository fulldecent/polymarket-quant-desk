"""
Table schemas and conversion functions for polygon_contract_events_v2.

The scraper stores events in SQLite as hex/decimal strings. The export step
converts these to typed Parquet columns using the converters defined here.

Directory layout:
  <root>/<contract>/<event>/1M=<N>/10K=<N>/data.parquet

Type mapping:
  - 32-byte identifiers → BLOB(32)
  - 20-byte addresses   → BLOB(20)
  - uint256 amounts     → VARCHAR (decimal string, lossless)
  - Small integers      → UINTEGER (uint32)
  - Arrays of uint256   → VARCHAR (JSON array of decimal strings)
"""

import pyarrow as pa

START_BLOCK = 33_605_403

_B32 = pa.binary(32)
_B20 = pa.binary(20)
_U32 = pa.uint32()
_STR = pa.string()

_10K = 10_000
_1M = 1_000_000

# ---------------------------------------------------------------------------
# Contracts
# ---------------------------------------------------------------------------

CONTRACTS = {
    "ConditionalTokens":   "0x4D97DCd97eC945f40cF65F87097ACe5EA0476045",  # block 4,023,686
    "CTFExchange":         "0x4bFb41d5B3570DeFd03C39a9A4D8dE6Bd8B8982E",  # block 33,605,403
    "NegRiskCtfExchange":  "0xC5d563A36AE78145C45a50134d48A1215220f80a",  # block 33,605,403
    "NegRiskAdapter":      "0xd91E80cF2E7be2e162c6513ceD06f1dD0dA35296",  # block 33,605,403
    "UmaCtfAdapter":       "0x157Ce2d672854c848c9b79C49a8Cc6cc89176a49",  # block 33,605,403
    "FeeModuleCTF":        "0xE3f18aCc55091e2c48d883fc8C8413319d4Ab7b0",  # block 75,253,526
    "FeeModuleNegRisk":    "0xB768891e3130F6dF18214Ac804d4DB76c2C37730",  # block 75,253,721
}

# Lowercased 20-byte binary for each contract (used for filtering parquet data)
CONTRACT_ADDRESS_BYTES = {
    name: bytes.fromhex(addr[2:].lower().zfill(40))
    for name, addr in CONTRACTS.items()
}

# Reverse lookup: 20-byte address → contract name
ADDRESS_TO_CONTRACT = {v: k for k, v in CONTRACT_ADDRESS_BYTES.items()}

# ---------------------------------------------------------------------------
# Conversion functions (SQLite string → typed Parquet)
# ---------------------------------------------------------------------------


def hex_to_b32(val):
    """Convert 0x-prefixed hex string to exactly 32 bytes."""
    if val is None:
        return None
    h = val[2:] if isinstance(val, str) and val.startswith("0x") else str(val)
    return bytes.fromhex(h.zfill(64))


def hex_to_b20(val):
    """Convert 0x-prefixed hex address to exactly 20 bytes."""
    if val is None:
        return None
    h = val[2:] if isinstance(val, str) and val.startswith("0x") else str(val)
    return bytes.fromhex(h.zfill(40))


def dec_to_b32(val):
    """Convert decimal string uint256 to 32 bytes big-endian."""
    if val is None:
        return None
    return int(val).to_bytes(32, "big")


def to_u32(val):
    """Cast to uint32."""
    if val is None:
        return None
    return int(val) & 0xFFFFFFFF


def keep(val):
    """Pass through unchanged."""
    return val


# ---------------------------------------------------------------------------
# Shared common columns (every v2 table starts with these)
# ---------------------------------------------------------------------------
#
# Each entry is (column_name, pyarrow_type, converter_function).
# The converter transforms a SQLite value to a typed Parquet value.
# contract_address is NOT included — it's implicit from the directory.

COMMON_COLUMNS = [
    ("block_number",      _U32, to_u32),
    ("transaction_index", _U32, to_u32),
    ("transaction_hash",  _B32, hex_to_b32),
    ("log_index",         _U32, to_u32),
]

# ---------------------------------------------------------------------------
# Per-contract, per-event schemas
# ---------------------------------------------------------------------------
#
# Organized as _CONTRACT_EVENTS[contract_name][event_name].
# Values are lists of (column_name, pyarrow_type, converter).
# Common columns are prepended automatically; these lists define only
# the event-specific columns.
#
# Events shared by CTFExchange and NegRiskCtfExchange have identical
# schemas — defined once as _EXCHANGE_EVENTS and referenced by both.

_FEE_MODULE_EVENTS = {
    "fee_refunded": [
        ("order_hash",  _B32, hex_to_b32),
        ("receiver",    _B20, hex_to_b20),
        ("token_id",    _B32, dec_to_b32),
        ("refund",      _STR, keep),
        ("fee_charged", _STR, keep),
    ],
}

_EXCHANGE_EVENTS = {
    "order_filled": [
        ("order_hash",          _B32, hex_to_b32),
        ("maker",               _B20, hex_to_b20),
        ("taker",               _B20, hex_to_b20),
        ("maker_asset_id",      _B32, dec_to_b32),
        ("taker_asset_id",      _B32, dec_to_b32),
        ("maker_amount_filled", _STR, keep),
        ("taker_amount_filled", _STR, keep),
        ("fee",                 _STR, keep),
    ],
    "orders_matched": [
        ("taker_order_hash",    _B32, hex_to_b32),
        ("taker_order_maker",   _B20, hex_to_b20),
        ("maker_asset_id",      _B32, dec_to_b32),
        ("taker_asset_id",      _B32, dec_to_b32),
        ("maker_amount_filled", _STR, keep),
        ("taker_amount_filled", _STR, keep),
    ],
    "fee_charged": [
        ("receiver", _B20, hex_to_b20),
        ("token_id", _B32, dec_to_b32),
        ("amount",   _STR, keep),
    ],
    "token_registered": [
        ("token0",       _B32, dec_to_b32),
        ("token1",       _B32, dec_to_b32),
        ("condition_id", _B32, hex_to_b32),
    ],
    "order_cancelled": [
        ("order_hash", _B32, hex_to_b32),
    ],
}

_CONTRACT_EVENTS = {
    "ConditionalTokens": {
        "condition_preparation": [
            ("condition_id",       _B32, hex_to_b32),
            ("oracle",             _B20, hex_to_b20),
            ("question_id",        _B32, hex_to_b32),
            ("outcome_slot_count", _U32, to_u32),
        ],
        "condition_resolution": [
            ("condition_id",       _B32, hex_to_b32),
            ("oracle",             _B20, hex_to_b20),
            ("question_id",        _B32, hex_to_b32),
            ("outcome_slot_count", _U32, to_u32),
            ("payout_numerators",  _STR, keep),
        ],
        "position_split": [
            ("stakeholder",          _B20, hex_to_b20),
            ("collateral_token",     _B20, hex_to_b20),
            ("parent_collection_id", _B32, hex_to_b32),
            ("condition_id",         _B32, hex_to_b32),
            ("partition",            _STR, keep),
            ("amount",               _STR, keep),
        ],
        "positions_merge": [
            ("stakeholder",          _B20, hex_to_b20),
            ("collateral_token",     _B20, hex_to_b20),
            ("parent_collection_id", _B32, hex_to_b32),
            ("condition_id",         _B32, hex_to_b32),
            ("partition",            _STR, keep),
            ("amount",               _STR, keep),
        ],
        "payout_redemption": [
            ("redeemer",             _B20, hex_to_b20),
            ("collateral_token",     _B20, hex_to_b20),
            ("parent_collection_id", _B32, hex_to_b32),
            ("condition_id",         _B32, hex_to_b32),
            ("index_sets",           _STR, keep),
            ("payout",               _STR, keep),
        ],
    },

    "CTFExchange":        _EXCHANGE_EVENTS,
    "NegRiskCtfExchange": _EXCHANGE_EVENTS,

    "FeeModuleCTF":       _FEE_MODULE_EVENTS,
    "FeeModuleNegRisk":   _FEE_MODULE_EVENTS,

    "NegRiskAdapter": {
        "market_prepared": [
            ("market_id", _B32, hex_to_b32),
            ("oracle",    _B20, hex_to_b20),
            ("fee_bips",  _U32, to_u32),
            ("data",      _STR, keep),
        ],
        "question_prepared": [
            ("market_id",   _B32, hex_to_b32),
            ("question_id", _B32, hex_to_b32),
            ("index_val",   _U32, to_u32),
            ("data",        _STR, keep),
        ],
        "outcome_reported": [
            ("market_id",   _B32, hex_to_b32),
            ("question_id", _B32, hex_to_b32),
            ("outcome",     _U32, to_u32),
        ],
        "position_split": [
            ("stakeholder",  _B20, hex_to_b20),
            ("condition_id", _B32, hex_to_b32),
            ("amount",       _STR, keep),
        ],
        "positions_merge": [
            ("stakeholder",  _B20, hex_to_b20),
            ("condition_id", _B32, hex_to_b32),
            ("amount",       _STR, keep),
        ],
        "positions_converted": [
            ("stakeholder", _B20, hex_to_b20),
            ("market_id",   _B32, hex_to_b32),
            ("index_set",   _STR, keep),
            ("amount",      _STR, keep),
        ],
        "payout_redemption": [
            ("redeemer",     _B20, hex_to_b20),
            ("condition_id", _B32, hex_to_b32),
            ("amounts",      _STR, keep),
            ("payout",       _STR, keep),
        ],
    },

    "UmaCtfAdapter": {
        "question_initialized": [
            ("question_id",       _B32, hex_to_b32),
            ("request_timestamp", _U32, to_u32),
            ("creator",           _B20, hex_to_b20),
            ("ancillary_data",    _STR, keep),
            ("reward_token",      _B20, hex_to_b20),
            ("reward",            _STR, keep),
            ("proposal_bond",     _STR, keep),
        ],
        "question_resolved": [
            ("question_id",   _B32, hex_to_b32),
            ("settled_price", _STR, keep),
            ("payouts",       _STR, keep),
        ],
        "question_reset": [
            ("question_id", _B32, hex_to_b32),
        ],
        "question_flagged": [
            ("question_id", _B32, hex_to_b32),
        ],
        "question_emergency_resolved": [
            ("question_id", _B32, hex_to_b32),
            ("payouts",     _STR, keep),
        ],
    },
}

# ---------------------------------------------------------------------------
# SQLite table name → list of (contract_name, event_name) for export
# ---------------------------------------------------------------------------
#
# The scraper writes events into SQLite tables using the old flat names.
# Exchange events (order_filled, orders_matched, fee_charged,
# token_registered, order_cancelled) go to a single SQLite table but must
# be split by contract_address when exporting to v2.
#
# Each value is a list of (contract, event) tuples. Single-contract tables
# have one entry; exchange tables have two (split by contract_address at
# export time).

SQLITE_TO_V2 = {
    # ConditionalTokens (single-contract)
    "condition_preparation":      [("ConditionalTokens", "condition_preparation")],
    "condition_resolution":       [("ConditionalTokens", "condition_resolution")],
    "position_split":             [("ConditionalTokens", "position_split")],
    "positions_merge":            [("ConditionalTokens", "positions_merge")],
    "payout_redemption":          [("ConditionalTokens", "payout_redemption")],

    # Exchange events (mixed — must split by contract_address)
    "order_filled":               [("CTFExchange", "order_filled"),
                                   ("NegRiskCtfExchange", "order_filled")],
    "orders_matched":             [("CTFExchange", "orders_matched"),
                                   ("NegRiskCtfExchange", "orders_matched")],
    "fee_charged":                [("CTFExchange", "fee_charged"),
                                   ("NegRiskCtfExchange", "fee_charged")],
    "token_registered":           [("CTFExchange", "token_registered"),
                                   ("NegRiskCtfExchange", "token_registered")],
    "order_cancelled":            [("CTFExchange", "order_cancelled"),
                                   ("NegRiskCtfExchange", "order_cancelled")],

    # NegRiskAdapter (single-contract, old names → new event names)
    "market_prepared":            [("NegRiskAdapter", "market_prepared")],
    "question_prepared":          [("NegRiskAdapter", "question_prepared")],
    "outcome_reported":           [("NegRiskAdapter", "outcome_reported")],
    "neg_risk_position_split":    [("NegRiskAdapter", "position_split")],
    "neg_risk_positions_merge":   [("NegRiskAdapter", "positions_merge")],
    "positions_converted":        [("NegRiskAdapter", "positions_converted")],
    "neg_risk_payout_redemption": [("NegRiskAdapter", "payout_redemption")],

    # Fee module events (mixed — must split by contract_address)
    "fee_refunded":               [("FeeModuleCTF", "fee_refunded"),
                                   ("FeeModuleNegRisk", "fee_refunded")],

    # UmaCtfAdapter (single-contract)
    "question_initialized":       [("UmaCtfAdapter", "question_initialized")],
    "question_resolved":          [("UmaCtfAdapter", "question_resolved")],
    "question_reset":             [("UmaCtfAdapter", "question_reset")],
    "question_flagged":           [("UmaCtfAdapter", "question_flagged")],
    "question_emergency_resolved":[("UmaCtfAdapter", "question_emergency_resolved")],
}


def is_mixed_sqlite_table(sqlite_table):
    """Return True if the SQLite table has events from multiple contracts."""
    targets = SQLITE_TO_V2.get(sqlite_table, [])
    return len(targets) > 1


# ---------------------------------------------------------------------------
# Public API
# ---------------------------------------------------------------------------


def get_v2_schema(contract, event):
    """Return the full v2 column spec: list of (name, pa_type, converter)."""
    contract_events = _CONTRACT_EVENTS.get(contract)
    if contract_events is None:
        raise ValueError(f"Unknown contract: {contract}")
    event_cols = contract_events.get(event)
    if event_cols is None:
        raise ValueError(f"Unknown event '{event}' for contract '{contract}'")
    return COMMON_COLUMNS + event_cols


def get_arrow_schema(contract, event):
    """Return a PyArrow schema for the v2 table."""
    cols = get_v2_schema(contract, event)
    return pa.schema([pa.field(name, typ) for name, typ, _ in cols])


def get_v2_column_names(contract, event):
    """Return the ordered list of v2 column names."""
    return [name for name, _, _ in get_v2_schema(contract, event)]


def all_events():
    """Yield all (contract_name, event_name) pairs."""
    for contract, events in _CONTRACT_EVENTS.items():
        for event in events:
            yield contract, event


def all_sqlite_table_names():
    """Return all SQLite table names (for the exporter/scraper)."""
    return list(SQLITE_TO_V2.keys())
