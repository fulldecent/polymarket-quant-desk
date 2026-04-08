-- Polymarket event log database schema
--
-- Design decisions:
--
-- 1. COMPLETE BLOCKS ONLY: events are only stored for blocks where we are
--    confident the entire block has been loaded for that contract. The
--    scraper achieves this by discarding events from the highest block
--    number in each API response (it may be partial). The next fetch
--    resumes from that block. This means every block in the database
--    has all of its events present — no partial blocks.
--
-- 2. INLINE ETL: raw hex data from the API is decoded in memory during
--    the scrape and written directly into typed tables. There is no raw
--    staging table. Resume state is tracked separately in scrape_progress.
--
-- 3. ONE TABLE PER EVENT TYPE: each Solidity event signature maps to its
--    own table with native column types. Events not useful for trading
--    analysis (TransferSingle, TransferBatch, admin events) are filtered
--    out at the RPC level using topic filters.
--
-- 4. MINIMAL TRANSACTION METADATA: every table carries block_number,
--    transaction_hash, transaction_index, log_index, and
--    contract_address. Columns not available from eth_getLogs
--    (block_hash, timestamp, gas_price, gas_used) are omitted.
--
-- 5. UINT256 AS TEXT: Solidity uint256 values can exceed SQLite's int64
--    range. These are stored as decimal strings. Smaller values known
--    to fit in int64 (e.g., outcome_slot_count, fee_bips) use INTEGER.
--
-- 6. ARRAYS AS JSON: Solidity dynamic arrays (uint[], uint256[]) are
--    stored as JSON arrays of decimal strings.
--
-- All hex values stored with 0x prefix as TEXT
-- uint256 values that may exceed int64 stored as TEXT (decimal string)
-- Arrays stored as JSON text

-- =====================================================================
-- Progress tracking (for resumable scraping)
-- =====================================================================

CREATE TABLE IF NOT EXISTS scrape_progress (
    contract_name       TEXT    PRIMARY KEY,
    last_complete_block INTEGER NOT NULL DEFAULT 0
);

-- =====================================================================
-- Shared columns in every event table:
--   block_number       INTEGER   — Polygon block number
--   transaction_hash   TEXT      — tx hash (0x...)
--   transaction_index  INTEGER   — position in block
--   log_index          INTEGER   — position in tx receipt
--   contract_address   TEXT      — emitting contract (0x...)
-- =====================================================================

-- =====================================================================
-- ConditionalTokens (0x4D97DCd97eC945f40cF65F87097ACe5EA0476045)
-- =====================================================================

CREATE TABLE IF NOT EXISTS condition_preparation (
    block_number       INTEGER NOT NULL,
    transaction_hash   TEXT    NOT NULL,
    transaction_index  INTEGER NOT NULL,
    log_index          INTEGER NOT NULL,
    contract_address   TEXT    NOT NULL,

    condition_id       TEXT    NOT NULL,  -- bytes32, indexed
    oracle             TEXT    NOT NULL,  -- address, indexed
    question_id        TEXT    NOT NULL,  -- bytes32, indexed
    outcome_slot_count INTEGER NOT NULL,  -- uint

    UNIQUE(block_number, log_index)
);

CREATE TABLE IF NOT EXISTS condition_resolution (
    block_number       INTEGER NOT NULL,
    transaction_hash   TEXT    NOT NULL,
    transaction_index  INTEGER NOT NULL,
    log_index          INTEGER NOT NULL,
    contract_address   TEXT    NOT NULL,

    condition_id       TEXT    NOT NULL,  -- bytes32, indexed
    oracle             TEXT    NOT NULL,  -- address, indexed
    question_id        TEXT    NOT NULL,  -- bytes32, indexed
    outcome_slot_count INTEGER NOT NULL,  -- uint
    payout_numerators  TEXT    NOT NULL,  -- uint[] as JSON array

    UNIQUE(block_number, log_index)
);

CREATE TABLE IF NOT EXISTS position_split (
    block_number       INTEGER NOT NULL,
    transaction_hash   TEXT    NOT NULL,
    transaction_index  INTEGER NOT NULL,
    log_index          INTEGER NOT NULL,
    contract_address   TEXT    NOT NULL,

    stakeholder        TEXT    NOT NULL,  -- address, indexed
    collateral_token   TEXT    NOT NULL,  -- address (from data)
    parent_collection_id TEXT  NOT NULL,  -- bytes32, indexed
    condition_id       TEXT    NOT NULL,  -- bytes32, indexed
    partition          TEXT    NOT NULL,  -- uint[] as JSON array
    amount             TEXT    NOT NULL,  -- uint256 as decimal string

    UNIQUE(block_number, log_index)
);

CREATE TABLE IF NOT EXISTS positions_merge (
    block_number       INTEGER NOT NULL,
    transaction_hash   TEXT    NOT NULL,
    transaction_index  INTEGER NOT NULL,
    log_index          INTEGER NOT NULL,
    contract_address   TEXT    NOT NULL,

    stakeholder        TEXT    NOT NULL,  -- address, indexed
    collateral_token   TEXT    NOT NULL,  -- address (from data)
    parent_collection_id TEXT  NOT NULL,  -- bytes32, indexed
    condition_id       TEXT    NOT NULL,  -- bytes32, indexed
    partition          TEXT    NOT NULL,  -- uint[] as JSON array
    amount             TEXT    NOT NULL,  -- uint256 as decimal string

    UNIQUE(block_number, log_index)
);

CREATE TABLE IF NOT EXISTS payout_redemption (
    block_number       INTEGER NOT NULL,
    transaction_hash   TEXT    NOT NULL,
    transaction_index  INTEGER NOT NULL,
    log_index          INTEGER NOT NULL,
    contract_address   TEXT    NOT NULL,

    redeemer           TEXT    NOT NULL,  -- address, indexed
    collateral_token   TEXT    NOT NULL,  -- address, indexed
    parent_collection_id TEXT  NOT NULL,  -- bytes32, indexed
    condition_id       TEXT    NOT NULL,  -- bytes32 (from data)
    index_sets         TEXT    NOT NULL,  -- uint[] as JSON array
    payout             TEXT    NOT NULL,  -- uint256 as decimal string

    UNIQUE(block_number, log_index)
);

-- =====================================================================
-- CTFExchange (0x4bFb41d5B3570DeFd03C39a9A4D8dE6Bd8B8982E)
-- NegRiskCtfExchange (0xC5d563A36AE78145C45a50134d48A1215220f80a)
--   (same event signatures, distinguished by contract_address)
-- =====================================================================

CREATE TABLE IF NOT EXISTS order_filled (
    block_number       INTEGER NOT NULL,
    transaction_hash   TEXT    NOT NULL,
    transaction_index  INTEGER NOT NULL,
    log_index          INTEGER NOT NULL,
    contract_address   TEXT    NOT NULL,

    order_hash         TEXT    NOT NULL,  -- bytes32, indexed
    maker              TEXT    NOT NULL,  -- address, indexed
    taker              TEXT    NOT NULL,  -- address, indexed
    maker_asset_id     TEXT    NOT NULL,  -- uint256 as decimal string
    taker_asset_id     TEXT    NOT NULL,  -- uint256 as decimal string
    maker_amount_filled TEXT   NOT NULL,  -- uint256 as decimal string
    taker_amount_filled TEXT   NOT NULL,  -- uint256 as decimal string
    fee                TEXT    NOT NULL,  -- uint256 as decimal string

    UNIQUE(block_number, log_index)
);

CREATE TABLE IF NOT EXISTS orders_matched (
    block_number       INTEGER NOT NULL,
    transaction_hash   TEXT    NOT NULL,
    transaction_index  INTEGER NOT NULL,
    log_index          INTEGER NOT NULL,
    contract_address   TEXT    NOT NULL,

    taker_order_hash   TEXT    NOT NULL,  -- bytes32, indexed
    taker_order_maker  TEXT    NOT NULL,  -- address, indexed
    maker_asset_id     TEXT    NOT NULL,  -- uint256 as decimal string
    taker_asset_id     TEXT    NOT NULL,  -- uint256 as decimal string
    maker_amount_filled TEXT   NOT NULL,  -- uint256 as decimal string
    taker_amount_filled TEXT   NOT NULL,  -- uint256 as decimal string

    UNIQUE(block_number, log_index)
);

CREATE TABLE IF NOT EXISTS fee_charged (
    block_number       INTEGER NOT NULL,
    transaction_hash   TEXT    NOT NULL,
    transaction_index  INTEGER NOT NULL,
    log_index          INTEGER NOT NULL,
    contract_address   TEXT    NOT NULL,

    receiver           TEXT    NOT NULL,  -- address, indexed
    token_id           TEXT    NOT NULL,  -- uint256 as decimal string
    amount             TEXT    NOT NULL,  -- uint256 as decimal string

    UNIQUE(block_number, log_index)
);

CREATE TABLE IF NOT EXISTS order_cancelled (
    block_number       INTEGER NOT NULL,
    transaction_hash   TEXT    NOT NULL,
    transaction_index  INTEGER NOT NULL,
    log_index          INTEGER NOT NULL,
    contract_address   TEXT    NOT NULL,

    order_hash         TEXT    NOT NULL,  -- bytes32, indexed

    UNIQUE(block_number, log_index)
);

CREATE TABLE IF NOT EXISTS token_registered (
    block_number       INTEGER NOT NULL,
    transaction_hash   TEXT    NOT NULL,
    transaction_index  INTEGER NOT NULL,
    log_index          INTEGER NOT NULL,
    contract_address   TEXT    NOT NULL,

    token0             TEXT    NOT NULL,  -- uint256, indexed (decimal string)
    token1             TEXT    NOT NULL,  -- uint256, indexed (decimal string)
    condition_id       TEXT    NOT NULL,  -- bytes32, indexed

    UNIQUE(block_number, log_index)
);

-- =====================================================================
-- NegRiskAdapter (0xd91E80cF2E7be2e162c6513ceD06f1dD0dA35296)
-- =====================================================================

CREATE TABLE IF NOT EXISTS market_prepared (
    block_number       INTEGER NOT NULL,
    transaction_hash   TEXT    NOT NULL,
    transaction_index  INTEGER NOT NULL,
    log_index          INTEGER NOT NULL,
    contract_address   TEXT    NOT NULL,

    market_id          TEXT    NOT NULL,  -- bytes32, indexed
    oracle             TEXT    NOT NULL,  -- address, indexed
    fee_bips           INTEGER NOT NULL,  -- uint256 (small enough for int)
    data               TEXT    NOT NULL,  -- bytes (hex string)

    UNIQUE(block_number, log_index)
);

CREATE TABLE IF NOT EXISTS question_prepared (
    block_number       INTEGER NOT NULL,
    transaction_hash   TEXT    NOT NULL,
    transaction_index  INTEGER NOT NULL,
    log_index          INTEGER NOT NULL,
    contract_address   TEXT    NOT NULL,

    market_id          TEXT    NOT NULL,  -- bytes32, indexed
    question_id        TEXT    NOT NULL,  -- bytes32, indexed
    index_val          INTEGER NOT NULL,  -- uint256 (question index, small)
    data               TEXT    NOT NULL,  -- bytes (hex string)

    UNIQUE(block_number, log_index)
);

CREATE TABLE IF NOT EXISTS outcome_reported (
    block_number       INTEGER NOT NULL,
    transaction_hash   TEXT    NOT NULL,
    transaction_index  INTEGER NOT NULL,
    log_index          INTEGER NOT NULL,
    contract_address   TEXT    NOT NULL,

    market_id          TEXT    NOT NULL,  -- bytes32, indexed
    question_id        TEXT    NOT NULL,  -- bytes32, indexed
    outcome            INTEGER NOT NULL,  -- bool (0 or 1)

    UNIQUE(block_number, log_index)
);

-- NegRiskAdapter also emits PositionSplit / PositionsMerge but with
-- a different signature (no collateral_token, no partition array).
-- These go into separate tables to avoid schema conflicts.

CREATE TABLE IF NOT EXISTS neg_risk_position_split (
    block_number       INTEGER NOT NULL,
    transaction_hash   TEXT    NOT NULL,
    transaction_index  INTEGER NOT NULL,
    log_index          INTEGER NOT NULL,
    contract_address   TEXT    NOT NULL,

    stakeholder        TEXT    NOT NULL,  -- address, indexed
    condition_id       TEXT    NOT NULL,  -- bytes32, indexed
    amount             TEXT    NOT NULL,  -- uint256 as decimal string

    UNIQUE(block_number, log_index)
);

CREATE TABLE IF NOT EXISTS neg_risk_positions_merge (
    block_number       INTEGER NOT NULL,
    transaction_hash   TEXT    NOT NULL,
    transaction_index  INTEGER NOT NULL,
    log_index          INTEGER NOT NULL,
    contract_address   TEXT    NOT NULL,

    stakeholder        TEXT    NOT NULL,  -- address, indexed
    condition_id       TEXT    NOT NULL,  -- bytes32, indexed
    amount             TEXT    NOT NULL,  -- uint256 as decimal string

    UNIQUE(block_number, log_index)
);

CREATE TABLE IF NOT EXISTS positions_converted (
    block_number       INTEGER NOT NULL,
    transaction_hash   TEXT    NOT NULL,
    transaction_index  INTEGER NOT NULL,
    log_index          INTEGER NOT NULL,
    contract_address   TEXT    NOT NULL,

    stakeholder        TEXT    NOT NULL,  -- address, indexed
    market_id          TEXT    NOT NULL,  -- bytes32, indexed
    index_set          TEXT    NOT NULL,  -- uint256, indexed (decimal string)
    amount             TEXT    NOT NULL,  -- uint256 as decimal string

    UNIQUE(block_number, log_index)
);

CREATE TABLE IF NOT EXISTS neg_risk_payout_redemption (
    block_number       INTEGER NOT NULL,
    transaction_hash   TEXT    NOT NULL,
    transaction_index  INTEGER NOT NULL,
    log_index          INTEGER NOT NULL,
    contract_address   TEXT    NOT NULL,

    redeemer           TEXT    NOT NULL,  -- address, indexed
    condition_id       TEXT    NOT NULL,  -- bytes32, indexed
    amounts            TEXT    NOT NULL,  -- uint256[] as JSON array of decimal strings
    payout             TEXT    NOT NULL,  -- uint256 as decimal string

    UNIQUE(block_number, log_index)
);

-- =====================================================================
-- FeeModuleCTF (0xE3f18aCc55091e2c48d883fc8C8413319d4Ab7b0)
-- FeeModuleNegRisk (0xB768891e3130F6dF18214Ac804d4DB76c2C37730)
--   (same event signature, distinguished by contract_address)
-- =====================================================================

CREATE TABLE IF NOT EXISTS fee_refunded (
    block_number       INTEGER NOT NULL,
    transaction_hash   TEXT    NOT NULL,
    transaction_index  INTEGER NOT NULL,
    log_index          INTEGER NOT NULL,
    contract_address   TEXT    NOT NULL,

    order_hash         TEXT    NOT NULL,  -- bytes32, indexed
    receiver           TEXT    NOT NULL,  -- address, indexed (Solidity: "to")
    token_id           TEXT    NOT NULL,  -- uint256 as decimal string
    refund             TEXT    NOT NULL,  -- uint256 as decimal string
    fee_charged        TEXT    NOT NULL,  -- uint256, indexed

    UNIQUE(block_number, log_index)
);

-- =====================================================================
-- UmaCtfAdapter (0x157Ce2d672854c848c9b79C49a8Cc6cc89176a49)
-- =====================================================================

CREATE TABLE IF NOT EXISTS question_initialized (
    block_number       INTEGER NOT NULL,
    transaction_hash   TEXT    NOT NULL,
    transaction_index  INTEGER NOT NULL,
    log_index          INTEGER NOT NULL,
    contract_address   TEXT    NOT NULL,

    question_id        TEXT    NOT NULL,  -- bytes32, indexed
    request_timestamp  INTEGER NOT NULL,  -- uint256, indexed
    creator            TEXT    NOT NULL,  -- address, indexed
    ancillary_data     TEXT    NOT NULL,  -- bytes (hex string)
    reward_token       TEXT    NOT NULL,  -- address
    reward             TEXT    NOT NULL,  -- uint256 as decimal string
    proposal_bond      TEXT    NOT NULL,  -- uint256 as decimal string

    UNIQUE(block_number, log_index)
);

CREATE TABLE IF NOT EXISTS question_resolved (
    block_number       INTEGER NOT NULL,
    transaction_hash   TEXT    NOT NULL,
    transaction_index  INTEGER NOT NULL,
    log_index          INTEGER NOT NULL,
    contract_address   TEXT    NOT NULL,

    question_id        TEXT    NOT NULL,  -- bytes32, indexed
    settled_price      TEXT    NOT NULL,  -- int256 as decimal string (signed)
    payouts            TEXT    NOT NULL,  -- uint256[] as JSON array

    UNIQUE(block_number, log_index)
);

CREATE TABLE IF NOT EXISTS question_reset (
    block_number       INTEGER NOT NULL,
    transaction_hash   TEXT    NOT NULL,
    transaction_index  INTEGER NOT NULL,
    log_index          INTEGER NOT NULL,
    contract_address   TEXT    NOT NULL,

    question_id        TEXT    NOT NULL,  -- bytes32, indexed

    UNIQUE(block_number, log_index)
);

CREATE TABLE IF NOT EXISTS question_flagged (
    block_number       INTEGER NOT NULL,
    transaction_hash   TEXT    NOT NULL,
    transaction_index  INTEGER NOT NULL,
    log_index          INTEGER NOT NULL,
    contract_address   TEXT    NOT NULL,

    question_id        TEXT    NOT NULL,  -- bytes32, indexed

    UNIQUE(block_number, log_index)
);

CREATE TABLE IF NOT EXISTS question_emergency_resolved (
    block_number       INTEGER NOT NULL,
    transaction_hash   TEXT    NOT NULL,
    transaction_index  INTEGER NOT NULL,
    log_index          INTEGER NOT NULL,
    contract_address   TEXT    NOT NULL,

    question_id        TEXT    NOT NULL,  -- bytes32, indexed
    payouts            TEXT    NOT NULL,  -- uint256[] as JSON array

    UNIQUE(block_number, log_index)
);

-- =====================================================================
-- Indexes for common query patterns
-- To drop indexes for bulk scraping: python3 scraping/drop_indexes_rpc.py
-- To restore after scraping:         python3 scraping/restore_indexes_rpc.py
-- =====================================================================

CREATE INDEX IF NOT EXISTS idx_order_filled_block       ON order_filled (block_number);
CREATE INDEX IF NOT EXISTS idx_orders_matched_block     ON orders_matched (block_number);
CREATE INDEX IF NOT EXISTS idx_position_split_block     ON position_split (block_number);
CREATE INDEX IF NOT EXISTS idx_positions_merge_block    ON positions_merge (block_number);
CREATE INDEX IF NOT EXISTS idx_condition_prep_block     ON condition_preparation (block_number);
CREATE INDEX IF NOT EXISTS idx_condition_res_block      ON condition_resolution (block_number);
CREATE INDEX IF NOT EXISTS idx_market_prepared_block    ON market_prepared (block_number);
CREATE INDEX IF NOT EXISTS idx_question_prepared_block  ON question_prepared (block_number);
CREATE INDEX IF NOT EXISTS idx_outcome_reported_block   ON outcome_reported (block_number);
CREATE INDEX IF NOT EXISTS idx_question_init_block      ON question_initialized (block_number);
CREATE INDEX IF NOT EXISTS idx_question_resolved_block  ON question_resolved (block_number);
CREATE INDEX IF NOT EXISTS idx_fee_charged_block        ON fee_charged (block_number);
CREATE INDEX IF NOT EXISTS idx_order_cancelled_block    ON order_cancelled (block_number);
CREATE INDEX IF NOT EXISTS idx_token_registered_block   ON token_registered (block_number);
CREATE INDEX IF NOT EXISTS idx_neg_risk_split_block     ON neg_risk_position_split (block_number);
CREATE INDEX IF NOT EXISTS idx_neg_risk_merge_block     ON neg_risk_positions_merge (block_number);
CREATE INDEX IF NOT EXISTS idx_positions_converted_block ON positions_converted (block_number);
CREATE INDEX IF NOT EXISTS idx_neg_risk_payout_block    ON neg_risk_payout_redemption (block_number);
CREATE INDEX IF NOT EXISTS idx_payout_redemption_block  ON payout_redemption (block_number);
CREATE INDEX IF NOT EXISTS idx_question_reset_block     ON question_reset (block_number);
CREATE INDEX IF NOT EXISTS idx_question_flagged_block   ON question_flagged (block_number);
CREATE INDEX IF NOT EXISTS idx_question_emerg_block     ON question_emergency_resolved (block_number);
CREATE INDEX IF NOT EXISTS idx_fee_refunded_block       ON fee_refunded (block_number);

-- Compound indexes on mixed tables (used by solidification DELETE with contract_address filter)
CREATE INDEX IF NOT EXISTS idx_order_filled_block_addr   ON order_filled (block_number, contract_address);
CREATE INDEX IF NOT EXISTS idx_orders_matched_block_addr ON orders_matched (block_number, contract_address);
CREATE INDEX IF NOT EXISTS idx_fee_charged_block_addr    ON fee_charged (block_number, contract_address);
CREATE INDEX IF NOT EXISTS idx_order_cancelled_block_addr ON order_cancelled (block_number, contract_address);
CREATE INDEX IF NOT EXISTS idx_token_registered_block_addr ON token_registered (block_number, contract_address);
CREATE INDEX IF NOT EXISTS idx_fee_refunded_block_addr   ON fee_refunded (block_number, contract_address);

-- =====================================================================
-- Completed block ranges (for resumable parallel scraping)
-- Tracks which block ranges have been fully fetched for ALL contracts.
-- Ranges are inclusive: (from_block=100, to_block=199) means blocks
-- 100 through 199 are complete.
-- =====================================================================

CREATE TABLE IF NOT EXISTS completed_ranges (
    from_block  INTEGER NOT NULL,
    to_block    INTEGER NOT NULL,
    PRIMARY KEY (from_block, to_block)
);
