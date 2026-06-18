-- DuckDB schema for polygon_contract_events_v3 (hot tier).
--
-- This schema is the in-DB mirror of the on-disk Parquet contract documented in DATA_DICTIONARY.md.
-- The Parquet schema is the source of truth; the DuckDB column types here are chosen so that
-- exporting these tables to Parquet produces files matching the dictionary's Parquet type contract
-- exactly.
--
-- Type mapping (Parquet logical type → DuckDB type):
--   "BLOB" (no logical annotation)   → BLOB
--   STRING (UTF-8)                   → VARCHAR
--   INT(bitWidth=32, isSigned=false) → UINTEGER
--
-- Conventions:
--
--   1. One table per (contract, event) pair. The partition layout is 
--      {contract}/{event}/1M=N/10K=N/.
--
--   2. No contract_address column. The emitting contract is implicit from the directory path.
--
--   3. Common column order is (block_number, transaction_index, transaction_hash, log_index),
--      matching DATA_DICTIONARY.md "Common columns" and the on-disk Parquet order.
--
--   4. Table names are formed as snake_case(contract) + "__" + event. Examples:
--      ctf_exchange__order_filled, conditional_tokens__position_split.
--
--   5. No NOT NULL / UNIQUE / CHECK constraints on event tables. Data integrity is enforced at the
--      application layer:
--
--        - The library wraps every event-ingestion call in a DuckDB transaction that INSERTs the
--          rows AND records the matching [from_block, to_block] in loaded_block_ranges. The two
--          either both commit or both roll back, so a block range is never double-loaded.
--
--        - Duplicate rows within an already-loaded range cannot arise from a duplicate INSERT
--          because of the rule above. Duplicates within a single INSERT (i.e. a decoder bug) are
--          detected at sink time (either by SELECT DISTINCT ON inside the COPY, or by a post-write
--          row-count check) and treated as a fatal logic error.
--
--      Skipping the constraints saves the INSERT-time index-maintenance cost, which is on the
--      critical path for high-throughput scrapes.
--
-- BLOB columns are exactly 20 bytes (addresses) or 32 bytes (hashes, IDs); the length is a
-- documentation guarantee enforced by the v3 assertion suite, not by the column type itself.

-- =====================================================================
-- loaded_block_ranges (scraper progress tracking)
--
-- This is the ONLY internal/bookkeeping table in this schema; everything else mirrors a Parquet
-- event table. It records every block range whose events have been ingested into the hot DB.
--
-- Columns are (from_block, to_block), with from_block and to_block inclusive.
--
-- The "sunk frontier" (highest block fully sunk to cold tier) is tracked in-memory via the
-- HotStore.sunk_frontier variable, populated from manifest files at startup. Deleted rows are
-- cleaned up asynchronously after writes are confirmed durable.
--
-- Invariants enforced by the application layer:
--
--   1. from_block <= to_block for every row.
--
--   2. Rows at or below sunk_frontier may remain temporarily until async cleanup runs.
--      All planning/ready-to-sink logic must treat only blocks > sunk_frontier as unsunk.
--
--   3. No two rows may overlap or even touch. Therefore, for any pair:
--      either a.to_block + 1 < b.from_block (gap of at least one block)
--      OR     a.from_block > b.to_block + 1.
--
-- See also DATA_DICTIONARY.md "Frontier ordering contract" for how these invariants enable
-- frontier-based incremental processing in the sink.

CREATE TABLE IF NOT EXISTS loaded_block_ranges (
    from_block       UINTEGER NOT NULL,
    to_block         UINTEGER NOT NULL,

    PRIMARY KEY (from_block, to_block),
    CHECK (from_block <= to_block)
);

-- =====================================================================
-- ConditionalTokens (0x4D97DCd97eC945f40cF65F87097ACe5EA0476045)
-- =====================================================================

CREATE TABLE IF NOT EXISTS conditional_tokens__condition_preparation (
    block_number         UINTEGER,
    transaction_index    UINTEGER,
    transaction_hash     BLOB    ,  -- 32 bytes
    log_index            UINTEGER,

    condition_id         BLOB    ,  -- 32 bytes
    oracle               BLOB    ,  -- 20 bytes
    question_id          BLOB    ,  -- 32 bytes
    outcome_slot_count   UINTEGER  -- [1, 256]
);

CREATE TABLE IF NOT EXISTS conditional_tokens__condition_resolution (
    block_number         UINTEGER,
    transaction_index    UINTEGER,
    transaction_hash     BLOB    ,  -- 32 bytes
    log_index            UINTEGER,

    condition_id         BLOB    ,  -- 32 bytes
    oracle               BLOB    ,  -- 20 bytes
    question_id          BLOB    ,  -- 32 bytes
    outcome_slot_count   UINTEGER,  -- [1, 256]
    payout_numerators    VARCHAR   -- JSON array of uint256 decimal strings
);

CREATE TABLE IF NOT EXISTS conditional_tokens__position_split (
    block_number         UINTEGER,
    transaction_index    UINTEGER,
    transaction_hash     BLOB    ,  -- 32 bytes
    log_index            UINTEGER,

    stakeholder          BLOB    ,  -- 20 bytes
    collateral_token     BLOB    ,  -- 20 bytes
    parent_collection_id BLOB    ,  -- 32 bytes (zero bytes for root)
    condition_id         BLOB    ,  -- 32 bytes
    partition            VARCHAR ,  -- JSON array of uint256 decimal strings
    amount               VARCHAR   -- uint256 decimal string
);

CREATE TABLE IF NOT EXISTS conditional_tokens__positions_merge (
    block_number         UINTEGER,
    transaction_index    UINTEGER,
    transaction_hash     BLOB    ,  -- 32 bytes
    log_index            UINTEGER,

    stakeholder          BLOB    ,  -- 20 bytes
    collateral_token     BLOB    ,  -- 20 bytes
    parent_collection_id BLOB    ,  -- 32 bytes
    condition_id         BLOB    ,  -- 32 bytes
    partition            VARCHAR ,  -- JSON array of uint256 decimal strings
    amount               VARCHAR   -- uint256 decimal string
);

CREATE TABLE IF NOT EXISTS conditional_tokens__payout_redemption (
    block_number         UINTEGER,
    transaction_index    UINTEGER,
    transaction_hash     BLOB    ,  -- 32 bytes
    log_index            UINTEGER,

    redeemer             BLOB    ,  -- 20 bytes
    collateral_token     BLOB    ,  -- 20 bytes
    parent_collection_id BLOB    ,  -- 32 bytes
    condition_id         BLOB    ,  -- 32 bytes
    index_sets           VARCHAR ,  -- JSON array; may be empty array "[]"
    payout               VARCHAR   -- uint256 decimal string
);

-- =====================================================================
-- CTFExchange (0x4bFb41d5B3570DeFd03C39a9A4D8dE6Bd8B8982E)
-- =====================================================================

CREATE TABLE IF NOT EXISTS ctf_exchange__order_filled (
    block_number         UINTEGER,
    transaction_index    UINTEGER,
    transaction_hash     BLOB    ,
    log_index            UINTEGER,

    order_hash           BLOB    ,  -- 32 bytes
    maker                BLOB    ,  -- 20 bytes
    taker                BLOB    ,  -- 20 bytes
    maker_asset_id       BLOB    ,  -- 32 bytes; zero bytes = USDC side
    taker_asset_id       BLOB    ,  -- 32 bytes; zero bytes = USDC side
    maker_amount_filled  VARCHAR ,  -- uint256 decimal string
    taker_amount_filled  VARCHAR ,  -- uint256 decimal string
    fee                  VARCHAR   -- uint256 decimal string (gross fee)
);

CREATE TABLE IF NOT EXISTS ctf_exchange__orders_matched (
    block_number         UINTEGER,
    transaction_index    UINTEGER,
    transaction_hash     BLOB    ,
    log_index            UINTEGER,

    taker_order_hash     BLOB    ,  -- 32 bytes
    taker_order_maker    BLOB    ,  -- 20 bytes
    maker_asset_id       BLOB    ,  -- 32 bytes
    taker_asset_id       BLOB    ,  -- 32 bytes
    maker_amount_filled  VARCHAR ,
    taker_amount_filled  VARCHAR 
);

CREATE TABLE IF NOT EXISTS ctf_exchange__fee_charged (
    block_number         UINTEGER,
    transaction_index    UINTEGER,
    transaction_hash     BLOB    ,
    log_index            UINTEGER,

    receiver             BLOB    ,  -- 20 bytes
    token_id             BLOB    ,  -- 32 bytes; zero bytes = USDC
    amount               VARCHAR   -- uint256 decimal string
);

CREATE TABLE IF NOT EXISTS ctf_exchange__token_registered (
    block_number         UINTEGER,
    transaction_index    UINTEGER,
    transaction_hash     BLOB    ,
    log_index            UINTEGER,

    token0               BLOB    ,  -- 32 bytes
    token1               BLOB    ,  -- 32 bytes
    condition_id         BLOB      -- 32 bytes
);

CREATE TABLE IF NOT EXISTS ctf_exchange__order_cancelled (
    block_number         UINTEGER,
    transaction_index    UINTEGER,
    transaction_hash     BLOB    ,
    log_index            UINTEGER,

    order_hash           BLOB      -- 32 bytes
);

-- =====================================================================
-- NegRiskCtfExchange (0xC5d563A36AE78145C45a50134d48A1215220f80a)
-- Same event signatures as CTFExchange, separate tables in v3.
-- =====================================================================

CREATE TABLE IF NOT EXISTS neg_risk_ctf_exchange__order_filled (
    block_number         UINTEGER,
    transaction_index    UINTEGER,
    transaction_hash     BLOB    ,
    log_index            UINTEGER,

    order_hash           BLOB    ,
    maker                BLOB    ,
    taker                BLOB    ,
    maker_asset_id       BLOB    ,
    taker_asset_id       BLOB    ,
    maker_amount_filled  VARCHAR ,
    taker_amount_filled  VARCHAR ,
    fee                  VARCHAR 
);

CREATE TABLE IF NOT EXISTS neg_risk_ctf_exchange__orders_matched (
    block_number         UINTEGER,
    transaction_index    UINTEGER,
    transaction_hash     BLOB    ,
    log_index            UINTEGER,

    taker_order_hash     BLOB    ,
    taker_order_maker    BLOB    ,
    maker_asset_id       BLOB    ,
    taker_asset_id       BLOB    ,
    maker_amount_filled  VARCHAR ,
    taker_amount_filled  VARCHAR 
);

CREATE TABLE IF NOT EXISTS neg_risk_ctf_exchange__fee_charged (
    block_number         UINTEGER,
    transaction_index    UINTEGER,
    transaction_hash     BLOB    ,
    log_index            UINTEGER,

    receiver             BLOB    ,
    token_id             BLOB    ,
    amount               VARCHAR 
);

CREATE TABLE IF NOT EXISTS neg_risk_ctf_exchange__token_registered (
    block_number         UINTEGER,
    transaction_index    UINTEGER,
    transaction_hash     BLOB    ,
    log_index            UINTEGER,

    token0               BLOB    ,
    token1               BLOB    ,
    condition_id         BLOB    
);

CREATE TABLE IF NOT EXISTS neg_risk_ctf_exchange__order_cancelled (
    block_number         UINTEGER,
    transaction_index    UINTEGER,
    transaction_hash     BLOB    ,
    log_index            UINTEGER,

    order_hash           BLOB    
);

-- =====================================================================
-- CTFExchangeV2 (0xE111180000d2663C0091e4f400237545B87B996B)
--
-- New-generation orderbook. Differences from v1:
--   - order_filled / orders_matched carry side + token_id (not maker_asset_id / taker_asset_id)
--   - fee_charged has no token_id
--   - No token_registered or order_cancelled events
--   - Adds order_preapproved / order_preapproval_invalidated
-- =====================================================================

CREATE TABLE IF NOT EXISTS ctf_exchange_v2__order_filled (
    block_number         UINTEGER,
    transaction_index    UINTEGER,
    transaction_hash     BLOB    ,
    log_index            UINTEGER,

    order_hash           BLOB    ,  -- 32 bytes
    maker                BLOB    ,  -- 20 bytes
    taker                BLOB    ,  -- 20 bytes
    side                 UINTEGER,  -- 0 = BUY, 1 = SELL
    token_id             BLOB    ,  -- 32 bytes outcome token id
    maker_amount_filled  VARCHAR ,
    taker_amount_filled  VARCHAR ,
    fee                  VARCHAR ,
    builder              BLOB    ,  -- 32 bytes opaque
    metadata             BLOB      -- 32 bytes opaque
);

CREATE TABLE IF NOT EXISTS ctf_exchange_v2__orders_matched (
    block_number         UINTEGER,
    transaction_index    UINTEGER,
    transaction_hash     BLOB    ,
    log_index            UINTEGER,

    taker_order_hash     BLOB    ,
    taker_order_maker    BLOB    ,
    side                 UINTEGER,
    token_id             BLOB    ,
    maker_amount_filled  VARCHAR ,
    taker_amount_filled  VARCHAR 
);

CREATE TABLE IF NOT EXISTS ctf_exchange_v2__fee_charged (
    block_number         UINTEGER,
    transaction_index    UINTEGER,
    transaction_hash     BLOB    ,
    log_index            UINTEGER,

    receiver             BLOB    ,
    amount               VARCHAR 
);

CREATE TABLE IF NOT EXISTS ctf_exchange_v2__order_preapproved (
    block_number         UINTEGER,
    transaction_index    UINTEGER,
    transaction_hash     BLOB    ,
    log_index            UINTEGER,

    order_hash           BLOB    
);

CREATE TABLE IF NOT EXISTS ctf_exchange_v2__order_preapproval_invalidated (
    block_number         UINTEGER,
    transaction_index    UINTEGER,
    transaction_hash     BLOB    ,
    log_index            UINTEGER,

    order_hash           BLOB    
);

-- =====================================================================
-- NegRiskCtfExchangeV2 (0xe2222d279d744050d28e00520010520000310F59)
-- Same event signatures as CTFExchangeV2, separate tables in v3.
-- =====================================================================

CREATE TABLE IF NOT EXISTS neg_risk_ctf_exchange_v2__order_filled (
    block_number         UINTEGER,
    transaction_index    UINTEGER,
    transaction_hash     BLOB    ,
    log_index            UINTEGER,

    order_hash           BLOB    ,
    maker                BLOB    ,
    taker                BLOB    ,
    side                 UINTEGER,
    token_id             BLOB    ,
    maker_amount_filled  VARCHAR ,
    taker_amount_filled  VARCHAR ,
    fee                  VARCHAR ,
    builder              BLOB    ,
    metadata             BLOB    
);

CREATE TABLE IF NOT EXISTS neg_risk_ctf_exchange_v2__orders_matched (
    block_number         UINTEGER,
    transaction_index    UINTEGER,
    transaction_hash     BLOB    ,
    log_index            UINTEGER,

    taker_order_hash     BLOB    ,
    taker_order_maker    BLOB    ,
    side                 UINTEGER,
    token_id             BLOB    ,
    maker_amount_filled  VARCHAR ,
    taker_amount_filled  VARCHAR 
);

CREATE TABLE IF NOT EXISTS neg_risk_ctf_exchange_v2__fee_charged (
    block_number         UINTEGER,
    transaction_index    UINTEGER,
    transaction_hash     BLOB    ,
    log_index            UINTEGER,

    receiver             BLOB    ,
    amount               VARCHAR 
);

CREATE TABLE IF NOT EXISTS neg_risk_ctf_exchange_v2__order_preapproved (
    block_number         UINTEGER,
    transaction_index    UINTEGER,
    transaction_hash     BLOB    ,
    log_index            UINTEGER,

    order_hash           BLOB    
);

CREATE TABLE IF NOT EXISTS neg_risk_ctf_exchange_v2__order_preapproval_invalidated (
    block_number         UINTEGER,
    transaction_index    UINTEGER,
    transaction_hash     BLOB    ,
    log_index            UINTEGER,

    order_hash           BLOB    
);

-- =====================================================================
-- NegRiskAdapter (0xd91E80cF2E7be2e162c6513ceD06f1dD0dA35296)
-- =====================================================================

CREATE TABLE IF NOT EXISTS neg_risk_adapter__market_prepared (
    block_number         UINTEGER,
    transaction_index    UINTEGER,
    transaction_hash     BLOB    ,
    log_index            UINTEGER,

    market_id            BLOB    ,  -- 32 bytes
    oracle               BLOB    ,  -- 20 bytes
    fee_bips             UINTEGER,  -- [0, 10_000]
    data                 VARCHAR   -- hex-encoded bytes (no 0x prefix); may be ""
);

CREATE TABLE IF NOT EXISTS neg_risk_adapter__question_prepared (
    block_number         UINTEGER,
    transaction_index    UINTEGER,
    transaction_hash     BLOB    ,
    log_index            UINTEGER,

    market_id            BLOB    ,
    question_id          BLOB    ,
    index_val            UINTEGER,
    data                 VARCHAR 
);

CREATE TABLE IF NOT EXISTS neg_risk_adapter__outcome_reported (
    block_number         UINTEGER,
    transaction_index    UINTEGER,
    transaction_hash     BLOB    ,
    log_index            UINTEGER,

    market_id            BLOB    ,
    question_id          BLOB    ,
    outcome              UINTEGER  -- {0, 1}
);

CREATE TABLE IF NOT EXISTS neg_risk_adapter__position_split (
    block_number         UINTEGER,
    transaction_index    UINTEGER,
    transaction_hash     BLOB    ,
    log_index            UINTEGER,

    stakeholder          BLOB    ,
    condition_id         BLOB    ,
    amount               VARCHAR 
);

CREATE TABLE IF NOT EXISTS neg_risk_adapter__positions_merge (
    block_number         UINTEGER,
    transaction_index    UINTEGER,
    transaction_hash     BLOB    ,
    log_index            UINTEGER,

    stakeholder          BLOB    ,
    condition_id         BLOB    ,
    amount               VARCHAR 
);

CREATE TABLE IF NOT EXISTS neg_risk_adapter__positions_converted (
    block_number         UINTEGER,
    transaction_index    UINTEGER,
    transaction_hash     BLOB    ,
    log_index            UINTEGER,

    stakeholder          BLOB    ,
    market_id            BLOB    ,
    index_set            VARCHAR ,  -- uint256 decimal string (bitmask)
    amount               VARCHAR 
);

CREATE TABLE IF NOT EXISTS neg_risk_adapter__payout_redemption (
    block_number         UINTEGER,
    transaction_index    UINTEGER,
    transaction_hash     BLOB    ,
    log_index            UINTEGER,

    redeemer             BLOB    ,
    condition_id         BLOB    ,
    amounts              VARCHAR ,  -- JSON array of uint256 decimal strings
    payout               VARCHAR   -- uint256 decimal string
);

-- =====================================================================
-- UmaCtfAdapter (0x157Ce2d672854c848c9b79C49a8Cc6cc89176a49)
-- =====================================================================

CREATE TABLE IF NOT EXISTS uma_ctf_adapter__question_initialized (
    block_number         UINTEGER,
    transaction_index    UINTEGER,
    transaction_hash     BLOB    ,
    log_index            UINTEGER,

    question_id          BLOB    ,
    request_timestamp    UINTEGER,  -- Unix seconds UTC; sanity range [2020, 2030)
    creator              BLOB    ,
    ancillary_data       VARCHAR ,  -- hex-encoded bytes (no 0x prefix)
    reward_token         BLOB    ,
    reward               VARCHAR ,
    proposal_bond        VARCHAR 
);

CREATE TABLE IF NOT EXISTS uma_ctf_adapter__question_resolved (
    block_number         UINTEGER,
    transaction_index    UINTEGER,
    transaction_hash     BLOB    ,
    log_index            UINTEGER,

    question_id          BLOB    ,
    settled_price        VARCHAR ,  -- int256 decimal string (signed)
    payouts              VARCHAR   -- JSON array of uint256 decimal strings
);

CREATE TABLE IF NOT EXISTS uma_ctf_adapter__question_reset (
    block_number         UINTEGER,
    transaction_index    UINTEGER,
    transaction_hash     BLOB    ,
    log_index            UINTEGER,

    question_id          BLOB    
);

CREATE TABLE IF NOT EXISTS uma_ctf_adapter__question_flagged (
    block_number         UINTEGER,
    transaction_index    UINTEGER,
    transaction_hash     BLOB    ,
    log_index            UINTEGER,

    question_id          BLOB    
);

CREATE TABLE IF NOT EXISTS uma_ctf_adapter__question_unflagged (
    block_number         UINTEGER,
    transaction_index    UINTEGER,
    transaction_hash     BLOB    ,
    log_index            UINTEGER,

    question_id          BLOB    
);

CREATE TABLE IF NOT EXISTS uma_ctf_adapter__question_paused (
    block_number         UINTEGER,
    transaction_index    UINTEGER,
    transaction_hash     BLOB    ,
    log_index            UINTEGER,

    question_id          BLOB    
);

CREATE TABLE IF NOT EXISTS uma_ctf_adapter__question_unpaused (
    block_number         UINTEGER,
    transaction_index    UINTEGER,
    transaction_hash     BLOB    ,
    log_index            UINTEGER,

    question_id          BLOB    
);

CREATE TABLE IF NOT EXISTS uma_ctf_adapter__question_emergency_resolved (
    block_number         UINTEGER,
    transaction_index    UINTEGER,
    transaction_hash     BLOB    ,
    log_index            UINTEGER,

    question_id          BLOB    ,
    payouts              VARCHAR 
);

-- =====================================================================
-- FeeModuleCTF (0xE3f18aCc55091e2c48d883fc8C8413319d4Ab7b0)
-- =====================================================================

CREATE TABLE IF NOT EXISTS fee_module_ctf__fee_refunded (
    block_number         UINTEGER,
    transaction_index    UINTEGER,
    transaction_hash     BLOB    ,
    log_index            UINTEGER,

    order_hash           BLOB    ,
    receiver             BLOB    ,  -- Solidity: "to"
    token_id             BLOB    ,  -- 32 bytes; zero bytes = USDC
    refund               VARCHAR ,
    fee_charged          VARCHAR   -- net fee retained by protocol
);

-- =====================================================================
-- FeeModuleNegRisk (0xB768891e3130F6dF18214Ac804d4DB76c2C37730)
-- =====================================================================

CREATE TABLE IF NOT EXISTS fee_module_neg_risk__fee_refunded (
    block_number         UINTEGER,
    transaction_index    UINTEGER,
    transaction_hash     BLOB    ,
    log_index            UINTEGER,

    order_hash           BLOB    ,
    receiver             BLOB    ,
    token_id             BLOB    ,
    refund               VARCHAR ,
    fee_charged          VARCHAR 
);
