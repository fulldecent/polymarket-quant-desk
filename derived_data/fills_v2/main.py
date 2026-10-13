#!/usr/bin/env python3
"""
Materializes the fills_v2 derived table.

One row per account leg of every matched fill on the four Polymarket exchanges
(CTFExchange, NegRiskCtfExchange = "v1"; CTFExchangeV2, NegRiskCtfExchangeV2 =
"v2"), enriched with the condition (and NegRisk market) the outcome token
belongs to and a running per-account YES position.

EVENT MODEL (verified empirically against the raw data)
-------------------------------------------------------
Each on-chain ``matchOrders`` call emits, in log order within its transaction:

    [maker OrderFilled] x N   (taker field = the real taker account)
    [taker OrderFilled]       (taker field = the exchange address)
    [OrdersMatched]

So every ``order_filled`` row maps to exactly one fills_v2 leg:

* maker legs  -> the N maker OrderFilled rows (account = ``maker``), is_taker = FALSE
* taker leg   -> the single taker-self OrderFilled (taker == exchange address),
                 account = ``maker`` (which is the real taker), is_taker = TRUE

The taker-self row natively carries the taker's own fee, so we do not need
``orders_matched`` at all. A maker leg's atomic match is the next taker-self
OrderFilled at a higher log_index in the same transaction (the match segment ends
with the taker-self fill); that taker-self order_hash is the match key.

BUY/SELL and the USDC side
--------------------------
* v1: ``maker_asset_id == ZERO32`` means the order's signer sent USDC (a BUY of
  the outcome token); otherwise the signer sent the outcome token (a SELL).
* v2: ``side == 0`` is BUY, ``side == 1`` is SELL.

The fee is always charged on the asset the signer RECEIVES: a BUY receives
outcome tokens (token-denominated fee), a SELL receives USDC (USDC-denominated
fee). ``fee_usdc`` reports only the USDC-denominated fees; token-denominated
(buy-side) fees are not converted and contribute 0 (see DATA_DICTIONARY.md).

OUTCOME CONVENTION (YES = index_set 1, NO = index_set 2)
--------------------------------------------------------
Per Polymarket's CTF documentation
(https://github.com/Polymarket/agent-skills/blob/main/ctf-operations.md:
"partition [1, 2] for binary (Yes=1, No=2)"), the YES outcome is index_set 1 and
NO is index_set 2. ``net_yes_tokens`` expresses every leg in YES-equivalent
terms. The producer fails fast if any traded token has index_set outside {1, 2}.

OUTPUT
------
Partitioned parquet at:

    {FILLS_V2_DIR}/year=YYYY/month=MM/day=DD/data.parquet
    {FILLS_V2_DIR}/year=YYYY/month=MM/day=DD/metadata.json

See DATA_DICTIONARY.md for the full schema and invariant documentation.

REQUIRED ENV VARS
-----------------
    POLYGON_CONTRACT_EVENTS_V4_DIR   root of raw {contract}/{event}/... parquet
    TOKEN_ID_MAP_V2_DIR              token_id -> (condition_id, index_set, market_id)
    FILLS_V2_DIR                     output directory
    SCRATCH_DIR                      DuckDB spill directory; safe to wipe between runs

USAGE
-----
    python derived_data/fills_v2/main.py [options]

    --dry-run       print the work plan without writing any data
    --sample N      process only the first N incomplete days
"""

import argparse
import logging
import re
import signal
import sys
import threading
import time
from pathlib import Path

import duckdb
from dotenv import load_dotenv

_project_root = Path(__file__).resolve().parent.parent.parent
load_dotenv(_project_root / ".env")

sys.path.insert(0, str(_project_root))
from lib.env import require_directory_env, require_env  # noqa: E402
from lib.git_utils import assert_git_clean  # noqa: E402
from lib.metadata_utils import create_parquet_metadata_json, parquet_content_hash  # noqa: E402
from lib.derived_frontier import scan_frontier_ymd_folders  # noqa: E402
from lib.run_logging import (  # noqa: E402
    PhaseWork,
    RunOutput,
    configure_duckdb_progress,
    print_paths,
    print_run_summary,
    print_work_plan,
    run_sql,
)
from lib.atomic_publish import (  # noqa: E402
    create_temp_location,
    publish_atomically,
    cleanup_temp,
)
from lib.ymd_partition import (  # noqa: E402
    assert_contiguous_day_keys,
    day_block_span,
    day_dir,
    day_key_from_hive_dir,
    day_key_parts,
    day_label,
    iter_manifest_day_keys,
)

from raw_data.polygon_contract_events_v4 import SCRAPE_START_BLOCK  # noqa: E402

# ============================================================================
# constants
# ============================================================================

# 32 zero bytes — the USDC side marker in maker_asset_id / taker_asset_id and the
# USDC denomination marker in fee_refunded.token_id.
ZERO32_SQL = "unhex('" + "00" * 32 + "')"

# Exchange configuration. ``addr`` is the lowercase, 0x-free contract address; a
# leg is the taker leg when its order_filled.taker equals this address. ``gen``
# selects the v1 (separate maker/taker asset ids + FeeModule refund) or v2
# (side flag + native net fee) decoding. ``fee_module`` names the FeeModule
# table that carries v1 net fees; v2 has none.
EXCHANGES = (
    {"name": "CTFExchange",          "addr": "4bfb41d5b3570defd03c39a9a4d8de6bd8b8982e", "gen": 1, "fee_module": "FeeModuleCTF"},
    {"name": "NegRiskCtfExchange",   "addr": "c5d563a36ae78145c45a50134d48a1215220f80a", "gen": 1, "fee_module": "FeeModuleNegRisk"},
    {"name": "CTFExchangeV2",        "addr": "e111180000d2663c0091e4f400237545b87b996b", "gen": 2, "fee_module": None},
    {"name": "NegRiskCtfExchangeV2", "addr": "e2222d279d744050d28e00520010520000310f59", "gen": 2, "fee_module": None},
)

# Output Parquet schema. Logical types mirror polygon_contract_events_v4 so the
# columns join cleanly: BLOB -> pa.binary() (variable-length BYTE_ARRAY, never
# FIXED_LEN_BYTE_ARRAY), uint32 -> pa.uint32(), signed amounts -> INT64.
# Column order is fixed and is part of the contract (see DATA_DICTIONARY.md).
_OUTPUT_COLUMNS = (
    "block_number",
    "block_timestamp",
    "logical_fill_index",
    "transaction_index",
    "log_index",
    "account",
    "token_id",
    "condition_id",
    "market_id",
    "is_taker",
    "net_yes_tokens",
    "gross_usdc",
    "fee_usdc",
    "net_yes_position_after",
)

_stop_event = threading.Event()
_global_con: duckdb.DuckDBPyConnection | None = None


# ============================================================================
# configuration
# ============================================================================

RAW       = require_env("POLYGON_CONTRACT_EVENTS_V4_DIR")
TOKEN_MAP = require_env("TOKEN_ID_MAP_V2_DIR")
OUT_DIR   = require_env("FILLS_V2_DIR")
SCRATCH_DIR = require_directory_env("SCRATCH_DIR")

BALANCES_SUBDIR = "_balances"

# Sidecar one-shot latest-key collapse is very fast when it fits in memory, but
# on large histories it can OOM after significant work. If historical snapshot
# row count exceeds this threshold, skip one-shot and go directly to batched
# incremental merge.
_SIDECAR_ONE_SHOT_MAX_ROWS = 75_000_000

_SIZE_UNITS = {
    "bytes": 1,
    "byte": 1,
    "b": 1,
    "kib": 1024,
    "mib": 1024**2,
    "gib": 1024**3,
    "tib": 1024**4,
    "kb": 1000,
    "mb": 1000**2,
    "gb": 1000**3,
    "tb": 1000**4,
}


# ============================================================================
# input discovery
# ============================================================================

def _partition_file(table: str, day_key: int) -> Path:
    """Path to one table's data.parquet for this UTC day."""
    return Path(RAW) / table / day_dir(day_key) / "data.parquet"


def _existing(table: str, day_key: int) -> str | None:
    """Return the data.parquet path for this table and day if it exists, else None."""
    p = _partition_file(table, day_key)
    return str(p) if p.exists() else None


def _manifest_days() -> list[int]:
    keys = iter_manifest_day_keys(RAW)
    assert_contiguous_day_keys(keys)
    return keys


def _print_day_sunk(out: RunOutput, day_key: int, rows: int, elapsed: float) -> None:
    out.print(f"  -> day {day_label(day_key)}  rows {rows:,}  {elapsed:.1f}s")


def _format_seconds(seconds: float) -> str:
    """Human-friendly fixed precision for timing telemetry."""
    return f"{seconds:.3f}s"


def _format_bytes(n_bytes: int | None) -> str:
    """Render byte counts in IEC units for consistent telemetry logs."""
    if n_bytes is None:
        return "unknown"
    if n_bytes < 1024:
        return f"{n_bytes} B"
    value = float(n_bytes)
    for unit in ("KiB", "MiB", "GiB", "TiB"):
        value /= 1024.0
        if value < 1024.0 or unit == "TiB":
            return f"{value:.2f} {unit}"
    return f"{n_bytes} B"


def _parse_size_text_to_bytes(size_text: str | None) -> int | None:
    """Parse DuckDB size strings (e.g. '123.4 MiB') into bytes."""
    if size_text is None:
        return None
    match = re.match(r"^\s*([0-9]+(?:\.[0-9]+)?)\s*([A-Za-z]+)\s*$", size_text)
    if not match:
        return None
    magnitude = float(match.group(1))
    unit = match.group(2).lower()
    factor = _SIZE_UNITS.get(unit)
    if factor is None:
        return None
    return int(magnitude * factor)


def _sql_quote(value: str) -> str:
    """Escape single quotes for safe embedding in SQL string literals."""
    return value.replace("'", "''")


def _duckdb_memory_usage_bytes(con: duckdb.DuckDBPyConnection) -> int | None:
    """Best-effort process memory telemetry from DuckDB runtime state."""
    try:
        row = con.execute("SELECT memory_usage FROM pragma_database_size() LIMIT 1").fetchone()
    except duckdb.Error:
        return None
    if not row:
        return None
    return _parse_size_text_to_bytes(row[0])


def _table_exists(con: duckdb.DuckDBPyConnection, table_name: str) -> bool:
    """Return whether a table exists in the current DuckDB connection."""
    row = con.execute(
        """
        SELECT COUNT(*)
        FROM information_schema.tables
        WHERE table_name = ?
        """,
        [table_name],
    ).fetchone()
    return bool(row and row[0])


def _log_balances_snapshot(
    con: duckdb.DuckDBPyConnection,
    log: logging.Logger,
    *,
    day_key: int,
) -> None:
    """Log current balance-store cardinality and rough memory envelope."""
    stats = con.execute("""
        SELECT
            COUNT(*) AS keys,
            COUNT(DISTINCT account) AS accounts,
            COUNT(DISTINCT condition_id) AS conditions,
            COALESCE(SUM(octet_length(account) + octet_length(condition_id) + 8), 0) AS payload_bytes
        FROM balances
    """).fetchone()
    duckdb_memory_bytes = _duckdb_memory_usage_bytes(con)
    log.info(
        "telemetry day=%s balances: keys=%s accounts=%s conditions=%s payload~%s duckdb_memory=%s",
        day_label(day_key),
        f"{int(stats[0]):,}",
        f"{int(stats[1]):,}",
        f"{int(stats[2]):,}",
        _format_bytes(int(stats[3])),
        _format_bytes(duckdb_memory_bytes),
    )


def _log_resolved_eviction_snapshot(
    con: duckdb.DuckDBPyConnection,
    log: logging.Logger,
    *,
    day_key: int,
) -> None:
    """Log how much in-memory state would be evictable by resolved-condition policy."""
    if not _table_exists(con, "condition_resolution"):
        return
    up_to_block = day_block_span(RAW, day_key)[1]
    stats = con.execute(
        """
        SELECT
            COUNT(*) AS evictable_keys,
            COUNT(DISTINCT b.condition_id) AS evictable_conditions
        FROM balances b
        JOIN condition_resolution r USING (condition_id)
        WHERE r.resolved_block <= ?
        """,
        [up_to_block],
    ).fetchone()
    log.info(
        "telemetry day=%s resolved-eviction: evictable_keys=%s evictable_conditions=%s (resolved_block<=%s)",
        day_label(day_key),
        f"{int(stats[0]):,}",
        f"{int(stats[1]):,}",
        up_to_block,
    )


# ============================================================================
# per-exchange leg SQL
# ============================================================================

def _legs_select(exch: dict, path: str) -> str:
    """A SELECT producing unified leg columns for one exchange's order_filled file.

    Unified columns:
        block_number, block_timestamp, transaction_index, log_index,
        order_hash, account, token_id, is_taker, buy, taker_buys_self,
        q (HUGEINT outcome tokens), c (HUGEINT USDC), fee_is_usdc, gross_fee,
        is_v1, fee_module
    """
    addr = exch["addr"]
    is_v1 = exch["gen"] == 1
    fee_module = exch["fee_module"] or ""

    if is_v1:
        # v1: BUY when the signer sends USDC (maker_asset_id == ZERO32).
        buy_expr = f"(maker_asset_id = {ZERO32_SQL})"
        token_expr = "CASE WHEN maker_asset_id = " + ZERO32_SQL + " THEN taker_asset_id ELSE maker_asset_id END"
        q_expr = "CASE WHEN maker_asset_id = " + ZERO32_SQL + " THEN taker_amount_filled ELSE maker_amount_filled END"
        c_expr = "CASE WHEN maker_asset_id = " + ZERO32_SQL + " THEN maker_amount_filled ELSE taker_amount_filled END"
    else:
        # v2: BUY when side == 0.
        buy_expr = "(side = 0)"
        token_expr = "token_id"
        q_expr = "CASE WHEN side = 0 THEN taker_amount_filled ELSE maker_amount_filled END"
        c_expr = "CASE WHEN side = 0 THEN maker_amount_filled ELSE taker_amount_filled END"

    return f"""
        SELECT
            block_number,
            block_timestamp,
            transaction_index,
            log_index,
            -log_index AS neg_log_index,
            order_hash,
            maker AS account,
            {token_expr} AS token_id,
            (lower(hex(taker)) = '{addr}') AS is_taker,
            {buy_expr} AS buy,
            CAST({q_expr} AS HUGEINT) AS q,
            CAST({c_expr} AS HUGEINT) AS c,
            -- fee is on the received asset: USDC only when the signer is selling.
            (NOT {buy_expr}) AS fee_is_usdc,
            CAST(fee AS HUGEINT) AS gross_fee,
            {str(is_v1).lower()} AS is_v1,
            '{fee_module}' AS fee_module
        FROM read_parquet('{path}')
    """


def _refund_select(day_key: int) -> str | None:
    """A SELECT of v1 USDC net fees keyed by (block_number, transaction_index, order_hash).

    Returns None when neither FeeModule has data in this partition.
    fee_refunded.fee_charged is the net protocol fee; only USDC-denominated
    refunds (token_id == ZERO32) contribute to fee_usdc.
    """
    parts = []
    for module in ("FeeModuleCTF", "FeeModuleNegRisk"):
        path = _existing(f"{module}/fee_refunded", day_key)
        if path:
            parts.append(
                f"SELECT block_number, transaction_index, order_hash, CAST(fee_charged AS HUGEINT) AS fee_charged "
                f"FROM read_parquet('{path}') WHERE token_id = {ZERO32_SQL}"
            )
    if not parts:
        return None
    return " UNION ALL ".join(parts)


def _build_partition_sql(day_key: int, leg_paths: list[tuple[dict, str]]) -> str:
    """Assemble the full per-partition query that yields the final, ordered rows.

    Stages:
      raw_legs    union of every exchange's order_filled legs
      taker_legs  the is_taker legs (one per atomic match)
      assoc       each maker leg's match key + the match's taker direction,
                  found by ASOF (next taker-self leg at a higher log_index)
    enriched_v1 v1-only enrichment (joins refunds)
    enriched_v2 v2-only enrichment (no refunds join)
    enriched    union of v1/v2 enriched rows
      indexed     assign logical_fill_index per block
      final       add net_yes_position_after running balance and project columns
    """
    legs_union = " UNION ALL ".join(_legs_select(exch, path) for exch, path in leg_paths)
    refund_sql = _refund_select(day_key)
    refund_cte = refund_sql if refund_sql else "SELECT NULL::UINTEGER block_number, NULL::UINTEGER transaction_index, NULL::BLOB order_hash, NULL::HUGEINT fee_charged WHERE FALSE"

    return f"""
    WITH raw_legs AS (
        {legs_union}
    ),
    taker_legs AS (
        SELECT block_number, transaction_index, neg_log_index, order_hash, buy
        FROM raw_legs WHERE is_taker
    ),
    refunds AS (
        {refund_cte}
    ),
    -- A maker leg belongs to the next taker-self leg at a higher log_index in the
    -- same transaction (the match segment ends with the taker-self fill). ASOF
    -- picks the closest such taker leg; neg_log_index turns "smallest greater"
    -- into ASOF's "largest not-greater".
    assoc AS (
        SELECT
            m.*,
            CASE WHEN m.is_taker THEN m.order_hash ELSE t.order_hash END AS match_key,
            CASE WHEN m.is_taker THEN m.buy        ELSE t.buy        END AS taker_buys
        FROM raw_legs m
        ASOF LEFT JOIN taker_legs t
          ON m.block_number = t.block_number
         AND m.transaction_index = t.transaction_index
         AND m.neg_log_index >= t.neg_log_index
        WHERE m.is_taker OR t.order_hash IS NOT NULL
    ),
    enriched_v1 AS (
        SELECT
            a.block_number,
            a.block_timestamp,
            a.transaction_index,
            a.log_index,
            a.account,
            a.token_id,
            a.order_hash,
            tc.condition_id,
            tc.market_id,
            a.is_taker,
            a.match_key,
            a.taker_buys,
            -- net_yes_tokens: q * direction(+buy/-sell) * outcome(+YES/-NO)
            ( a.q
              * (CASE WHEN a.buy THEN 1 ELSE -1 END)
              * (CASE WHEN tok_index.index_set = 1 THEN 1 ELSE -1 END) ) AS net_yes_tokens,
            -- gross_usdc: +c for buys (spend), -c for sells (receive)
            ( a.c * (CASE WHEN a.buy THEN 1 ELSE -1 END) ) AS gross_usdc,
                        -- fee_usdc (v1): USDC-denominated net fee only; v1 nets via FeeModule
                        -- refund; buy-side (token) fees contribute 0.
            ( CASE
                WHEN NOT a.fee_is_usdc THEN 0
                                ELSE COALESCE(r.fee_charged, 0)
              END ) AS fee_usdc,
            -- best-for-taker maker price ordering value: taker buying -> cheapest
            -- first (ascending price); taker selling -> highest first (descending).
            ( (a.c::DOUBLE / a.q::DOUBLE) * (CASE WHEN a.taker_buys THEN 1 ELSE -1 END) ) AS price_rank,
            -- Suppression flag: keep if within [first_seen, resolved) range.
            ( tc.condition_id IS NULL
              OR a.block_number < tc.first_seen_block
              OR (tc.resolved_block IS NOT NULL 
                  AND (a.block_number > tc.resolved_block
                       OR (a.block_number = tc.resolved_block AND a.log_index >= tc.resolved_log_index)))
            ) AS _suppressed
        FROM assoc a
        LEFT JOIN token_cache tc ON a.token_id = tc.token_id
        LEFT JOIN (
            SELECT token_id, index_set FROM read_parquet('{TOKEN_MAP}/**/*.parquet')
        ) tok_index ON a.token_id = tok_index.token_id
        LEFT JOIN refunds r
                    ON a.block_number = r.block_number AND a.transaction_index = r.transaction_index AND a.order_hash = r.order_hash
                WHERE a.is_v1
        ),
        enriched_v2 AS (
                SELECT
                        a.block_number,
                        a.block_timestamp,
                        a.transaction_index,
                        a.log_index,
                        a.account,
                        a.token_id,
                        a.order_hash,
                        tc.condition_id,
                        tc.market_id,
                        a.is_taker,
                        a.match_key,
                        a.taker_buys,
                        -- net_yes_tokens: q * direction(+buy/-sell) * outcome(+YES/-NO)
                        ( a.q
                            * (CASE WHEN a.buy THEN 1 ELSE -1 END)
                            * (CASE WHEN tok_index.index_set = 1 THEN 1 ELSE -1 END) ) AS net_yes_tokens,
                        -- gross_usdc: +c for buys (spend), -c for sells (receive)
                        ( a.c * (CASE WHEN a.buy THEN 1 ELSE -1 END) ) AS gross_usdc,
                        -- fee_usdc (v2): USDC-denominated native fee only; buy-side (token)
                        -- fees contribute 0.
                        ( CASE
                                WHEN NOT a.fee_is_usdc THEN 0
                                ELSE a.gross_fee
                            END ) AS fee_usdc,
                        -- best-for-taker maker price ordering value: taker buying -> cheapest
                        -- first (ascending price); taker selling -> highest first (descending).
                        ( (a.c::DOUBLE / a.q::DOUBLE) * (CASE WHEN a.taker_buys THEN 1 ELSE -1 END) ) AS price_rank,
                        -- Suppression flag: keep if within [first_seen, resolved) range.
                        ( tc.condition_id IS NULL
                          OR a.block_number < tc.first_seen_block
                          OR (tc.resolved_block IS NOT NULL 
                              AND (a.block_number > tc.resolved_block
                                   OR (a.block_number = tc.resolved_block AND a.log_index >= tc.resolved_log_index)))
                        ) AS _suppressed
                FROM assoc a
                LEFT JOIN token_cache tc ON a.token_id = tc.token_id
                LEFT JOIN (
                    SELECT token_id, index_set FROM read_parquet('{TOKEN_MAP}/**/*.parquet')
                ) tok_index ON a.token_id = tok_index.token_id
                WHERE NOT a.is_v1
        ),
        enriched AS (
                SELECT
                        block_number,
                        block_timestamp,
                        transaction_index,
                        log_index,
                        account,
                        token_id,
                        order_hash,
                        condition_id,
                        market_id,
                        is_taker,
                        match_key,
                        taker_buys,
                        net_yes_tokens,
                        gross_usdc,
                        fee_usdc,
                        price_rank,
                        _suppressed
                FROM enriched_v1
                UNION ALL
                SELECT
                        block_number,
                        block_timestamp,
                        transaction_index,
                        log_index,
                        account,
                        token_id,
                        order_hash,
                        condition_id,
                        market_id,
                        is_taker,
                        match_key,
                        taker_buys,
                        net_yes_tokens,
                        gross_usdc,
                        fee_usdc,
                        price_rank,
                        _suppressed
                FROM enriched_v2
    ),
    tok_index_all AS (
        SELECT DISTINCT token_id, index_set FROM read_parquet('{TOKEN_MAP}/**/*.parquet')
    ),
    keyed AS (
        SELECT
            e.*,
            tok.index_set,
            COALESCE(e.match_key, e.order_hash) AS match_order_key
        FROM enriched e
        LEFT JOIN tok_index_all tok ON e.token_id = tok.token_id
        WHERE NOT e._suppressed AND e.condition_id IS NOT NULL
    ),
    indexed AS (
        SELECT
            *,
            CAST(ROW_NUMBER() OVER (
                PARTITION BY block_number
                ORDER BY
                    transaction_index,
                    match_order_key,
                    is_taker DESC,
                    price_rank,
                    log_index
            ) - 1 AS UINTEGER) AS logical_fill_index
        FROM keyed
    )
    SELECT
        CAST(block_number AS UINTEGER)        AS block_number,
        CAST(block_timestamp AS UINTEGER)     AS block_timestamp,
        logical_fill_index,
        CAST(transaction_index AS UINTEGER)   AS transaction_index,
        CAST(log_index AS UINTEGER)           AS log_index,
        account,
        token_id,
        condition_id,
        market_id,
        is_taker,
        CAST(net_yes_tokens AS BIGINT)        AS net_yes_tokens,
        CAST(gross_usdc AS BIGINT)            AS gross_usdc,
        CAST(fee_usdc AS BIGINT)              AS fee_usdc,
        CAST(
            COALESCE(bal.bal, 0)
            + SUM(net_yes_tokens) OVER (
                PARTITION BY account, condition_id
                ORDER BY block_number, logical_fill_index
                ROWS UNBOUNDED PRECEDING)
            AS BIGINT)                        AS net_yes_position_after,
        index_set
    FROM indexed
    LEFT JOIN balances bal USING (account, condition_id)
    ORDER BY block_number, logical_fill_index
    """


# ============================================================================
# token_id_map + running-balance state
# ============================================================================

def _build_unified_token_cache(
    con: duckdb.DuckDBPyConnection,
    log: logging.Logger,
    frontier: int,
    work: PhaseWork | None = None,
) -> int:
    """Build unified token lifecycle cache: token_id → condition_id, market_id, first_seen_block, resolved_block, resolved_log_index.
    
    One-time startup operation that creates a temp table used for fill suppression and lifecycle tracking.
    Table is incrementally updated per partition as new resolutions are encountered.

    Returns the number of rows in ``token_cache``.
    """
    # first_seen_block is the min_block of the UTC day that first materialized the token.
    day_keys = _manifest_days()
    span_rows = []
    for key in day_keys:
        year, month, day = day_key_parts(key)
        min_block, _max_block = day_block_span(RAW, key)
        span_rows.append(f"({year}, {month}, {day}, {int(min_block)})")
    if not span_rows:
        raise RuntimeError("no manifested raw days; cannot assign token first_seen_block")
    span_sql = ", ".join(span_rows)
    tok_glob = f"{TOKEN_MAP}/year=*/month=*/day=*/data.parquet"
    run_sql(con, """
        CREATE OR REPLACE TEMP TABLE token_cache (
            token_id BLOB,
            condition_id BLOB,
            market_id BLOB,
            first_seen_block UINTEGER,
            resolved_block UINTEGER,
            resolved_log_index UINTEGER
        )
    """, work=work)
    try:
        if work is not None:
            work.phase("tokens")
        run_sql(con, f"""
            CREATE OR REPLACE TEMP TABLE tok_raw AS
            SELECT
                token_id,
                condition_id,
                market_id,
                CAST(year AS INTEGER) AS year,
                CAST(month AS INTEGER) AS month,
                CAST(day AS INTEGER) AS day
            FROM read_parquet('{_sql_quote(tok_glob)}', hive_partitioning=1)
        """, work=work)
    except duckdb.IOException:
        raise RuntimeError(
            "token_id_map_v2 has no parquet files; fills_v2 requires a complete token map. "
            "Run token_id_map_v2 first and ensure TOKEN_ID_MAP_V2_DIR is populated."
        )
    missing = con.execute(f"""
        SELECT COUNT(*)
        FROM tok_raw t
        ANTI JOIN (
            SELECT * FROM (VALUES {span_sql}) AS s(year, month, day, min_block)
        ) s USING (year, month, day)
    """).fetchone()[0]
    if missing:
        raise RuntimeError(
            f"{missing:,} token_id_map rows sit on a UTC day with no raw block span"
        )
    run_sql(con, f"""
        INSERT INTO token_cache
        SELECT
            t.token_id,
            t.condition_id,
            t.market_id,
            CAST(MIN(s.min_block) AS UINTEGER) AS first_seen_block,
            CAST(NULL AS UINTEGER) AS resolved_block,
            CAST(NULL AS UINTEGER) AS resolved_log_index
        FROM tok_raw t
        JOIN (SELECT * FROM (VALUES {span_sql}) AS s(year, month, day, min_block)) s
          USING (year, month, day)
        GROUP BY t.token_id, t.condition_id, t.market_id
    """, work=work)

    n = con.execute("SELECT COUNT(*) FROM token_cache").fetchone()[0]
    if n == 0:
        raise RuntimeError(
            "token_id_map_v2 is empty; fills_v2 requires a complete token map. "
            "Run token_id_map_v2 first and ensure TOKEN_ID_MAP_V2_DIR is populated."
        )
    log.info(f"loaded {n:,} tokens into unified cache (first_seen_block assigned)")

    res_paths = [
        _partition_file("ConditionalTokens/condition_resolution", key).as_posix()
        for key in day_keys
        if _partition_file("ConditionalTokens/condition_resolution", key).exists()
    ]
    if not res_paths:
        log.info("condition_resolution not found in raw data; tokens will not have resolution bounds")
    else:
        if work is not None:
            work.phase("resolutions")
        res_list = "[" + ", ".join("'" + _sql_quote(p) + "'" for p in res_paths) + "]"
        run_sql(con, f"""
            WITH ordered AS (
                SELECT
                    condition_id,
                    block_number,
                    log_index,
                    ROW_NUMBER() OVER (
                        PARTITION BY condition_id
                        ORDER BY block_number, log_index
                    ) AS rn
                FROM read_parquet({res_list})
                WHERE block_number <= ?
            ),
            resolutions AS (
                SELECT
                    condition_id,
                    CAST(block_number AS UINTEGER) AS resolved_block,
                    CAST(log_index AS UINTEGER) AS resolved_log_index
                FROM ordered
                WHERE rn = 1
            )
            UPDATE token_cache tc
            SET
                resolved_block = r.resolved_block,
                resolved_log_index = r.resolved_log_index
            FROM resolutions r
            WHERE tc.condition_id = r.condition_id
        """, [frontier], work=work)
    
    resolved_count = con.execute("SELECT COUNT(*) FROM token_cache WHERE resolved_block IS NOT NULL").fetchone()[0]
    log.info(f"merged {resolved_count:,} resolved conditions into token cache")
    return int(n)


def _balances_sidecar_dir() -> Path:
    """Root directory for the internal balances sidecar (implementation detail)."""
    return Path(OUT_DIR) / BALANCES_SUBDIR


def _balances_partition_path(day_key: int) -> Path:
    """Path to the immutable per-day balances sidecar file."""
    return _balances_sidecar_dir() / day_dir(day_key) / "balances.parquet"


def _ensure_balances_sidecar(con: duckdb.DuckDBPyConnection, log: logging.Logger) -> None:
    """Ensure the balances sidecar exists and covers all landed fills partitions.

    If the sidecar directory is missing or empty, perform a one-time bootstrap:
    walk every landed fills partition in order, compute the ending balance for
    each touched (account, condition_id), and write the immutable per-partition
    sidecar file. After bootstrap (or if sidecar already existed), load the
    latest balances into the temp table for the current run.

    The sidecar is the sole source of balance carry-in. There is no fallback
    historical scan during normal operation.
    """
    sidecar_root = _balances_sidecar_dir()
    sidecar_root.mkdir(parents=True, exist_ok=True)

    # Check if any sidecar partition exists
    has_sidecar = any(sidecar_root.rglob("balances.parquet"))

    if not has_sidecar:
        log.info("balances sidecar missing; bootstrap from historical fills (one-time cost)")
        _bootstrap_balances_sidecar(con, log)

    # Load sidecar into temp table (purpose-built, read-only for this run).
    # The sidecar stores immutable per-partition snapshots of touched keys, so
    # the same (account, condition_id) can appear across many files over time.
    # Carry-in requires the latest snapshot per key, not all historical rows.
    con.execute("""
        CREATE OR REPLACE TEMP TABLE balances (
            account BLOB,
            condition_id BLOB,
            bal HUGEINT
        )
    """)
    sidecar_files = list(sidecar_root.rglob("balances.parquet"))
    if not sidecar_files:
        log.info("balances sidecar has no files; starting with empty carry-in balances")
        return

    glob = _sql_quote(f"{sidecar_root}/**/*.parquet")

    t_load_start = time.perf_counter()
    raw_rows = con.execute(f"SELECT COUNT(*) FROM read_parquet('{glob}')").fetchone()[0]

    def _load_balances_batched_incremental() -> None:
        # Fallback path: process newest years first. Each year does an
        # intra-batch latest-per-key collapse then anti-joins against already
        # loaded keys.
        def _year_from_sidecar_path(path: Path) -> int:
            match = re.search(r"year=([0-9]+)", path.as_posix())
            if not match:
                raise RuntimeError(f"invalid sidecar path without year label: {path}")
            return int(match.group(1))

        year_dirs = [d for d in sidecar_root.glob("year=*") if d.is_dir()]
        year_dirs.sort(key=_year_from_sidecar_path, reverse=True)
        total_groups = len(year_dirs)

        for idx, year_dir in enumerate(year_dirs, start=1):
            batch_glob = _sql_quote(
                (year_dir / "month=*" / "day=*" / "balances.parquet").as_posix()
            )
            con.execute(f"""
                INSERT INTO balances
                SELECT b.account, b.condition_id, b.bal
                FROM (
                    WITH latest_in_batch AS (
                        SELECT
                            account,
                            condition_id,
                            ending_balance,
                            ROW_NUMBER() OVER (
                                PARTITION BY account, condition_id
                                ORDER BY year DESC, month DESC, day DESC
                            ) AS rn
                        FROM read_parquet('{batch_glob}', hive_partitioning = 1)
                    )
                    SELECT account, condition_id, ending_balance AS bal
                    FROM latest_in_batch
                    WHERE rn = 1
                ) b
                LEFT JOIN balances existing
                  ON existing.account = b.account AND existing.condition_id = b.condition_id
                WHERE existing.account IS NULL
            """)
            if idx % 10 == 0 or idx == total_groups:
                log.info(f"sidecar latest-load progress: groups={idx:,}/{total_groups:,}")
        log.info(
            f"sidecar latest-load strategy=batched_incremental_merge elapsed={_format_seconds(time.perf_counter() - t_load_start)}"
        )

    if raw_rows > _SIDECAR_ONE_SHOT_MAX_ROWS:
        log.info(
            "sidecar latest-load skipping one-shot: raw_rows=%s exceeds threshold=%s",
            f"{raw_rows:,}",
            f"{_SIDECAR_ONE_SHOT_MAX_ROWS:,}",
        )
        _load_balances_batched_incremental()
    else:
        # Fast path: latest snapshot per key from the year/month/day hive columns.
        try:
            con.execute(f"""
                INSERT INTO balances
                WITH latest AS (
                    SELECT
                        account,
                        condition_id,
                        ending_balance,
                        ROW_NUMBER() OVER (
                            PARTITION BY account, condition_id
                            ORDER BY year DESC, month DESC, day DESC
                        ) AS rn
                    FROM read_parquet('{glob}', hive_partitioning = 1)
                )
                SELECT account, condition_id, ending_balance AS bal
                FROM latest
                WHERE rn = 1
            """)
            log.info(
                f"sidecar latest-load strategy=one_shot_hive_partition elapsed={_format_seconds(time.perf_counter() - t_load_start)}"
            )
        except duckdb.OutOfMemoryException:
            log.warning("sidecar one-shot latest-load OOM; falling back to batched incremental merge strategy")
            _load_balances_batched_incremental()

    dup_keys = con.execute("""
        SELECT COUNT(*)
        FROM (
            SELECT account, condition_id
            FROM balances
            GROUP BY account, condition_id
            HAVING COUNT(*) > 1
        )
    """).fetchone()[0]
    if dup_keys:
        raise RuntimeError(
            f"balances carry-in must be unique by (account, condition_id); found {dup_keys:,} duplicate keys"
        )

    n = con.execute("SELECT COUNT(*) FROM balances").fetchone()[0]
    log.info(
        f"loaded {n:,} latest running (account, condition) balances from sidecar "
        f"({raw_rows:,} historical snapshot rows scanned)"
    )


def _bootstrap_balances_sidecar(con: duckdb.DuckDBPyConnection, log: logging.Logger) -> None:
    """Rebuild the entire balances sidecar from landed fills days.

    This is the only place that ever reads historical fills data.parquet files
    to derive balances. It runs only when the sidecar is missing. The result
    is a set of immutable per-day sidecar files under _balances/.
    """
    landed: list[int] = []
    for day_dir_path in Path(OUT_DIR).glob("year=*/month=*/day=*"):
        if not day_dir_path.is_dir():
            continue
        if day_dir_path.name.endswith(".tmp"):
            continue
        if (day_dir_path / "data.parquet").exists() and (day_dir_path / "metadata.json").exists():
            landed.append(day_key_from_hive_dir(day_dir_path))
    landed.sort()

    if not landed:
        log.info("no landed fills days; sidecar will be empty")
        return

    log.info(f"bootstrap will process {len(landed)} landed fills days")

    for idx, day_key in enumerate(landed, 1):
        if _stop_event.is_set():
            log.info("bootstrap interrupted by user")
            raise RuntimeError("bootstrap interrupted; sidecar is incomplete")

        data_path = Path(OUT_DIR) / day_dir(day_key) / "data.parquet"
        if not data_path.exists():
            continue

        con.execute(f"""
            CREATE OR REPLACE TEMP TABLE part_bal AS
            SELECT
                account,
                condition_id,
                net_yes_position_after AS ending_balance
            FROM read_parquet('{_sql_quote(data_path.as_posix())}')
            QUALIFY ROW_NUMBER() OVER (
                PARTITION BY account, condition_id
                ORDER BY logical_fill_index DESC
            ) = 1
        """)

        row_count = con.execute("SELECT COUNT(*) FROM part_bal").fetchone()[0]

        sidecar_path = _balances_partition_path(day_key)
        sidecar_path.parent.mkdir(parents=True, exist_ok=True)
        con.execute(f"""
            COPY (SELECT account, condition_id, ending_balance FROM part_bal)
            TO '{_sql_quote(sidecar_path.as_posix())}' (FORMAT PARQUET, COMPRESSION ZSTD)
        """)

        log.info(
            f"bootstrap: wrote sidecar for {day_label(day_key)} "
            f"({row_count:,} keys) [{idx}/{len(landed)}]"
        )

    log.info("balances sidecar bootstrap complete")


def _write_balances_sidecar_partition(
    con: duckdb.DuckDBPyConnection,
    day_key: int,
    max_block: int,
    log: logging.Logger,
) -> None:
    """Write the immutable sidecar entry for the just-processed day.

    Only the (account, condition_id) pairs touched by this day are recorded,
    with their ending net_yes_position_after after the last fill for that key.
    """
    sidecar_path = _balances_partition_path(day_key)
    sidecar_path.parent.mkdir(parents=True, exist_ok=True)

    if _table_exists(con, "condition_resolution"):
        con.execute(f"""
            CREATE OR REPLACE TEMP TABLE touched AS
            SELECT
                c.account,
                c.condition_id,
                c.net_yes_position_after AS ending_balance
            FROM chunk_rows c
            LEFT JOIN condition_resolution r
              ON c.condition_id = r.condition_id
             AND r.resolved_block <= {int(max_block)}
            WHERE r.condition_id IS NULL
            QUALIFY ROW_NUMBER() OVER (
                PARTITION BY c.account, c.condition_id
                ORDER BY c.logical_fill_index DESC
            ) = 1
        """)
    else:
        con.execute(f"""
            CREATE OR REPLACE TEMP TABLE touched AS
            SELECT
                account,
                condition_id,
                net_yes_position_after AS ending_balance
            FROM chunk_rows
            QUALIFY ROW_NUMBER() OVER (
                PARTITION BY account, condition_id
                ORDER BY logical_fill_index DESC
            ) = 1
        """)

    con.execute(f"""
        COPY (SELECT account, condition_id, ending_balance FROM touched)
        TO '{sidecar_path.as_posix()}' (FORMAT PARQUET, COMPRESSION ZSTD)
    """)
    log.info(f"wrote balances sidecar for {day_label(day_key)}")


# ============================================================================
# per-partition processing
# ============================================================================

def process_chunk(
    con: duckdb.DuckDBPyConnection,
    day_key: int,
    min_block: int,
    max_block: int,
    log: logging.Logger,
    *,
    partition_idx: int,
    total_partitions: int,
    telemetry_every: int,
    profile_build_query: bool,
    profile_output_dir: Path | None,
    work: PhaseWork | None = None,
) -> int:
    """Process one UTC day: build legs, assign order/position, write atomically.

    Every day ALWAYS produces an output (data.parquet + metadata.json), even
    with zero fills. Legs whose token_id is absent from token_id_map_v2 are dropped
    (token map is high-coverage but not complete). The producer still fails fast if
    any mapped token has index_set outside {1, 2} (the binary YES=1/NO=2 invariant).
    """
    t0 = time.perf_counter()
    label = day_label(day_key)
    chunk_dir = Path(OUT_DIR) / day_dir(day_key)
    if chunk_dir.exists():
        sys.exit(f"FATAL: refusing to overwrite immutable partition: {chunk_dir}")
    chunk_dir.parent.mkdir(parents=True, exist_ok=True)

    leg_paths = [(exch, _existing(f"{exch['name']}/order_filled", day_key)) for exch in EXCHANGES]
    leg_paths = [(exch, path) for exch, path in leg_paths if path]

    dropped = 0
    suppressed_rows = 0
    suppressed_conditions = 0
    if work is not None:
        work.phase("building")
    if leg_paths:
        t_build_start = time.perf_counter()
        sql = _build_partition_sql(day_key, leg_paths)
        profile_path: Path | None = None
        if profile_build_query:
            if profile_output_dir is None:
                raise RuntimeError("profile_output_dir must be set when profile_build_query is enabled")
            profile_output_dir.mkdir(parents=True, exist_ok=True)
            profile_path = profile_output_dir / f"build-{label}.json"
            con.execute("PRAGMA enable_profiling='json'")
            con.execute(f"PRAGMA profiling_output='{_sql_quote(profile_path.as_posix())}'")

        try:
            run_sql(con, f"CREATE OR REPLACE TEMP TABLE chunk_rows AS {sql}", work=work)
        finally:
            if profile_build_query:
                con.execute("PRAGMA disable_profiling")

        t_build = time.perf_counter() - t_build_start
        if profile_path is not None:
            log.info(f"telemetry day={label} duckdb_build_profile={profile_path.as_posix()}")

        # Fail fast: mapped tokens must resolve to a binary condition.
        bad = con.execute("""
            SELECT
                COUNT(*) FILTER (WHERE index_set NOT IN (1, 2)) AS nonbinary
            FROM chunk_rows
        """).fetchone()
        if bad[0]:
            raise RuntimeError(
                f"{label}: {bad[0]} fill legs have index_set outside {{1, 2}} "
                f"(non-binary condition; the YES=1/NO=2 model requires binary conditions)"
            )
    else:
        t_build_start = time.perf_counter()
        con.execute("DROP TABLE IF EXISTS chunk_rows")
        con.execute(_empty_chunk_rows_sql())
        t_build = time.perf_counter() - t_build_start

    select_cols = ", ".join(_OUTPUT_COLUMNS)
    row_count = con.execute("SELECT COUNT(*) FROM chunk_rows").fetchone()[0]

    temp_loc = create_temp_location(parent_dir=chunk_dir.parent, final_name=chunk_dir.name, temp_suffix=".tmp")
    if work is not None:
        work.phase("hashing")
    input_hashes = _partition_input_hashes(day_key)
    try:
        out_parquet = temp_loc.path / "data.parquet"
        if work is not None:
            work.phase("writing")
        t_write_start = time.perf_counter()
        run_sql(con, f"""
            COPY (SELECT {select_cols} FROM chunk_rows ORDER BY block_number, logical_fill_index)
            TO '{out_parquet.as_posix()}' (FORMAT PARQUET, COMPRESSION ZSTD)
        """, work=work)
        t_write = time.perf_counter() - t_write_start

        t_meta_start = time.perf_counter()
        _write_metadata(con, temp_loc.path, day_key, min_block, max_block, input_hashes, log)
        t_meta = time.perf_counter() - t_meta_start

        t_publish_start = time.perf_counter()
        publish_atomically(temp_loc)
        t_publish = time.perf_counter() - t_publish_start

        if work is not None:
            work.phase("balances")
        t_bal_start = time.perf_counter()
        touched_keys, touched_conditions, new_keys = _update_balances(con)
        
        # Prune resolved balances: once a condition is resolved, no more fills can occur.
        # Delete the balances for that condition to shrink state continuously.
        pruned_keys, pruned_conditions = _prune_resolved_balances(con, max_block, log)
        
        t_bal = time.perf_counter() - t_bal_start

        total_elapsed = time.perf_counter() - t0
        log.info(f"{label}: wrote {row_count:,} fill legs")
        log.info(
            "telemetry day=%s partition=%s/%s rows=%s touched_keys=%s touched_conditions=%s new_keys=%s pruned_keys=%s pruned_conditions=%s",
            label,
            partition_idx,
            total_partitions,
            f"{row_count:,}",
            f"{touched_keys:,}",
            f"{touched_conditions:,}",
            f"{new_keys:,}",
            f"{pruned_keys:,}",
            f"{pruned_conditions:,}",
        )
        log.info(
            "telemetry day=%s timing build=%s write=%s metadata=%s publish=%s update_balances=%s total=%s",
            label,
            _format_seconds(t_build),
            _format_seconds(t_write),
            _format_seconds(t_meta),
            _format_seconds(t_publish),
            _format_seconds(t_bal),
            _format_seconds(total_elapsed),
        )
        return row_count
    except Exception:
        cleanup_temp(temp_loc)
        raise


def _empty_chunk_rows_sql() -> str:
    """Create an empty chunk_rows table with the exact output column types."""
    return """
    CREATE TEMP TABLE chunk_rows AS
    SELECT
        CAST(NULL AS UINTEGER) AS block_number,
        CAST(NULL AS UINTEGER) AS block_timestamp,
        CAST(NULL AS UINTEGER) AS logical_fill_index,
        CAST(NULL AS UINTEGER) AS transaction_index,
        CAST(NULL AS UINTEGER) AS log_index,
        CAST(NULL AS BLOB)     AS account,
        CAST(NULL AS BLOB)     AS token_id,
        CAST(NULL AS BLOB)     AS condition_id,
        CAST(NULL AS BLOB)     AS market_id,
        CAST(NULL AS BOOLEAN)  AS is_taker,
        CAST(NULL AS BIGINT)   AS net_yes_tokens,
        CAST(NULL AS BIGINT)   AS gross_usdc,
        CAST(NULL AS BIGINT)   AS fee_usdc,
        CAST(NULL AS BIGINT)   AS net_yes_position_after,
        CAST(NULL AS UINTEGER) AS index_set
    WHERE FALSE
    """


def _update_balances(con: duckdb.DuckDBPyConnection) -> tuple[int, int, int]:
    """Fold this partition's net_yes_tokens deltas into the running balance table.

    Returns:
        touched_keys, touched_conditions, new_keys
    """
    con.execute("""
        CREATE OR REPLACE TEMP TABLE deltas AS
        SELECT account, condition_id, SUM(net_yes_tokens)::HUGEINT AS delta
        FROM chunk_rows GROUP BY account, condition_id
    """)
    touched_stats = con.execute("""
        SELECT
            COUNT(*) AS touched_keys,
            COUNT(DISTINCT condition_id) AS touched_conditions
        FROM deltas
    """).fetchone()
    new_keys = con.execute("""
        SELECT COUNT(*)
        FROM deltas d
        LEFT JOIN balances b ON b.account = d.account AND b.condition_id = d.condition_id
        WHERE b.account IS NULL
    """).fetchone()[0]
    con.execute("""
        UPDATE balances b SET bal = b.bal + d.delta
        FROM deltas d WHERE b.account = d.account AND b.condition_id = d.condition_id
    """)
    con.execute("""
        INSERT INTO balances
        SELECT d.account, d.condition_id, d.delta
        FROM deltas d
        LEFT JOIN balances b ON b.account = d.account AND b.condition_id = d.condition_id
        WHERE b.account IS NULL
    """)
    return int(touched_stats[0]), int(touched_stats[1]), int(new_keys)


def _prune_resolved_balances(con: duckdb.DuckDBPyConnection, up_to_block: int, log: logging.Logger) -> tuple[int, int]:
    """Delete balances for conditions that have resolved up to this block.
    
    Once a condition is resolved, no more fills can occur (they are suppressed by the query),
    so we can delete the balance entries to reduce memory footprint.
    
    Returns:
        pruned_keys, pruned_conditions
    """
    stats = con.execute("""
        SELECT
            COUNT(*) AS pruned_keys,
            COUNT(DISTINCT b.condition_id) AS pruned_conditions
        FROM balances b
        INNER JOIN token_cache tc ON b.condition_id = tc.condition_id
        WHERE tc.resolved_block IS NOT NULL AND tc.resolved_block <= ?
    """, [up_to_block]).fetchone()
    
    pruned_keys, pruned_conditions = int(stats[0]), int(stats[1])
    
    if pruned_keys > 0:
        con.execute("""
            DELETE FROM balances b
            WHERE condition_id IN (
                SELECT condition_id FROM token_cache
                WHERE resolved_block IS NOT NULL AND resolved_block <= ?
            )
        """, [up_to_block])
    
    return pruned_keys, pruned_conditions


# ============================================================================
# provenance
# ============================================================================

def _partition_input_hashes(day_key: int) -> dict[str, str]:
    """SHA-256 of every raw input file read to build this UTC day (portable keys)."""
    import os

    tables = [f"{e['name']}/order_filled" for e in EXCHANGES]
    tables += ["FeeModuleCTF/fee_refunded", "FeeModuleNegRisk/fee_refunded"]
    hashes: dict[str, str] = {}
    for table in tables:
        path = _existing(table, day_key)
        if path:
            rel = os.path.relpath(path, RAW)
            hashes[rel] = parquet_content_hash(Path(path))
    return hashes


def _write_metadata(
    con,
    chunk_dir: Path,
    day_key: int,
    min_block: int,
    max_block: int,
    input_hashes: dict[str, str],
    log,
) -> None:
    part = chunk_dir / "data.parquet"
    if not part.exists():
        raise RuntimeError(f"data.parquet not found at {part} after write; cannot create metadata")
    year, month, day = day_key_parts(day_key)
    create_parquet_metadata_json(
        part,
        dataset="fills_v2",
        source_script="derived_data/fills_v2/main.py",
        input_hashes=input_hashes,
        parameters={
            "year": year,
            "month": month,
            "day": day,
            "min_block": min_block,
            "max_block": max_block,
        },
        row_count_connection=con,
        created_at=time.strftime("%Y-%m-%dT%H:%M:%SZ", time.gmtime()),
    )


# ============================================================================
# main
# ============================================================================

def main() -> None:
    global _global_con
    parser = argparse.ArgumentParser(description="Materialize fills_v2")
    parser.add_argument("--dry-run", action="store_true", help="print work plan without writing")
    parser.add_argument("--sample", type=int, default=0, help="process only first N days")
    parser.add_argument(
        "--telemetry-every",
        type=int,
        default=10,
        help="log full balances snapshot every N processed days (0 disables periodic snapshots)",
    )
    parser.add_argument(
        "--duckdb-profile-build",
        action="store_true",
        help="write DuckDB JSON profile for each build query (CREATE TABLE chunk_rows AS ...)",
    )
    parser.add_argument(
        "--duckdb-profile-dir",
        default="",
        help="directory for DuckDB build-query profiles (default: fills_v2/logs/profiles/<run-timestamp>)",
    )
    args = parser.parse_args()

    assert_git_clean(_project_root)

    out = RunOutput("fills_v2", __file__)
    run_start = time.monotonic()
    none_below = SCRAPE_START_BLOCK - 1
    print_paths(
        out,
        output=OUT_DIR,
        extra=[
            ("raw", RAW),
            ("token_id_map", TOKEN_MAP),
            ("scratch", SCRATCH_DIR),
        ],
    )

    profile_output_dir: Path | None = None
    if args.duckdb_profile_build:
        if args.duckdb_profile_dir:
            profile_output_dir = Path(args.duckdb_profile_dir)
        else:
            run_ts = time.strftime("%Y-%m-%dT%H%M%SZ", time.gmtime())
            profile_output_dir = Path(__file__).resolve().parent / "logs" / "profiles" / run_ts
        profile_output_dir.mkdir(parents=True, exist_ok=True)
        out.log_only(f"duckdb build-query profiling enabled; output_dir={profile_output_dir.as_posix()}")

    def _handle_sigint(sig, frame):
        _stop_event.set()
        try:
            if _global_con is not None:
                _global_con.interrupt()
        except Exception:
            pass

    signal.signal(signal.SIGINT, _handle_sigint)

    # Every contiguous manifested day is one output folder. A day with no fills
    # still writes a zero-row file. token_id_map must already cover the last day.
    raw_days = _manifest_days()
    cold_frontier = day_block_span(RAW, raw_days[-1])[1] if raw_days else none_below
    token_map_latest = (
        scan_frontier_ymd_folders(
            base_path=TOKEN_MAP,
            starting_day_key=raw_days[0],
            tmp_suffix=".tmp",
            cb_progress=lambda _day_key: None,
        )
        if raw_days
        else None
    )
    token_map_frontier = (
        day_block_span(RAW, token_map_latest)[1]
        if token_map_latest is not None
        else none_below
    )
    if raw_days and (token_map_latest is None or token_map_latest < raw_days[-1]):
        raise RuntimeError(
            f"token_id_map_v2 frontier ({token_map_frontier:,}) is behind the cold frontier "
            f"({cold_frontier:,}); fills_v2 requires complete token coverage up to the cold "
            f"frontier. Advance token_id_map_v2 first."
        )
    available = raw_days
    frontier = cold_frontier
    todo = [key for key in available if not (Path(OUT_DIR) / day_dir(key)).exists()]
    already_landed = len(available) - len(todo)
    if args.sample:
        todo = todo[: args.sample]

    latest_self = (
        scan_frontier_ymd_folders(
            base_path=OUT_DIR,
            starting_day_key=available[0],
            tmp_suffix=".tmp",
            cb_progress=lambda _day_key: None,
        )
        if available
        else None
    )
    self_frontier = day_block_span(RAW, latest_self)[1] if latest_self is not None else none_below

    print_work_plan(
        out,
        frontier=frontier,
        total=len(available),
        already_landed=already_landed,
        todo=len(todo),
        sample=args.sample,
    )
    out.log_only(f"# main — start at {time.strftime('%Y-%m-%dT%H:%M:%SZ', time.gmtime())}")
    out.log_only(
        f"cold frontier:        {cold_frontier:,}"
        if cold_frontier > none_below
        else "cold frontier:        none"
    )
    out.log_only(
        f"token_id_map frontier: {token_map_frontier:,}"
        if token_map_frontier > none_below
        else "token_id_map frontier: none"
    )
    out.log_only(
        f"upstream frontier:    {frontier:,}"
        if frontier > none_below
        else "upstream frontier:    none"
    )
    out.log_only(
        f"self frontier:        {self_frontier:,}"
        if self_frontier > none_below
        else "self frontier:        none"
    )
    out.log_only(f"days total:           {len(available):,}")
    out.log_only(f"already landed:       {already_landed:,}")
    out.log_only(f"to process:           {len(todo):,}")
    if args.sample:
        out.log_only(f"sample:               {args.sample:,}")

    if args.dry_run:
        if todo:
            out.print(f"dry-run: would process {len(todo):,} days")
            out.print(f"  first: {day_label(todo[0])}")
            out.print(f"  last:  {day_label(todo[-1])}")
        else:
            out.print("dry-run: nothing to do")
        return

    if not todo:
        print_run_summary(
            out,
            status="nothing to do",
            elapsed=time.monotonic() - run_start,
            partitions_done=0,
            rows_done=0,
            self_frontier=self_frontier,
            upstream_frontier=frontier,
            none_below=none_below,
        )
        return

    con = duckdb.connect()
    _global_con = con
    con.execute(f"SET temp_directory = '{SCRATCH_DIR}'")
    con.execute("SET preserve_insertion_order = false")
    configure_duckdb_progress(con)

    processed = 0
    rows_done = 0
    status = "OK"
    with out.status:
        out.status.total(0, len(todo))
        load = PhaseWork(out.status, "Loading token cache")
        load.phase()
        t_load = time.monotonic()
        n_tokens = _build_unified_token_cache(con, out.log, frontier, work=load)
        out.print(f"loaded {n_tokens:,} tokens in {time.monotonic() - t_load:.1f}s")

        con.execute("""
            CREATE OR REPLACE TEMP TABLE balances (
                account BLOB,
                condition_id BLOB,
                bal HUGEINT
            )
        """)
        out.log_only("initialized running balance table")

        for partition_idx, day_key in enumerate(todo, start=1):
            if _stop_event.is_set():
                status = "interrupted"
                out.log_only("interrupted by user")
                break
            work = PhaseWork(out.status, day_label(day_key))
            work.phase("building")
            t0 = time.monotonic()
            min_block, max_block = day_block_span(RAW, day_key)
            row_count = process_chunk(
                con,
                day_key,
                min_block,
                max_block,
                out.log,
                partition_idx=partition_idx,
                total_partitions=len(todo),
                telemetry_every=args.telemetry_every,
                profile_build_query=args.duckdb_profile_build,
                profile_output_dir=profile_output_dir,
                work=work,
            )
            _print_day_sunk(out, day_key, row_count, time.monotonic() - t0)
            processed += 1
            rows_done += row_count
            out.status.total(processed, len(todo))
        out.status.clear_partition()
    if _stop_event.is_set():
        status = "interrupted"

    if processed:
        self_frontier = day_block_span(RAW, todo[processed - 1])[1]

    print_run_summary(
        out,
        status=status,
        elapsed=time.monotonic() - run_start,
        partitions_done=processed,
        rows_done=rows_done,
        self_frontier=self_frontier,
        upstream_frontier=frontier,
        none_below=none_below,
    )


if __name__ == "__main__":
    main()
