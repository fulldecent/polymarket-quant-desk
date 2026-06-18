#!/usr/bin/env python3
"""
Materializes the fills_v1 derived table.

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

So every ``order_filled`` row maps to exactly one fills_v1 leg:

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

    {FILLS_V1_DIR}/1M={N}/10K={K}/data.parquet
    {FILLS_V1_DIR}/1M={N}/10K={K}/metadata.json

See DATA_DICTIONARY.md for the full schema and invariant documentation.

REQUIRED ENV VARS
-----------------
    POLYGON_CONTRACT_EVENTS_V3_DIR   root of raw {contract}/{event}/... parquet
    TOKEN_ID_MAP_V1_DIR              token_id -> (condition_id, index_set, market_id)
    FILLS_V1_DIR                     output directory
    TEMP_DIR                         DuckDB spill directory

USAGE
-----
    python derived_data/fills_v1/main.py [options]

    --dry-run       print the work plan without writing any data
    --sample N      process only the first N incomplete partitions
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
from rich.console import Console

_project_root = Path(__file__).resolve().parent.parent.parent
load_dotenv(_project_root / ".env")

sys.path.insert(0, str(_project_root))
from lib.env import require_env  # noqa: E402
from lib.git_utils import assert_git_clean  # noqa: E402
from lib.metadata_utils import create_parquet_metadata_json, parquet_content_hash  # noqa: E402
from lib.partition_utils import (  # noqa: E402
    PARTITION_10K_LABEL,
    PARTITION_1M_LABEL,
    enumerate_partitions,
    partition_dir,
    partition_end,
)
from lib.run_logging import make_progress, setup_logging  # noqa: E402
from lib.atomic_publish import (  # noqa: E402
    create_temp_location,
    publish_atomically,
    cleanup_temp,
)
from lib.derived_frontier import scan_frontier_1M_10K_folders

from raw_data.polygon_contract_events_v3 import get_sunk_frontier, SCRAPE_START_BLOCK  # noqa: E402

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

# Output Parquet schema. Logical types mirror polygon_contract_events_v3 so the
# columns join cleanly: BLOB -> pa.binary() (variable-length BYTE_ARRAY, never
# FIXED_LEN_BYTE_ARRAY), uint32 -> pa.uint32(), signed amounts -> INT64.
# Column order is fixed and is part of the contract (see DATA_DICTIONARY.md).
_OUTPUT_COLUMNS = (
    "block_number",
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

console = Console()

_stop_event = threading.Event()
_global_con: duckdb.DuckDBPyConnection | None = None


# ============================================================================
# configuration
# ============================================================================

RAW       = require_env("POLYGON_CONTRACT_EVENTS_V3_DIR")
TOKEN_MAP = require_env("TOKEN_ID_MAP_V1_DIR")
OUT_DIR   = require_env("FILLS_V1_DIR")
TEMP_DIR  = require_env("TEMP_DIR")

BALANCES_SUBDIR = "_balances"

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

def _partition_file(table: str, k_val: int) -> Path:
    """Path to one table's data.parquet for the 10K partition starting at k_val."""
    return Path(RAW) / table / partition_dir(k_val) / "data.parquet"


def _existing(table: str, k_val: int) -> str | None:
    """Return the data.parquet path for this table+partition if it exists, else None."""
    p = _partition_file(table, k_val)
    return str(p) if p.exists() else None


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
    k_val: int,
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
        "telemetry 10K=%s balances: keys=%s accounts=%s conditions=%s payload~%s duckdb_memory=%s",
        k_val,
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
    k_val: int,
) -> None:
    """Log how much in-memory state would be evictable by resolved-condition policy."""
    if not _table_exists(con, "condition_resolution"):
        return
    up_to_block = partition_end(k_val)
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
        "telemetry 10K=%s resolved-eviction: evictable_keys=%s evictable_conditions=%s (resolved_block<=%s)",
        k_val,
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
        block_number, transaction_index, log_index, transaction_hash,
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
            transaction_index,
            log_index,
            -log_index AS neg_log_index,
            transaction_hash,
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


def _refund_select(k_val: int) -> str | None:
    """A SELECT of v1 USDC net fees keyed by (transaction_hash, order_hash).

    Returns None when neither FeeModule has data in this partition.
    fee_refunded.fee_charged is the net protocol fee; only USDC-denominated
    refunds (token_id == ZERO32) contribute to fee_usdc.
    """
    parts = []
    for module in ("FeeModuleCTF", "FeeModuleNegRisk"):
        path = _existing(f"{module}/fee_refunded", k_val)
        if path:
            parts.append(
                f"SELECT transaction_hash, order_hash, CAST(fee_charged AS HUGEINT) AS fee_charged "
                f"FROM read_parquet('{path}') WHERE token_id = {ZERO32_SQL}"
            )
    if not parts:
        return None
    return " UNION ALL ".join(parts)


def _build_partition_sql(k_val: int, leg_paths: list[tuple[dict, str]]) -> str:
    """Assemble the full per-partition query that yields the final, ordered rows.

    Stages:
      raw_legs    union of every exchange's order_filled legs
      taker_legs  the is_taker legs (one per atomic match)
      assoc       each maker leg's match key + the match's taker direction,
                  found by ASOF (next taker-self leg at a higher log_index)
      enriched    join token_id_map (condition_id, index_set, market_id) + fees
      indexed     assign logical_fill_index per block
      final       add net_yes_position_after running balance and project columns
    """
    legs_union = " UNION ALL ".join(_legs_select(exch, path) for exch, path in leg_paths)
    refund_sql = _refund_select(k_val)
    refund_cte = refund_sql if refund_sql else "SELECT NULL::BLOB transaction_hash, NULL::BLOB order_hash, NULL::HUGEINT fee_charged WHERE FALSE"

    return f"""
    WITH raw_legs AS (
        {legs_union}
    ),
    taker_legs AS (
        SELECT transaction_hash, neg_log_index, order_hash, buy
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
          ON m.transaction_hash = t.transaction_hash
         AND m.neg_log_index >= t.neg_log_index
        WHERE m.is_taker OR t.order_hash IS NOT NULL
    ),
    enriched AS (
        SELECT
            a.block_number,
            a.transaction_index,
            a.log_index,
            a.account,
            a.token_id,
            tok.condition_id,
            tok.market_id,
            tok.index_set,
            a.is_taker,
            a.match_key,
            a.taker_buys,
            -- net_yes_tokens: q * direction(+buy/-sell) * outcome(+YES/-NO)
            ( a.q
              * (CASE WHEN a.buy THEN 1 ELSE -1 END)
              * (CASE WHEN tok.index_set = 1 THEN 1 ELSE -1 END) ) AS net_yes_tokens,
            -- gross_usdc: +c for buys (spend), -c for sells (receive)
            ( a.c * (CASE WHEN a.buy THEN 1 ELSE -1 END) ) AS gross_usdc,
            -- fee_usdc: USDC-denominated net fee only; v1 nets via FeeModule refund,
            -- v2 uses the native fee; buy-side (token) fees contribute 0.
            ( CASE
                WHEN NOT a.fee_is_usdc THEN 0
                WHEN a.is_v1 THEN COALESCE(r.fee_charged, 0)
                ELSE a.gross_fee
              END ) AS fee_usdc,
            -- best-for-taker maker price ordering value: taker buying -> cheapest
            -- first (ascending price); taker selling -> highest first (descending).
            ( (a.c::DOUBLE / a.q::DOUBLE) * (CASE WHEN a.taker_buys THEN 1 ELSE -1 END) ) AS price_rank
        FROM assoc a
        LEFT JOIN tok ON a.token_id = tok.token_id
        LEFT JOIN refunds r
          ON a.is_v1 AND a.transaction_hash = r.transaction_hash AND a.order_hash = r.order_hash
    ),
    keyed AS (
        SELECT
            *,
            MIN(log_index) OVER (PARTITION BY block_number, transaction_index, match_key) AS match_order_key
        FROM enriched
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
        index_set,
        condition_id IS NULL                  AS _missing_token
    FROM indexed
    LEFT JOIN balances bal USING (account, condition_id)
    ORDER BY block_number, logical_fill_index
    """


# ============================================================================
# token_id_map + running-balance state
# ============================================================================

def _load_token_map(con: duckdb.DuckDBPyConnection, log: logging.Logger) -> None:
    """Load token_id -> (condition_id, index_set, market_id) into a temp table once."""
    glob = f"{TOKEN_MAP}/**/*.parquet"
    con.execute("""
        CREATE OR REPLACE TEMP TABLE tok (
            token_id BLOB, condition_id BLOB, index_set UINTEGER, market_id BLOB
        )
    """)
    try:
        con.execute(f"""
            INSERT INTO tok
            SELECT token_id, condition_id, index_set, market_id
            FROM read_parquet('{glob}')
        """)
    except duckdb.IOException:
        raise RuntimeError(
            "token_id_map_v1 has no parquet files; fills_v1 requires a complete token map. "
            "Run token_id_map_v1 first and ensure TOKEN_ID_MAP_V1_DIR is populated."
        )
    n = con.execute("SELECT COUNT(*) FROM tok").fetchone()[0]
    if n == 0:
        raise RuntimeError(
            "token_id_map_v1 is empty; fills_v1 requires a complete token map. "
            "Run token_id_map_v1 first and ensure TOKEN_ID_MAP_V1_DIR is populated."
        )
    log.info(f"loaded {n:,} token_id_map rows")


def _load_condition_resolution(con: duckdb.DuckDBPyConnection, log: logging.Logger, frontier: int) -> None:
    """Load first resolution block per condition for telemetry-only what-if analysis."""
    glob = f"{RAW}/ConditionalTokens/condition_resolution/**/*.parquet"
    con.execute("""
        CREATE OR REPLACE TEMP TABLE condition_resolution (
            condition_id BLOB,
            resolved_block UINTEGER
        )
    """)
    try:
        con.execute(
            f"""
            INSERT INTO condition_resolution
            SELECT
                condition_id,
                CAST(MIN(block_number) AS UINTEGER) AS resolved_block
            FROM read_parquet('{glob}')
            WHERE block_number <= ?
            GROUP BY condition_id
            """,
            [frontier],
        )
    except duckdb.IOException:
        log.warning("condition_resolution table not found in raw data; resolved-condition telemetry disabled")
        con.execute("DROP TABLE IF EXISTS condition_resolution")
        return

    n = con.execute("SELECT COUNT(*) FROM condition_resolution").fetchone()[0]
    log.info(f"loaded {n:,} resolved conditions for telemetry")


def _balances_sidecar_dir() -> Path:
    """Root directory for the internal balances sidecar (implementation detail)."""
    return Path(OUT_DIR) / BALANCES_SUBDIR


def _balances_partition_path(k_val: int) -> Path:
    """Path to the immutable per-partition balances sidecar file."""
    return _balances_sidecar_dir() / partition_dir(k_val) / "balances.parquet"


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

    # Load sidecar into temp table (purpose-built, read-only for this run)
    con.execute("""
        CREATE OR REPLACE TEMP TABLE balances (account BLOB, condition_id BLOB, bal HUGEINT)
    """)
    glob = f"{sidecar_root}/**/*.parquet"
    try:
        con.execute(f"""
            INSERT INTO balances
            SELECT account, condition_id, ending_balance AS bal
            FROM read_parquet('{glob}')
        """)
        n = con.execute("SELECT COUNT(*) FROM balances").fetchone()[0]
        log.info(f"loaded {n:,} running (account, condition) balances from sidecar")
    except duckdb.IOException:
        raise RuntimeError(
            "balances sidecar is empty after bootstrap; this is a bug in the bootstrap logic"
        )


def _bootstrap_balances_sidecar(con: duckdb.DuckDBPyConnection, log: logging.Logger) -> None:
    """Rebuild the entire balances sidecar from landed fills partitions.

    This is the only place that ever reads historical fills data.parquet files
    to derive balances. It runs only when the sidecar is missing. The result
    is a set of immutable per-partition sidecar files under _balances/.
    """
    from lib.partition_utils import enumerate_partitions as _enum_parts

    # Find all landed fills partitions by scanning the output directory
    landed: list[tuple[int, int]] = []
    for m_dir in Path(OUT_DIR).glob("1M=*"):
        if not m_dir.is_dir():
            continue
        for k_dir in m_dir.glob("10K=*"):
            if (k_dir / "data.parquet").exists() and (k_dir / "metadata.json").exists():
                k_val = int(k_dir.name.split("=")[1])
                m_val = int(m_dir.name.split("=")[1])
                landed.append((m_val, k_val))
    landed.sort(key=lambda x: (x[0], x[1]))

    if not landed:
        log.info("no landed fills partitions; sidecar will be empty")
        return

    log.info(f"bootstrap will process {len(landed)} landed fills partitions")

    # Process each landed partition in order, writing its sidecar entry
    for idx, (m_val, k_val) in enumerate(landed, 1):
        if _stop_event.is_set():
            log.info("bootstrap interrupted by user")
            raise RuntimeError("bootstrap interrupted; sidecar is incomplete")

        data_path = Path(OUT_DIR) / partition_dir(k_val) / "data.parquet"
        if not data_path.exists():
            continue

        # Compute ending balance per (account, condition_id) for this partition
        # (the row with the highest logical_fill_index for each key)
        con.execute(f"""
            CREATE OR REPLACE TEMP TABLE part_bal AS
            SELECT
                account,
                condition_id,
                net_yes_position_after AS ending_balance
            FROM read_parquet('{data_path.as_posix()}')
            QUALIFY ROW_NUMBER() OVER (
                PARTITION BY account, condition_id
                ORDER BY logical_fill_index DESC
            ) = 1
        """)

        row_count = con.execute("SELECT COUNT(*) FROM part_bal").fetchone()[0]

        # Write immutable sidecar entry
        sidecar_path = _balances_partition_path(k_val)
        sidecar_path.parent.mkdir(parents=True, exist_ok=True)
        con.execute(f"""
            COPY (SELECT account, condition_id, ending_balance FROM part_bal)
            TO '{sidecar_path.as_posix()}' (FORMAT PARQUET, COMPRESSION ZSTD)
        """)

        log.info(f"bootstrap: wrote sidecar for 10K={k_val} ({row_count:,} keys) [{idx}/{len(landed)}]")

    log.info("balances sidecar bootstrap complete")


def _write_balances_sidecar_partition(con: duckdb.DuckDBPyConnection, k_val: int, log: logging.Logger) -> None:
    """Write the immutable sidecar entry for the just-processed partition.

    Only the (account, condition_id) pairs touched by this partition are recorded,
    with their ending net_yes_position_after after the last fill for that key.
    """
    sidecar_path = _balances_partition_path(k_val)
    sidecar_path.parent.mkdir(parents=True, exist_ok=True)

    # The balances table has been updated by _update_balances with this partition's deltas.
    # We need the ending balance for exactly the keys that had activity in this partition.
    # We can derive it from chunk_rows (which still exists) or from the updated balances.
    # Using chunk_rows is precise: the last row per key in chunk_rows has the ending value.
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
    log.info(f"wrote balances sidecar for 10K={k_val}")


# ============================================================================
# per-partition processing
# ============================================================================

def process_chunk(
    con: duckdb.DuckDBPyConnection,
    m_val: int,
    k_val: int,
    log: logging.Logger,
    *,
    partition_idx: int,
    total_partitions: int,
    telemetry_every: int,
) -> int:
    """Process one 10K partition: build legs, assign order/position, write atomically.

    Every partition ALWAYS produces an output (data.parquet + metadata.json), even
    with zero fills. Legs whose token_id is absent from token_id_map_v1 are dropped
    (token map is high-coverage but not complete). The producer still fails fast if
    any mapped token has index_set outside {1, 2} (the binary YES=1/NO=2 invariant).
    """
    t0 = time.perf_counter()
    chunk_dir = Path(OUT_DIR) / partition_dir(k_val)
    chunk_dir.parent.mkdir(parents=True, exist_ok=True)

    leg_paths = [(exch, _existing(f"{exch['name']}/order_filled", k_val)) for exch in EXCHANGES]
    leg_paths = [(exch, path) for exch, path in leg_paths if path]

    dropped = 0
    if leg_paths:
        t_build_start = time.perf_counter()
        sql = _build_partition_sql(k_val, leg_paths)
        con.execute(f"CREATE OR REPLACE TEMP TABLE chunk_rows AS {sql}")
        t_build = time.perf_counter() - t_build_start

        # Drop legs for token_ids not present in token_id_map_v1; this is an allowed
        # approximation documented by token_id_map_v1 and fills_v1 contracts.
        dropped = con.execute("""
            SELECT COUNT(*)
            FROM chunk_rows
            WHERE _missing_token
        """).fetchone()[0]
        if dropped:
            con.execute("DELETE FROM chunk_rows WHERE _missing_token")
            log.warning(
                f"10K={k_val}: dropped {dropped} fill legs with token_ids absent from token_id_map_v1"
            )

        # Fail fast: mapped tokens must resolve to a binary condition.
        bad = con.execute("""
            SELECT
                COUNT(*) FILTER (WHERE index_set NOT IN (1, 2)) AS nonbinary
            FROM chunk_rows
        """).fetchone()
        if bad[0]:
            raise RuntimeError(
                f"10K={k_val}: {bad[0]} fill legs have index_set outside {{1, 2}} "
                f"(non-binary condition; the YES=1/NO=2 model requires binary conditions)"
            )
    else:
        t_build_start = time.perf_counter()
        con.execute("DROP TABLE IF EXISTS chunk_rows")
        con.execute(_empty_chunk_rows_sql())
        t_build = time.perf_counter() - t_build_start

    select_cols = ", ".join(_OUTPUT_COLUMNS)
    row_count = con.execute("SELECT COUNT(*) FROM chunk_rows").fetchone()[0]

    post_resolution_rows = 0
    post_resolution_conditions = 0
    if _table_exists(con, "condition_resolution"):
        post_resolution_rows, post_resolution_conditions = con.execute("""
            SELECT
                COUNT(*) AS rows_after_resolution,
                COUNT(DISTINCT c.condition_id) AS conditions_after_resolution
            FROM chunk_rows c
            JOIN condition_resolution r USING (condition_id)
            WHERE c.block_number > r.resolved_block
        """).fetchone()

    temp_loc = create_temp_location(parent_dir=chunk_dir.parent, final_name=chunk_dir.name, temp_suffix=".tmp")
    input_hashes = _partition_input_hashes(k_val)
    try:
        out_parquet = temp_loc.path / "data.parquet"
        t_write_start = time.perf_counter()
        con.execute(f"""
            COPY (SELECT {select_cols} FROM chunk_rows ORDER BY block_number, logical_fill_index)
            TO '{out_parquet.as_posix()}' (FORMAT PARQUET, COMPRESSION ZSTD)
        """)
        t_write = time.perf_counter() - t_write_start

        t_meta_start = time.perf_counter()
        _write_metadata(con, temp_loc.path, m_val, k_val, input_hashes, log)
        t_meta = time.perf_counter() - t_meta_start

        t_publish_start = time.perf_counter()
        publish_atomically(temp_loc)
        t_publish = time.perf_counter() - t_publish_start

        t_bal_start = time.perf_counter()
        touched_keys, touched_conditions, new_keys = _update_balances(con)
        t_bal = time.perf_counter() - t_bal_start

        t_sidecar_start = time.perf_counter()
        _write_balances_sidecar_partition(con, k_val, log)
        t_sidecar = time.perf_counter() - t_sidecar_start

        total_elapsed = time.perf_counter() - t0
        log.info(f"10K={k_val}: wrote {row_count:,} fill legs")
        log.info(
            "telemetry 10K=%s partition=%s/%s rows=%s dropped_missing=%s touched_keys=%s touched_conditions=%s new_keys=%s post_resolution_rows=%s post_resolution_conditions=%s",
            k_val,
            partition_idx,
            total_partitions,
            f"{row_count:,}",
            f"{int(dropped):,}",
            f"{touched_keys:,}",
            f"{touched_conditions:,}",
            f"{new_keys:,}",
            f"{int(post_resolution_rows):,}",
            f"{int(post_resolution_conditions):,}",
        )
        log.info(
            "telemetry 10K=%s timing build=%s write=%s metadata=%s publish=%s update_balances=%s sidecar=%s total=%s",
            k_val,
            _format_seconds(t_build),
            _format_seconds(t_write),
            _format_seconds(t_meta),
            _format_seconds(t_publish),
            _format_seconds(t_bal),
            _format_seconds(t_sidecar),
            _format_seconds(total_elapsed),
        )
        if telemetry_every > 0 and (
            partition_idx == 1
            or partition_idx == total_partitions
            or partition_idx % telemetry_every == 0
        ):
            _log_balances_snapshot(con, log, k_val=k_val)
            _log_resolved_eviction_snapshot(con, log, k_val=k_val)
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
        CAST(NULL AS UINTEGER) AS index_set,
        FALSE                  AS _missing_token
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


# ============================================================================
# provenance
# ============================================================================

def _partition_input_hashes(k_val: int) -> dict[str, str]:
    """SHA-256 of every raw input file read to build this partition (portable keys)."""
    import os

    tables = [f"{e['name']}/order_filled" for e in EXCHANGES]
    tables += ["FeeModuleCTF/fee_refunded", "FeeModuleNegRisk/fee_refunded"]
    hashes: dict[str, str] = {}
    for table in tables:
        path = _existing(table, k_val)
        if path:
            rel = os.path.relpath(path, RAW)
            hashes[rel] = parquet_content_hash(Path(path))
    return hashes


def _write_metadata(con, chunk_dir: Path, m_val: int, k_val: int, input_hashes: dict[str, str], log) -> None:
    part = chunk_dir / "data.parquet"
    if not part.exists():
        raise RuntimeError(f"data.parquet not found at {part} after write; cannot create metadata")
    create_parquet_metadata_json(
        part,
        dataset="fills_v1",
        source_script="derived_data/fills_v1/main.py",
        input_hashes=input_hashes,
        parameters={"1M": m_val, "10K": k_val, "min_block": k_val, "max_block": partition_end(k_val)},
        row_count_connection=con,
        created_at=time.strftime("%Y-%m-%dT%H:%M:%SZ", time.gmtime()),
    )


# ============================================================================
# main
# ============================================================================

def main() -> None:
    global _global_con
    parser = argparse.ArgumentParser(description="Materialize fills_v1")
    parser.add_argument("--dry-run", action="store_true", help="print work plan without writing")
    parser.add_argument("--sample", type=int, default=0, help="process only first N partitions")
    parser.add_argument(
        "--telemetry-every",
        type=int,
        default=10,
        help="log full balances snapshot every N processed partitions (0 disables periodic snapshots)",
    )
    args = parser.parse_args()

    assert_git_clean(_project_root)

    log = setup_logging("fills_v1", __file__, console)
    log.info("fills_v1 materializer starting")

    def _handle_sigint(sig, frame):
        _stop_event.set()
        try:
            if _global_con is not None:
                _global_con.interrupt()
        except Exception:
            pass

    signal.signal(signal.SIGINT, _handle_sigint)

    # Discover work by block range up to the upstream frontier (no source-folder
    # discovery): a 10K range with no fills still produces a zero-row output.
    # The effective frontier is the MIN of the cold dataset and token_id_map_v1;
    # fills_v1 may only consume partitions where BOTH sources are complete.
    cold_frontier = get_sunk_frontier(RAW)
    token_map_latest = scan_frontier_1M_10K_folders(
        base_path=TOKEN_MAP,
        starting_partition=SCRAPE_START_BLOCK,
        tmp_suffix=".tmp",
        cb_progress=lambda _partition: None,
    )
    token_map_frontier = (
        partition_end(token_map_latest)
        if token_map_latest is not None
        else SCRAPE_START_BLOCK - 1
    )
    if token_map_frontier < cold_frontier:
        raise RuntimeError(
            f"token_id_map_v1 frontier ({token_map_frontier}) is behind the cold frontier ({cold_frontier}); "
            f"fills_v1 requires complete token coverage up to the cold frontier. "
            f"Advance token_id_map_v1 first."
        )
    frontier = min(cold_frontier, token_map_frontier)
    all_partitions = enumerate_partitions(SCRAPE_START_BLOCK, frontier)
    todo = [
        (m, k)
        for (m, k) in all_partitions
        if not (Path(OUT_DIR) / f"{PARTITION_1M_LABEL}={m}" / f"{PARTITION_10K_LABEL}={k}").exists()
    ]
    log.info(f"frontier={frontier}, total={len(all_partitions)}, todo={len(todo)}")

    if args.dry_run:
        for m, k in todo[:10]:
            log.info(f"DRY-RUN would process 1M={m} 10K={k}")
        if len(todo) > 10:
            log.info(f"... and {len(todo) - 10} more")
        return

    if args.sample:
        todo = todo[: args.sample]

    con = duckdb.connect()
    _global_con = con
    con.execute(f"SET temp_directory = '{TEMP_DIR}'")
    con.execute("SET preserve_insertion_order = false")

    _load_token_map(con, log)
    _load_condition_resolution(con, log, frontier)
    _ensure_balances_sidecar(con, log)

    console.print(
        f"frontier={frontier}  |  total={len(all_partitions):,}  |  "
        f"[green]{len(all_partitions) - len(todo):,} already landed[/green]  |  "
        f"[yellow]{len(todo):,} to process[/yellow]"
    )
    if not todo:
        console.print("[green]Nothing to do.[/green]")
        return

    with make_progress(console) as progress:
        task = progress.add_task("Materializing fills_v1", total=len(todo))
        processed = 0
        for partition_idx, (m_val, k_val) in enumerate(todo, start=1):
            if _stop_event.is_set():
                log.info("interrupted by user")
                break
            process_chunk(
                con,
                m_val,
                k_val,
                log,
                partition_idx=partition_idx,
                total_partitions=len(todo),
                telemetry_every=args.telemetry_every,
            )
            processed += 1
            progress.update(task, advance=1)

    log.info(f"fills_v1 materializer finished. Processed {processed} partitions.")
    console.print(f"[green]Complete! Processed {processed} partitions.[/green]")


if __name__ == "__main__":
    main()
