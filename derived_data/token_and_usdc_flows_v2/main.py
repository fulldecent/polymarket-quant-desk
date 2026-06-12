#!/usr/bin/env python3
"""
Materializes the token_and_usdc_flows_v2 derived table from raw blockchain event
parquet files. Each row tracks a single USDC or token movement on Polymarket,
reading from polygon_contract_events_v3 (Polymarket V1 and V2 combined).

OUTPUT
------
Partitioned parquet at:

    {TOKEN_AND_USDC_FLOWS_V2_DIR}/1M={N}/10K={K}/data.parquet
    {TOKEN_AND_USDC_FLOWS_V2_DIR}/1M={N}/10K={K}/metadata.json

Partitions are produced in strict block order up to the upstream frontier.
Both files are written atomically into a temp-named folder, then the folder is
renamed to the final name.

See DATA_DICTIONARY.md for full schema and invariant documentation.

EVENT SOURCES
-------------
- CTFExchange/order_filled        -> flow_type = 'trade_buy' / 'trade_sell'
- NegRiskCtfExchange/order_filled -> flow_type = 'trade_buy' / 'trade_sell'
- CTFExchangeV2/order_filled      -> flow_type = 'trade_buy' / 'trade_sell' (V2)
- NegRiskCtfExchangeV2/order_filled -> flow_type = 'trade_buy' / 'trade_sell' (V2)
- ConditionalTokens/position_split  -> flow_type = 'split'
- NegRiskAdapter/position_split     -> flow_type = 'split'
- ConditionalTokens/positions_merge -> flow_type = 'merge'
- NegRiskAdapter/positions_merge    -> flow_type = 'merge'
- ConditionalTokens/payout_redemption -> flow_type = 'redeem'
- NegRiskAdapter/payout_redemption    -> flow_type = 'redeem'
- NegRiskAdapter/positions_converted  -> flow_type = 'convert'

REQUIRED ENV VARS
-----------------
    POLYGON_CONTRACT_EVENTS_V3_DIR   root of raw {contract}/{event}/... parquet
    TOKEN_AND_USDC_FLOWS_V2_DIR      output directory
    TOKEN_ID_MAP_V1_DIR              token ID lookup table (derived)
    TEMP_DIR                         DuckDB spill directory

USAGE
-----
    python derived_data/token_and_usdc_flows_v2/main.py [options]

    --dry-run       print the work plan without writing any data
    --sample N      process only the first N incomplete chunks
    --force         recompute chunks that already have a data.parquet
    --skip-errors   log errors and continue instead of stopping
"""

import argparse
import logging
import os
import signal
import sys
import threading
import time
from datetime import datetime, timezone
from pathlib import Path

import duckdb
import pyarrow as pa
import pyarrow.parquet as pq
from dotenv import load_dotenv
from rich.console import Console
from rich.logging import RichHandler
from rich.progress import (
    BarColumn,
    MofNCompleteColumn,
    Progress,
    SpinnerColumn,
    TaskProgressColumn,
    TextColumn,
    TimeElapsedColumn,
    TimeRemainingColumn,
)

_project_root = Path(__file__).resolve().parent.parent.parent
load_dotenv(_project_root / ".env")

sys.path.insert(0, str(_project_root))
from lib.git_utils import assert_git_clean  # noqa: E402
from lib.metadata_utils import create_parquet_metadata_json, parquet_content_hash  # noqa: E402
from lib.partition_utils import (
    PARTITION_10K_LABEL,
    PARTITION_1M_LABEL,
    partition_dir,
    partition_end,
)
from derived_data.common.partition_planner import plan_partitions
from lib.atomic_publish import (  # noqa: E402
    create_temp_location,
    publish_atomically,
    cleanup_old_temp_artifacts,
    cleanup_on_failure,
    cleanup_temp,
)
from raw_data.polygon_contract_events_v3 import get_sunk_frontier  # noqa: E402

# ============================================================================
# constants
# ============================================================================

ZERO32 = b"\x00" * 32
ZERO32_HEX = "00" * 32
USDC_E = bytes.fromhex("2791bca1f2de4661ed88a30c99a7a9449aa84174")
USDC_E_HEX = "2791bca1f2de4661ed88a30c99a7a9449aa84174"
CTF_EXCHANGE = bytes.fromhex("4bfb41d5b3570defd03c39a9a4d8de6bd8b8982e")
CTF_EXCHANGE_HEX = "4bfb41d5b3570defd03c39a9a4d8de6bd8b8982e"
NEGRISK_EXCHANGE = bytes.fromhex("c5d563a36ae78145c45a50134d48a1215220f80a")
NEGRISK_EXCHANGE_HEX = "c5d563a36ae78145c45a50134d48a1215220f80a"
CTF_EXCHANGE_V2_HEX = "e111180000d2663c0091e4f400237545b87b996b"
NEGRISK_EXCHANGE_V2_HEX = "e2222d279d744050d28e00520010520000310f59"

# Filtered exchange addresses (no real traders)
CTF_EXCHANGE_V2 = bytes.fromhex(CTF_EXCHANGE_V2_HEX)
NEGRISK_EXCHANGE_V2 = bytes.fromhex(NEGRISK_EXCHANGE_V2_HEX)

# Maximum permissible net_usdc for USDC.e rows: ±100M USDC (6 decimals)
# Observed large redemptions in some partitions required this threshold.
# If you see values above this, inspect raw `payout_redemption` / `positions_merge` events.
MAX_USDC = 100_000_000_000_000

# Output Parquet schema: BLOB columns (raw bytes, not hex strings)
_OUTPUT_SCHEMA = pa.schema([
    pa.field("block_number",      pa.uint32()),
    pa.field("transaction_index", pa.uint32()),
    pa.field("transaction_hash",  pa.binary(32)),  # BLOB: 32 bytes
    pa.field("log_index",         pa.uint32()),
    pa.field("sub_index",         pa.uint32()),
    pa.field("raw_source",        pa.string()),
    pa.field("account",           pa.binary(20)),  # BLOB: 20 bytes
    pa.field("token_id",          pa.binary(32), nullable=True),  # BLOB: 32 bytes, nullable
    pa.field("condition_id",      pa.binary(32)),  # BLOB: 32 bytes
    pa.field("flow_type",         pa.string()),
    pa.field("net_usdc",          pa.int64()),
    pa.field("net_tokens",        pa.int64(), nullable=True),
    pa.field("price_1e18",        pa.uint64(), nullable=True),
    pa.field("collateral_token",  pa.binary(20)),  # BLOB: 20 bytes
])

console = Console()

# Global stop event used by the SIGINT handler and long-running loops
_stop_event = threading.Event()
# Global connection reference so the SIGINT handler can interrupt running DuckDB queries
_global_con: duckdb.DuckDBPyConnection | None = None


# ============================================================================
# configuration
# ============================================================================

def _require_env(name: str) -> str:
    val = os.environ.get(name, "")
    if not val:
        sys.exit(f"{name} is not set. Add it to .env.")
    return val


RAW      = _require_env("POLYGON_CONTRACT_EVENTS_V3_DIR")
OUT_DIR  = _require_env("TOKEN_AND_USDC_FLOWS_V2_DIR")
TOKEN_ID_MAP_DIR = _require_env("TOKEN_ID_MAP_V1_DIR")
TEMP_DIR = _require_env("TEMP_DIR")


# ============================================================================
# logging
# ============================================================================

def _setup_logging() -> logging.Logger:
    # One timestamped log file per run, under a logs/ folder next to this script.
    # Filename embeds the run start time in ISO 8601 zulu (basic format, no colons
    # so it is filesystem-safe).
    log_dir = Path(__file__).resolve().parent / "logs"
    log_dir.mkdir(parents=True, exist_ok=True)
    ts = datetime.now(timezone.utc).strftime("%Y-%m-%dT%H%M%SZ")
    log_path = log_dir / f"main-{ts}.log"

    fmt = logging.Formatter(
        "%(asctime)s  %(levelname)-7s  %(message)s",
        datefmt="%Y-%m-%dT%H:%M:%S",
    )
    fh = logging.FileHandler(log_path)
    fh.setLevel(logging.DEBUG)
    fh.setFormatter(fmt)

    # Route console logs through the shared rich Console so they cooperate with
    # the live Progress display (log lines scroll above a pinned progress bar).
    ch = RichHandler(
        console=console,
        show_path=False,
        rich_tracebacks=True,
        omit_repeated_times=False,
    )
    ch.setLevel(logging.INFO)
    ch.setFormatter(logging.Formatter("%(message)s", datefmt="%Y-%m-%dT%H:%M:%S"))

    log = logging.getLogger("token_and_usdc_flows_v2")
    log.setLevel(logging.DEBUG)
    log.addHandler(fh)
    log.addHandler(ch)
    return log


# ============================================================================
# provenance helpers
# ============================================================================

def _write_metadata(
    con: duckdb.DuckDBPyConnection,
    chunk_dir: Path,
    m_val: int,
    k_val: int,
    input_hashes: dict[str, str],
    log: logging.Logger,
) -> None:
    """Write metadata.json per docs/Metadata files.md schema.

    No 'version' field; includes 'input_hashes' (map of input paths to content hashes).
    """
    part = chunk_dir / "data.parquet"
    if not part.exists():
        log.warning(f"data.parquet not found at {part}")
        return

    create_parquet_metadata_json(
        part,
        dataset="token_and_usdc_flows_v2",
        source_script="derived_data/token_and_usdc_flows_v2/main.py",
        input_hashes=input_hashes,
        parameters={
            "1M": m_val,
            "10K": k_val,
            "min_block": k_val,
            "max_block": partition_end(k_val),
        },
        row_count_connection=con,
        created_at=time.strftime("%Y-%m-%dT%H:%M:%SZ", time.gmtime()),
    )


# ============================================================================
# token ID lookup is now provided by TOKEN_ID_MAP_V1_DIR (loaded at startup)
# No per-1M keccak computation or in-memory cache in this job.
# See load_global_lookups() for the read_parquet of the map.

        {
            "collateral": coll,
            "parent": parent,
            "condition": cond,
            "index_set": idx,
            "token_id": tid,
        }
        for (coll, parent, cond, idx), tid in _token_id_cache.items()
    ]
    if not rows:
        # Empty table
        con.execute("""
            CREATE OR REPLACE TEMP TABLE computed_token_ids AS
            SELECT
                CAST(NULL AS BLOB) AS collateral_token,
                CAST(NULL AS BLOB) AS parent_collection_id,
                CAST(NULL AS BLOB) AS condition_id,
                CAST(NULL AS UINTEGER) AS index_set,
                CAST(NULL AS BLOB) AS token_id
            WHERE 1=0
        """)
        return

    arrays = {
        "collateral_token":  pa.array([r["collateral"] for r in rows], type=pa.binary(20)),
        "parent_collection_id":      pa.array([r["parent"] for r in rows], type=pa.binary(32)),
        "condition_id":   pa.array([r["condition"] for r in rows], type=pa.binary(32)),
        "index_set":   pa.array([r["index_set"] for r in rows], type=pa.uint32()),
        "token_id":    pa.array([r["token_id"] for r in rows], type=pa.binary(32)),
    }
    table = pa.table(arrays)
    con.register("computed_token_ids", table)


# ============================================================================
# global lookup table loading
# ============================================================================

def load_global_lookups(
    con: duckdb.DuckDBPyConnection,
    log: logging.Logger,
) -> None:
    """Load global lookup tables used across all chunks."""
    t0 = time.monotonic()

    # token_registered: token_id -> condition_id
    src_dir = Path(RAW) / "CTFExchange" / "token_registered"
    if src_dir.exists():
        glob = f"{src_dir.parent}/{src_dir.name}/**/data.parquet"
        con.execute(f"""
            CREATE OR REPLACE TEMP TABLE token_condition AS
            SELECT DISTINCT token0 AS token_id, condition_id FROM read_parquet('{glob}')
            UNION ALL
            SELECT DISTINCT token1 AS token_id, condition_id FROM read_parquet('{glob}')
        """)
        log.debug("loaded token_registered (CTFExchange) into token_condition")
    if _stop_event.is_set():
        log.info("load_global_lookups interrupted by user after token_condition")
        raise KeyboardInterrupt()

    nr_src_dir = Path(RAW) / "NegRiskCtfExchange" / "token_registered"
    if nr_src_dir.exists():
        glob = f"{nr_src_dir.parent}/{nr_src_dir.name}/**/data.parquet"
        con.execute(f"""
            INSERT INTO token_condition
            SELECT DISTINCT token0, condition_id FROM read_parquet('{glob}')
            UNION ALL
            SELECT DISTINCT token1, condition_id FROM read_parquet('{glob}')
        """)
        log.debug("loaded token_registered (NegRiskCtfExchange) into token_condition")
    if _stop_event.is_set():
        log.info("load_global_lookups interrupted by user")
        raise KeyboardInterrupt()

    # Deduplicate across both exchanges (same token can be registered in both)
    con.execute("""
        CREATE OR REPLACE TEMP TABLE token_condition AS
        SELECT DISTINCT token_id, condition_id FROM token_condition
    """)

    # condition_preparation: condition_id -> outcome_slot_count
    src_dir = Path(RAW) / "ConditionalTokens" / "condition_preparation"
    if src_dir.exists():
        glob = f"{src_dir.parent}/{src_dir.name}/**/data.parquet"
        con.execute(f"""
            CREATE OR REPLACE TEMP TABLE condition_prep AS
            SELECT condition_id, outcome_slot_count FROM read_parquet('{glob}')
        """)
        log.debug("loaded condition_preparation into condition_prep")
    if _stop_event.is_set():
        log.info("load_global_lookups interrupted by user")
        raise KeyboardInterrupt()

    # question_prepared: market_id -> (question_id, index_val)
    src_dir = Path(RAW) / "NegRiskAdapter" / "question_prepared"
    if src_dir.exists():
        glob = f"{src_dir.parent}/{src_dir.name}/**/data.parquet"
        con.execute(f"""
            CREATE OR REPLACE TEMP TABLE question_prep AS
            SELECT DISTINCT market_id, question_id, index_val FROM read_parquet('{glob}')
        """)
        log.debug("loaded question_prepared into question_prep")
    if _stop_event.is_set():
        log.info("load_global_lookups interrupted by user")
        raise KeyboardInterrupt()

    # question_to_condition: map NegRisk question ids to corresponding condition ids
    con.execute(f"""
        CREATE OR REPLACE TEMP TABLE question_to_condition AS
        SELECT
            qp.market_id,
            qp.index_val,
            prep.condition_id
        FROM question_prep qp
        JOIN (
            SELECT question_id, condition_id
            FROM read_parquet('{RAW}/ConditionalTokens/condition_preparation/**/data.parquet')
        ) prep ON qp.question_id = prep.question_id
    """)

    # Load the token_id_map_v1 derived table (replaces per-1M keccak computation)
    token_id_map_glob = f"{TOKEN_ID_MAP_DIR}/**/*.parquet"
    con.execute(f"""
        CREATE OR REPLACE TEMP TABLE computed_token_ids AS
        SELECT * FROM read_parquet('{token_id_map_glob}')
    """)
    log.debug("loaded token_id_map_v1 into computed_token_ids")

    log.info("global lookups loaded in %.1fs total", time.monotonic() - t0)


def _src_path(contract_event: str, m_val: int, k_val: int) -> str | None:
    """Return parquet path for a 10K partition, or None if missing."""
    p = Path(RAW) / contract_event / partition_dir(k_val) / "data.parquet"
    if p.exists():
        return str(p)
    return None


class FillAssumptionViolation(RuntimeError):
    """Raised when the at-most-one-fill-per-(transaction, order_hash) assumption fails.

    See _assert_unique_fills_per_tx for the full statement of the assumption.
    """


def _assert_unique_fills_per_tx(
    con: duckdb.DuckDBPyConnection,
    contract_event: str,
    fee_module_event: str,
    exchange_hex: str,
    m_val: int,
    k_val: int,
    log: logging.Logger,
) -> None:
    """Fail fast if the (transaction_hash, order_hash) trade->fee join key is not unique.

    BEHAVIORAL ASSUMPTION (NOT enforced by the smart contracts): within a single
    transaction a given order_hash is filled at most once, and the FeeModule emits at
    most one FeeRefunded for it. We rely on this so that joining order_filled to
    fee_refunded on (transaction_hash, order_hash) is a clean 1-to-(0 or 1) mapping.

    Why this is only an assumption: the V1 CTFExchange permits an order to be filled
    incrementally across many transactions (partial fills). `_updateOrderStatus` merely
    decrements the order's remaining amount; it does NOT forbid two fills of the same
    order within one transaction. An operator using exec_many (multiple matchOrders /
    fillOrder calls batched into one transaction) could therefore place two fills of the
    same order_hash in a single transaction. Nothing on-chain prevents it.

    If that ever happens, the LEFT JOIN explodes into a cartesian product: each
    order_filled row matches multiple fee_refunded rows, attaching the wrong fee and
    emitting duplicate (block_number, log_index, sub_index) grain tuples — silent data
    corruption. Rather than risk that, we verify the assumption on every partition and
    abort loudly. The check is cheap (single-partition aggregation) and runs before the
    join is used.
    """
    src = _src_path(contract_event, m_val, k_val)
    if not src:
        return

    exch = f"unhex('{exchange_hex}')"

    # order_filled side: real trades only (exchange-as-intermediary rows are dropped).
    dup_of = con.execute(f"""
        SELECT block_number, transaction_index, COUNT(*) AS n
        FROM read_parquet('{src}')
        WHERE maker != {exch} AND taker != {exch}
        GROUP BY block_number, transaction_index, order_hash
        HAVING COUNT(*) > 1
        LIMIT 5
    """).fetchall()
    if dup_of:
        raise FillAssumptionViolation(
            f"{contract_event} 1M={m_val} 10K={k_val}: the same order_hash is filled "
            f"more than once within a single transaction (exec_many batching). The "
            f"(transaction_hash, order_hash) fee join is unsafe here. Example "
            f"(block_number, transaction_index, count): {dup_of}"
        )

    fee_src = _src_path(fee_module_event, m_val, k_val)
    if fee_src:
        dup_fr = con.execute(f"""
            SELECT block_number, transaction_index, COUNT(*) AS n
            FROM read_parquet('{fee_src}')
            GROUP BY block_number, transaction_index, order_hash
            HAVING COUNT(*) > 1
            LIMIT 5
        """).fetchall()
        if dup_fr:
            raise FillAssumptionViolation(
                f"{fee_module_event} 1M={m_val} 10K={k_val}: multiple FeeRefunded rows "
                f"share one (transaction_hash, order_hash). The fee join would multiply "
                f"trade rows. Example (block_number, transaction_index, count): {dup_fr}"
            )

    log.debug(
        "fill-uniqueness assumption holds for %s 1M=%s 10K=%s",
        contract_event, m_val, k_val,
    )


def _src_or_empty(con: duckdb.DuckDBPyConnection, contract_event: str,
                  m_val: int, k_val: int, table_alias: str) -> str:
    """Return a SQL subquery for the 10K source, or an empty result set."""
    path = _src_path(contract_event, m_val, k_val)
    if path:
        return f"(SELECT * FROM read_parquet('{path}')) AS {table_alias}"
    glob = f"{RAW}/{contract_event}/**/data.parquet"
    return f"(SELECT * FROM read_parquet('{glob}') LIMIT 0) AS {table_alias}"


# ============================================================================
# SQL generators for each event type
# ============================================================================

def _trade_sql(con: duckdb.DuckDBPyConnection, contract_event: str,
               exchange_hex: str, fee_module_event: str,
               m_val: int, k_val: int, log: logging.Logger) -> str:
    """Generate SQL for trade_buy/trade_sell rows from V1 order_filled."""
    src = _src_path(contract_event, m_val, k_val)
    if not src:
        return ""

    fee_src = _src_path(fee_module_event, m_val, k_val)

    # Fail fast if the (transaction_hash, order_hash) fee join key is not unique in
    # this partition. This guards the LEFT JOIN below against cartesian explosion.
    _assert_unique_fills_per_tx(
        con, contract_event, fee_module_event, exchange_hex, m_val, k_val, log,
    )

    raw_source = contract_event
    zero = f"unhex('{ZERO32_HEX}')"
    exch = f"unhex('{exchange_hex}')"
    usdc = f"unhex('{USDC_E_HEX}')"

    # Pre-join order_filled with fee_refunded.
    # Join key (block_number, transaction_index, order_hash) == (transaction_hash, order_hash).
    # Safe only because _assert_unique_fills_per_tx (called above) has proven this key is
    # unique on both sides for this partition. See that function for the assumption details.
    if fee_src:
        fills_subq = f"""(
            SELECT
                of.block_number,
                of.transaction_index,
                of.transaction_hash,
                of.log_index,
                of.maker,
                of.taker,
                of.maker_asset_id,
                of.taker_asset_id,
                CAST(of.maker_amount_filled AS BIGINT) AS maker_amount_filled,
                CAST(of.taker_amount_filled AS BIGINT) AS taker_amount_filled,
                COALESCE(CAST(fr.fee_charged AS BIGINT), CAST(of.fee AS BIGINT)) AS actual_fee
            FROM read_parquet('{src}') of
            LEFT JOIN read_parquet('{fee_src}') fr
                ON of.order_hash = fr.order_hash
                AND of.block_number = fr.block_number
                AND of.transaction_index = fr.transaction_index
            WHERE of.maker != {exch} AND of.taker != {exch}
        )"""
    else:
        fills_subq = f"""(
            SELECT
                of.block_number,
                of.transaction_index,
                of.transaction_hash,
                of.log_index,
                of.maker,
                of.taker,
                of.maker_asset_id,
                of.taker_asset_id,
                CAST(of.maker_amount_filled AS BIGINT) AS maker_amount_filled,
                CAST(of.taker_amount_filled AS BIGINT) AS taker_amount_filled,
                CAST(of.fee AS BIGINT) AS actual_fee
            FROM read_parquet('{src}') of
            WHERE of.maker != {exch} AND of.taker != {exch}
        )"""

    temp_name = f"_tmp_fills_{raw_source.replace('/', '_')}_{m_val}_{k_val}"
    con.execute(f"CREATE OR REPLACE TEMP TABLE {temp_name} AS SELECT * FROM {fills_subq}")

    return f"""
    -- === buyer rows (sub_index=0) from {raw_source} ===
    SELECT
        f.block_number,
        f.transaction_index,
        f.transaction_hash,
        f.log_index,
        CAST(0 AS UINTEGER) AS sub_index,
        '{raw_source}' AS raw_source,
        CASE WHEN f.maker_asset_id = {zero} THEN f.maker ELSE f.taker END AS account,
        CASE WHEN f.maker_asset_id != {zero} THEN f.maker_asset_id
             ELSE f.taker_asset_id END AS token_id,
        tc.condition_id,
        'trade_buy' AS flow_type,
        CASE WHEN f.maker_asset_id = {zero}
            THEN -f.maker_amount_filled
            ELSE -f.taker_amount_filled
        END AS net_usdc,
        CASE WHEN f.maker_asset_id = {zero}
            THEN f.taker_amount_filled - f.actual_fee
            ELSE f.maker_amount_filled
        END AS net_tokens,
        {usdc} AS collateral_token
    FROM {temp_name} f
    JOIN token_condition tc
        ON tc.token_id = (CASE WHEN f.maker_asset_id != {zero}
                               THEN f.maker_asset_id
                               ELSE f.taker_asset_id END)

    UNION ALL

    -- === seller rows (sub_index=1) from {raw_source} ===
    SELECT
        f.block_number,
        f.transaction_index,
        f.transaction_hash,
        f.log_index,
        CAST(1 AS UINTEGER) AS sub_index,
        '{raw_source}' AS raw_source,
        CASE WHEN f.maker_asset_id = {zero} THEN f.taker ELSE f.maker END AS account,
        CASE WHEN f.maker_asset_id != {zero} THEN f.maker_asset_id
             ELSE f.taker_asset_id END AS token_id,
        tc.condition_id,
        'trade_sell' AS flow_type,
        CASE WHEN f.maker_asset_id = {zero}
            THEN f.maker_amount_filled
            ELSE f.taker_amount_filled - f.actual_fee
        END AS net_usdc,
        CASE WHEN f.maker_asset_id = {zero}
            THEN -f.taker_amount_filled
            ELSE -f.maker_amount_filled
        END AS net_tokens,
        {usdc} AS collateral_token
    FROM {temp_name} f
    JOIN token_condition tc
        ON tc.token_id = (CASE WHEN f.maker_asset_id != {zero}
                               THEN f.maker_asset_id
                               ELSE f.taker_asset_id END)
    """


def _trade_sql_v2(con: duckdb.DuckDBPyConnection, contract_event: str,
                  exchange_hex: str, m_val: int, k_val: int) -> str:
    """Generate SQL for trade_buy/trade_sell rows from V2 order_filled."""
    src = _src_path(contract_event, m_val, k_val)
    if not src:
        return ""

    raw_source = contract_event
    usdc = f"unhex('{USDC_E_HEX}')"
    exch = f"unhex('{exchange_hex}')"

    temp_name = f"_tmp_fills_{raw_source.replace('/', '_')}_{m_val}_{k_val}"
    con.execute(f"""
        CREATE OR REPLACE TEMP TABLE {temp_name} AS
        SELECT
            of.block_number,
            of.transaction_index,
            of.transaction_hash,
            of.log_index,
            of.maker,
            of.taker,
            of.side,
            of.token_id,
            CAST(of.maker_amount_filled AS BIGINT) AS maker_amount_filled,
            CAST(of.taker_amount_filled AS BIGINT) AS taker_amount_filled,
            CAST(of.fee AS BIGINT) AS actual_fee
        FROM read_parquet('{src}') of
        WHERE of.maker != {exch} AND of.taker != {exch}
    """)

    return f"""
    -- === buyer rows (sub_index=0) from {raw_source} ===
    SELECT
        f.block_number,
        f.transaction_index,
        f.transaction_hash,
        f.log_index,
        CAST(0 AS UINTEGER) AS sub_index,
        '{raw_source}' AS raw_source,
        CASE WHEN f.side = 0 THEN f.maker ELSE f.taker END AS account,
        f.token_id,
        tc.condition_id,
        'trade_buy' AS flow_type,
        CASE WHEN f.side = 0
            THEN -f.maker_amount_filled
            ELSE -f.taker_amount_filled
        END AS net_usdc,
        CASE WHEN f.side = 0
            THEN f.taker_amount_filled - f.actual_fee
            ELSE f.maker_amount_filled
        END AS net_tokens,
        {usdc} AS collateral_token
    FROM {temp_name} f
    JOIN token_condition tc ON tc.token_id = f.token_id

    UNION ALL

    -- === seller rows (sub_index=1) from {raw_source} ===
    SELECT
        f.block_number,
        f.transaction_index,
        f.transaction_hash,
        f.log_index,
        CAST(1 AS UINTEGER) AS sub_index,
        '{raw_source}' AS raw_source,
        CASE WHEN f.side = 0 THEN f.taker ELSE f.maker END AS account,
        f.token_id,
        tc.condition_id,
        'trade_sell' AS flow_type,
        CASE WHEN f.side = 0
            THEN f.maker_amount_filled
            ELSE f.taker_amount_filled - f.actual_fee
        END AS net_usdc,
        CASE WHEN f.side = 0
            THEN -f.taker_amount_filled
            ELSE -f.maker_amount_filled
        END AS net_tokens,
        {usdc} AS collateral_token
    FROM {temp_name} f
    JOIN token_condition tc ON tc.token_id = f.token_id
    """


def _ct_split_sql(m_val: int, k_val: int) -> str:
    """CT split -> USDC row (sub_index=0) + per-partition-element token rows."""
    src = _src_path("ConditionalTokens/position_split", m_val, k_val)
    if not src:
        return ""

    return f"""
    -- === CT split USDC rows (sub_index=0) ===
    SELECT
        ps.block_number,
        ps.transaction_index,
        ps.transaction_hash,
        ps.log_index,
        CAST(0 AS UINTEGER) AS sub_index,
        'ConditionalTokens/position_split' AS raw_source,
        ps.stakeholder AS account,
        CAST(NULL AS BLOB) AS token_id,
        ps.condition_id,
        'split' AS flow_type,
        -CAST(ps.amount AS BIGINT) AS net_usdc,
        CAST(0 AS BIGINT) AS net_tokens,
        ps.collateral_token
    FROM read_parquet('{src}') ps

    UNION ALL

    -- === CT split token rows (sub_index=1..) ===
    SELECT
        ex2.block_number,
        ex2.transaction_index,
        ex2.transaction_hash,
        ex2.log_index,
        CAST(ex2.elem_idx AS UINTEGER) AS sub_index,
        'ConditionalTokens/position_split' AS raw_source,
        ex2.stakeholder AS account,
        ct.token_id,
        ex2.condition_id,
        'split' AS flow_type,
        CAST(0 AS BIGINT) AS net_usdc,
        CAST(ex2.amount AS BIGINT) AS net_tokens,
        ex2.collateral_token
    FROM (
        SELECT ex1.*,
            CAST(row_number() OVER (
                PARTITION BY ex1.block_number, ex1.log_index
                ORDER BY ex1.index_set_val
            ) AS UINTEGER) AS elem_idx
        FROM (
            SELECT ps.block_number, ps.transaction_index, ps.transaction_hash,
                   ps.log_index, ps.stakeholder, ps.collateral_token,
                   ps.parent_collection_id, ps.condition_id, ps.amount,
                   CAST(unnest(CAST(json(ps.partition) AS BIGINT[])) AS BIGINT) AS index_set_val
            FROM read_parquet('{src}') ps
        ) ex1
    ) ex2
    JOIN computed_token_ids ct
        ON ct.collateral_token = ex2.collateral_token
        AND ct.parent_collection_id = ex2.parent_collection_id
        AND ct.condition_id = ex2.condition_id
        AND ct.index_set = ex2.index_set_val
    """


def _nr_split_sql(m_val: int, k_val: int) -> str:
    """NR split -> USDC row + YES token row + NO token row."""
    src = _src_path("NegRiskAdapter/position_split", m_val, k_val)
    if not src:
        return ""

    usdc = f"unhex('{USDC_E_HEX}')"
    zero = f"unhex('{ZERO32_HEX}')"

    return f"""
    -- === NR split USDC rows (sub_index=0) ===
    SELECT
        ns.block_number, ns.transaction_index, ns.transaction_hash, ns.log_index,
        CAST(0 AS UINTEGER) AS sub_index,
        'NegRiskAdapter/position_split' AS raw_source,
        ns.stakeholder AS account,
        CAST(NULL AS BLOB) AS token_id,
        ns.condition_id,
        'split' AS flow_type,
        -CAST(ns.amount AS BIGINT) AS net_usdc,
        CAST(0 AS BIGINT) AS net_tokens,
        {usdc} AS collateral_token
    FROM read_parquet('{src}') ns

    UNION ALL

    -- === NR split YES token rows (sub_index=1) ===
    SELECT
        ns.block_number, ns.transaction_index, ns.transaction_hash, ns.log_index,
        CAST(1 AS UINTEGER) AS sub_index,
        'NegRiskAdapter/position_split' AS raw_source,
        ns.stakeholder AS account,
        ct_yes.token_id,
        ns.condition_id,
        'split' AS flow_type,
        CAST(0 AS BIGINT) AS net_usdc,
        CAST(ns.amount AS BIGINT) AS net_tokens,
        {usdc} AS collateral_token
    FROM read_parquet('{src}') ns
    JOIN computed_token_ids ct_yes
        ON ct_yes.collateral_token = {usdc}
        AND ct_yes.parent_collection_id = {zero}
        AND ct_yes.condition_id = ns.condition_id
        AND ct_yes.index_set = 1

    UNION ALL

    -- === NR split NO token rows (sub_index=2) ===
    SELECT
        ns.block_number, ns.transaction_index, ns.transaction_hash, ns.log_index,
        CAST(2 AS UINTEGER) AS sub_index,
        'NegRiskAdapter/position_split' AS raw_source,
        ns.stakeholder AS account,
        ct_no.token_id,
        ns.condition_id,
        'split' AS flow_type,
        CAST(0 AS BIGINT) AS net_usdc,
        CAST(ns.amount AS BIGINT) AS net_tokens,
        {usdc} AS collateral_token
    FROM read_parquet('{src}') ns
    JOIN computed_token_ids ct_no
        ON ct_no.collateral_token = {usdc}
        AND ct_no.parent_collection_id = {zero}
        AND ct_no.condition_id = ns.condition_id
        AND ct_no.index_set = 2
    """


def _ct_merge_sql(m_val: int, k_val: int) -> str:
    """CT merge -> USDC row + per-partition-element token rows (inverse of split)."""
    src = _src_path("ConditionalTokens/positions_merge", m_val, k_val)
    if not src:
        return ""

    return f"""
    -- === CT merge USDC rows (sub_index=0) ===
    SELECT
        pm.block_number, pm.transaction_index, pm.transaction_hash, pm.log_index,
        CAST(0 AS UINTEGER) AS sub_index,
        'ConditionalTokens/positions_merge' AS raw_source,
        pm.stakeholder AS account,
        CAST(NULL AS BLOB) AS token_id,
        pm.condition_id,
        'merge' AS flow_type,
        CAST(pm.amount AS BIGINT) AS net_usdc,
        CAST(0 AS BIGINT) AS net_tokens,
        pm.collateral_token
    FROM read_parquet('{src}') pm

    UNION ALL

    -- === CT merge token rows (sub_index=1..) ===
    SELECT
        ex2.block_number, ex2.transaction_index, ex2.transaction_hash, ex2.log_index,
        CAST(ex2.elem_idx AS UINTEGER) AS sub_index,
        'ConditionalTokens/positions_merge' AS raw_source,
        ex2.stakeholder AS account,
        ct.token_id,
        ex2.condition_id,
        'merge' AS flow_type,
        CAST(0 AS BIGINT) AS net_usdc,
        -CAST(ex2.amount AS BIGINT) AS net_tokens,
        ex2.collateral_token
    FROM (
        SELECT ex1.*,
            CAST(row_number() OVER (
                PARTITION BY ex1.block_number, ex1.log_index
                ORDER BY ex1.index_set_val
            ) AS UINTEGER) AS elem_idx
        FROM (
            SELECT pm.block_number, pm.transaction_index, pm.transaction_hash,
                   pm.log_index, pm.stakeholder, pm.collateral_token,
                   pm.parent_collection_id, pm.condition_id, pm.amount,
                   CAST(unnest(CAST(json(pm.partition) AS BIGINT[])) AS BIGINT) AS index_set_val
            FROM read_parquet('{src}') pm
        ) ex1
    ) ex2
    JOIN computed_token_ids ct
        ON ct.collateral_token = ex2.collateral_token
        AND ct.parent_collection_id = ex2.parent_collection_id
        AND ct.condition_id = ex2.condition_id
        AND ct.index_set = ex2.index_set_val
    """


def _nr_merge_sql(m_val: int, k_val: int) -> str:
    """NR merge -> USDC row + YES/NO token rows (inverse of NR split)."""
    src = _src_path("NegRiskAdapter/positions_merge", m_val, k_val)
    if not src:
        return ""

    usdc = f"unhex('{USDC_E_HEX}')"
    zero = f"unhex('{ZERO32_HEX}')"

    return f"""
    -- === NR merge USDC rows (sub_index=0) ===
    SELECT
        nm.block_number, nm.transaction_index, nm.transaction_hash, nm.log_index,
        CAST(0 AS UINTEGER) AS sub_index,
        'NegRiskAdapter/positions_merge' AS raw_source,
        nm.stakeholder AS account,
        CAST(NULL AS BLOB) AS token_id,
        nm.condition_id,
        'merge' AS flow_type,
        CAST(nm.amount AS BIGINT) AS net_usdc,
        CAST(0 AS BIGINT) AS net_tokens,
        {usdc} AS collateral_token
    FROM read_parquet('{src}') nm

    UNION ALL

    -- === NR merge YES token rows (sub_index=1) ===
    SELECT
        nm.block_number, nm.transaction_index, nm.transaction_hash, nm.log_index,
        CAST(1 AS UINTEGER) AS sub_index,
        'NegRiskAdapter/positions_merge' AS raw_source,
        nm.stakeholder AS account,
        ct_yes.token_id,
        nm.condition_id,
        'merge' AS flow_type,
        CAST(0 AS BIGINT) AS net_usdc,
        -CAST(nm.amount AS BIGINT) AS net_tokens,
        {usdc} AS collateral_token
    FROM read_parquet('{src}') nm
    JOIN computed_token_ids ct_yes
        ON ct_yes.collateral_token = {usdc}
        AND ct_yes.parent_collection_id = {zero}
        AND ct_yes.condition_id = nm.condition_id
        AND ct_yes.index_set = 1

    UNION ALL

    -- === NR merge NO token rows (sub_index=2) ===
    SELECT
        nm.block_number, nm.transaction_index, nm.transaction_hash, nm.log_index,
        CAST(2 AS UINTEGER) AS sub_index,
        'NegRiskAdapter/positions_merge' AS raw_source,
        nm.stakeholder AS account,
        ct_no.token_id,
        nm.condition_id,
        'merge' AS flow_type,
        CAST(0 AS BIGINT) AS net_usdc,
        -CAST(nm.amount AS BIGINT) AS net_tokens,
        {usdc} AS collateral_token
    FROM read_parquet('{src}') nm
    JOIN computed_token_ids ct_no
        ON ct_no.collateral_token = {usdc}
        AND ct_no.parent_collection_id = {zero}
        AND ct_no.condition_id = nm.condition_id
        AND ct_no.index_set = 2
    """


def _ct_redeem_sql(m_val: int, k_val: int) -> str:
    """CT redeem -> USDC row + per-index-set token rows (net_tokens=NULL)."""
    src = _src_path("ConditionalTokens/payout_redemption", m_val, k_val)
    if not src:
        return ""

    return f"""
    -- === CT redeem USDC rows (sub_index=0) ===
    SELECT
        pr.block_number, pr.transaction_index, pr.transaction_hash, pr.log_index,
        CAST(0 AS UINTEGER) AS sub_index,
        'ConditionalTokens/payout_redemption' AS raw_source,
        pr.redeemer AS account,
        CAST(NULL AS BLOB) AS token_id,
        pr.condition_id,
        'redeem' AS flow_type,
        CAST(pr.payout AS BIGINT) AS net_usdc,
        CAST(0 AS BIGINT) AS net_tokens,
        pr.collateral_token
    FROM read_parquet('{src}') pr

    UNION ALL

    -- === CT redeem token rows (sub_index=1.., net_tokens=NULL) ===
    SELECT
        ex2.block_number, ex2.transaction_index, ex2.transaction_hash, ex2.log_index,
        CAST(ex2.elem_idx AS UINTEGER) AS sub_index,
        'ConditionalTokens/payout_redemption' AS raw_source,
        ex2.redeemer AS account,
        ct.token_id,
        ex2.condition_id,
        'redeem' AS flow_type,
        CAST(0 AS BIGINT) AS net_usdc,
        CAST(NULL AS BIGINT) AS net_tokens,
        ex2.collateral_token
    FROM (
        SELECT ex1.*,
            CAST(row_number() OVER (
                PARTITION BY ex1.block_number, ex1.log_index
                ORDER BY ex1.index_set_val
            ) AS UINTEGER) AS elem_idx
        FROM (
            SELECT pr.block_number, pr.transaction_index, pr.transaction_hash,
                   pr.log_index, pr.redeemer, pr.collateral_token,
                   pr.parent_collection_id, pr.condition_id,
                   CAST(unnest(CAST(json(pr.index_sets) AS BIGINT[])) AS BIGINT) AS index_set_val
            FROM read_parquet('{src}') pr
        ) ex1
    ) ex2
    JOIN computed_token_ids ct
        ON ct.collateral_token = ex2.collateral_token
        AND ct.parent_collection_id = ex2.parent_collection_id
        AND ct.condition_id = ex2.condition_id
        AND ct.index_set = ex2.index_set_val
    """


def _nr_redeem_sql(m_val: int, k_val: int) -> str:
    """NR redeem -> USDC row + per-nonzero-amount token rows."""
    src = _src_path("NegRiskAdapter/payout_redemption", m_val, k_val)
    if not src:
        return ""

    usdc = f"unhex('{USDC_E_HEX}')"
    zero = f"unhex('{ZERO32_HEX}')"

    return f"""
    -- === NR redeem USDC rows (sub_index=0) ===
    SELECT
        nr.block_number, nr.transaction_index, nr.transaction_hash, nr.log_index,
        CAST(0 AS UINTEGER) AS sub_index,
        'NegRiskAdapter/payout_redemption' AS raw_source,
        nr.redeemer AS account,
        CAST(NULL AS BLOB) AS token_id,
        nr.condition_id,
        'redeem' AS flow_type,
        CAST(nr.payout AS BIGINT) AS net_usdc,
        CAST(0 AS BIGINT) AS net_tokens,
        {usdc} AS collateral_token
    FROM read_parquet('{src}') nr

    UNION ALL

    -- === NR redeem token rows (only non-zero amounts) ===
    SELECT
        ex.block_number, ex.transaction_index, ex.transaction_hash, ex.log_index,
        CAST(ex.elem_idx AS UINTEGER) AS sub_index,
        'NegRiskAdapter/payout_redemption' AS raw_source,
        ex.redeemer AS account,
        ct.token_id,
        ex.condition_id,
        'redeem' AS flow_type,
        CAST(0 AS BIGINT) AS net_usdc,
        -CAST(ex.amount_val AS BIGINT) AS net_tokens,
        {usdc} AS collateral_token
    FROM (
        -- NegRisk: amounts[0] = outcome 0 -> index_set 1 (YES token),
        --          amounts[1] = outcome 1 -> index_set 2 (NO token)
        SELECT ex1.*,
            CAST(row_number() OVER (
                PARTITION BY ex1.block_number, ex1.log_index
            ) AS UINTEGER) AS elem_idx,
            CAST(
                1 << (row_number() OVER (
                    PARTITION BY ex1.block_number, ex1.log_index
                ) - 1) AS BIGINT
            ) AS index_set_val
        FROM (
            SELECT nr.block_number, nr.transaction_index, nr.transaction_hash,
                   nr.log_index, nr.redeemer, nr.condition_id,
                   CAST(unnest(CAST(json(nr.amounts) AS BIGINT[])) AS BIGINT) AS amount_val
            FROM read_parquet('{src}') nr
        ) ex1
    ) ex
    JOIN computed_token_ids ct
        ON ct.collateral_token = {usdc}
        AND ct.parent_collection_id = {zero}
        AND ct.condition_id = ex.condition_id
        AND ct.index_set = ex.index_set_val
    WHERE ex.amount_val != 0
    """


def _convert_rows_python(
    con: duckdb.DuckDBPyConnection,
    m_val: int,
    k_val: int,
) -> list[dict]:
    """Process positions_converted events in Python (rare, complex lookup)."""
    src = _src_path("NegRiskAdapter/positions_converted", m_val, k_val)
    if not src:
        return []

    events = con.execute(f"""
        SELECT block_number, transaction_index, transaction_hash, log_index,
               stakeholder, market_id, index_set, amount
        FROM read_parquet('{src}')
    """).fetchall()

    if not events:
        return []

    market_ids = {bytes(ev[5]) for ev in events}

    market_conditions: dict[bytes, list[tuple[int, bytes]]] = {}
    qtc_rows = con.execute("SELECT market_id, index_val, condition_id FROM question_to_condition").fetchall()
    for mid, idx_val, cid in qtc_rows:
        mid_b = bytes(mid)
        if mid_b in market_ids:
            market_conditions.setdefault(mid_b, []).append((int(idx_val), bytes(cid)))

    rows: list[dict] = []
    for ev in events:
        block_number = int(ev[0])
        tx_index = int(ev[1])
        tx_hash = bytes(ev[2])
        log_index = int(ev[3])
        stakeholder = bytes(ev[4])
        market_id = bytes(ev[5])
        index_set = int(ev[6])
        amount = int(ev[7])

        conditions = market_conditions.get(market_id, [])
        if not conditions:
            raise ValueError(
                f"No conditions found for market_id {market_id.hex()} "
                f"in block {block_number}, log_index {log_index}."
            )

        conditions.sort(key=lambda x: x[0])

        for sub_idx, (idx_val, cond_id) in enumerate(conditions):
            bit_set = (index_set & (1 << idx_val)) != 0

            if bit_set:
                token_id = compute_token_id(USDC_E, ZERO32, cond_id, 1)
                net_tokens = amount
            else:
                token_id = compute_token_id(USDC_E, ZERO32, cond_id, 2)
                net_tokens = -amount

            rows.append({
                "block_number": block_number,
                "transaction_index": tx_index,
                "transaction_hash": tx_hash,
                "log_index": log_index,
                "sub_index": sub_idx,
                "raw_source": "NegRiskAdapter/positions_converted",
                "account": stakeholder,
                "token_id": token_id,
                "condition_id": cond_id,
                "flow_type": "convert",
                "net_usdc": 0,
                "net_tokens": net_tokens,
                "price_1e18": None,
                "collateral_token": USDC_E,
            })

    return rows


def _convert_rows_to_arrow(rows: list[dict]) -> pa.Table | None:
    """Convert convert-row dicts to an Arrow table matching _OUTPUT_SCHEMA."""
    if not rows:
        return None

    arrays = {
        "block_number":      pa.array([r["block_number"] for r in rows], type=pa.uint32()),
        "transaction_index": pa.array([r["transaction_index"] for r in rows], type=pa.uint32()),
        "transaction_hash":  pa.array([r["transaction_hash"] for r in rows], type=pa.binary(32)),
        "log_index":         pa.array([r["log_index"] for r in rows], type=pa.uint32()),
        "sub_index":         pa.array([r["sub_index"] for r in rows], type=pa.uint32()),
        "raw_source":        pa.array([r["raw_source"] for r in rows], type=pa.string()),
        "account":           pa.array([r["account"] for r in rows], type=pa.binary(20)),
        "token_id":          pa.array([r["token_id"] for r in rows], type=pa.binary(32)),
        "condition_id":      pa.array([r["condition_id"] for r in rows], type=pa.binary(32)),
        "flow_type":         pa.array([r["flow_type"] for r in rows], type=pa.string()),
        "net_usdc":          pa.array([r["net_usdc"] for r in rows], type=pa.int64()),
        "net_tokens":        pa.array([r["net_tokens"] for r in rows], type=pa.int64()),
        "price_1e18":        pa.array([r["price_1e18"] for r in rows], type=pa.uint64()),
        "collateral_token":  pa.array([r["collateral_token"] for r in rows], type=pa.binary(20)),
    }
    return pa.table(arrays, schema=_OUTPUT_SCHEMA)


def _compute_input_hashes(m_val: int, k_val: int) -> dict[str, str]:
    """Return input file hashes used for a 10K partition metadata record."""
    source_tables = [
        "CTFExchange/order_filled",
        "NegRiskCtfExchange/order_filled",
        "CTFExchangeV2/order_filled",
        "NegRiskCtfExchangeV2/order_filled",
        "ConditionalTokens/position_split",
        "ConditionalTokens/positions_merge",
        "ConditionalTokens/payout_redemption",
        "NegRiskAdapter/position_split",
        "NegRiskAdapter/positions_merge",
        "NegRiskAdapter/payout_redemption",
        "NegRiskAdapter/positions_converted",
        "FeeModuleCTF/fee_refunded",
        "FeeModuleNegRisk/fee_refunded",
    ]

    hashes: dict[str, str] = {}
    for table in source_tables:
        path = _src_path(table, m_val, k_val)
        if not path:
            continue
        h = parquet_content_hash(Path(path))
        rel = os.path.relpath(path, RAW)
        hashes[rel] = h

    return hashes


def process_chunk(
    con: duckdb.DuckDBPyConnection,
    m_val: int,
    k_val: int,
    *,
    log: logging.Logger,
) -> tuple[int, dict[str, str]]:
    """Process one 10K partition: generate rows, validate, and write atomically."""
    chunk_dir = Path(OUT_DIR) / partition_dir(k_val)
    chunk_dir.parent.mkdir(parents=True, exist_ok=True)

    # Load token ID map (global, not per-1M).
    # The map is loaded once at startup in load_global_lookups(); here we ensure the temp table exists.
    if "computed_token_ids" not in [r[0] for r in con.execute("SHOW TABLES").fetchall()]:
        _load_token_id_map(con, log)

    sql_parts: list[str] = []

    # Trades (V1)
    trade_ctf = _trade_sql(
        con,
        "CTFExchange/order_filled",
        CTF_EXCHANGE_HEX,
        "FeeModuleCTF/fee_refunded",
        m_val,
        k_val,
        log,
    )
    if trade_ctf:
        sql_parts.append(trade_ctf)

    trade_nr = _trade_sql(
        con,
        "NegRiskCtfExchange/order_filled",
        NEGRISK_EXCHANGE_HEX,
        "FeeModuleNegRisk/fee_refunded",
        m_val,
        k_val,
        log,
    )
    if trade_nr:
        sql_parts.append(trade_nr)

    # Trades (V2)
    trade_ctf_v2 = _trade_sql_v2(con, "CTFExchangeV2/order_filled", CTF_EXCHANGE_V2_HEX, m_val, k_val)
    if trade_ctf_v2:
        sql_parts.append(trade_ctf_v2)

    trade_nr_v2 = _trade_sql_v2(
        con,
        "NegRiskCtfExchangeV2/order_filled",
        NEGRISK_EXCHANGE_V2_HEX,
        m_val,
        k_val,
    )
    if trade_nr_v2:
        sql_parts.append(trade_nr_v2)

    # Splits / merges / redeems
    for part in (
        _ct_split_sql(m_val, k_val),
        _nr_split_sql(m_val, k_val),
        _ct_merge_sql(m_val, k_val),
        _nr_merge_sql(m_val, k_val),
        _ct_redeem_sql(m_val, k_val),
        _nr_redeem_sql(m_val, k_val),
    ):
        if part:
            sql_parts.append(part)

    t0 = time.monotonic()
    if sql_parts:
        union_sql = "\n    UNION ALL\n".join(sql_parts)
        full_sql = f"""
        SELECT
            CAST(block_number AS UINTEGER) AS block_number,
            CAST(transaction_index AS UINTEGER) AS transaction_index,
            transaction_hash,
            CAST(log_index AS UINTEGER) AS log_index,
            CAST(sub_index AS UINTEGER) AS sub_index,
            raw_source,
            account,
            token_id,
            condition_id,
            flow_type,
            net_usdc,
            net_tokens,
            CASE
                WHEN flow_type IN ('trade_buy', 'trade_sell') AND net_tokens != 0
                THEN TRY_CAST(
                    CAST(ABS(net_usdc) AS HUGEINT) * 1000000000000000000 / ABS(net_tokens)
                AS UBIGINT)
                ELSE NULL
            END AS price_1e18,
            collateral_token
        FROM ({union_sql})
        ORDER BY block_number, log_index, sub_index
        """
        arrow_table = con.execute(full_sql).fetch_arrow_table().cast(_OUTPUT_SCHEMA)
    else:
        arrow_table = pa.table(
            {f.name: pa.array([], type=f.type) for f in _OUTPUT_SCHEMA},
            schema=_OUTPUT_SCHEMA,
        )

    convert_rows = _convert_rows_python(con, m_val, k_val)
    if convert_rows:
        convert_table = _convert_rows_to_arrow(convert_rows)
        if convert_table is not None:
            merged = pa.concat_tables([arrow_table, convert_table])
            con.register("_merged", merged)
            arrow_table = con.execute(
                """
                SELECT *
                FROM _merged
                ORDER BY block_number, log_index, sub_index
                """
            ).fetch_arrow_table().cast(_OUTPUT_SCHEMA)
            con.unregister("_merged")

    n = arrow_table.num_rows
    if n > 0:
        # Fail fast on duplicate grain. This indicates either a source data error or a logic error in the materializer.
        # Do not mask this by deduplicating; investigate the root cause.
        con.register("_chunk", arrow_table)
        dups = con.execute(
            "SELECT block_number, log_index, sub_index, COUNT(*), "
            "LIST(DISTINCT raw_source) AS sources "
            "FROM _chunk GROUP BY 1,2,3 HAVING COUNT(*) > 1 LIMIT 1"
        ).fetchone()
        con.unregister("_chunk")
        if dups:
            raise ValueError(
                f"Duplicate grain in 10K={k_val}: "
                f"blk={dups[0]} log={dups[1]} sub={dups[2]} cnt={dups[3]} sources={dups[4]}"
            )

        con.register("_bounds", arrow_table)
        bounds = con.execute(
            "SELECT min(net_usdc), max(net_usdc) FROM _bounds "
            "WHERE collateral_token = $1",
            [USDC_E],
        ).fetchone()
        con.unregister("_bounds")
        if bounds and bounds[0] is not None:
            min_usdc, max_usdc = bounds
            if min_usdc < -MAX_USDC or max_usdc > MAX_USDC:
                raise ValueError(
                    f"net_usdc out of range [{-MAX_USDC}, {MAX_USDC}]: "
                    f"min={min_usdc}, max={max_usdc} in 10K={k_val}"
                )

        con.register("_hex", arrow_table)
        bad_hex = con.execute("""
            SELECT 'transaction_hash' AS col FROM _hex
                WHERE octet_length(transaction_hash) != 32
            UNION ALL
            SELECT 'account' FROM _hex
                WHERE octet_length(account) != 20
            UNION ALL
            SELECT 'token_id' FROM _hex
                WHERE token_id IS NOT NULL AND octet_length(token_id) != 32
            UNION ALL
            SELECT 'condition_id' FROM _hex
                WHERE octet_length(condition_id) != 32
            UNION ALL
            SELECT 'collateral_token' FROM _hex
                WHERE octet_length(collateral_token) != 20
            LIMIT 1
        """).fetchone()
        con.unregister("_hex")
        if bad_hex:
            raise ValueError(f"BLOB length violation in 10K={k_val}: {bad_hex[0]}")

    if _stop_event.is_set():
        log.info("aborting write for 1M=%d/10K=%d due to user interrupt", m_val, k_val)
        raise KeyboardInterrupt()

    input_hashes = _compute_input_hashes(m_val, k_val)

    # Create temp location using shared atomic publish primitive
    chunk_dir = Path(OUT_DIR) / partition_dir(k_val)
    chunk_parent = chunk_dir.parent
    chunk_name = chunk_dir.name

    temp_location = create_temp_location(
        parent_dir=chunk_parent,
        final_name=chunk_name,
        temp_suffix=".tmp",
    )

    try:
        out = temp_location.path / "data.parquet"
        pq.write_table(
            arrow_table,
            str(out),
            compression="zstd",
            use_dictionary=True,
            write_statistics=True,
        )
        _write_metadata(con, temp_location.path, m_val, k_val, input_hashes, log)

        # Atomically publish to final location
        publish_atomically(temp_location, allow_overwrite=False)
    except Exception:
        # Clean up temp on any error
        cleanup_temp(temp_location)
        raise

    elapsed = time.monotonic() - t0
    log.debug("  10K=%-10d  %d rows  %.1fs", k_val, n, elapsed)
    return n, input_hashes


# ============================================================================
# bootstrap
# ============================================================================

def setup(con: duckdb.DuckDBPyConnection, log: logging.Logger) -> None:
    os.makedirs(TEMP_DIR, exist_ok=True)
    con.execute(f"SET temp_directory = '{TEMP_DIR}'")
    con.execute("SET memory_limit = '8GB'")
    con.execute("SET threads = 4")
    con.execute("SET preserve_insertion_order = false")
    # Enable DuckDB's native progress bar for long-running SQL operations
    con.execute("SET enable_progress_bar = true")
    con.execute("SET enable_progress_bar_print = true")
    con.execute("CREATE OR REPLACE TEMP TABLE _token_id_precompute (collateral_token BLOB, parent_collection_id BLOB, condition_id BLOB, index_set UINTEGER)")
    log.info("setup complete")


def cleanup_temp_partitions(log: logging.Logger) -> None:
    """Remove any incomplete temp-named partition folders on startup.
    
    Uses shared cleanup primitives to remove .tmp directories that may be
    left from interrupted runs.
    """
    out_path = Path(OUT_DIR)
    if not out_path.exists():
        return
    
    removed = cleanup_old_temp_artifacts(
        root_dir=out_path,
        temp_suffix=".tmp",
    )
    if removed > 0:
        log.info(f"Removed {removed} incomplete temp partitions")


# ============================================================================
# main
# ============================================================================

def main() -> None:
    parser = argparse.ArgumentParser(
        description=__doc__,
        formatter_class=argparse.RawDescriptionHelpFormatter,
    )
    parser.add_argument(
        "--dry-run", action="store_true",
        help="print work plan without writing any data",
    )
    parser.add_argument(
        "--sample", type=int, default=0, metavar="N",
        help="process only the first N incomplete chunks",
    )
    args = parser.parse_args()

    log = _setup_logging()
    
    # Git check before anything else
    assert_git_clean(_project_root)
    
    log.info("starting  RAW=%s  OUT_DIR=%s", RAW, OUT_DIR)
    console.print(f"starting  RAW={RAW}  OUT_DIR={OUT_DIR}")

    con = duckdb.connect()
    global _global_con
    _global_con = con

    setup(con, log)

    # SIGINT handler
    original_sigint = signal.getsignal(signal.SIGINT)

    def _handle_sigint(sig, frame):
        _stop_event.set()
        try:
            if _global_con is not None:
                _global_con.interrupt()
        except Exception:
            pass

    signal.signal(signal.SIGINT, _handle_sigint)

    # Load global lookup tables
    console.print("loading global lookup tables...")
    load_global_lookups(con, log)

    # Clean up any temp partitions from previous interrupted runs
    log.info("cleaning up incomplete partitions...")
    cleanup_temp_partitions(log)

    # Get frontier and plan partitions
    frontier = get_sunk_frontier(RAW)
    log.info(f"frontier from upstream: block {frontier}")
    console.print(f"frontier from upstream: block {frontier}")

    log.info("planning partitions from sources...")
    console.print("planning partitions from sources...")

    source_tables = [
        "CTFExchange/order_filled",
        "NegRiskCtfExchange/order_filled",
        "CTFExchangeV2/order_filled",
        "NegRiskCtfExchangeV2/order_filled",
        "ConditionalTokens/position_split",
        "ConditionalTokens/positions_merge",
        "ConditionalTokens/payout_redemption",
        "NegRiskAdapter/position_split",
        "NegRiskAdapter/positions_merge",
        "NegRiskAdapter/payout_redemption",
        "NegRiskAdapter/positions_converted",
    ]

    plan = plan_partitions(
        source_bases=["manifests"],
        source_root=RAW,
        output_root=OUT_DIR,
        frontier=frontier,
    )

    total_all = len(plan.all_partitions)
    todo = plan.todo_partitions
    done_count = len(plan.completed_partitions)
    console.print(
        f"  {total_all:,} total  |  [green]{done_count:,} already done[/green]"
        f"  |  [yellow]{len(todo):,} to process[/yellow]"
    )

    if args.sample:
        todo = todo[:args.sample]
        console.print(f"[yellow]--sample {args.sample}: limiting to first {len(todo)} chunks[/yellow]")

    if args.dry_run:
        console.print("\n[bold]Dry-run: work plan[/bold]")
        for m, k in todo[:10]:
            console.print(f"  1M={m}/10K={k}")
        if len(todo) > 10:
            console.print(f"  ... and {len(todo) - 10} more")
        console.print("\n[yellow]No data written (--dry-run)[/yellow]")
        return

    # Process partitions in order
    with Progress(
        SpinnerColumn(),
        TextColumn("[progress.description]{task.description}"),
        BarColumn(),
        TaskProgressColumn(),
        MofNCompleteColumn(),
        TimeElapsedColumn(),
        TimeRemainingColumn(),
        console=console,
    ) as progress:
        task = progress.add_task("Processing partitions", total=len(todo))
        
        for m, k in todo:
            if _stop_event.is_set():
                log.info("interrupted by user")
                break

            log.info("processing 1M=%d/10K=%d", m, k)
            try:
                row_count, input_hashes = process_chunk(con, m, k, log=log)
                progress.update(task, advance=1, description=f"1M={m}/10K={k}: {row_count} rows")
            except duckdb.InterruptException:
                log.info("Interrupted by user (SIGINT). Exiting cleanly.")
                break
            except duckdb.InvalidInputException as e:
                # Upstream parquet file has invalid encoding (corrupt data).
                # Log and skip so the run can complete; the bad partition can be
                # re-scraped later from the raw source.
                log.error("Invalid data in upstream parquet for 1M=%d/10K=%d: %s", m, k, e)
                console.print(f"[red]Skipping corrupt partition 1M={m}/10K={k}[/red]")
                progress.update(task, advance=1, description=f"1M={m}/10K={k}: SKIPPED (corrupt)")
            except Exception as e:
                log.error("Unexpected error processing 1M=%d/10K=%d: %s", m, k, e)
                raise

    console.print("[green]Complete![/green]")


if __name__ == "__main__":
    main()
