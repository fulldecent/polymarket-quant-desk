#!/usr/bin/env python3
"""
Scrape Polymarket event logs from Polygon via any JSON-RPC endpoint.

Fetches all 5 Polymarket contracts in parallel using block-range slicing.
Multiple eth_getLogs calls fly simultaneously, each covering a different
block range but all contracts at once.

Completed 10K-block partitions are automatically solidified to Parquet
and deleted from the hot database. The database stays small by design.

USAGE
    python3 scrape_events_rpc.py                      # scrape all contracts
    python3 scrape_events_rpc.py --max-calls 100      # limit API calls per run
    python3 scrape_events_rpc.py --lag-tolerance 10   # exit when within 10 blocks of head

ENVIRONMENT (all required, set in .env or export)
    RPC_DB_PATH                            path to SQLite database file
    POLYGON_CONTRACT_EVENTS_V2_DIR         output directory for Parquet files
    POLYGON_RPC_URL                        JSON-RPC endpoint URL
    POLYGON_RPC_MAX_GETLOGS_BLOCK_SPAN     max blocks per eth_getLogs
    POLYGON_RPC_MAX_REQUESTS_PER_SECOND    rate limit

SAFETY
    - Interrupt safe (Ctrl-C). Completed ranges committed to DB.
    - Resume safe: gaps in completed_ranges are re-queued on restart.
    - Idempotent: UNIQUE(block_number, log_index) prevents dupes.
    - Ranges that fail with too-many-results get split and re-queued.

REQUIREMENTS
    pip install eth_abi pycryptodome

API CONTRACT (JSON-RPC eth_getLogs)
    Endpoint: any Polygon JSON-RPC provider (e.g., dRPC, Alchemy, Infura)
    Method: eth_getLogs with address[] and topics[] filters
    Block range: fromBlock/toBlock as hex, provider-dependent max span
    Rate limit: provider-dependent (configured via POLYGON_RPC_MAX_REQUESTS_PER_SECOND)
    Assumptions:
      - Returns all matching logs in the requested block range
      - If the response is too large, the provider either:
        (a) returns an error (detected as TooManyResults, triggers range split)
        (b) truncates the JSON stream (detected as JSONDecodeError, triggers split)
      - Response time >6s signals the range is too wide (triggers adaptive span reduction)
      - Response time >10s is treated as a timeout error
      - Logs are returned in block order within each response
      - Each log includes blockNumber, transactionHash, logIndex, topics, data
      - Logs do NOT include block timestamp (not stored in our DB)
"""

import argparse
import gzip
import http.client
import itertools
import json
import os
import shutil
import signal
import sqlite3
import ssl
import sys
import threading
import time
import urllib.parse
import urllib.request
import concurrent.futures
from concurrent.futures import ThreadPoolExecutor, as_completed

from pathlib import Path

from dotenv import load_dotenv
from eth_abi import decode as abi_decode

sys.path.insert(0, str(Path(__file__).resolve().parent))
from lib.v2_schemas import (
    SQLITE_TO_V2,
    is_mixed_sqlite_table,
    all_sqlite_table_names,
    _10K,
)
from lib.parquet_writer import (
    export_partition,
    cleanup_orphaned_temp_dirs,
    find_existing_partitions,
    dest_path_for_partition,
    verify_parquet,
)

# ---------------------------------------------------------------------------
# Configuration
# ---------------------------------------------------------------------------

_script_dir = os.path.dirname(os.path.abspath(__file__))
_project_root = Path(__file__).resolve().parent.parent

# Load .env from project root
load_dotenv(_project_root / ".env")

_REQUIRED_ENV = [
    "RPC_DB_PATH",
    "POLYGON_CONTRACT_EVENTS_V2_DIR",
    "POLYGON_RPC_URL",
    "POLYGON_RPC_MAX_GETLOGS_BLOCK_SPAN",
    "POLYGON_RPC_MAX_REQUESTS_PER_SECOND",
]
_missing = [v for v in _REQUIRED_ENV if not os.environ.get(v)]
if _missing:
    sys.exit(
        f"Missing required environment variables: {', '.join(_missing)}\n"
        "Copy scraping/.env.example to scraping/.env and fill in all values."
    )

DB_PATH = os.environ["RPC_DB_PATH"]
if not os.path.isabs(DB_PATH):
    DB_PATH = os.path.join(_script_dir, DB_PATH)
OUTPUT_DIR = os.environ["POLYGON_CONTRACT_EVENTS_V2_DIR"]
RPC_URL = os.environ["POLYGON_RPC_URL"]
MAX_BLOCK_SPAN = int(os.environ["POLYGON_RPC_MAX_GETLOGS_BLOCK_SPAN"])
MAX_CALLS_PER_SEC = int(os.environ["POLYGON_RPC_MAX_REQUESTS_PER_SECOND"])

MAX_RESULTS_LIMIT = 10000  # most providers cap at 10k logs per call

# Logical start of the block range for progress tracking. Set to 0 so
# that downstream ETL can partition blocks cleanly from genesis.
# (ConditionalTokens deployed at block 4,023,686 — no events exist before.)
EARLIEST_BLOCK = 0

# RPC call settings
RPC_CALL_TIMEOUT = 30            # seconds for eth_getLogs calls
RPC_BLOCK_NUMBER_TIMEOUT = 5     # seconds for eth_blockNumber calls
RPC_MAX_RETRIES = 3              # max retry attempts per RPC call
RETRY_BACKOFF_BASE = 0.5         # base seconds for exponential backoff
SLOW_RESPONSE_THRESHOLD_MS = 15000  # count as "slow" above this

# Worker thresholds
SLOW_FETCH_THRESHOLD_SEC = 20.0   # trigger adaptive span reduction above this

# DB flush settings
FLUSH_INTERVAL = 2.0             # seconds between periodic DB flushes
FLUSH_CHUNK_SIZE = 2000          # rows per executemany batch
MAX_PENDING_ROWS = 6000          # flush when buffer exceeds this
FLUSH_WARNING_THRESHOLD_MS = 5000  # warn if a flush takes longer

# Adaptive tuning
MAX_CHUNK_RETRIES = 3
MIN_BLOCK_SPAN = 1
COUNT_SKIP_DB_SIZE = 10 * 1024**3  # skip COUNT(*) for databases > 10 GB
GROW_WORKERS_SLOW_START = 3      # fast responses before doubling workers
GROW_WORKERS_LINEAR = 20         # fast responses before +1 worker
SPAN_BUMP_UTILIZATION = 0.5      # bump span immediately when events < this × MAX_RESULTS_LIMIT

# Display
DEFAULT_TERMINAL_WIDTH = 120
POLYGON_BLOCK_TIME_SEC = 2       # approximate seconds per Polygon block
API_MS_DISPLAY_THRESHOLD = 1500  # show API latency in status when above this
FLUSH_MS_DISPLAY_THRESHOLD = 100 # show flush time in status when above this
POLL_TIMEOUT_SEC = 1.0           # futures polling interval
DRAIN_TIMEOUT_SEC = 30           # timeout when draining remaining futures

CONTRACTS = {
    "ConditionalTokens": "0x4D97DCd97eC945f40cF65F87097ACe5EA0476045",
    "CTFExchange": "0x4bFb41d5B3570DeFd03C39a9A4D8dE6Bd8B8982E",
    "NegRiskCtfExchange": "0xC5d563A36AE78145C45a50134d48A1215220f80a",
    "NegRiskAdapter": "0xd91E80cF2E7be2e162c6513ceD06f1dD0dA35296",
    "UmaCtfAdapter": "0x157Ce2d672854c848c9b79C49a8Cc6cc89176a49",
    "FeeModuleCTF": "0xE3f18aCc55091e2c48d883fc8C8413319d4Ab7b0",
    "FeeModuleNegRisk": "0xB768891e3130F6dF18214Ac804d4DB76c2C37730",
}

ALL_ADDRESSES = list(CONTRACTS.values())

# ---------------------------------------------------------------------------
# Topic0 → event name mapping
# ---------------------------------------------------------------------------

TOPIC0_MAP = {
    # ConditionalTokens
    "0xab3760c3bd2bb38b5bcf54dc79802ed67338b4cf29f3054ded67ed24661e4177": "ConditionPreparation",
    "0xb44d84d3289691f71497564b85d4233648d9dbae8cbdbb4329f301c3a0185894": "ConditionResolution",
    "0x2e6bb91f8cbcda0c93623c54d0403a43514fabc40084ec96b6d5379a74786298": "PositionSplit_CT",
    "0x6f13ca62553fcc2bcd2372180a43949c1e4cebba603901ede2f4e14f36b282ca": "PositionsMerge_CT",
    "0x2682012a4a4f1973119f1c9b90745d1bd91fa2bab387344f044cb3586864d18d": "PayoutRedemption_CT",
    # CTFExchange / NegRiskCtfExchange
    "0xd0a08e8c493f9c94f29311604c9de1b4e8c8d4c06bd0c789af57f2d65bfec0f6": "OrderFilled",
    "0x63bf4d16b7fa898ef4c4b2b6d90fd201e9c56313b65638af6088d149d2ce956c": "OrdersMatched",
    "0xacffcc86834d0f1a64b0d5a675798deed6ff0bcfc2231edd3480e7288dba7ff4": "FeeCharged",
    "0x5152abf959f6564662358c2e52b702259b78bac5ee7842a0f01937e670efcc7d": "OrderCancelled",
    "0xbc9a2432e8aeb48327246cddd6e872ef452812b4243c04e6bfb786a2cd8faf0d": "TokenRegistered",
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
    "0x566c3fbdd12dd86bb341787f6d531f79fd7ad4ce7e3ae2d15ac0ca1b601af9df": "QuestionResolved",
    "0x7981b5832932948db4e32a4a16a0f44b2ce7ff088574afb9364b313f70f82e8f": "QuestionReset",
    "0x2435a0347185933b12027c6f394a5fd9c03646dba233e956f50658719dfc0b35": "QuestionFlagged",
    "0x6edb5841a476c9c29c34a652d1a44f785fe71a6157a3da9a6a6a589a1bd2945a": "QuestionEmergencyResolved",
    # FeeModuleCTF / FeeModuleNegRisk
    "0xb608d2bf25d8b4b744ba23ce2ea9802ea955e216c064a62f42152fbf98958d24": "FeeRefunded",
}

# RPC-level topic filter: only fetch events we care about
WANTED_TOPICS = list(TOPIC0_MAP.keys())

# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------


def parse_hex(val):
    if not val or val == "0x":
        return 0
    return int(val, 16)


def hex_to_bytes(val):
    if val.startswith("0x"):
        val = val[2:]
    if len(val) % 2 != 0:
        val = "0" + val
    return bytes.fromhex(val)


def to_hex(n):
    return hex(n)


def pad_address(topic):
    """Extract an address from a 32-byte topic (lower 20 bytes), lowercase."""
    return ("0x" + topic[-40:]).lower()


def u256_str(val):
    return str(val)


def decode_data(types, data_hex):
    raw = hex_to_bytes(data_hex)
    if not raw:
        return tuple(0 for _ in types)
    return abi_decode(types, raw)


# ---------------------------------------------------------------------------
# JSON-RPC client (thread-safe, persistent connections)
# ---------------------------------------------------------------------------

_rpc_id_counter = itertools.count(1)

# Parse RPC URL once at startup for http.client
_rpc_parsed = urllib.parse.urlparse(RPC_URL)
_rpc_host = _rpc_parsed.hostname
_rpc_port = _rpc_parsed.port or (443 if _rpc_parsed.scheme == "https" else 80)
_rpc_path = _rpc_parsed.path or "/"
if _rpc_parsed.query:
    _rpc_path += "?" + _rpc_parsed.query
_rpc_is_https = _rpc_parsed.scheme == "https"
_rpc_ssl_ctx = ssl.create_default_context() if _rpc_is_https else None

# Thread-local storage for persistent HTTP connections
_thread_local = threading.local()

_RPC_HEADERS = {
    "Content-Type": "application/json",
    "User-Agent": "polymarket-scraper/1.0",
    "Connection": "keep-alive",
    "Accept-Encoding": "gzip",
}


def _get_connection(timeout):
    """Get or create a persistent HTTP(S) connection for the current thread."""
    conn = getattr(_thread_local, "conn", None)
    if conn is not None:
        # Update the live socket timeout (conn.timeout only affects new sockets)
        conn.timeout = timeout
        if conn.sock is not None:
            conn.sock.settimeout(timeout)
        return conn
    if _rpc_is_https:
        conn = http.client.HTTPSConnection(
            _rpc_host, _rpc_port, timeout=timeout, context=_rpc_ssl_ctx
        )
    else:
        conn = http.client.HTTPConnection(
            _rpc_host, _rpc_port, timeout=timeout
        )
    _thread_local.conn = conn
    return conn


def _discard_connection():
    """Close and discard the thread-local connection (e.g., after an error)."""
    conn = getattr(_thread_local, "conn", None)
    if conn is not None:
        try:
            conn.close()
        except Exception:
            pass
        _thread_local.conn = None


def rpc_call(method, params, timeout=RPC_CALL_TIMEOUT):
    """Make a JSON-RPC call using a persistent connection. Returns the 'result' field or raises."""
    call_id = next(_rpc_id_counter)
    payload = json.dumps({
        "jsonrpc": "2.0",
        "method": method,
        "params": params,
        "id": call_id,
    }).encode()
    max_retries = RPC_MAX_RETRIES
    for attempt in range(max_retries):
        if _stop_event.is_set():
            raise RuntimeError("interrupted")
        t0 = time.time()
        try:
            conn = _get_connection(timeout)
            try:
                conn.request("POST", _rpc_path, body=payload, headers=_RPC_HEADERS)
                resp = conn.getresponse()
            except (http.client.RemoteDisconnected, ConnectionResetError,
                    BrokenPipeError, OSError):
                # Server closed the keep-alive connection; reconnect once
                _discard_connection()
                conn = _get_connection(timeout)
                conn.request("POST", _rpc_path, body=payload, headers=_RPC_HEADERS)
                resp = conn.getresponse()

            status_code = resp.status
            body = resp.read()
            if resp.getheader("Content-Encoding", "").lower() == "gzip":
                body = gzip.decompress(body)

            if status_code == 400 and method == "eth_getLogs":
                raise TooManyResults(f"HTTP 400")
            if status_code >= 400:
                raise RuntimeError(f"HTTP {status_code}")

            data = json.loads(body)
            api_ms = (time.time() - t0) * 1000
            _stats["last_api_ms"] = api_ms
            if api_ms > SLOW_RESPONSE_THRESHOLD_MS:
                _stats["slow_responses"] += 1
            if "error" in data:
                err = data["error"]
                msg = err.get("message", str(err))
                # Range too wide — caller should reduce batch size
                if any(k in msg.lower() for k in (
                    "too many", "limit", "exceed", "too large",
                    "block range", "query returned", "response size",
                )):
                    raise TooManyResults(msg)
                raise RuntimeError(f"RPC error: {msg}")
            return data.get("result")
        except TooManyResults:
            raise
        except json.JSONDecodeError as e:
            _stats["api_errors"] += 1
            _discard_connection()
            # Truncated response (connection dropped mid-transfer).
            # For getLogs this means the response was too large —
            # signal the caller to split the range.
            if method == "eth_getLogs":
                raise TooManyResults(f"Truncated JSON response ({e})")
            if attempt < max_retries - 1:
                _stop_event.wait(RETRY_BACKOFF_BASE * (2 ** attempt))
            else:
                raise RuntimeError(f"RPC call {method} failed after {max_retries} attempts: {e}")
        except Exception as e:
            _stats["api_errors"] += 1
            _discard_connection()
            err_str = str(e).lower()
            is_timeout = "timed out" in err_str or "timeout" in err_str
            # Timeouts on eth_getLogs signal overload — fail fast so the
            # caller can reduce concurrency instead of hammering the RPC.
            if is_timeout and method == "eth_getLogs":
                raise RPCTimeout(str(e))
            if attempt < max_retries - 1:
                _stop_event.wait(RETRY_BACKOFF_BASE * (2 ** attempt))
            else:
                raise RuntimeError(f"RPC call {method} failed after {max_retries} attempts: {e}")


class TooManyResults(Exception):
    """Raised when eth_getLogs returns a response-too-large error."""
    pass


class RPCTimeout(Exception):
    """Raised when eth_getLogs times out, signaling the caller to reduce load."""
    pass


def get_block_number():
    result = rpc_call("eth_blockNumber", [], timeout=RPC_BLOCK_NUMBER_TIMEOUT)
    return parse_hex(result)



def get_logs(addresses, from_block, to_block):
    """
    Call eth_getLogs for a block range across one or more addresses.
    Returns list of log objects.
    Raises TooManyResults if provider rejects the range.
    """
    params = {
        "address": addresses,
        "fromBlock": to_hex(from_block),
        "toBlock": to_hex(to_block),
        "topics": [WANTED_TOPICS],
    }
    return rpc_call("eth_getLogs", [params])



# ---------------------------------------------------------------------------
# Event decoders — same logic as scrape_events_etherscan.py but adapted for
# eth_getLogs response format (slightly different field names)
# ---------------------------------------------------------------------------


def _common_fields(log, timestamp):
    """Extract fields common to every event from an eth_getLogs response."""
    return {
        "block_number": parse_hex(log["blockNumber"]),
        "transaction_hash": log["transactionHash"],
        "transaction_index": parse_hex(log["transactionIndex"]),
        "log_index": parse_hex(log["logIndex"]),
        "contract_address": log["address"],
    }


def decode_event(log, timestamp):
    topics = log.get("topics", [])
    if not topics:
        return None
    topic0 = topics[0].lower()
    event_name = TOPIC0_MAP.get(topic0)
    if event_name is None:
        return None

    decoder = EVENT_DECODERS.get(event_name)
    if decoder is None:
        return None

    try:
        return decoder(log, topics, timestamp)
    except Exception:
        return None


# --- ConditionalTokens ---

def _decode_condition_preparation(log, topics, timestamp):
    row = _common_fields(log, timestamp)
    row["condition_id"] = topics[1]
    row["oracle"] = pad_address(topics[2])
    row["question_id"] = topics[3]
    (outcome_slot_count,) = decode_data(["uint256"], log["data"])
    row["outcome_slot_count"] = outcome_slot_count
    return ("condition_preparation", row)


def _decode_condition_resolution(log, topics, timestamp):
    row = _common_fields(log, timestamp)
    row["condition_id"] = topics[1]
    row["oracle"] = pad_address(topics[2])
    row["question_id"] = topics[3]
    (outcome_slot_count, payout_numerators) = decode_data(
        ["uint256", "uint256[]"], log["data"]
    )
    row["outcome_slot_count"] = outcome_slot_count
    row["payout_numerators"] = json.dumps([str(x) for x in payout_numerators])
    return ("condition_resolution", row)


def _decode_position_split_ct(log, topics, timestamp):
    row = _common_fields(log, timestamp)
    row["stakeholder"] = pad_address(topics[1])
    row["parent_collection_id"] = topics[2]
    row["condition_id"] = topics[3]
    (collateral_token, partition, amount) = decode_data(
        ["address", "uint256[]", "uint256"], log["data"]
    )
    row["collateral_token"] = collateral_token.lower()
    row["partition"] = json.dumps([str(x) for x in partition])
    row["amount"] = u256_str(amount)
    return ("position_split", row)


def _decode_positions_merge_ct(log, topics, timestamp):
    row = _common_fields(log, timestamp)
    row["stakeholder"] = pad_address(topics[1])
    row["parent_collection_id"] = topics[2]
    row["condition_id"] = topics[3]
    (collateral_token, partition, amount) = decode_data(
        ["address", "uint256[]", "uint256"], log["data"]
    )
    row["collateral_token"] = collateral_token.lower()
    row["partition"] = json.dumps([str(x) for x in partition])
    row["amount"] = u256_str(amount)
    return ("positions_merge", row)


def _decode_payout_redemption_ct(log, topics, timestamp):
    row = _common_fields(log, timestamp)
    row["redeemer"] = pad_address(topics[1])
    row["collateral_token"] = pad_address(topics[2])
    row["parent_collection_id"] = topics[3]
    (condition_id, index_sets, payout) = decode_data(
        ["bytes32", "uint256[]", "uint256"], log["data"]
    )
    row["condition_id"] = "0x" + condition_id.hex()
    row["index_sets"] = json.dumps([str(x) for x in index_sets])
    row["payout"] = u256_str(payout)
    return ("payout_redemption", row)


# --- CTFExchange / NegRiskCtfExchange ---

def _decode_order_filled(log, topics, timestamp):
    row = _common_fields(log, timestamp)
    row["order_hash"] = topics[1]
    row["maker"] = pad_address(topics[2])
    row["taker"] = pad_address(topics[3])
    (maker_asset_id, taker_asset_id, maker_amount, taker_amount, fee) = decode_data(
        ["uint256", "uint256", "uint256", "uint256", "uint256"], log["data"]
    )
    row["maker_asset_id"] = u256_str(maker_asset_id)
    row["taker_asset_id"] = u256_str(taker_asset_id)
    row["maker_amount_filled"] = u256_str(maker_amount)
    row["taker_amount_filled"] = u256_str(taker_amount)
    row["fee"] = u256_str(fee)
    return ("order_filled", row)


def _decode_orders_matched(log, topics, timestamp):
    row = _common_fields(log, timestamp)
    row["taker_order_hash"] = topics[1]
    row["taker_order_maker"] = pad_address(topics[2])
    (maker_asset_id, taker_asset_id, maker_amount, taker_amount) = decode_data(
        ["uint256", "uint256", "uint256", "uint256"], log["data"]
    )
    row["maker_asset_id"] = u256_str(maker_asset_id)
    row["taker_asset_id"] = u256_str(taker_asset_id)
    row["maker_amount_filled"] = u256_str(maker_amount)
    row["taker_amount_filled"] = u256_str(taker_amount)
    return ("orders_matched", row)


def _decode_fee_charged(log, topics, timestamp):
    row = _common_fields(log, timestamp)
    row["receiver"] = pad_address(topics[1])
    (token_id, amount) = decode_data(["uint256", "uint256"], log["data"])
    row["token_id"] = u256_str(token_id)
    row["amount"] = u256_str(amount)
    return ("fee_charged", row)


def _decode_order_cancelled(log, topics, timestamp):
    row = _common_fields(log, timestamp)
    row["order_hash"] = topics[1]
    return ("order_cancelled", row)


def _decode_token_registered(log, topics, timestamp):
    row = _common_fields(log, timestamp)
    row["token0"] = u256_str(parse_hex(topics[1]))
    row["token1"] = u256_str(parse_hex(topics[2]))
    row["condition_id"] = topics[3]
    return ("token_registered", row)


# --- NegRiskAdapter ---

def _decode_market_prepared(log, topics, timestamp):
    row = _common_fields(log, timestamp)
    row["market_id"] = topics[1]
    row["oracle"] = pad_address(topics[2])
    (fee_bips, data_bytes) = decode_data(["uint256", "bytes"], log["data"])
    row["fee_bips"] = fee_bips
    row["data"] = "0x" + data_bytes.hex()
    return ("market_prepared", row)


def _decode_question_prepared(log, topics, timestamp):
    row = _common_fields(log, timestamp)
    row["market_id"] = topics[1]
    row["question_id"] = topics[2]
    (index_val, data_bytes) = decode_data(["uint256", "bytes"], log["data"])
    row["index_val"] = index_val
    row["data"] = "0x" + data_bytes.hex()
    return ("question_prepared", row)


def _decode_outcome_reported(log, topics, timestamp):
    row = _common_fields(log, timestamp)
    row["market_id"] = topics[1]
    row["question_id"] = topics[2]
    (outcome,) = decode_data(["bool"], log["data"])
    row["outcome"] = 1 if outcome else 0
    return ("outcome_reported", row)


def _decode_position_split_nr(log, topics, timestamp):
    row = _common_fields(log, timestamp)
    row["stakeholder"] = pad_address(topics[1])
    row["condition_id"] = topics[2]
    (amount,) = decode_data(["uint256"], log["data"])
    row["amount"] = u256_str(amount)
    return ("neg_risk_position_split", row)


def _decode_positions_merge_nr(log, topics, timestamp):
    row = _common_fields(log, timestamp)
    row["stakeholder"] = pad_address(topics[1])
    row["condition_id"] = topics[2]
    (amount,) = decode_data(["uint256"], log["data"])
    row["amount"] = u256_str(amount)
    return ("neg_risk_positions_merge", row)


def _decode_positions_converted(log, topics, timestamp):
    row = _common_fields(log, timestamp)
    row["stakeholder"] = pad_address(topics[1])
    row["market_id"] = topics[2]
    row["index_set"] = u256_str(parse_hex(topics[3]))
    (amount,) = decode_data(["uint256"], log["data"])
    row["amount"] = u256_str(amount)
    return ("positions_converted", row)


def _decode_payout_redemption_nr(log, topics, timestamp):
    row = _common_fields(log, timestamp)
    row["redeemer"] = pad_address(topics[1])
    row["condition_id"] = topics[2]
    (amounts, payout) = decode_data(["uint256[]", "uint256"], log["data"])
    row["amounts"] = json.dumps([str(x) for x in amounts])
    row["payout"] = u256_str(payout)
    return ("neg_risk_payout_redemption", row)


# --- UmaCtfAdapter ---

def _decode_question_initialized(log, topics, timestamp):
    row = _common_fields(log, timestamp)
    row["question_id"] = topics[1]
    row["request_timestamp"] = parse_hex(topics[2])
    row["creator"] = pad_address(topics[3])
    (ancillary_data, reward_token, reward, proposal_bond) = decode_data(
        ["bytes", "address", "uint256", "uint256"], log["data"]
    )
    row["ancillary_data"] = "0x" + ancillary_data.hex()
    row["reward_token"] = reward_token.lower()
    row["reward"] = u256_str(reward)
    row["proposal_bond"] = u256_str(proposal_bond)
    return ("question_initialized", row)


def _decode_question_resolved(log, topics, timestamp):
    row = _common_fields(log, timestamp)
    row["question_id"] = topics[1]
    # settledPrice is indexed (topic[2]), not in data
    raw = parse_hex(topics[2])
    row["settled_price"] = str(raw - 2**256 if raw >= 2**255 else raw)
    (payouts,) = decode_data(["uint256[]"], log["data"])
    row["payouts"] = json.dumps([str(x) for x in payouts])
    return ("question_resolved", row)


def _decode_question_reset(log, topics, timestamp):
    row = _common_fields(log, timestamp)
    row["question_id"] = topics[1]
    return ("question_reset", row)


def _decode_question_flagged(log, topics, timestamp):
    row = _common_fields(log, timestamp)
    row["question_id"] = topics[1]
    return ("question_flagged", row)


def _decode_question_emergency_resolved(log, topics, timestamp):
    row = _common_fields(log, timestamp)
    row["question_id"] = topics[1]
    (payouts,) = decode_data(["uint256[]"], log["data"])
    row["payouts"] = json.dumps([str(x) for x in payouts])
    return ("question_emergency_resolved", row)


# --- FeeModuleCTF / FeeModuleNegRisk ---

def _decode_fee_refunded(log, topics, timestamp):
    row = _common_fields(log, timestamp)
    row["order_hash"] = topics[1]
    row["receiver"] = pad_address(topics[2])
    row["fee_charged"] = u256_str(parse_hex(topics[3]))
    (token_id, refund) = decode_data(["uint256", "uint256"], log["data"])
    row["token_id"] = u256_str(token_id)
    row["refund"] = u256_str(refund)
    return ("fee_refunded", row)


EVENT_DECODERS = {
    "ConditionPreparation": _decode_condition_preparation,
    "ConditionResolution": _decode_condition_resolution,
    "PositionSplit_CT": _decode_position_split_ct,
    "PositionsMerge_CT": _decode_positions_merge_ct,
    "PayoutRedemption_CT": _decode_payout_redemption_ct,
    "OrderFilled": _decode_order_filled,
    "OrdersMatched": _decode_orders_matched,
    "FeeCharged": _decode_fee_charged,
    "OrderCancelled": _decode_order_cancelled,
    "TokenRegistered": _decode_token_registered,
    "MarketPrepared": _decode_market_prepared,
    "QuestionPrepared": _decode_question_prepared,
    "OutcomeReported": _decode_outcome_reported,
    "PositionSplit_NR": _decode_position_split_nr,
    "PositionsMerge_NR": _decode_positions_merge_nr,
    "PositionsConverted": _decode_positions_converted,
    "PayoutRedemption_NR": _decode_payout_redemption_nr,
    "QuestionInitialized": _decode_question_initialized,
    "QuestionResolved": _decode_question_resolved,
    "QuestionReset": _decode_question_reset,
    "QuestionFlagged": _decode_question_flagged,
    "QuestionEmergencyResolved": _decode_question_emergency_resolved,
    "FeeRefunded": _decode_fee_refunded,
}


# ---------------------------------------------------------------------------
# Database
# ---------------------------------------------------------------------------


def init_db(db_path):
    """Open the database. Crashes if it does not exist.

    Create the database first with:
        sqlite3 events_rpc.db < schema_rpc.sql
    """
    if not os.path.exists(db_path):
        print(f"Database not found: {db_path}")
        print("Create it first: sqlite3 events_rpc.db < schema_rpc.sql")
        sys.exit(1)
    conn = sqlite3.connect(db_path)
    conn.execute("PRAGMA journal_mode=WAL")
    conn.execute("PRAGMA synchronous=NORMAL")

    # Ensure compound indexes exist for mixed-table solidification DELETEs
    for table in ("order_filled", "orders_matched", "fee_charged",
                  "order_cancelled", "token_registered", "fee_refunded"):
        conn.execute(
            f"CREATE INDEX IF NOT EXISTS idx_{table}_block_addr "
            f"ON {table} (block_number, contract_address)"
        )

    return conn


def insert_row(conn, table_name, row_dict):
    cols = ", ".join(row_dict.keys())
    placeholders = ", ".join(["?"] * len(row_dict))
    sql = f"INSERT OR IGNORE INTO {table_name} ({cols}) VALUES ({placeholders})"
    cursor = conn.execute(sql, list(row_dict.values()))
    return cursor.rowcount > 0


def get_completed_ranges(conn):
    """Return sorted list of (from_block, to_block) inclusive ranges."""
    return conn.execute(
        "SELECT from_block, to_block FROM completed_ranges "
        "ORDER BY from_block"
    ).fetchall()


def add_completed_range(conn, from_block, to_block):
    """Insert a completed range and merge with adjacent/overlapping ones."""
    overlapping = conn.execute(
        "SELECT from_block, to_block FROM completed_ranges "
        "WHERE from_block <= ? AND to_block >= ?",
        (to_block + 1, from_block - 1),
    ).fetchall()

    merged_from = from_block
    merged_to = to_block
    for (f, t) in overlapping:
        merged_from = min(merged_from, f)
        merged_to = max(merged_to, t)

    if overlapping:
        conn.execute(
            "DELETE FROM completed_ranges "
            "WHERE from_block <= ? AND to_block >= ?",
            (to_block + 1, from_block - 1),
        )

    conn.execute(
        "INSERT OR REPLACE INTO completed_ranges (from_block, to_block) "
        "VALUES (?, ?)",
        (merged_from, merged_to),
    )


def find_gaps(completed_ranges, start_block, end_block):
    """
    Given sorted completed ranges and a target [start_block, end_block],
    return list of (from, to) gaps that need fetching.
    """
    gaps = []
    cursor = start_block
    for (f, t) in completed_ranges:
        if f > end_block:
            break
        if t < cursor:
            continue
        if f > cursor:
            gaps.append((cursor, f - 1))
        cursor = max(cursor, t + 1)
    if cursor <= end_block:
        gaps.append((cursor, end_block))
    return gaps


def migrate_from_scrape_progress(conn):
    """
    If completed_ranges is empty but scrape_progress has data,
    migrate by treating min(last_complete_block) as fully done.
    """
    has_ranges = conn.execute(
        "SELECT COUNT(*) FROM completed_ranges"
    ).fetchone()[0]
    if has_ranges > 0:
        return

    try:
        rows = conn.execute(
            "SELECT last_complete_block FROM scrape_progress"
        ).fetchall()
    except sqlite3.OperationalError:
        return

    if not rows:
        return

    min_block = min(r[0] for r in rows)
    if min_block > EARLIEST_BLOCK:
        add_completed_range(conn, EARLIEST_BLOCK, min_block)
        conn.commit()
        print(f"Migrated progress: blocks {EARLIEST_BLOCK:,} - {min_block:,} marked complete")


# ---------------------------------------------------------------------------
# Solidification (hot SQLite → cold Parquet)
# ---------------------------------------------------------------------------


def _complete_10k_partitions(completed_ranges):
    """Return sorted set of 10K partition starts fully covered by completed_ranges."""
    partitions = set()
    for from_b, to_b in completed_ranges:
        # Find first 10K boundary at or after from_b
        first = ((from_b + _10K - 1) // _10K) * _10K
        # Find last 10K boundary that ends within this range
        for p in range(first, to_b - _10K + 2, _10K):
            if p + _10K - 1 <= to_b:
                partitions.add(p)
    return sorted(partitions)


def _tables_with_rows_in_range(conn, block_start, block_end):
    """Return list of sqlite table names that have rows in the given block range."""
    tables = []
    for table_name in all_sqlite_table_names():
        try:
            count = conn.execute(
                f"SELECT COUNT(*) FROM {table_name} "
                f"WHERE block_number >= ? AND block_number <= ?",
                (block_start, block_end),
            ).fetchone()[0]
            if count > 0:
                tables.append(table_name)
        except sqlite3.OperationalError:
            pass
    return tables


def solidify_partition(conn, block_start, output_dir, print_fn=print):
    """Solidify one 10K partition: write Parquet for each target, then delete.

    For each SQLite table with data in the range, writes one Parquet file
    per (contract, event) target. Mixed tables produce multiple files.
    After all Parquet files are written and verified, deletes the SQLite
    rows in a single transaction.

    Returns (num_files_written, total_rows, elapsed_seconds).
    """
    block_end = block_start + _10K - 1
    t0 = time.time()
    files_written = 0
    total_rows = 0
    delete_ops = []  # (table_name, contract_or_None) to delete after all writes

    tables = _tables_with_rows_in_range(conn, block_start, block_end)
    if not tables:
        return 0, 0, 0

    for sqlite_table in tables:
        targets = SQLITE_TO_V2.get(sqlite_table, [])
        for contract, event in targets:
            rows, _ = export_partition(
                conn, sqlite_table, contract, event,
                block_start, block_end, output_dir,
            )
            if rows == -1:
                # Already exists — still need to delete SQLite rows
                delete_ops.append((sqlite_table, contract))
                continue
            if rows > 0:
                # Verify
                parquet_path = os.path.join(
                    dest_path_for_partition(output_dir, contract, event, block_start),
                    "data.parquet",
                )
                verified = verify_parquet(parquet_path)
                if verified != rows:
                    print_fn(
                        f"  SOLIDIFY ERROR: {contract}/{event} blocks "
                        f"{block_start}-{block_end}: wrote {rows} but "
                        f"verified {verified}. Skipping delete."
                    )
                    continue
            files_written += 1
            total_rows += max(rows, 0)
            delete_ops.append((sqlite_table, contract))

    # Delete from SQLite in one transaction
    if delete_ops:
        for sqlite_table, contract in delete_ops:
            if is_mixed_sqlite_table(sqlite_table):
                conn.execute(
                    f"DELETE FROM {sqlite_table} "
                    f"WHERE block_number >= ? AND block_number <= ? "
                    f"AND contract_address = ?",
                    (block_start, block_end,
                     CONTRACTS[contract].lower()),
                )
            else:
                conn.execute(
                    f"DELETE FROM {sqlite_table} "
                    f"WHERE block_number >= ? AND block_number <= ?",
                    (block_start, block_end),
                )
        conn.commit()

    elapsed = time.time() - t0
    return files_written, total_rows, elapsed


def solidify_all_ready(conn, output_dir, print_fn=print):
    """Solidify all complete 10K partitions. Returns count solidified."""
    completed = get_completed_ranges(conn)
    ready = _complete_10k_partitions(completed)
    if not ready:
        return 0

    count = 0
    for block_start in ready:
        block_end = block_start + _10K - 1
        tables = _tables_with_rows_in_range(conn, block_start, block_end)
        if not tables:
            continue
        files, rows, elapsed = solidify_partition(
            conn, block_start, output_dir, print_fn=print_fn,
        )
        if files > 0 or rows == 0:
            count += 1
            print_fn(
                f"  solidified 10K={block_start:,}: "
                f"{files} files, {rows:,} rows ({elapsed:.1f}s)"
            )
    return count


# ---------------------------------------------------------------------------
# Rate limiter (thread-safe)
# ---------------------------------------------------------------------------


# Global stop event — set on Ctrl+C so threads can exit quickly
_stop_event = threading.Event()

# Separate event to stop the heartbeat thread (set on both normal exit
# and interrupt, unlike _stop_event which is interrupt-only).
_heartbeat_stop = threading.Event()

# Live telemetry counters (updated by workers, read by main loop)
_stats = {
    "api_errors": 0,       # total RPC call failures
    "slow_responses": 0,   # RPC calls that took >5s
    "last_api_ms": 0,      # most recent RPC response time in ms
    "last_flush_ms": 0,    # most recent DB flush time in ms
    "flush_rows": 0,       # rows written in last flush
}


class RateLimiter:
    def __init__(self, max_calls_per_sec):
        self.max_calls_per_sec = max_calls_per_sec
        self.start_time = None
        self.total_calls = 0
        self._lock = threading.Lock()

    def wait(self, num_calls=1):
        with self._lock:
            if self.start_time is None:
                self.start_time = time.time()
            required_elapsed = (self.total_calls + num_calls) / self.max_calls_per_sec
            actual_elapsed = time.time() - self.start_time
            sleep_time = required_elapsed - actual_elapsed
            self.total_calls += num_calls
        # Sleep in small increments so we can bail on Ctrl+C
        while sleep_time > 0 and not _stop_event.is_set():
            time.sleep(min(sleep_time, 0.1))
            sleep_time -= 0.1


# ---------------------------------------------------------------------------
# Worker function (runs in thread pool)
# ---------------------------------------------------------------------------


# Map thread ident → stable worker number (assigned on first sight)
_worker_ids = {}
_worker_id_lock = threading.Lock()
_worker_id_counter = itertools.count(1)


def _get_worker_id():
    tid = threading.get_ident()
    with _worker_id_lock:
        if tid not in _worker_ids:
            _worker_ids[tid] = next(_worker_id_counter)
        return _worker_ids[tid]


def fetch_range(from_block, to_block, rate_limiter):
    """
    Fetch logs for a block range across all contracts.
    Returns one of:
        ("ok", from_block, to_block, decoded_rows, raw_log_count, worker_id, elapsed)
        ("slow", from_block, to_block, decoded_rows, raw_log_count, worker_id, elapsed)
        ("split", from_block, to_block, worker_id, elapsed)
        ("timeout", from_block, to_block, worker_id, elapsed)
        ("error", from_block, to_block, error_msg, worker_id, elapsed)
    """
    wid = _get_worker_id()
    if _stop_event.is_set():
        return ("error", from_block, to_block, "interrupted", wid, 0.0)
    rate_limiter.wait()
    t0 = time.time()
    try:
        logs = get_logs(ALL_ADDRESSES, from_block, to_block)
    except TooManyResults:
        return ("split", from_block, to_block, wid, time.time() - t0)
    except RPCTimeout:
        return ("timeout", from_block, to_block, wid, time.time() - t0)
    except RuntimeError as e:
        return ("error", from_block, to_block, str(e), wid, time.time() - t0)

    elapsed = time.time() - t0
    raw_log_count = len(logs)
    decoded = []
    for log in logs:
        result = decode_event(log, 0)
        if result is not None:
            decoded.append(result)

    if elapsed > SLOW_FETCH_THRESHOLD_SEC:
        return ("slow", from_block, to_block, decoded, raw_log_count, wid, elapsed)
    return ("ok", from_block, to_block, decoded, raw_log_count, wid, elapsed)


# ---------------------------------------------------------------------------
# Main
# ---------------------------------------------------------------------------


def format_duration(seconds):
    h = int(seconds) // 3600
    m = (int(seconds) % 3600) // 60
    s = int(seconds) % 60
    return f"{h:02d}:{m:02d}:{s:02d}"


def _file_size_str(path):
    try:
        size = os.path.getsize(path)
    except OSError:
        return "?"
    for unit in ("B", "KB", "MB", "GB"):
        if size < 1024:
            return f"{size:.1f} {unit}" if unit != "B" else f"{size} {unit}"
        size /= 1024
    return f"{size:.1f} TB"


def _blocks_behind_str(blocks):
    """Approximate wall-clock time for a number of Polygon blocks (~2s/block)."""
    seconds = blocks * POLYGON_BLOCK_TIME_SEC
    if seconds < 120:
        return f"{seconds:.0f}s"
    if seconds < 7200:
        return f"{seconds / 60:.0f}m"
    if seconds < 172800:
        return f"{seconds / 3600:.1f}h"
    return f"{seconds / 86400:.1f}d"


# ---------------------------------------------------------------------------
# Status line / heartbeat infrastructure
# ---------------------------------------------------------------------------

# Shared live state — main thread writes, heartbeat thread reads.
# CPython dict updates are atomic enough for our needs.
_live = {}
_print_lock = threading.Lock()


def _term_width():
    try:
        return shutil.get_terminal_size().columns
    except Exception:
        return DEFAULT_TERMINAL_WIDTH


def _print_line(text):
    """Overwrite the current terminal line."""
    tw = _term_width()
    with _print_lock:
        print(f"\r{text}".ljust(tw)[:tw], end="", flush=True)


def _print_msg(text):
    """Print a message on its own line, then repaint the status line."""
    tw = _term_width()
    with _print_lock:
        print(f"\r{' ' * tw}\r{text}", flush=True)
        # Immediately repaint the status line so it doesn't stay blank
        # until the next heartbeat tick.
        status = _build_status_line()
        if status:
            print(f"\r{status}".ljust(tw)[:tw], end="", flush=True)


def _build_status_line():
    """Build the status line string from shared _live dict."""
    s = _live
    run_start = s.get("run_start", 0)
    if not run_start:
        return ""
    elapsed = time.time() - run_start

    bd = s.get("blocks_done", 0)
    tb = s.get("total_blocks", 1)
    ok = s.get("ok", 0)

    # rates
    blk_s = bd / elapsed if elapsed > 1 and bd > 0 else 0
    ev_s = ok / elapsed if elapsed > 1 else 0

    # ETA
    remaining_blks = tb - bd
    if blk_s > 0:
        eta = format_duration(remaining_blks / blk_s)
    else:
        eta = "--:--:--"

    # request rate
    calls = s.get("calls", 0)
    req_s = calls / elapsed if elapsed > 1 and calls > 0 else 0

    parts = [
        f"[{format_duration(elapsed)}]",
        f"-{remaining_blks:,} blks",
        f"{blk_s:,.0f} blk/s",
        f"{ev_s:,.0f} ev/s",
        f"{req_s:,.0f} req/s",
        f"ETA {eta}",
    ]
    prc = s.get("pending_rows", 0)
    if prc:
        parts.append(f"buf:{prc:,}")
    api_ms = _stats.get("last_api_ms", 0)
    if api_ms > API_MS_DISPLAY_THRESHOLD:
        parts.append(f"api:{api_ms/1000:.1f}s")
    flush_ms = _stats.get("last_flush_ms", 0)
    if flush_ms > FLUSH_MS_DISPLAY_THRESHOLD:
        parts.append(f"db:{flush_ms:.0f}ms")
    db_wait = s.get("db_wait", 0)
    if db_wait > 1:
        parts.append(f"db_tot:{db_wait:.0f}s")
    span = s.get("span", 0)
    if span and span != s.get("max_span"):
        parts.append(f"span:{span:,}")
    aw = s.get("active_workers", 0)
    mw = s.get("max_workers", 0)
    if aw and mw and aw < mw:
        parts.append(f"wrk:{aw}/{mw}")
    if s.get("flushing"):
        parts.append("FLUSH")
    return "  ".join(parts)


def _heartbeat_worker():
    """Background daemon thread: reprint status every 1s."""
    while not _heartbeat_stop.wait(1.0):
        line = _build_status_line()
        if line:
            _print_line(line)


def main():
    # First Ctrl-C: skip flush, fast exit.
    # Second Ctrl-C: os._exit immediately.
    _sigint_count = [0]
    _skip_flush = [False]

    def _sigint_handler(sig, frame):
        _sigint_count[0] += 1
        _stop_event.set()
        _heartbeat_stop.set()
        _skip_flush[0] = True
        if _sigint_count[0] >= 2:
            print("\n  Force exit (second interrupt).")
            os._exit(1)
        raise KeyboardInterrupt
    signal.signal(signal.SIGINT, _sigint_handler)

    parser = argparse.ArgumentParser(
        description="Scrape Polymarket event logs from Polygon via JSON-RPC"
    )
    parser.add_argument("--max-calls", type=int, default=None,
                        help="stop after N RPC calls (default: unlimited)")
    parser.add_argument("--parallel", type=int, default=1,
                        help="number of worker threads (default: 1)")
    parser.add_argument("--lag-tolerance", type=int, default=2,
                        help="stop when within N blocks of chain head (default: 2)")
    args = parser.parse_args()

    max_calls = args.max_calls
    num_workers = args.parallel
    lag_tolerance = args.lag_tolerance

    conn = None
    rate_limiter = RateLimiter(MAX_CALLS_PER_SEC)
    run_start = time.time()
    total_inserted = 0
    chunks_done = 0
    blocks_done = 0
    db_wait_sec = 0
    last_flush_time = time.time()
    print_lock = threading.Lock()
    stop_flag = False
    chunk_failures = {}
    pending_rows = {}
    pending_ranges = []
    futures = {}
    pool = None
    current_block = 0
    total_covered = 0

    try:
        conn = init_db(DB_PATH)
        rate_limiter.wait()
        current_block = get_block_number()

        # Migrate from old per-contract progress if needed
        migrate_from_scrape_progress(conn)

        # Clean up orphaned temp dirs from interrupted solidifications
        orphans = cleanup_orphaned_temp_dirs(OUTPUT_DIR)
        if orphans:
            print(f"Cleaned up {orphans} orphaned temp director{'y' if orphans == 1 else 'ies'}")

        # Startup recovery: solidify any complete 10K partitions still in SQLite
        recovered = solidify_all_ready(conn, OUTPUT_DIR)
        if recovered:
            print(f"Startup recovery: solidified {recovered} partition{'s' if recovered != 1 else ''}")

        # Find gaps to fill
        completed = get_completed_ranges(conn)
        gaps = find_gaps(completed, EARLIEST_BLOCK, current_block)

        total_chain = current_block - EARLIEST_BLOCK
        total_covered = sum(t - f + 1 for f, t in completed)
        total_gap_blocks = sum(t - f + 1 for f, t in gaps) if gaps else 0
        # --- startup banner ---
        print(f"polymarket scraper (rpc)")
        print(f"database:   {DB_PATH} ({_file_size_str(DB_PATH)})")
        print(f"chain head: block {current_block:,}")
        print(f"config:     {MAX_CALLS_PER_SEC} rps, max {num_workers} workers, span {MAX_BLOCK_SPAN:,}")
        print()
        print(f"covered:    {total_covered:,} / {total_chain:,} blocks")
        if gaps:
            print(f"  gap:      {total_gap_blocks:,} blocks (~{_blocks_behind_str(total_gap_blocks)} behind),  {len(gaps)} range{'s' if len(gaps) != 1 else ''}")
            if len(gaps) <= 5:
                for f, t in gaps:
                    print(f"            {f:>12,} - {t:>12,}  ({t - f + 1:,} blocks)")
            else:
                for f, t in gaps[:2]:
                    print(f"            {f:>12,} - {t:>12,}  ({t - f + 1:,} blocks)")
                print(f"            ... {len(gaps) - 4} more ...")
                for f, t in gaps[-2:]:
                    print(f"            {f:>12,} - {t:>12,}  ({t - f + 1:,} blocks)")
        print()

        if not gaps:
            print("All blocks already scraped.")
            return

        # Adaptive block span — TCP slow-start style, same as workers.
        # Start at 1, double until first slow/split sets a ceiling, then
        # grow linearly (+10%) after consecutive fast responses.
        current_block_span = 1
        span_ceiling = MAX_BLOCK_SPAN   # lowered on first slow response
        span_slow_start = True          # doubling phase
        consecutive_ok_for_workers = 0  # for adaptive concurrency growth

        # TCP slow-start style concurrency: start at 1, double until first
        # timeout sets a ceiling, then grow linearly.
        active_workers = 1
        worker_ceiling = num_workers  # max we'll grow to (lowered on timeout)
        slow_start_phase = True       # doubling phase

        # Build work queue from gaps, splitting into current_block_span-sized chunks
        work_queue = []
        for gap_from, gap_to in gaps:
            cursor = gap_from
            while cursor <= gap_to:
                chunk_to = min(cursor + current_block_span - 1, gap_to)
                work_queue.append((cursor, chunk_to))
                cursor = chunk_to + 1

        run_start = time.time()
        total_inserted = 0
        chunks_done = 0
        blocks_done = 0
        last_flush_time = time.time()
        stop_flag = False
        chunk_failures = {}
        db_wait_sec = 0.0  # cumulative seconds blocked on DB writes
        resolution_seq = 0  # sequential counter for worker completions

        # Accumulate results in memory, flush to DB periodically
        pending_rows = {}       # table_name -> list of row dicts
        pending_ranges = []     # list of (from_b, to_b)

        # Initialise shared status for heartbeat thread
        _live.update({
            "run_start": run_start,
            "total_blocks": total_gap_blocks,
            "chain_head": current_block,
            "max_span": MAX_BLOCK_SPAN,
            "span": current_block_span,
            "blocks_done": 0,
            "ok": 0,
            "in_flight": 0,
            "pending_rows": 0,
            "db_wait": 0.0,
            "flushing": False,
            "calls": 0,
            "active_workers": active_workers,
            "max_workers": num_workers,
        })
        heartbeat = threading.Thread(
            target=_heartbeat_worker, daemon=True
        )
        heartbeat.start()

        def flush_to_db():
            """Bulk-write pending rows and ranges to SQLite in chunks."""
            nonlocal total_inserted, pending_rows, pending_ranges, last_flush_time
            nonlocal db_wait_sec
            if not pending_rows and not pending_ranges:
                return

            _live["flushing"] = True
            t_flush = time.time()
            flush_row_count = sum(len(rows) for rows in pending_rows.values())

            for table_name, rows in pending_rows.items():
                if not rows:
                    continue
                cols = list(rows[0].keys())
                col_str = ", ".join(cols)
                placeholders = ", ".join(["?"] * len(cols))
                sql = f"INSERT OR IGNORE INTO {table_name} ({col_str}) VALUES ({placeholders})"
                for i in range(0, len(rows), FLUSH_CHUNK_SIZE):
                    chunk = rows[i:i + FLUSH_CHUNK_SIZE]
                    values = [[r[c] for c in cols] for r in chunk]
                    cur = conn.executemany(sql, values)
                    total_inserted += cur.rowcount

            if pending_ranges:
                sorted_ranges = sorted(pending_ranges)
                merged = [list(sorted_ranges[0])]
                for f, t in sorted_ranges[1:]:
                    if f <= merged[-1][1] + 1:
                        merged[-1][1] = max(merged[-1][1], t)
                    else:
                        merged.append([f, t])
                for f, t in merged:
                    add_completed_range(conn, f, t)

            conn.commit()
            flush_ms = (time.time() - t_flush) * 1000
            db_wait_sec += flush_ms / 1000
            _stats["last_flush_ms"] = flush_ms
            _stats["flush_rows"] = flush_row_count
            _live["ok"] = total_inserted
            _live["db_wait"] = db_wait_sec
            _live["flushing"] = False
            if flush_ms > FLUSH_WARNING_THRESHOLD_MS:
                _print_msg(
                    f"  WARNING: DB flush took {flush_ms/1000:.1f}s "
                    f"for {flush_row_count:,} rows — disk I/O bottleneck"
                )
            pending_rows = {}
            pending_ranges = []
            _live["pending_rows"] = 0
            last_flush_time = time.time()

        def pending_row_count():
            return sum(len(rows) for rows in pending_rows.values())

        pool = ThreadPoolExecutor(max_workers=num_workers)
        futures = {}
        work_idx = 0

        def submit_chunk(from_b, to_b):
            """Submit a chunk to the pool. Returns True if submitted."""
            if stop_flag:
                return False
            if max_calls and rate_limiter.total_calls >= max_calls:
                return False
            f = pool.submit(fetch_range, from_b, to_b, rate_limiter)
            futures[f] = (from_b, to_b)
            return True

        def _rechunk_remaining():
            """Re-chunk remaining work queue items to current_block_span.

            Merges adjacent small chunks and splits oversized ones so every
            queued item is at most current_block_span blocks wide.
            """
            nonlocal work_queue
            remaining = work_queue[work_idx:]
            if not remaining:
                _live["span"] = current_block_span
                return

            # 1. Merge contiguous ranges into large spans
            merged = [list(remaining[0])]
            for rq_from, rq_to in remaining[1:]:
                if rq_from <= merged[-1][1] + 1:
                    merged[-1][1] = max(merged[-1][1], rq_to)
                else:
                    merged.append([rq_from, rq_to])

            # 2. Re-slice merged spans into current_block_span-sized chunks
            new_queue = []
            for rq_from, rq_to in merged:
                cursor = rq_from
                while cursor <= rq_to:
                    chunk_to = min(cursor + current_block_span - 1, rq_to)
                    new_queue.append((cursor, chunk_to))
                    cursor = chunk_to + 1
            work_queue[work_idx:] = new_queue
            _live["span"] = current_block_span

        # Fill the pipeline
        while work_idx < len(work_queue) and len(futures) < active_workers:
            f_b, t_b = work_queue[work_idx]
            if not submit_chunk(f_b, t_b):
                break
            work_idx += 1

        while futures and not _stop_event.is_set():
            # Poll with short timeout so Ctrl+C is responsive
            done, _ = concurrent.futures.wait(
                futures, timeout=POLL_TIMEOUT_SEC,
                return_when=concurrent.futures.FIRST_COMPLETED,
            )
            if not done:
                # Heartbeat thread handles status output; just update in_flight
                _live["in_flight"] = len(futures)
                _live["calls"] = rate_limiter.total_calls
                continue
            for future in done:
                from_b, to_b = futures.pop(future)
                resolution_seq += 1

                try:
                    result = future.result()
                except Exception as e:
                    _print_msg(
                        f"  #{resolution_seq:<4} FAIL  w:?  "
                        f"blks:{to_b - from_b + 1:,}  "
                        f"[{from_b:,}-{to_b:,}]  "
                        f"err: {e}"
                    )
                    key = (from_b, to_b)
                    chunk_failures[key] = chunk_failures.get(key, 0) + 1
                    if chunk_failures[key] < MAX_CHUNK_RETRIES:
                        submit_chunk(from_b, to_b)
                    else:
                        _print_msg(f"  Giving up on [{from_b:,}-{to_b:,}] after {MAX_CHUNK_RETRIES} retries")
                    continue

                # Log each worker response as it resolves
                status_tag = result[0].upper()
                if result[0] in ("ok", "slow"):
                    _, _, _, decoded_rows, raw_log_count, wid, elapsed_s = result
                    _print_msg(
                        f"  #{resolution_seq:<4} {'OK  ' if result[0] == 'ok' else 'SLOW'}  "
                        f"w:{wid}  "
                        f"blks:{to_b - from_b + 1:>6,}  "
                        f"evts:{raw_log_count:>5,}  "
                        f"{elapsed_s:5.1f}s  "
                        f"[{from_b:,}-{to_b:,}]"
                    )
                elif result[0] == "split":
                    _, _, _, wid, elapsed_s = result
                    _print_msg(
                        f"  #{resolution_seq:<4} SPLIT w:{wid}  "
                        f"blks:{to_b - from_b + 1:>6,}  "
                        f"{elapsed_s:5.1f}s  "
                        f"[{from_b:,}-{to_b:,}]  "
                        f"range too large, splitting"
                    )
                elif result[0] == "timeout":
                    _, _, _, wid, elapsed_s = result
                    _print_msg(
                        f"  #{resolution_seq:<4} TMOUT w:{wid}  "
                        f"blks:{to_b - from_b + 1:>6,}  "
                        f"{elapsed_s:5.1f}s  "
                        f"[{from_b:,}-{to_b:,}]"
                    )
                elif result[0] == "error":
                    _, _, _, err_msg, wid, elapsed_s = result
                    _print_msg(
                        f"  #{resolution_seq:<4} ERROR w:{wid}  "
                        f"blks:{to_b - from_b + 1:>6,}  "
                        f"{elapsed_s:5.1f}s  "
                        f"[{from_b:,}-{to_b:,}]  "
                        f"err: {err_msg}"
                    )

                if result[0] in ("ok", "slow"):
                    for table_name, row_dict in decoded_rows:
                        pending_rows.setdefault(table_name, []).append(row_dict)
                    pending_ranges.append((from_b, to_b))
                    chunks_done += 1
                    blocks_done += to_b - from_b + 1
                    consecutive_ok_for_workers += 1

                    chunk_span = to_b - from_b + 1
                    utilization = raw_log_count / MAX_RESULTS_LIMIT if MAX_RESULTS_LIMIT else 1

                    if result[0] == "slow" and chunk_span <= current_block_span and current_block_span > MIN_BLOCK_SPAN:
                        old_span = current_block_span
                        # First slow ends slow-start and sets ceiling
                        if span_slow_start:
                            span_slow_start = False
                            span_ceiling = max(MIN_BLOCK_SPAN, current_block_span - 1)
                        current_block_span = max(MIN_BLOCK_SPAN, current_block_span // 2)
                        if current_block_span != old_span:
                            _rechunk_remaining()
                            _print_msg(f"  Slow response — span {old_span:,} → {current_block_span:,}")

                    elif result[0] == "ok" and utilization < SPAN_BUMP_UTILIZATION and chunk_span >= current_block_span:
                        # Response well below provider limit — grow span
                        # immediately based on how much headroom exists.
                        old_span = current_block_span
                        max_span = span_ceiling if not span_slow_start else MAX_BLOCK_SPAN
                        if utilization > 0 and current_block_span < max_span:
                            # Scale span toward filling ~50% of the limit
                            multiplier = min(SPAN_BUMP_UTILIZATION / utilization, 4.0)
                            current_block_span = min(max_span, int(current_block_span * multiplier))
                        elif utilization == 0 and current_block_span < max_span:
                            # Zero events — double span (or 4× in slow-start)
                            factor = 4 if span_slow_start else 2
                            current_block_span = min(max_span, current_block_span * factor)
                        if current_block_span != old_span:
                            _rechunk_remaining()
                            reason = f"{raw_log_count} events" if raw_log_count else "0 events"
                            _print_msg(f"  Low utilization ({reason}) — span {old_span:,} → {current_block_span:,}")

                    # Grow concurrency: double in slow-start, +1 in linear.
                    # Both ok and slow count — only timeouts penalize workers.
                    if active_workers < worker_ceiling:
                        threshold = GROW_WORKERS_SLOW_START if slow_start_phase else GROW_WORKERS_LINEAR
                        if consecutive_ok_for_workers >= threshold:
                            old_aw = active_workers
                            if slow_start_phase:
                                active_workers = min(worker_ceiling, active_workers * 2)
                            else:
                                active_workers = min(worker_ceiling, active_workers + 1)
                            consecutive_ok_for_workers = 0
                            phase = "ramp" if slow_start_phase else "grow"
                            _print_msg(f"  {phase} — workers {old_aw} → {active_workers}")

                elif result[0] == "error":
                    _, _, _, err_msg, wid, elapsed_s = result
                    if err_msg == "interrupted":
                        continue
                    _print_msg(f"  Error [{from_b:,}-{to_b:,}]: {err_msg}")
                    consecutive_ok_for_workers = 0
                    key = (from_b, to_b)
                    chunk_failures[key] = chunk_failures.get(key, 0) + 1
                    if chunk_failures[key] < MAX_CHUNK_RETRIES:
                        submit_chunk(from_b, to_b)
                    else:
                        _print_msg(f"  Giving up on [{from_b:,}-{to_b:,}] after {MAX_CHUNK_RETRIES} retries")

                elif result[0] == "split":
                    # RPC provider rejected range as too large. Binary-split
                    # and retry both halves. Can split down to single blocks.
                    #
                    # Single-block failure: theoretically possible if a block
                    # has more logs than the provider's response limit. Polygon
                    # block gas limit is ~71M; cheapest log (LOG0) costs 375
                    # gas, giving a theoretical max of ~189K logs per block.
                    # In practice, our 5 contracts generate far fewer logs per
                    # block, but the provider's response size limit is the real
                    # constraint and varies by provider.
                    #
                    # Source: Polygon block gas limit observed at
                    # https://polygonscan.com/blocks (Feb 2026: ~71M gas).
                    # EVM LOG0 opcode cost: 375 gas (per Yellow Paper).
                    mid = (from_b + to_b) // 2
                    if mid > from_b:
                        submit_chunk(from_b, mid)
                        submit_chunk(mid + 1, to_b)
                    else:
                        _print_msg(
                            f"  Single-block range {from_b} rejected by provider. "
                            f"Skipping (will retry on next run)."
                        )

                elif result[0] == "timeout":
                    # RPC overloaded — reduce both span and concurrency,
                    # then re-queue the range (not counted as a retry).
                    consecutive_ok_for_workers = 0

                    # Reduce span and set ceiling
                    if span_slow_start:
                        span_slow_start = False
                        span_ceiling = max(MIN_BLOCK_SPAN, current_block_span - 1)
                    chunk_span = to_b - from_b + 1
                    if current_block_span > MIN_BLOCK_SPAN:
                        old_span = current_block_span
                        current_block_span = max(MIN_BLOCK_SPAN, current_block_span // 2)
                        if current_block_span != old_span:
                            _print_msg(f"  Timeout [{from_b:,}-{to_b:,}] — span {old_span:,} → {current_block_span:,}")
                    else:
                        _print_msg(f"  Timeout [{from_b:,}-{to_b:,}] (span already at minimum)")

                    # Reduce concurrency and set ceiling
                    slow_start_phase = False  # first timeout ends slow-start
                    if active_workers > 1:
                        old_aw = active_workers
                        # Set ceiling to just below where we timed out
                        active_workers = max(1, active_workers // 2)
                        _print_msg(f"  Timeout — workers {old_aw} → {active_workers}")

                    # Re-insert range into work queue for re-chunking
                    work_queue.insert(work_idx, (from_b, to_b))
                    _rechunk_remaining()

                # Flush to DB periodically, or when pending rows exceed cap
                prc = pending_row_count()
                _live["pending_rows"] = prc
                now = time.time()
                if (now - last_flush_time >= FLUSH_INTERVAL
                        or prc >= MAX_PENDING_ROWS):
                    flush_to_db()

                    # Solidify any newly complete 10K partitions
                    solidify_all_ready(conn, OUTPUT_DIR, print_fn=_print_msg)

                # Refill pipeline — but pause submission when buffer is full
                while (work_idx < len(work_queue)
                       and len(futures) < active_workers
                       and pending_row_count() < MAX_PENDING_ROWS):
                    if max_calls and rate_limiter.total_calls >= max_calls:
                        break
                    f_b, t_b = work_queue[work_idx]
                    if not submit_chunk(f_b, t_b):
                        break
                    work_idx += 1

                # Update shared status for heartbeat thread
                _live["blocks_done"] = blocks_done
                _live["ok"] = total_inserted
                _live["in_flight"] = len(futures)
                _live["calls"] = rate_limiter.total_calls
                _live["pending_rows"] = pending_row_count()
                _live["active_workers"] = active_workers

                # Check call limit
                if max_calls and rate_limiter.total_calls >= max_calls:
                    for f in list(futures):
                        f.cancel()
                    # Drain remaining completed futures
                    for f in as_completed(futures, timeout=DRAIN_TIMEOUT_SEC):
                        from_b2, to_b2 = futures.pop(f)
                        try:
                            res = f.result()
                            if res[0] == "ok":
                                _, _, _, drows, _ = res
                                for tn, rd in drows:
                                    pending_rows.setdefault(tn, []).append(rd)
                                pending_ranges.append((from_b2, to_b2))
                                blocks_done += to_b2 - from_b2 + 1
                        except Exception:
                            pass
                    futures.clear()
                    print(f"\nReached --max-calls limit ({max_calls}). Stopping.")
                    flush_to_db()
                    solidify_all_ready(conn, OUTPUT_DIR, print_fn=_print_msg)
                    break

                if not futures:
                    # Pass complete — check if we're within lag tolerance
                    flush_to_db()
                    solidify_all_ready(conn, OUTPUT_DIR, print_fn=_print_msg)
                    rate_limiter.wait()
                    new_head = get_block_number()
                    new_completed = get_completed_ranges(conn)
                    new_gaps = find_gaps(new_completed, EARLIEST_BLOCK, new_head)
                    new_gap_blocks = sum(t - f + 1 for f, t in new_gaps) if new_gaps else 0

                    if new_gap_blocks <= lag_tolerance:
                        current_block = new_head  # for summary
                        break

                    # More blocks to scrape — rebuild work queue and continue
                    _print_msg(f"\n  Chain advanced to {new_head:,} (+{new_gap_blocks:,} blocks). Continuing...\n")
                    current_block = new_head
                    _live["chain_head"] = new_head
                    _live["total_blocks"] += new_gap_blocks

                    # Build new work queue from new gaps
                    work_queue = []
                    for gap_from, gap_to in new_gaps:
                        cursor = gap_from
                        while cursor <= gap_to:
                            chunk_to = min(cursor + current_block_span - 1, gap_to)
                            work_queue.append((cursor, chunk_to))
                            cursor = chunk_to + 1
                    work_idx = 0

                    # Refill pipeline
                    while work_idx < len(work_queue) and len(futures) < active_workers:
                        f_b, t_b = work_queue[work_idx]
                        if not submit_chunk(f_b, t_b):
                            break
                        work_idx += 1

                    if not futures:
                        # No work submitted (e.g., max_calls hit)
                        break

    except KeyboardInterrupt:
        stop_flag = True
        _stop_event.set()
        for f in list(futures):
            f.cancel()
        futures.clear()
        if pool is not None:
            pool.shutdown(wait=False, cancel_futures=True)
    else:
        if pool is not None:
            if _stop_event.is_set():
                # Signal handler ran but KeyboardInterrupt didn't propagate
                # (common on macOS with concurrent.futures + threads).
                for f in list(futures):
                    f.cancel()
                futures.clear()
                pool.shutdown(wait=False, cancel_futures=True)
            else:
                pool.shutdown(wait=False)
    finally:
        # Ensure stop_flag is set if signal handler fired, even when
        # KeyboardInterrupt didn't propagate to the except block.
        stop_flag = stop_flag or _stop_event.is_set()

        # Stop heartbeat thread before printing summary
        _heartbeat_stop.set()
        sys.stdout.write(f"\r{' ' * _term_width()}\r")
        sys.stdout.flush()

        # On interrupt, skip flush — unflushed ranges will be re-fetched.
        if _skip_flush[0]:
            prc = pending_row_count()
            print()
            if prc:
                print(f"  Skipping flush of {prc:,} pending rows "
                      f"({len(pending_ranges)} ranges). Will re-fetch on next run.")
            else:
                print("  Interrupted. No pending data to flush.")
        else:
            if callable(locals().get('flush_to_db')):
                flush_to_db()
                solidify_all_ready(conn, OUTPUT_DIR)
        elapsed = time.time() - run_start
        effective_cps = rate_limiter.total_calls / elapsed if elapsed > 1 else 0
        blk_s = blocks_done / elapsed if elapsed > 1 and blocks_done > 0 else 0
        ev_s = total_inserted / elapsed if elapsed > 1 else 0

        # Compute final coverage
        final_ranges = get_completed_ranges(conn) if conn else []
        final_covered = sum(t - f + 1 for f, t in final_ranges)
        total_chain = current_block - EARLIEST_BLOCK if current_block > EARLIEST_BLOCK else 1
        final_gap = total_chain - final_covered
        start_covered = total_covered  # from startup

        print(f"\n\n{'='*70}")
        print(f"run complete  ({format_duration(elapsed)})")
        print(f"{'='*70}")

        print(f"\nprogress")
        print(f"  start:       {start_covered:>12,} blocks covered")
        print(f"  end:         {final_covered:>12,} blocks covered")
        print(f"  advanced:    +{blocks_done:,} blocks this run")
        if final_gap > 0:
            print(f"  remaining:   {final_gap:,} blocks  (~{_blocks_behind_str(final_gap)} behind real-time)")
            if blk_s > 0:
                eta_sec = final_gap / blk_s
                print(f"  ETA to sync: ~{format_duration(eta_sec)} at current rate")

        print(f"\nthroughput")
        print(f"  blocks/sec:  {blk_s:,.0f}")
        print(f"  events/sec:  {ev_s:,.0f}")
        print(f"  API calls:   {rate_limiter.total_calls:,}  ({effective_cps:.1f}/s effective)")

        has_bottlenecks = (
            _stats["api_errors"] or _stats["slow_responses"] or db_wait_sec > 1
        )
        if has_bottlenecks:
            print(f"\nbottlenecks")
            if _stats["api_errors"]:
                print(f"  API errors:    {_stats['api_errors']:,}")
            if _stats["slow_responses"]:
                print(f"  slow (>5s):    {_stats['slow_responses']:,}")
            if db_wait_sec > 1:
                db_pct = db_wait_sec / elapsed * 100 if elapsed > 0 else 0
                print(f"  DB wait:       {db_wait_sec:.1f}s ({db_pct:.1f}% of run time)")

        print(f"\nstorage")
        print(f"  events added:  {total_inserted:,}")
        print(f"  database:      {_file_size_str(DB_PATH)}")

        if final_ranges:
            print(f"\ncompleted ranges: {len(final_ranges)}, covering {final_covered:,} blocks")
            if len(final_ranges) <= 8:
                for f, t in final_ranges:
                    print(f"  {f:>12,} - {t:>12,}  ({t - f + 1:,} blocks)")
            else:
                for f, t in final_ranges[:3]:
                    print(f"  {f:>12,} - {t:>12,}  ({t - f + 1:,} blocks)")
                print(f"  ... {len(final_ranges) - 6} more ...")
                for f, t in final_ranges[-3:]:
                    print(f"  {f:>12,} - {t:>12,}  ({t - f + 1:,} blocks)")

        if not stop_flag and conn is not None:
            db_size = os.path.getsize(DB_PATH) if os.path.exists(DB_PATH) else 0
            if db_size > COUNT_SKIP_DB_SIZE:
                print(f"\nevents by table: skipped (database > {COUNT_SKIP_DB_SIZE // 1024**3} GB)")
            else:
                # Allow Ctrl+C to interrupt long SQLite queries. The
                # progress handler fires every N VM opcodes; returning
                # non-zero aborts the query.
                def _sqlite_interrupt_check():
                    return 1 if _stop_event.is_set() else 0
                conn.set_progress_handler(_sqlite_interrupt_check, 10_000)

                table_counts = []
                try:
                    for trow in conn.execute(
                        "SELECT name FROM sqlite_master WHERE type='table' "
                        "AND name NOT IN ('scrape_progress', 'completed_ranges')"
                    ):
                        cnt = conn.execute(f"SELECT COUNT(*) FROM [{trow[0]}]").fetchone()[0]
                        if cnt > 0:
                            table_counts.append((trow[0], cnt))
                except (KeyboardInterrupt, sqlite3.OperationalError):
                    print("\n  Interrupted during row count — skipping.")
                    stop_flag = True
                if table_counts:
                    print(f"\nevents by table")
                    for tname, cnt in sorted(table_counts, key=lambda x: -x[1]):
                        print(f"  {tname:<30s} {cnt:>12,}")
                conn.set_progress_handler(None, 0)

        if conn is not None:
            if not stop_flag:
                # Clean shutdown: checkpoint WAL back into main file
                def _sqlite_interrupt_check():
                    return 1 if _stop_event.is_set() else 0
                conn.set_progress_handler(_sqlite_interrupt_check, 10_000)
                try:
                    conn.execute("PRAGMA wal_checkpoint(TRUNCATE)")
                except (KeyboardInterrupt, sqlite3.OperationalError):
                    print("\n  Interrupted during WAL checkpoint — skipping.")
                conn.set_progress_handler(None, 0)
            # On interrupt: skip checkpoint, WAL is replayed on next open
            conn.close()
        # Always force-exit to avoid atexit thread-join blocking
        sys.stdout.flush()
        os._exit(0)


if __name__ == "__main__":
    main()
