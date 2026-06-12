"""Validate that token_id_map_v1 resolves the vast majority of traded outcome tokens.

token_id_map_v1 is an approximation: a token enters the map only when a
ConditionalTokens split/merge shares a transaction with an exchange
``orders_matched`` event (see DATA_DICTIONARY.md "Scope"). This test guarantees
that the approximation is good enough to be useful by asserting that more than
99% of exchange trading resolves to a token in the map, measured two ways:

  1. Row-weighted (trade-volume) coverage: of every ``order_filled`` row's
     outcome-token leg, the fraction whose token_id is in the map.
  2. Distinct-token coverage: of the distinct outcome token_ids ever traded, the
     fraction present in the map.

The 32-byte zero asset id is USDC (the cash leg) and is excluded; only outcome
tokens are counted.
"""
import os
import sys
from pathlib import Path

import duckdb
import pytest
from dotenv import load_dotenv

_project_root = Path(__file__).resolve().parents[4]
load_dotenv(_project_root / ".env")
sys.path.insert(0, str(_project_root))

_ZERO32_HEX = "00" * 32
_MIN_COVERAGE = 0.99

# v1 exchanges carry the outcome token in whichever of maker/taker asset id is
# not the zero (USDC) id; v2 exchanges carry it in an explicit token_id column.
_V1_ORDER_FILLED = ["CTFExchange/order_filled", "NegRiskCtfExchange/order_filled"]
_V2_ORDER_FILLED = ["CTFExchangeV2/order_filled", "NegRiskCtfExchangeV2/order_filled"]


def _map_dir() -> Path:
    val = os.environ.get("TOKEN_ID_MAP_V1_DIR", "")
    if not val:
        pytest.skip("TOKEN_ID_MAP_V1_DIR is not set")
    out = Path(val)
    if not out.exists():
        pytest.skip(f"TOKEN_ID_MAP_V1_DIR does not exist: {out}")
    return out


def _raw_dir() -> Path:
    val = os.environ.get("POLYGON_CONTRACT_EVENTS_V3_DIR", "")
    if not val:
        pytest.skip("POLYGON_CONTRACT_EVENTS_V3_DIR is not set")
    raw = Path(val)
    if not raw.exists():
        pytest.skip(f"POLYGON_CONTRACT_EVENTS_V3_DIR does not exist: {raw}")
    return raw


def _connect() -> duckdb.DuckDBPyConnection:
    con = duckdb.connect()
    temp_dir = os.environ.get("TEMP_DIR", "")
    if temp_dir:
        con.execute(f"SET temp_directory = '{temp_dir}'")
    con.execute("SET preserve_insertion_order = false")
    return con


def _traded_tokens_sql(raw: Path) -> str:
    """A UNION ALL SELECT yielding one outcome-token id per order_filled row."""
    parts: list[str] = []
    for t in _V1_ORDER_FILLED:
        g = f"{raw}/{t}/**/data.parquet"
        parts.append(
            f"SELECT CASE WHEN maker_asset_id = unhex('{_ZERO32_HEX}') "
            f"THEN taker_asset_id ELSE maker_asset_id END AS token_id "
            f"FROM read_parquet('{g}')"
        )
    for t in _V2_ORDER_FILLED:
        g = f"{raw}/{t}/**/data.parquet"
        parts.append(f"SELECT token_id FROM read_parquet('{g}')")
    return " UNION ALL ".join(parts)


def test_row_weighted_coverage_exceeds_threshold():
    """>99% of order_filled outcome-token legs resolve to a token in the map."""
    raw = _raw_dir()
    map_dir = _map_dir()
    con = _connect()

    con.execute(
        f"CREATE TEMP TABLE map_tokens AS "
        f"SELECT DISTINCT token_id FROM read_parquet('{map_dir}/**/data.parquet')"
    )
    total, covered = con.execute(f"""
        WITH ref AS (
            SELECT token_id FROM ({_traded_tokens_sql(raw)})
            WHERE token_id <> unhex('{_ZERO32_HEX}')
        )
        SELECT
            COUNT(*),
            COUNT(*) FILTER (WHERE m.token_id IS NOT NULL)
        FROM ref
        LEFT JOIN map_tokens m USING (token_id)
    """).fetchone()

    assert total > 0, "no order_filled outcome-token legs found"
    coverage = covered / total
    assert coverage > _MIN_COVERAGE, (
        f"row-weighted token coverage {coverage:.4%} is below the required "
        f"{_MIN_COVERAGE:.0%} ({covered:,} of {total:,} fills resolved)"
    )


def test_distinct_token_coverage_exceeds_threshold():
    """>99% of distinct traded outcome tokens are present in the map."""
    raw = _raw_dir()
    map_dir = _map_dir()
    con = _connect()

    con.execute(
        f"CREATE TEMP TABLE map_tokens AS "
        f"SELECT DISTINCT token_id FROM read_parquet('{map_dir}/**/data.parquet')"
    )
    total, covered = con.execute(f"""
        WITH ref AS (
            SELECT DISTINCT token_id FROM ({_traded_tokens_sql(raw)})
            WHERE token_id <> unhex('{_ZERO32_HEX}')
        )
        SELECT
            COUNT(*),
            COUNT(*) FILTER (WHERE m.token_id IS NOT NULL)
        FROM ref
        LEFT JOIN map_tokens m USING (token_id)
    """).fetchone()

    assert total > 0, "no distinct traded outcome tokens found"
    coverage = covered / total
    assert coverage > _MIN_COVERAGE, (
        f"distinct-token coverage {coverage:.4%} is below the required "
        f"{_MIN_COVERAGE:.0%} ({covered:,} of {total:,} distinct tokens resolved)"
    )
