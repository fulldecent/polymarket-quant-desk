"""
polygon_contract_events_v3 library

Pure-library modules: no CLI, no env loading, no logging side effects.
The caller (the scraper script) owns all I/O orchestration; this library
owns the in-DB schema, the hot-DB ingestion path, the cold-tier Parquet
sink, the JSON-RPC client, the event decoders, and the block-range
bookkeeping that connects them.

Layout:
    tables.py         — schema as Python data (table & column names,
                        canonical column orders, deployment blocks)
    persistence.py    — HotStore: hot DuckDB, atomic event ingestion,
                        ranges / frontier queries
    parquet_sink.py   — write_partition_files: produces one partition's
                        cold-tier Parquet files (orchestrator follows
                        up with HotStore.commit_sink)
    rpc_client.py     — RpcClient: thread-safe Polygon JSON-RPC client
    event_decoders.py — decode_log: raw eth_getLogs entry to
                        (contract, event, row) ready for HotStore.persist
    errors.py         — exception types
"""
from .errors import (
    V3Error,
    DuplicateRowError,
    SchemaMismatchError,
    PartitionFrontierError,
    OperationCancelled,
)

__all__ = [
    "V3Error",
    "DuplicateRowError",
    "SchemaMismatchError",
    "PartitionFrontierError",
    "OperationCancelled",
]
