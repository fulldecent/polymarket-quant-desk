"""Public API for polygon_contract_events_v3.

The package's implementation modules live under ``_internal``.
Only stable entry points are re-exported here.
"""

from ._internal.parquet_sink import get_sunk_frontier

__all__ = ["get_sunk_frontier"]
