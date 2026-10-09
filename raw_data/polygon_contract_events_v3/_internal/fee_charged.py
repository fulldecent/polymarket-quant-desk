"""Detect the two on-disk encodings of a buggy FeeRefunded.fee_charged.

The v3 decoder used to emit the indexed ``feeCharged`` topic as 32 raw
bytes into a VARCHAR column. That is poison:

  * Arrow persist of valid-UTF-8 32-byte big-endian (feeCharged in
    0..127, 8000, 156200, …) stores a 32-character string of NULs plus
    a few ASCII bytes. ``TRY_CAST AS HUGEINT`` is NULL.
  * DuckDB row-by-row bind of invalid-UTF-8 bytes stores the escaped
    text ``\\x00\\x00…!\\x98``. ``TRY_CAST AS HUGEINT`` is NULL.
  * Arrow persist of invalid-UTF-8 bytes (8600 = 0x2198, …) raises and
    does not write. Hot may still hold earlier blocks of the same 10K
    in the NUL-padded form.

Refuse those encodings at hot open, persist, and sink so a retry cannot
publish them. Landed v3 parquet on this desk is decimal; the dataset
version stays ``v3``.
"""

from __future__ import annotations

from .errors import V3Error

FEE_REFUNDED_TABLES: tuple[str, ...] = (
    "fee_module_ctf__fee_refunded",
    "fee_module_neg_risk__fee_refunded",
)

POISON_MESSAGE = (
    "fee_charged is not a uint256 decimal string. The decoder must emit "
    "a canonical uint256 decimal, not the 32-byte indexed feeCharged topic."
)

_DECIMAL = "^(0|[1-9][0-9]*)$"


def is_decimal_uint256(value: object) -> bool:
    """True iff ``value`` is the canonical uint256 decimal string."""
    if not isinstance(value, str):
        return False
    if value == "0":
        return True
    return value.isdigit() and not value.startswith("0")


def sql_poison_predicate(column: str = "fee_charged") -> str:
    """SQL boolean: ``column`` is not a canonical uint256 decimal string."""
    return (
        f"{column} IS NULL "
        f"OR NOT regexp_matches(CAST({column} AS VARCHAR), '{_DECIMAL}') "
        f"OR try_cast({column} AS HUGEINT) IS NULL"
    )


def reject_poison_value(value: object, *, where: str) -> None:
    if is_decimal_uint256(value):
        return
    raise V3Error(f"{POISON_MESSAGE} {where}: {value!r}")


def reject_poison_table(conn, table: str) -> None:
    """Fail if ``table`` already holds a non-decimal fee_charged."""
    n = conn.execute(
        f"SELECT COUNT(*) FROM {table} WHERE {sql_poison_predicate()}"
    ).fetchone()[0]
    if int(n) > 0:
        sample = conn.execute(
            f"SELECT fee_charged FROM {table} WHERE {sql_poison_predicate()} LIMIT 1"
        ).fetchone()[0]
        raise V3Error(f"{POISON_MESSAGE} {table} has {n} bad row(s), e.g. {sample!r}")
