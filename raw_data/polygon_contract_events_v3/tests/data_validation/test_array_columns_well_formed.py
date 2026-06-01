"""
Assert that every JSON-array column is well-formed and that the strongest
contract-enforced length invariant holds.

The data dictionary's Parquet type contract specifies that every
JSON-array column MUST be a syntactically valid JSON array; the empty
array is canonically encoded as the two-character string ``[]`` and is a
valid value, not an error. This test enforces "valid JSON array" (which
``json_array_length(...) IS NOT NULL`` is sufficient to prove in
DuckDB) on every such column.

In addition, the Gnosis ConditionalTokens contract enforces, in
``reportPayouts``, ``payouts.length == outcomeSlotCount``. So every row
in ``ConditionalTokens/condition_resolution`` must also satisfy
``json_array_length(payout_numerators) == outcome_slot_count``. That is
the only length invariant we can prove from on-chain semantics; for the
other array columns the producer cannot stipulate a length and we
accept any non-negative length, including zero.

Columns covered:

* ``ConditionalTokens/condition_resolution.payout_numerators`` (length
  must match ``outcome_slot_count``)
* ``ConditionalTokens/position_split.partition``
* ``ConditionalTokens/positions_merge.partition``
* ``ConditionalTokens/payout_redemption.index_sets``
* ``NegRiskAdapter/payout_redemption.amounts``
* ``UmaCtfAdapter/question_resolved.payouts``
* ``UmaCtfAdapter/question_emergency_resolved.payouts``

A consumer should still expect each non-empty array entry to be a
non-negative uint256 decimal string; that is enforced by
``test_amounts_are_non_negative_integers`` for the columns where the
producer guarantees a numeric semantic.

Scope: per-column as listed above.
"""
from helpers import RAW, EVENT_LOCATIONS, glob_all

# Each entry pins an exact ``contract/event`` directory plus the array
# column to validate. We pin the directory rather than unioning across
# all directories that share an event name, because event tables with
# the same logical name can have different schemas on different
# contracts (e.g. ``ConditionalTokens/position_split.partition`` vs
# ``NegRiskAdapter/position_split`` which has no ``partition`` column).
_ARRAY_COLUMNS = [
    ("ConditionalTokens/condition_resolution",        "payout_numerators"),
    ("ConditionalTokens/position_split",              "partition"),
    ("ConditionalTokens/positions_merge",             "partition"),
    ("ConditionalTokens/payout_redemption",           "index_sets"),
    ("NegRiskAdapter/payout_redemption",              "amounts"),
    ("UmaCtfAdapter/question_resolved",               "payouts"),
    ("UmaCtfAdapter/question_emergency_resolved",     "payouts"),
]


def test_payout_numerators_length_matches_outcome_slot_count(con):
    """Strong invariant from Gnosis CTF: payouts.length == outcomeSlotCount."""
    src = glob_all("condition_resolution")
    bad = con.execute(f"""
        SELECT outcome_slot_count,
               json_array_length(payout_numerators) AS arr_len,
               COUNT(*) AS n
        FROM {src}
        WHERE payout_numerators IS NULL
           OR json_array_length(payout_numerators) IS NULL
           OR json_array_length(payout_numerators) != outcome_slot_count
        GROUP BY outcome_slot_count, json_array_length(payout_numerators)
        ORDER BY n DESC
        LIMIT 5
    """).fetchall()
    assert not bad, (
        f"{len(bad)} distinct (outcome_slot_count, payout_numerators length) "
        f"pair(s) in condition_resolution violate "
        f"json_array_length(payout_numerators) == outcome_slot_count; "
        f"first offenders: {bad[:5]}"
    )


def test_array_columns_are_valid_json_arrays(con):
    """Every value must be a syntactically valid JSON array.

    The empty array ``[]`` is allowed by the contract; NULL is not.
    DuckDB's ``json_array_length`` returns NULL when the input is NULL,
    not a JSON value at all, or a JSON value that is not an array — so
    "valid non-NULL JSON array" reduces to ``json_array_length(col) IS
    NOT NULL``.
    """
    failures: list[tuple[str, str, int]] = []
    for path, column in _ARRAY_COLUMNS:
        _contract, event = path.split("/", 1)
        # Only check directories that actually exist on disk.
        if path not in EVENT_LOCATIONS.get(event, []):
            continue
        src = f"read_parquet('{RAW}/{path}/**/*.parquet')"
        bad = con.execute(f"""
            SELECT COUNT(*)
            FROM {src}
            WHERE {column} IS NULL
               OR json_array_length({column}) IS NULL
        """).fetchone()
        n = int(bad[0]) if bad else 0
        if n > 0:
            failures.append((path, column, n))

    assert not failures, (
        f"{len(failures)} (path, column) tuple(s) have rows whose value "
        f"is NULL or not a valid JSON array; offenders: {failures}"
    )
