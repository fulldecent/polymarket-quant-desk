# lib/

Reusable Python modules shared across scripts in this project.

## modules

### ct_helpers.py

Pure-Python port of the `CTHelpers` library from the Gnosis ConditionalTokens Solidity contract. Computes ERC-1155 token IDs from `(conditionId, indexSet)` pairs, matching the on-chain implementation at `0x4D97DCd97eC945f40cF65F87097ACe5EA0476045` on Polygon.

**Key functions:**

| function | description |
|---|---|
| `get_collection_id(parent, condition_id, index_set)` | bytes → bytes; mirrors `CTHelpers.getCollectionId` |
| `get_position_id(collateral_token, collection_id)` | bytes → int; mirrors `CTHelpers.getPositionId` |
| `get_collection_id_hex(...)` | same as above, accepts/returns `0x`-prefixed hex strings |
| `get_position_id_hex(...)` | same as above, accepts `0x`-prefixed hex strings |
| `token_id_from_condition(collateral, parent, condition_id, index_set)` | end-to-end shortcut; result is LRU-cached |

**dependency:** `pycryptodome` — install with `pip install pycryptodome`.

## running the self-tests

Each module that contains a self-test runs it when executed directly. The test checks computed values against known on-chain token IDs.

```sh
# from the project root, activate whatever venv applies to the calling script, then:
python lib/ct_helpers.py
```

Expected output:

```
  index_set=1: PASS
  index_set=2: PASS
```

Exit code is `0` on success, `1` on any failure. This makes the tests usable in CI:

```sh
python lib/ct_helpers.py || echo "ct_helpers self-test failed"
```

## conventions for new modules

- place each module directly in `lib/` as a flat `.py` file.
- include a `if __name__ == "__main__":` self-test block with at least one known-good value verified against an on-chain source.
- document the test vectors (condition ID, expected token IDs, etc.) with a comment explaining where the expected values came from.
- add the module to the table above.
