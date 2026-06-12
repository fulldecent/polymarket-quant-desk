#!/usr/bin/env python3
"""
ct_helpers.py — ConditionalTokens helper functions

Ports getCollectionId() and getPositionId() from the Gnosis ConditionalTokens
Solidity contract to pure Python. These compute ERC-1155 token IDs from
(conditionId, indexSet) pairs, matching the on-chain implementation at
0x4D97DCd97eC945f40cF65F87097ACe5EA0476045 on Polygon.

Uses the alt_bn128 (BN254) elliptic curve: y^2 = x^3 + 3 (mod P).

Dependencies: pycryptodome (pip install pycryptodome)
"""

from Crypto.Hash import keccak


# ---------------------------------------------------------------------------
# Keccak-256 (Ethereum's hash, NOT SHA-3/FIPS-202)
# ---------------------------------------------------------------------------

def _keccak256(data: bytes) -> bytes:
    return keccak.new(digest_bits=256, data=data).digest()


# ---------------------------------------------------------------------------
# alt_bn128 (BN254) curve: y^2 = x^3 + B  (mod P)
# ---------------------------------------------------------------------------

P = 21888242871839275222246405745257275088696311157297823662689037894645226208583
B = 3


def _mod_sqrt(yy: int) -> int:
    """Modular square root for P ≡ 3 (mod 4): sqrt(yy) = yy^((P+1)/4)."""
    return pow(yy, (P + 1) // 4, P)


def _ec_add(x1: int, y1: int, x2: int, y2: int) -> tuple:
    """Elliptic curve point addition on alt_bn128."""
    if x1 == 0 and y1 == 0:
        return x2, y2
    if x2 == 0 and y2 == 0:
        return x1, y1
    if x1 == x2:
        if y1 == y2:
            # Point doubling: λ = 3x²/(2y)
            lam = 3 * x1 * x1 * pow(2 * y1, P - 2, P) % P
        else:
            return 0, 0  # point at infinity
    else:
        lam = (y2 - y1) * pow(x2 - x1, P - 2, P) % P
    x3 = (lam * lam - x1 - x2) % P
    y3 = (lam * (x1 - x3) - y1) % P
    return x3, y3


# ---------------------------------------------------------------------------
# Public API
# ---------------------------------------------------------------------------

def get_collection_id(
    parent_collection_id: bytes, condition_id: bytes, index_set: int
) -> bytes:
    """
    Port of CTHelpers.getCollectionId from the ConditionalTokens contract.

    Args:
        parent_collection_id: 32 bytes (usually all zeros)
        condition_id: 32 bytes
        index_set: uint256 bitmask of outcome slots (must be > 0)

    Returns:
        32-byte collection ID
    """
    assert index_set > 0, "invalid index set"

    # x1 = uint(keccak256(abi.encodePacked(conditionId, indexSet)))
    packed = condition_id + index_set.to_bytes(32, "big")
    x1 = int.from_bytes(_keccak256(packed), "big")

    # odd = x1 >> 255 != 0
    odd = (x1 >> 255) != 0

    # Find point on curve: increment x1 until x^3 + B is a quadratic residue
    while True:
        x1 = (x1 + 1) % P
        yy = (pow(x1, 3, P) + B) % P
        y1 = _mod_sqrt(yy)
        if y1 * y1 % P == yy:
            break

    # Match y parity to hash parity
    if (odd and y1 % 2 == 0) or (not odd and y1 % 2 == 1):
        y1 = P - y1

    # EC-add parent collection point if non-zero
    x2 = int.from_bytes(parent_collection_id, "big")
    if x2 != 0:
        odd2 = (x2 >> 254) != 0
        x2 = x2 & ((1 << 254) - 1)            # clear top 2 bits
        yy2 = (pow(x2, 3, P) + B) % P
        y2 = _mod_sqrt(yy2)
        assert y2 * y2 % P == yy2, "invalid parent collection ID"
        if (odd2 and y2 % 2 == 0) or (not odd2 and y2 % 2 == 1):
            y2 = P - y2
        x1, y1 = _ec_add(x1, y1, x2, y2)

    # Encode y-parity into bit 254
    if y1 % 2 == 1:
        x1 ^= 1 << 254

    return x1.to_bytes(32, "big")


def get_position_id(collateral_token: bytes, collection_id: bytes) -> int:
    """
    Port of CTHelpers.getPositionId from the ConditionalTokens contract.

    Args:
        collateral_token: 20-byte address
        collection_id: 32 bytes

    Returns:
        uint256 token ID (ERC-1155 ID)
    """
    packed = collateral_token + collection_id
    return int.from_bytes(_keccak256(packed), "big")


# ---------------------------------------------------------------------------
# Hex-string convenience wrappers
# ---------------------------------------------------------------------------

def get_collection_id_hex(
    parent_collection_id_hex: str, condition_id_hex: str, index_set: int
) -> str:
    """Same as get_collection_id but accepts/returns 0x-prefixed hex strings."""
    result = get_collection_id(
        bytes.fromhex(parent_collection_id_hex.removeprefix("0x")),
        bytes.fromhex(condition_id_hex.removeprefix("0x")),
        index_set,
    )
    return "0x" + result.hex()


def get_position_id_hex(
    collateral_token_hex: str, collection_id_hex: str
) -> int:
    """Same as get_position_id but accepts 0x-prefixed hex strings."""
    return get_position_id(
        bytes.fromhex(collateral_token_hex.removeprefix("0x")),
        bytes.fromhex(collection_id_hex.removeprefix("0x")),
    )


import functools


def token_id_from_condition(
    collateral_token_hex: str,
    parent_collection_id_hex: str,
    condition_id_hex: str,
    index_set: int,
) -> int:
    """
    End-to-end: compute the ERC-1155 token ID for one outcome of a condition.

    This is the composition: getPositionId(collateral, getCollectionId(...)).
    Results are cached — the ECC math only runs once per unique input tuple.
    """
    return _token_id_from_condition_cached(
        collateral_token_hex, parent_collection_id_hex, condition_id_hex, index_set
    )


@functools.lru_cache(maxsize=None)
def _token_id_from_condition_cached(
    collateral_token_hex: str,
    parent_collection_id_hex: str,
    condition_id_hex: str,
    index_set: int,
) -> int:
    coll = get_collection_id_hex(
        parent_collection_id_hex, condition_id_hex, index_set
    )
    return get_position_id_hex(collateral_token_hex, coll)


# ---------------------------------------------------------------------------
# Self-test against known Polymarket values
# ---------------------------------------------------------------------------

if __name__ == "__main__":
    CONDITION = "0x9c930ccdce11158a4de95a3f3f5e9da8aa9176784a8082202c81e2a0a2c0253e"
    COLLATERAL = "0x2791bca1f2de4661ed88a30c99a7a9449aa84174"
    PARENT = "0x" + "00" * 32

    EXPECTED = {
        1: 47756312234909391864093638304641104786505078015591144731389023729388049333957,
        2: 59279318572419004558001720629503812230408676045060703659290980661005759439535,
    }

    ok = True
    for idx_set, expected_tid in EXPECTED.items():
        tid = token_id_from_condition(COLLATERAL, PARENT, CONDITION, idx_set)
        status = "PASS" if tid == expected_tid else "FAIL"
        if tid != expected_tid:
            ok = False
        print(f"  index_set={idx_set}: {status}")
        if tid != expected_tid:
            print(f"    got:      {tid}")
            print(f"    expected: {expected_tid}")

    raise SystemExit(0 if ok else 1)
