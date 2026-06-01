"""
Assert: every ``(contract, event)`` 10K partition that should exist on disk
actually exists.

The data dictionary's Frontier ordering contract says: "A
``(contract, event)`` partition is expected only when the partition end
block is at or after that contract's ``deployment_block``; otherwise it
does not exist by design."

This test enumerates every expected ``(contract, event, 10K)`` cell from
``SCRAPE_START_BLOCK`` to the highest partition present on disk, and
flags any cell whose ``data.parquet`` file is missing when the contract
was already deployed by the end of that partition.

Scope: directory names only — this test never opens a parquet file.
"""
import os

from helpers import RAW

_10K = 10_000

# Deployment blocks per contract. These values match the [Contracts] table
# in DATA_DICTIONARY.md. If the dictionary changes, this list must change.
_DEPLOYMENT_BLOCKS = {
    "ConditionalTokens":    33_605_403,
    "CTFExchange":          33_605_743,
    "NegRiskCtfExchange":   45_169_177,
    "CTFExchangeV2":        84_902_353,
    "NegRiskCtfExchangeV2": 85_058_176,
    "NegRiskAdapter":       45_169_177,
    "UmaCtfAdapter":        33_605_574,
    "FeeModuleCTF":         75_253_526,
    "FeeModuleNegRisk":     75_253_721,
}


def test_no_partition_before_contract_deployment():
    contract_dirs = sorted(
        e.name for e in os.scandir(RAW) if e.is_dir() and e.name in _DEPLOYMENT_BLOCKS
    )
    assert contract_dirs, "no recognized contract directories found"

    # Find the highest 10K partition start present on disk for any contract.
    highest_k10 = 0
    for contract in contract_dirs:
        contract_path = os.path.join(RAW, contract)
        for event_entry in os.scandir(contract_path):
            if not event_entry.is_dir():
                continue
            for m_entry in os.scandir(event_entry.path):
                if not m_entry.is_dir() or not m_entry.name.startswith("1M="):
                    continue
                for k_entry in os.scandir(m_entry.path):
                    if not k_entry.is_dir() or not k_entry.name.startswith("10K="):
                        continue
                    k10 = int(k_entry.name.split("=", 1)[1])
                    if k10 > highest_k10:
                        highest_k10 = k10

    if highest_k10 == 0:
        # No partitions at all — nothing to check.
        return

    offenders: list[tuple[str, str, int]] = []  # (contract, event, partition_start)

    for contract in contract_dirs:
        deployment_block = _DEPLOYMENT_BLOCKS[contract]
        first_valid_k10 = (deployment_block // _10K) * _10K

        contract_path = os.path.join(RAW, contract)
        for event_entry in os.scandir(contract_path):
            if not event_entry.is_dir():
                continue
            event = event_entry.name
            for k10 in range(first_valid_k10, highest_k10 + _10K, _10K):
                pq = os.path.join(
                    contract_path, event,
                    f"1M={(k10 // 1_000_000) * 1_000_000}",
                    f"10K={k10}",
                    "data.parquet",
                )
                if not os.path.isfile(pq):
                    offenders.append((contract, event, k10))
                    if len(offenders) >= 20:
                        break
            if len(offenders) >= 20:
                break
        if len(offenders) >= 20:
            break

    assert not offenders, (
        f"{len(offenders)} (contract, event) partition(s) are missing even though "
        f"the contract was deployed by the end of that 10K range; first offenders "
        f"(contract, event, partition_start): {offenders[:5]}"
    )
