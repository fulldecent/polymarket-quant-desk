"""
Assert: for each ``(contract, event)`` pair, the 10K partitions on disk
form a contiguous chain from the contract's first valid 10K partition
up to its highest existing partition, with no gaps in between.

The data dictionary's Frontier ordering contract says: "all on-disk
Parquet partitions must form a contiguous chain from
``SCRAPE_START_BLOCK`` with no gaps or islands." For a given contract,
the first valid 10K partition is
``floor(deployment_block / 10_000) * 10_000`` (see
``test_no_partition_before_contract_deployment``).

Scope: directory names only — this test never opens a parquet file.

Implementation: for each ``(contract, event)``, collect every 10K
partition start that has a ``data.parquet`` file, sort them, and verify
that consecutive values differ by exactly 10_000.
"""
import os

from helpers import RAW

_10K = 10_000

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


def _partition_starts_for(contract_event_path: str) -> list[int]:
    starts: list[int] = []
    for m_entry in os.scandir(contract_event_path):
        if not m_entry.is_dir() or not m_entry.name.startswith("1M="):
            continue
        for k_entry in os.scandir(m_entry.path):
            if not k_entry.is_dir() or not k_entry.name.startswith("10K="):
                continue
            pq = os.path.join(k_entry.path, "data.parquet")
            if not os.path.isfile(pq):
                continue
            starts.append(int(k_entry.name.split("=", 1)[1]))
    return sorted(starts)


def test_partition_paths_have_no_gaps():
    contract_dirs = sorted(
        e.name for e in os.scandir(RAW) if e.is_dir() and e.name in _DEPLOYMENT_BLOCKS
    )
    assert contract_dirs, "no recognized contract directories found"

    offenders: list[tuple[str, str, int, int]] = []  # (contract, event, gap_after, gap_before)

    for contract in contract_dirs:
        deployment_block = _DEPLOYMENT_BLOCKS[contract]
        first_valid = (deployment_block // _10K) * _10K

        contract_path = os.path.join(RAW, contract)
        for event_entry in os.scandir(contract_path):
            if not event_entry.is_dir():
                continue
            event = event_entry.name
            starts = _partition_starts_for(event_entry.path)
            if not starts:
                continue

            # Gap before the chain: the chain must begin at first_valid.
            if starts[0] != first_valid:
                offenders.append(
                    (contract, event, first_valid - _10K, starts[0])
                )
                if len(offenders) >= 20:
                    break

            # Gaps within the chain.
            for prev, curr in zip(starts, starts[1:], strict=False):
                if curr != prev + _10K:
                    offenders.append((contract, event, prev, curr))
                    if len(offenders) >= 20:
                        break
            if len(offenders) >= 20:
                break
        if len(offenders) >= 20:
            break

    assert not offenders, (
        f"{len(offenders)} gap(s) detected in partition chains; each entry "
        f"is (contract, event, last_partition_before_gap, "
        f"first_partition_after_gap). The chain must start at "
        f"floor(deployment_block / 10000) * 10000 and step by 10000 with no "
        f"missing 10K cells. First offenders: {offenders[:5]}"
    )
