"""
Assert: for each ``(contract, event)`` pair, the UTC day folders on disk
form a contiguous calendar chain from that pair's earliest day through its
latest day.

The first day is the UTC day the producer opened for that contract. It is
not a function of ``deployment_block`` alone, because the day of a block is
the day of its header time. A hole between the first and last day is a gap.

Scope: directory names only — this test never opens a parquet file.
"""
import os

from helpers import RAW
from lib.ymd_partition import day_key_from_parts, next_day_key

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


def _day_keys_for(contract_event_path: str) -> list[int]:
    keys: list[int] = []
    for year in os.scandir(contract_event_path):
        if not year.is_dir() or not year.name.startswith("year="):
            continue
        y = int(year.name.split("=", 1)[1])
        for month in os.scandir(year.path):
            if not month.is_dir() or not month.name.startswith("month="):
                continue
            m = int(month.name.split("=", 1)[1])
            for day in os.scandir(month.path):
                if (
                    not day.is_dir()
                    or not day.name.startswith("day=")
                    or day.name.endswith(".tmp")
                ):
                    continue
                pq = os.path.join(day.path, "data.parquet")
                if not os.path.isfile(pq):
                    continue
                d = int(day.name.split("=", 1)[1])
                keys.append(day_key_from_parts(y, m, d))
    return sorted(keys)


def test_partition_paths_have_no_gaps():
    contract_dirs = sorted(
        e.name for e in os.scandir(RAW)
        if e.is_dir() and e.name in _DEPLOYMENT_BLOCKS
    )
    assert contract_dirs, "no recognized contract directories found"

    offenders: list[tuple[str, str, int, int]] = []

    for contract in contract_dirs:
        contract_path = os.path.join(RAW, contract)
        for event_entry in os.scandir(contract_path):
            if not event_entry.is_dir():
                continue
            keys = _day_keys_for(event_entry.path)
            for prev, curr in zip(keys, keys[1:], strict=False):
                if curr != next_day_key(prev):
                    offenders.append((contract, event_entry.name, prev, curr))
                    if len(offenders) >= 20:
                        break
            if len(offenders) >= 20:
                break
        if len(offenders) >= 20:
            break

    assert not offenders, (
        f"{len(offenders)} gap(s) in UTC day chains; each entry is "
        f"(contract, event, last_day_before_gap, first_day_after_gap). "
        f"First offenders: {offenders[:5]}"
    )
