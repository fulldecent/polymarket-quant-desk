"""
Assert: a manifested UTC day includes every contract that was deployed by
that day's ``max_block``, and no event folder exists for a day that ended
before the contract deployed.

Scope: ``metadata.json`` parameters and file names. This test does not read
parquet row groups.
"""
import json
import os
from pathlib import Path

from helpers import RAW
from lib.ymd_partition import day_block_span, day_dir, iter_manifest_day_keys
from raw_data.polygon_contract_events_v4._internal.tables import targets_active_at


def test_no_partition_before_contract_deployment():
    day_keys = list(iter_manifest_day_keys(RAW))
    if not day_keys:
        return

    missing: list[tuple[str, str, int]] = []
    for day_key in day_keys:
        _first, last = day_block_span(RAW, day_key)
        rel = day_dir(day_key)
        for contract, event in targets_active_at(last):
            parquet = os.path.join(RAW, contract, event, rel, "data.parquet")
            if not os.path.isfile(parquet):
                missing.append((contract, event, day_key))
                if len(missing) >= 20:
                    break
        if len(missing) >= 20:
            break

    assert not missing, (
        f"{len(missing)} manifested day(s) are missing an event file whose "
        f"contract was deployed by that day's max_block. First offenders "
        f"(contract, event, day_key): {missing[:5]}"
    )


def test_no_day_folder_ends_before_deployment():
    """An event folder whose max_block is before deployment must not exist."""
    from raw_data.polygon_contract_events_v4._internal.tables import CONTRACTS_BY_NAME

    offenders: list[tuple[str, str, int, int]] = []
    if not os.path.isdir(RAW):
        return
    for contract_entry in os.scandir(RAW):
        if not contract_entry.is_dir() or contract_entry.name not in CONTRACTS_BY_NAME:
            continue
        deployment = CONTRACTS_BY_NAME[contract_entry.name].deployment_block
        for event_entry in os.scandir(contract_entry.path):
            if not event_entry.is_dir():
                continue
            for meta in Path(event_entry.path).glob(
                "year=*/month=*/day=*/metadata.json"
            ):
                payload = json.loads(meta.read_text(encoding="utf-8"))
                params = payload.get("parameters") or {}
                max_block = params.get("max_block")
                if max_block is None:
                    continue
                if int(max_block) < deployment:
                    offenders.append(
                        (contract_entry.name, event_entry.name, int(max_block), deployment)
                    )
                    if len(offenders) >= 20:
                        break
            if len(offenders) >= 20:
                break
        if len(offenders) >= 20:
            break

    assert not offenders, (
        f"{len(offenders)} day folder(s) end before the contract deployed. "
        f"First offenders (contract, event, max_block, deployment): {offenders[:5]}"
    )
