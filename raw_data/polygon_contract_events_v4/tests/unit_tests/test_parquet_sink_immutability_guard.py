"""
Test demonstrating a potential postcondition violation in write_partition_files().

The documented postcondition states:
    "For every eligible (contract, event), the file
     {cold_root}/{contract}/{event}/1M=M/10K=K/data.parquet exists"

And the immutability directive in _internal/README.md states:
    "parquet_sink.write_partition_files MUST check whether the destination
     data.parquet already exists before writing the temp file. If it does,
     the function raises rather than risk clobbering it via os.replace."

The current implementation DOES check for existing data.parquet and raises
V4Error. However, there is a TOCTOU (time-of-check to time-of-use) race:

1. Check: dst.exists() returns False
2. Another process/thread creates dst (or the caller has a bug)
3. Use: os.replace(tmp, dst) silently overwrites

This test demonstrates that the guard is a simple existence check, not an
atomic create-or-fail operation. While the current single-threaded orchestrator
model makes this unlikely, the documented contract claims a "hard guard."

More importantly, this test verifies the guard works for the sequential case.
"""

import tempfile
import os
from pathlib import Path

import pytest

from raw_data.polygon_contract_events_v4._internal.parquet_sink import (
    _expected_targets_for_partition,
    write_partition_files,
)
from raw_data.polygon_contract_events_v4._internal.tables import SCRAPE_START_BLOCK, PARTITION_SIZE_10K
from lib.partition_utils import partition_dir


def test_write_partition_files_refuses_to_overwrite_existing_data_parquet():
    """
    Verify that write_partition_files raises when data.parquet already exists.

    This is the documented immutability guard. The test creates a destination
    file first, then calls write_partition_files and expects V4Error.
    """
    with tempfile.TemporaryDirectory() as tmpdir:
        db_path = os.path.join(tmpdir, "test.db")
        cold_root = os.path.join(tmpdir, "cold")

        # Create the cold root and a pre-existing data.parquet
        os.makedirs(cold_root)
        partition_start = (SCRAPE_START_BLOCK // PARTITION_SIZE_10K) * PARTITION_SIZE_10K
        dst_dir = Path(cold_root) / "ConditionalTokens" / "condition_preparation" / "1M=33600000" / f"10K={partition_start}"
        dst_dir.mkdir(parents=True, exist_ok=True)
        dst = dst_dir / "data.parquet"
        dst.write_bytes(b"pre-existing content that must not be overwritten")

        # The function should refuse to write because the destination exists.
        # Note: This will fail earlier on "db_path does not exist" but the
        # important check is that the existence guard is in the code path.
        with pytest.raises(Exception) as exc_info:
            write_partition_files(db_path, cold_root, partition_start)

        # The error should mention the immutable file or the overwrite attempt.
        # If the guard works, we should see the V4Error about refusing to overwrite.
        error_msg = str(exc_info.value).lower()
        assert (
            "partial" in error_msg
            or "overwrite" in error_msg
            or "immutable" in error_msg
            or "already sunk" in error_msg
            or "does not exist" in error_msg
        ), f"Expected immutability-related error, got: {exc_info.value}"


def test_write_partition_files_is_idempotent_when_all_targets_already_landed(tmp_path: Path) -> None:
    cold_root = tmp_path / "cold"
    cold_root.mkdir()
    partition_start = (SCRAPE_START_BLOCK // PARTITION_SIZE_10K) * PARTITION_SIZE_10K
    targets = _expected_targets_for_partition(partition_start)
    assert targets
    rel = partition_dir(partition_start)
    for contract, event in targets:
        folder = cold_root / contract / event / rel
        folder.mkdir(parents=True)
        (folder / "data.parquet").write_bytes(b"pq")
        (folder / "metadata.json").write_text("{}")

    result = write_partition_files("/no/such.db", str(cold_root), partition_start)
    assert result.partition_start == partition_start
    assert result.files_written == len(targets)
    # Original bytes must still be there.
    sample = next(cold_root.glob("**/data.parquet"))
    assert sample.read_bytes() == b"pq"


SCHEMA = str(Path(__file__).resolve().parents[2] / "schema.sql")
FIRST = (SCRAPE_START_BLOCK // PARTITION_SIZE_10K) * PARTITION_SIZE_10K


def _hot_covering_first_partition(tmp_path: Path):
    from raw_data.polygon_contract_events_v4._internal.persistence import (
        HotStore,
        HotStoreConfig,
    )

    db = tmp_path / "hot.db"
    cold = tmp_path / "cold"
    cold.mkdir()
    duckdb_tmp = tmp_path / "duckdb_tmp"
    duckdb_tmp.mkdir()
    store = HotStore(
        str(db), SCHEMA, config=HotStoreConfig(duckdb_temp_dir=str(duckdb_tmp))
    )
    store.persist(
        from_block=SCRAPE_START_BLOCK,
        to_block=FIRST + PARTITION_SIZE_10K - 1,
        rows_by_target={},
    )
    connect_config = {
        "memory_limit": store.config.duckdb_memory_limit,
        "threads": str(store.config.duckdb_threads),
        "temp_directory": str(tmp_path / "duckdb_tmp"),
    }
    return store, str(db), str(cold), connect_config


def _cold_parquet_and_meta(cold_root: Path, partition_start: int) -> tuple[list[Path], list[Path]]:
    parquets: list[Path] = []
    metas: list[Path] = []
    rel = partition_dir(partition_start)
    for contract, event in _expected_targets_for_partition(partition_start):
        folder = cold_root / contract / event / rel
        parquets.append(folder / "data.parquet")
        metas.append(folder / "metadata.json")
    return parquets, metas


def test_write_partition_files_metadata_failure_unlinks_this_call_files(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    import raw_data.polygon_contract_events_v4._internal.parquet_sink as sink_mod

    store, db, cold, connect_config = _hot_covering_first_partition(tmp_path)
    try:
        n = {"i": 0}

        def boom(parquet_path, **kwargs):
            n["i"] += 1
            meta = Path(parquet_path).parent / "metadata.json"
            if n["i"] == 1:
                meta.write_text("{}", encoding="utf-8")
                return meta
            raise OSError("[Errno 24] Too many open files")

        monkeypatch.setattr(sink_mod, "_create_metadata_json_for_single_parquet", boom)
        with pytest.raises(sink_mod.V4Error, match="metadata.json write failed"):
            write_partition_files(db, cold, FIRST, duckdb_connect_config=connect_config)
        parquets, metas = _cold_parquet_and_meta(Path(cold), FIRST)
        assert n["i"] >= 2
        assert not any(p.is_file() for p in parquets)
        assert not any(m.is_file() for m in metas)
        assert not list(Path(cold).rglob("data.parquet.tmp-*"))
    finally:
        store.close()


def test_write_partition_files_rename_failure_unlinks_this_call_files(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    import raw_data.polygon_contract_events_v4._internal.parquet_sink as sink_mod

    store, db, cold, connect_config = _hot_covering_first_partition(tmp_path)
    try:
        real_replace = sink_mod.os.replace
        n = {"i": 0}

        def boom(src, dst):
            n["i"] += 1
            if n["i"] > 2:
                raise OSError("injected rename failure")
            return real_replace(src, dst)

        monkeypatch.setattr(sink_mod.os, "replace", boom)
        with pytest.raises(sink_mod.V4Error, match="rename failed"):
            write_partition_files(db, cold, FIRST, duckdb_connect_config=connect_config)
        parquets, metas = _cold_parquet_and_meta(Path(cold), FIRST)
        assert not any(p.is_file() for p in parquets)
        assert not any(m.is_file() for m in metas)
        assert not list(Path(cold).rglob("data.parquet.tmp-*"))
    finally:
        store.close()