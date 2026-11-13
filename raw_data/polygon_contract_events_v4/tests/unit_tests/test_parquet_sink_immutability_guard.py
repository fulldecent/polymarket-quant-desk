"""write_partition_files refuses to clobber a day that is already on disk."""

import sys
from pathlib import Path

import pytest

sys.path.insert(0, str(Path(__file__).resolve().parents[4]))
sys.path.insert(0, str(Path(__file__).resolve().parent))

from raw_data.polygon_contract_events_v4._internal.parquet_sink import (
    _expected_targets_for_day,
    write_partition_files,
)
from day_layout import D0, bounds, plant_day
from lib.ymd_partition import day_dir


def test_write_partition_files_refuses_a_partial_day(tmp_path: Path) -> None:
    """A parquet file without its sibling metadata is not a finished day."""
    from raw_data.polygon_contract_events_v4._internal.persistence import (
        HotStore,
        HotStoreConfig,
    )

    schema = str(Path(__file__).resolve().parents[2] / "schema.sql")
    db = tmp_path / "hot.db"
    cold = tmp_path / "cold"
    cold.mkdir()
    duck = tmp_path / "duck"
    duck.mkdir()
    store = HotStore(str(db), schema, config=HotStoreConfig(duckdb_temp_dir=str(duck)))
    try:
        first, last = bounds(0)
        store.open_first_day(D0)
        store.persist(first, last, {})
        store.connection.execute(
            "UPDATE utc_day_span SET last_block = ? WHERE day_key = ?",
            [last, D0],
        )
        target = _expected_targets_for_day(last)[0]
        folder = cold / target[0] / target[1] / day_dir(D0)
        folder.mkdir(parents=True)
        (folder / "data.parquet").write_bytes(b"pre-existing")
        connect_config = {
            "memory_limit": store.config.duckdb_memory_limit,
            "threads": str(store.config.duckdb_threads),
            "temp_directory": str(duck),
        }

        with pytest.raises(Exception) as exc_info:
            write_partition_files(
                str(db), str(cold), D0, duckdb_connect_config=connect_config
            )
        error_msg = str(exc_info.value).lower()
        assert "partial" in error_msg or "overwrite" in error_msg or "immutable" in error_msg
    finally:
        store.close()


def test_write_partition_files_is_idempotent_when_all_targets_already_landed(
    tmp_path: Path,
) -> None:
    cold_root = tmp_path / "cold"
    first, last = bounds(0)
    targets = _expected_targets_for_day(last)
    folders = plant_day(cold_root, D0, first, last)
    assert folders

    result = write_partition_files("/no/such.db", str(cold_root), D0)
    assert result.day_key == D0
    assert result.files_written == len(targets)
    sample = next(cold_root.glob("**/data.parquet"))
    assert sample.read_bytes() == b"PAR1"


SCHEMA = str(Path(__file__).resolve().parents[2] / "schema.sql")


def _hot_covering_first_day(tmp_path: Path):
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
    first, last = bounds(0)
    store.open_first_day(D0)
    store.persist(from_block=first, to_block=last, rows_by_target={})
    store.connection.execute(
        "UPDATE utc_day_span SET last_block = ? WHERE day_key = ?",
        [last, D0],
    )
    connect_config = {
        "memory_limit": store.config.duckdb_memory_limit,
        "threads": str(store.config.duckdb_threads),
        "temp_directory": str(duckdb_tmp),
    }
    return store, str(db), str(cold), connect_config


def _cold_parquet_and_meta(cold_root: Path) -> tuple[list[Path], list[Path]]:
    parquets: list[Path] = []
    metas: list[Path] = []
    rel = day_dir(D0)
    _first, last = bounds(0)
    for contract, event in _expected_targets_for_day(last):
        folder = cold_root / contract / event / rel
        parquets.append(folder / "data.parquet")
        metas.append(folder / "metadata.json")
    return parquets, metas


def test_write_partition_files_metadata_failure_unlinks_this_call_files(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    import raw_data.polygon_contract_events_v4._internal.parquet_sink as sink_mod

    store, db, cold, connect_config = _hot_covering_first_day(tmp_path)
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
            write_partition_files(db, cold, D0, duckdb_connect_config=connect_config)
        parquets, metas = _cold_parquet_and_meta(Path(cold))
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

    store, db, cold, connect_config = _hot_covering_first_day(tmp_path)
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
            write_partition_files(db, cold, D0, duckdb_connect_config=connect_config)
        parquets, metas = _cold_parquet_and_meta(Path(cold))
        assert not any(p.is_file() for p in parquets)
        assert not any(m.is_file() for m in metas)
        assert not list(Path(cold).rglob("data.parquet.tmp-*"))
    finally:
        store.close()


