"""Shared helpers for parquet metadata/provenance files."""

from __future__ import annotations

import hashlib
import json
import subprocess
from datetime import datetime, timezone
from functools import lru_cache
from pathlib import Path
from typing import Any, Mapping

import duckdb


_JSON_ENCODING = "utf-8"
_JSON_COMPACT_SEPARATORS = (",", ":")


@lru_cache(maxsize=1)
def _current_git_commit(*, repo_root: Path | None = None) -> str:
    """Return current HEAD commit hash, or ``unknown`` if unavailable."""
    root = repo_root if repo_root is not None else Path(__file__).resolve().parents[1]
    try:
        result = subprocess.run(
            ["git", "rev-parse", "HEAD"],
            capture_output=True,
            text=True,
            cwd=str(root),
        )
        commit = result.stdout.strip()
        return commit if commit else "unknown"
    except Exception:
        return "unknown"


def _parquet_content_hash(parquet_path: Path, *, chunk_size: int = 1 << 20) -> str:
    """Compute SHA256 hash for a parquet file with ``sha256:`` prefix."""
    sha256_hash = hashlib.sha256()
    with open(parquet_path, "rb") as f:
        for chunk in iter(lambda: f.read(chunk_size), b""):
            sha256_hash.update(chunk)
    return f"sha256:{sha256_hash.hexdigest()}"


def parquet_content_hash(parquet_path: Path, *, chunk_size: int = 1 << 20) -> str:
    """Public helper to compute SHA256 hash with ``sha256:`` prefix."""
    return _parquet_content_hash(parquet_path, chunk_size=chunk_size)


def _parquet_row_count(
    parquet_path: Path,
    *,
    connection: duckdb.DuckDBPyConnection | None = None,
) -> int:
    """Return row count for a parquet file using DuckDB."""
    if connection is not None:
        row = connection.execute(
            "SELECT COUNT(*) FROM read_parquet(?)",
            [parquet_path.as_posix()],
        ).fetchone()
        return int(0 if row is None else row[0])

    con = duckdb.connect()
    try:
        row = con.execute(
            "SELECT COUNT(*) FROM read_parquet(?)",
            [parquet_path.as_posix()],
        ).fetchone()
        return int(0 if row is None else row[0])
    finally:
        con.close()


def _build_metadata_payload(
    *,
    parquet_path: Path,
    dataset: str,
    source_script: str,
    input_hashes: Mapping[str, str] | None = None,
    parameters: Mapping[str, Any] | None = None,
    row_count: int | None = None,
    created_at: str | None = None,
) -> dict[str, Any]:
    """Create spec-compliant metadata payload for one parquet output."""
    payload: dict[str, Any] = {
        "dataset": dataset,
        "created_at": created_at
        if created_at is not None
        else datetime.now(timezone.utc).replace(microsecond=0).isoformat().replace("+00:00", "Z"),
        "source_script": source_script,
        "git_commit": _current_git_commit(),
        "row_count": row_count if row_count is not None else _parquet_row_count(parquet_path),
        "file_size_bytes": parquet_path.stat().st_size,
        "content_hash": _parquet_content_hash(parquet_path),
    }

    if input_hashes is not None:
        payload["input_hashes"] = dict(input_hashes)
    if parameters is not None:
        payload["parameters"] = dict(parameters)

    return payload


def _write_metadata_json(
    metadata_path: Path,
    payload: Mapping[str, Any],
) -> None:
    """Write metadata payload as JSON to disk."""
    text = json.dumps(payload, separators=_JSON_COMPACT_SEPARATORS)
    metadata_path.write_text(text, encoding=_JSON_ENCODING)


def create_parquet_metadata_json(
    parquet_path: str | Path,
    *,
    dataset: str,
    source_script: str,
    input_hashes: Mapping[str, str] | None = None,
    parameters: Mapping[str, Any] | None = None,
    row_count_connection: duckdb.DuckDBPyConnection | None = None,
    created_at: str | None = None,
) -> Path:
    """Create ``metadata.json`` next to one parquet file."""
    parquet = Path(parquet_path)
    if not parquet.is_file():
        raise FileNotFoundError(f"data.parquet does not exist: {parquet}")

    metadata_path = parquet.parent / "metadata.json"
    if metadata_path.exists():
        raise FileExistsError(f"metadata.json already exists: {metadata_path}")

    payload = _build_metadata_payload(
        parquet_path=parquet,
        dataset=dataset,
        source_script=source_script,
        input_hashes=input_hashes,
        parameters=parameters,
        row_count=_parquet_row_count(parquet, connection=row_count_connection),
        created_at=created_at,
    )
    _write_metadata_json(
        metadata_path,
        payload,
    )
    return metadata_path
