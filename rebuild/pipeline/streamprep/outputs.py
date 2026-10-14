"""Deterministic Parquet and JSON writers with .sha256 sidecars (11.1.7, 11.9)."""
from __future__ import annotations

import os
from pathlib import Path

import pyarrow as pa
import pyarrow.compute as pc
import pyarrow.parquet as pq

from .contract import BuildError, canonical_json_bytes, sha256_file

ROW_GROUP_SIZE = 65_536


def _assert_no_nan(table: pa.Table, what: str) -> None:
    for name, col in zip(table.column_names, table.columns):
        if pa.types.is_floating(col.type) and col.null_count < len(col):
            if pc.any(pc.is_nan(col)).as_py():
                raise BuildError(f"{what}: column {name} would write NaN (11.1.7)")


def write_sidecar(path: Path) -> str:
    sha = sha256_file(path)
    (path.parent / (path.name + ".sha256")).write_bytes((sha + "\n").encode("ascii"))
    return sha


def write_parquet(table: pa.Table, path: Path) -> str:
    _assert_no_nan(table, path.name)
    table = table.replace_schema_metadata(None)
    path.parent.mkdir(parents=True, exist_ok=True)
    tmp = path.with_name(path.name + ".tmp")
    pq.write_table(table, tmp, compression="zstd", row_group_size=ROW_GROUP_SIZE,
                   use_dictionary=True, write_statistics=True)
    os.replace(tmp, path)
    return write_sidecar(path)


def write_json(obj: dict, path: Path, sidecar: bool = False) -> str | None:
    path.parent.mkdir(parents=True, exist_ok=True)
    tmp = path.with_name(path.name + ".tmp")
    tmp.write_bytes(canonical_json_bytes(obj) + b"\n")
    os.replace(tmp, path)
    return write_sidecar(path) if sidecar else None


def float_column(values) -> pa.Array:
    """NaN becomes a Parquet null, so NaN is never written."""
    return pa.array(values, type=pa.float64(), from_pandas=True)
