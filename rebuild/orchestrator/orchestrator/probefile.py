"""probes.parquet (protocol 11.8): one row per probe sample."""
from __future__ import annotations

import io
import math

import pyarrow as pa
import pyarrow.parquet as pq

PROBE_TYPES = ("tcp", "tcp_burst", "controller", "flink_jobs", "flink_tms", "loadgen", "consumer")

PROBE_SCHEMA = pa.schema([
    pa.field("ts", pa.float64(), nullable=False),
    pa.field("probe", pa.string(), nullable=False),
    pa.field("src_pod", pa.string(), nullable=True),
    pa.field("dst", pa.string(), nullable=True),
    pa.field("tried_count", pa.int32(), nullable=False),
    pa.field("ok_count", pa.int32(), nullable=False),
    pa.field("timeout_s", pa.float64(), nullable=False),
    pa.field("latency_s", pa.float64(), nullable=True),
    pa.field("value", pa.float64(), nullable=True),
    pa.field("detail", pa.string(), nullable=True),
    pa.field("error", pa.string(), nullable=True),
])
COLUMNS = [f.name for f in PROBE_SCHEMA]


def validate_row(r: dict) -> None:
    if set(r) != set(COLUMNS):
        raise ValueError(f"probe row has columns {sorted(r)}")
    if r["probe"] not in PROBE_TYPES:
        raise ValueError(f"unknown probe {r['probe']!r}")
    if not isinstance(r["ts"], float) or not math.isfinite(r["ts"]):
        raise ValueError("probe ts must be a finite float")
    if not 0 <= r["ok_count"] <= r["tried_count"]:
        raise ValueError("ok_count must be between 0 and tried_count")
    expected_tries = {"tcp": 1, "tcp_burst": 10}.get(r["probe"], 1)
    if r["tried_count"] != expected_tries:
        raise ValueError(f"{r['probe']} must have tried_count {expected_tries}")
    for k in ("latency_s", "value"):
        if r[k] is not None and not math.isfinite(r[k]):
            raise ValueError(f"{k} must be finite or null")


def to_parquet(rows: list[dict]) -> bytes:
    rows = sorted(rows, key=lambda r: (r["ts"], r["probe"], r["src_pod"] or "", r["dst"] or ""))
    for r in rows:
        validate_row(r)
    table = pa.Table.from_pylist(rows, schema=PROBE_SCHEMA)
    buf = io.BytesIO()
    pq.write_table(table, buf, compression="zstd")
    return buf.getvalue()


def read_parquet(data: bytes) -> list[dict]:
    return pq.read_table(io.BytesIO(data)).to_pylist()


def row(ts: float, probe: str, *, src_pod=None, dst=None, tried=1, ok=0, timeout_s=0.0, latency_s=None, value=None,
        detail=None, error=None) -> dict:
    return {"ts": float(ts), "probe": probe, "src_pod": src_pod, "dst": dst, "tried_count": int(tried),
            "ok_count": int(ok), "timeout_s": float(timeout_s), "latency_s": latency_s, "value": value,
            "detail": detail, "error": error}
