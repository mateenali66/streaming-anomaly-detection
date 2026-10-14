"""Metric exporter: Prometheus range queries to the long layout of protocol 11.8.

Runs as the Job export-<attempt_key> (or export-baseline-<chunk_start>, export-trailing-<chunk_start>) under the
ServiceAccount metric-exporter, whose IAM role may write only baseline/ and export/ (11.7). It exits non-zero on
any query or upload error and never writes a partial file.

Rules implemented here (11.8):
  * one row per sample of one series; ts is int64 epoch seconds, a multiple of 10 (the range-query step);
  * series_id = first 16 hex of SHA-256 of metric + "|" + canonical JSON of {"pod": <pod>, "labels": <labels>};
  * labels = the remaining labels after removing instance, pod, pod_ip, host, tm_id, kubernetes_pod_name, uid,
    endpoint, service, namespace, job and prometheus (and __name__), never truncated; the registry host is stripped from
    every image reference (`image`, `image_id`, `image_spec`) before hashing, because an ECR host holds the account ID
    (decision 29, C24);
  * rows sorted by ts then series_id; a repeated (ts, series_id) pair fails the export; NaN is never written;
  * Parquet written with pyarrow, zstd, no pandas index; an absent sample has no row.
"""
from __future__ import annotations

import argparse
import io
import json
import math
import os
import re
import sys
from typing import Callable

import pyarrow as pa
import pyarrow.parquet as pq

from . import ids, queries
from .canonical import canonical_str, sha256_hex
from .clock import ceil10, floor10
from .config import FEATURE_SOURCES, VERIFICATION_SOURCES
from .errors import OrchestratorError, QueryError
from .prom import Prom

REMOVED_LABELS = {"__name__", "instance", "pod", "pod_ip", "host", "tm_id", "kubernetes_pod_name", "uid", "endpoint",
                  "service", "namespace", "job", "prometheus"}
STEP = 10
PIECE_S = 6 * 3600

METRIC_SCHEMA = pa.schema([
    pa.field("ts", pa.int64(), nullable=False),
    pa.field("series_id", pa.string(), nullable=False),
    pa.field("metric", pa.string(), nullable=False),
    pa.field("metric_type", pa.string(), nullable=False),
    pa.field("source", pa.string(), nullable=False),
    pa.field("role", pa.string(), nullable=False),
    pa.field("pod", pa.string(), nullable=False),
    pa.field("pod_uid", pa.string(), nullable=True),
    pa.field("broker_id", pa.int32(), nullable=True),
    pa.field("container", pa.string(), nullable=True),
    pa.field("labels", pa.string(), nullable=False),
    pa.field("value", pa.float64(), nullable=False),
])


IMAGE_LABEL_KEYS = ("image", "image_id", "image_spec")


def strip_registry_host(ref: str) -> str:
    """An image reference without its registry host: `<account>.dkr.ecr.<region>.amazonaws.com/repo@sha256:...` becomes
    `repo@sha256:...`. The first path component is a host when it holds a dot or a colon or is localhost (the Docker
    reference grammar). docker-pullable:// and similar scheme prefixes of image IDs are kept."""
    scheme = ""
    m = re.match(r"^([a-z0-9+.-]+://)(.*)$", ref)
    if m:
        scheme, ref = m.group(1), m.group(2)
    first, sep, rest = ref.partition("/")
    if sep and ("." in first or ":" in first or first == "localhost"):
        ref = rest
    return scheme + ref


def series_id(metric: str, pod: str, labels: dict) -> str:
    return sha256_hex((metric + "|" + canonical_str({"pod": pod, "labels": labels})).encode("utf-8"))[:16]


def role_of(pod: str, labels: dict) -> str:
    if ids.broker_id_from_pod(pod) is not None:
        return "broker"
    comp = labels.get("flink_component")
    if comp == "taskmanager" or (comp is None and "-taskmanager-" in pod):
        return "taskmanager"
    if comp == "jobmanager" or (comp is None and pod.startswith(f"{ids.FLINK_DEPLOYMENT}-") and "-taskmanager-" not in pod):
        return "jobmanager"
    if pod.startswith("load-generator"):
        return "loadgen"
    return "other"


def metric_type_of(metric: str, meta: dict[str, list[dict]]) -> str:
    candidates = [metric]
    for suffix in ("_total", "_count", "_sum", "_bucket", "_created"):
        if metric.endswith(suffix):
            candidates.append(metric[: -len(suffix)])
    for c in candidates:
        entries = meta.get(c)
        if entries:
            types = {e.get("type") for e in entries}
            if len(types) == 1:
                t = types.pop()
                if t in ("counter", "gauge", "summary", "histogram"):
                    return t
            return "unknown"
    return "unknown"


class UidLookup:
    """pod -> {ts: uid} from kube_pod_info, used for series that do not carry their own uid label."""

    def __init__(self) -> None:
        self.by_pod: dict[str, dict[int, str]] = {}

    def add(self, pod: str, ts: int, uid: str) -> None:
        self.by_pod.setdefault(pod, {})[ts] = uid

    def get(self, pod: str, ts: int) -> str | None:
        return self.by_pod.get(pod, {}).get(ts)


def build_uid_lookup(prom: Prom, cfg: dict, start: int, end: int) -> UidLookup:
    q = queries.selector("kube_pod_info", queries.source_matchers(cfg, "kube_state"))
    lookup = UidLookup()
    for labels, values in prom.range(q, start, end, STEP):
        pod, uid = labels.get("pod"), labels.get("uid")
        if pod and uid:
            for t, _ in values:
                lookup.add(pod, int(round(t)), uid)
    return lookup


def fetch_rows(prom: Prom, cfg: dict, source: str, pattern: str, start: int, end: int, meta: dict,
               uids: UidLookup) -> list[dict]:
    """Rows for one allowlist pattern. start and end are inclusive multiples of 10."""
    if start % STEP or end % STEP:
        raise OrchestratorError("export bounds must be multiples of 10")
    if end < start:
        return []
    q = queries.selector(pattern, queries.source_matchers(cfg, source))
    rx = re.compile(f"^(?:{pattern})$")
    rows = []
    pieces = []
    for lo in range(start, end + 1, PIECE_S):
        hi = min(end, lo + PIECE_S - STEP)
        pieces += prom.range(q, lo, hi, STEP)  # at most 2,160 points per series per query
    for labels, values in pieces:
        metric = labels.get("__name__")
        if not metric or not rx.match(metric):
            raise QueryError(f"series without a matching __name__ returned for {q!r}")
        if labels.get("job") == cfg["prometheus_jobs"]["probes"]:
            raise QueryError("a probe-job series reached the metric export")  # belt and braces, 2.3
        pod = labels.get("pod", "")
        rest = {k: (strip_registry_host(v) if k in IMAGE_LABEL_KEYS else v)
                for k, v in labels.items() if k not in REMOVED_LABELS}
        sid = series_id(metric, pod, rest)
        lab = canonical_str(rest)
        container = labels.get("container") or None
        bid = ids.broker_id_from_pod(pod)
        mtype = metric_type_of(metric, meta)
        role = role_of(pod, labels)
        own_uid = labels.get("uid")
        for t, v in values:
            if math.isnan(v):
                continue  # 11.1.7 and 11.8: NaN is never written, an absent sample has no row
            ts = int(round(t))
            if ts % STEP:
                raise QueryError(f"sample timestamp {t} is not on the 10 s grid")
            rows.append({"ts": ts, "series_id": sid, "metric": metric, "metric_type": mtype, "source": source,
                         "role": role, "pod": pod, "pod_uid": own_uid or (uids.get(pod, ts) if pod else None),
                         "broker_id": bid, "container": container, "labels": lab, "value": float(v)})
    return rows


def collect(prom: Prom, cfg: dict, allow: dict, sources: tuple[str, ...], start: int, end: int,
            meta: dict | None = None, uids: UidLookup | None = None) -> list[dict]:
    meta = prom.metadata() if meta is None else meta
    uids = build_uid_lookup(prom, cfg, start, end) if uids is None else uids
    seen_series: dict[str, str] = {}
    rows: list[dict] = []
    for source in sources:
        source_rows = 0
        for pattern in allow[source]:
            got = fetch_rows(prom, cfg, source, pattern, start, end, meta, uids)
            fresh = []
            for r in got:
                prev = seen_series.get(r["series_id"])
                if prev is None:
                    seen_series[r["series_id"]] = source
                elif prev != source:
                    raise OrchestratorError(f"series {r['metric']} matches allowlist sources {prev} and {source}")
                fresh.append(r)
            rows.extend(fresh)
            source_rows += len(fresh)
        if source_rows == 0:
            # An empty result is never read as "nothing happened": a whole source with no samples is an error.
            raise QueryError(f"source {source} returned no samples for {start}..{end}")
    return finalize(rows)


def finalize(rows: list[dict]) -> list[dict]:
    rows.sort(key=lambda r: (r["ts"], r["series_id"]))
    out, last = [], None
    for r in rows:
        k = (r["ts"], r["series_id"])
        if k == last:
            if out and out[-1]["value"] == r["value"] and out[-1]["metric"] == r["metric"]:
                continue  # the same series fetched by two patterns of one source
            raise OrchestratorError(f"repeated (ts, series_id) pair {k} with different values")
        out.append(r)
        last = k
    return out


def to_parquet(rows: list[dict]) -> bytes:
    table = pa.Table.from_pylist(rows, schema=METRIC_SCHEMA)
    buf = io.BytesIO()
    pq.write_table(table, buf, compression="zstd")
    return buf.getvalue()


def read_parquet(data: bytes, columns: list[str] | None = None) -> list[dict]:
    return pq.read_table(io.BytesIO(data), columns=columns).to_pylist()


# The only columns of features.parquet the gate may read: sample presence per source, never a value (2.1).
FEATURE_PRESENCE_COLUMNS = ["ts", "source", "role", "broker_id", "pod"]


def ts_range(data: bytes) -> tuple[int, int] | None:
    """(min ts, max ts) of an exported metric or probe file, from the ts column alone; None for an empty file."""
    import pyarrow.compute as pc
    t = pq.read_table(io.BytesIO(data), columns=["ts"])
    if t.num_rows == 0:
        return None
    mm = pc.min_max(t.column("ts")).as_py()
    return mm["min"], mm["max"]


def keys_for(kind: str, key: str) -> tuple[str, str]:
    if kind == "attempt":
        return f"export/attempts/{key}/features.parquet", f"export/attempts/{key}/verification.parquet"
    if kind == "baseline":
        return f"baseline/features/chunk-{key}.parquet", f"baseline/verification/chunk-{key}.parquet"
    if kind == "trailing":
        return f"export/trailing/features/chunk-{key}.parquet", f"export/trailing/verification/chunk-{key}.parquet"
    raise ValueError(kind)


def export_span(kind: str, start_at: float, end_at: float) -> tuple[int, int]:
    """Inclusive multiples of 10. Attempts: started_at rounded down to ended_at rounded up (11.7).
    Chunks: samples inside [start, end), so adjacent chunks never repeat a row."""
    if kind == "attempt":
        return floor10(start_at), ceil10(end_at)
    lo = ceil10(start_at)
    hi = ceil10(end_at) - STEP
    return lo, hi


def run_export(prom: Prom, store, cfg: dict, allow: dict, kind: str, key: str, start_at: float, end_at: float) -> dict:
    lo, hi = export_span(kind, start_at, end_at)
    meta = prom.metadata()
    uids = build_uid_lookup(prom, cfg, lo, hi)
    features = collect(prom, cfg, allow, FEATURE_SOURCES, lo, hi, meta, uids)
    verification = collect(prom, cfg, allow, VERIFICATION_SOURCES, lo, hi, meta, uids)
    fkey, vkey = keys_for(kind, key)
    fbytes, vbytes = to_parquet(features), to_parquet(verification)
    store.put(fkey, fbytes, "application/vnd.apache.parquet")
    store.put(vkey, vbytes, "application/vnd.apache.parquet")
    return {"features_key": fkey, "features_sha256": sha256_hex(fbytes), "features_rows": len(features),
            "verification_key": vkey, "verification_sha256": sha256_hex(vbytes),
            "verification_rows": len(verification), "span": [lo, hi]}


def main(argv: list[str] | None = None, prom_factory: Callable | None = None, store_factory: Callable | None = None) -> int:
    from .config import load_allowlist, load_config
    from .prom import HttpProm
    from .store import S3Store

    ap = argparse.ArgumentParser(prog="orchestrator export")
    ap.add_argument("--kind", required=True, choices=["attempt", "baseline", "trailing"])
    ap.add_argument("--key", required=True, help="attempt key, or the chunk start epoch")
    ap.add_argument("--start-at", required=True, type=float)
    ap.add_argument("--end-at", required=True, type=float)
    ap.add_argument("--config", default=None)
    args = ap.parse_args(argv)
    try:
        cfg = load_config(args.config)
        allow, allow_bytes = load_allowlist(os.environ.get("FEATURE_ALLOWLIST_FILE"))
        expected = os.environ.get("FEATURE_ALLOWLIST_SHA256")
        if not expected or expected != sha256_hex(allow_bytes):
            raise OrchestratorError("FEATURE_ALLOWLIST_SHA256 missing or different from the allowlist in this image")
        prom = (prom_factory or (lambda: HttpProm(os.environ.get("PROMETHEUS_URL", ids.PROMETHEUS_URL))))()
        def default_store():
            # RESULTS_BUCKET plus RESULTS_PREFIX=campaign/<CAMPAIGN_ID>, the one form of 11.11 (CONTRACT_CHANGES C5)
            bucket, prefix, cid = (os.environ.get(k, "").strip() for k in ("RESULTS_BUCKET", "RESULTS_PREFIX", "CAMPAIGN_ID"))
            if not bucket or "/" in bucket or prefix != f"campaign/{cid}" or not cid:
                raise OrchestratorError(f"RESULTS_BUCKET {bucket!r} and RESULTS_PREFIX {prefix!r} must be a bucket name and "
                                        f"campaign/<CAMPAIGN_ID>")
            return S3Store(f"s3://{bucket}/{prefix}/")
        store = (store_factory or default_store)()
        result = run_export(prom, store, cfg, allow, args.kind, args.key, args.start_at, args.end_at)
    except Exception as e:  # any failure is a non-zero exit; the orchestrator retries the Job (11.7)
        print(json.dumps({"ok": False, "error": f"{type(e).__name__}: {e}"}), flush=True)
        return 1
    print(json.dumps({"ok": True, **result}), flush=True)
    return 0


if __name__ == "__main__":
    sys.exit(main())
