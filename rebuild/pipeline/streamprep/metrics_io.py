"""Read the exported feature files (11.8) into one long, deduplicated sample table.

The data stay in long form (step, series, value) so a campaign with thousands of short-lived
series does not need a dense steps-by-series matrix. Every file is checked against the 11.8
layout before its rows are used.
"""
from __future__ import annotations

import ipaddress
import json
import re
from dataclasses import dataclass, field

import numpy as np
import pyarrow as pa
import pyarrow.compute as pc
import pyarrow.parquet as pq

from .contract import (
    FEATURE_SOURCES, METRIC_TYPES, POD_ROLES, REMOVED_LABELS, BuildError, ContractError, Errors,
    canonical_json_bytes, series_id_for,
)
from .loader import METRIC_COLUMNS, METRIC_NULLABLE, Campaign, MetricFile, check_parquet_schema

META_COLUMNS = ["series_id", "metric", "metric_type", "source", "role", "pod", "broker_id", "container", "labels"]
STRING_COLUMNS = ["series_id", "metric", "metric_type", "source", "role", "pod", "pod_uid", "container", "labels"]


def _codes(col) -> tuple[np.ndarray, list]:
    """Integer codes (-1 for null) and dictionary values of a string column."""
    if not pa.types.is_dictionary(col.type):
        col = pc.dictionary_encode(col)
    arr = col.unify_dictionaries().combine_chunks() if isinstance(col, pa.ChunkedArray) else col
    idx = arr.indices
    codes = idx.fill_null(-1).to_numpy().astype(np.int64) if idx.null_count else idx.to_numpy().astype(np.int64)
    return codes, arr.dictionary.to_pylist()
METRIC_NAME_RE = re.compile(r"^[a-zA-Z_:][a-zA-Z0-9_:]*$")


@dataclass
class SeriesTable:
    series_id: list[str] = field(default_factory=list)
    metric: list[str] = field(default_factory=list)
    metric_type: list[str] = field(default_factory=list)
    source: list[str] = field(default_factory=list)
    role: list[str] = field(default_factory=list)
    pod: list[str] = field(default_factory=list)
    broker_id: list[int | None] = field(default_factory=list)
    container: list[str | None] = field(default_factory=list)
    labels: list[dict] = field(default_factory=list)
    labels_str: list[str] = field(default_factory=list)

    def __len__(self) -> int:
        return len(self.series_id)


@dataclass
class LongMetrics:
    t0_ms: int
    n_steps: int
    step: np.ndarray  # int32, index into the 10 s grid starting at t0_ms
    sid: np.ndarray  # int32, index into series
    value: np.ndarray  # float64 raw sample
    series: SeriesTable
    series_id_form: str  # "dict" or "string", see README A9
    duplicates_dropped: int = 0

    def instants_ms(self) -> np.ndarray:
        return self.t0_ms + np.arange(self.n_steps, dtype=np.int64) * 10_000


def _looks_like_ip(s: str) -> bool:
    try:
        ipaddress.ip_address(s.split(":")[0])
        return True
    except ValueError:
        return False


def _register_series(row: dict, reg: dict, st: SeriesTable, errs: Errors, what: str,
                     form_votes: dict) -> None:
    sid = row["series_id"]
    meta = tuple(row[c] for c in META_COLUMNS[1:])
    if sid in reg:
        if reg[sid][1] != meta:
            errs.add(f"{what}: series {sid} has different metadata than in an earlier file")
        return
    labels_str = row["labels"]
    try:
        labels = json.loads(labels_str)
    except json.JSONDecodeError:
        errs.add(f"{what}: series {sid} labels are not JSON")
        labels = {}
    if not isinstance(labels, dict) or any(not isinstance(v, str) for v in labels.values()):
        errs.add(f"{what}: series {sid} labels must be an object of strings")
        labels = {}
    elif canonical_json_bytes(labels).decode("utf-8") != labels_str:
        errs.add(f"{what}: series {sid} labels are not canonical JSON")
    removed = sorted(set(labels) & set(REMOVED_LABELS))
    if removed:
        errs.add(f"{what}: series {sid} keeps labels {removed} that 11.8 removes")
    if row["source"] not in FEATURE_SOURCES:
        errs.add(f"{what}: series {sid} source {row['source']!r} is not a feature source")
    if row["metric_type"] not in METRIC_TYPES:
        errs.add(f"{what}: series {sid} metric_type {row['metric_type']!r} is unknown to 11.8")
    if row["role"] not in POD_ROLES:
        errs.add(f"{what}: series {sid} role {row['role']!r} is unknown to 11.8")
    if not METRIC_NAME_RE.match(row["metric"] or ""):
        errs.add(f"{what}: series {sid} metric {row['metric']!r} is not a Prometheus metric name")
    pod = row["pod"]
    if pod and _looks_like_ip(pod):
        errs.add(f"{what}: series {sid} pod {pod!r} is an IP address (11.8)")
    bid = row["broker_id"]
    if row["role"] == "broker":
        if bid is None or bid not in (0, 1, 2):
            errs.add(f"{what}: broker series {sid} has broker_id {bid}, expected 0, 1 or 2")
        elif not pod.endswith(f"-{bid}"):
            errs.add(f"{what}: broker series {sid} pod {pod!r} does not end in its ordinal {bid}")
    elif bid is not None:
        errs.add(f"{what}: non-broker series {sid} carries broker_id {bid}")
    as_dict = series_id_for(row["metric"], pod, labels)
    as_str = series_id_for(row["metric"], pod, labels_str)
    if sid == as_dict:
        form_votes["dict"] += 1
    if sid == as_str:
        form_votes["string"] += 1
    if sid not in (as_dict, as_str):
        form_votes["none"].append(sid)
    reg[sid] = (len(st), meta)
    st.series_id.append(sid)
    st.metric.append(row["metric"])
    st.metric_type.append(row["metric_type"])
    st.source.append(row["source"])
    st.role.append(row["role"])
    st.pod.append(pod)
    st.broker_id.append(bid)
    st.container.append(row["container"])
    st.labels.append(labels)
    st.labels_str.append(labels_str)


def read_feature_files(c: Campaign, t0_ms: int, n_steps: int, cfg: dict) -> LongMetrics:
    errs = Errors()
    reg: dict[str, tuple[int, tuple]] = {}
    st = SeriesTable()
    votes = {"dict": 0, "string": 0, "none": []}
    steps, sids, vals = [], [], []
    for mf in c.feature_files:
        _read_one(mf, t0_ms, n_steps, reg, st, votes, steps, sids, vals, errs)
    errs.raise_if_any()
    if votes["none"]:
        raise ContractError(f"series_id is not sha256(metric|canonical {{pod, labels}})[:16] for "
                            f"{len(votes['none'])} series, for example {votes['none'][:3]}")
    n_series = len(st)
    if votes["dict"] == n_series:
        form = "dict"
    elif votes["string"] == n_series:
        form = "string"
    else:
        raise ContractError("series_id mixes the labels-object and labels-string forms across series")
    # Canonical series order (sorted series_id), so outputs do not depend on how the exporter split
    # or ordered its files. Floating sums over series then always run in the same order.
    perm = sorted(range(n_series), key=lambda i: st.series_id[i])
    new_index = np.empty(n_series, dtype=np.int32)
    new_index[perm] = np.arange(n_series, dtype=np.int32)
    for f_name in ("series_id", "metric", "metric_type", "source", "role", "pod", "broker_id", "container",
                   "labels", "labels_str"):
        old_list = getattr(st, f_name)
        setattr(st, f_name, [old_list[i] for i in perm])
    sids = [new_index[s_] for s_ in sids]
    # Files were appended in read order, so a stable sort by (series, step) keeps the later file's
    # sample last among duplicates.
    key = np.concatenate([s_.astype(np.int64) * n_steps + st_ for s_, st_ in zip(sids, steps)]) if steps \
        else np.zeros(0, np.int64)
    value = np.concatenate(vals) if vals else np.zeros(0, np.float64)
    del steps, sids, vals
    order = np.argsort(key, kind="stable")
    key = key[order]
    value = value[order]
    del order
    dup_next = np.zeros(len(key), dtype=bool)
    if len(key) > 1:
        dup_next[:-1] = key[1:] == key[:-1]
    n_dup = int(dup_next.sum())
    policy = cfg["export_duplicates"]
    if n_dup:
        i = np.flatnonzero(dup_next)
        differ = value[i] != value[i + 1]
        if differ.any() and policy == "error":
            j = i[differ][:5]
            msgs = [f"series {st.series_id[int(key[k] // n_steps)]} at ts {(t0_ms + int(key[k] % n_steps) * 10_000) // 1000}: "
                    f"{value[k]!r} and {value[k + 1]!r} in two export files" for k in j]
            raise BuildError(["conflicting duplicate samples across export files "
                              f"({int(differ.sum())} pairs). Set export_duplicates to prefer_later "
                              "only after the exporter boundary behaviour is understood"] + msgs)
    keep = ~dup_next
    key, value = key[keep], value[keep]
    step = (key % n_steps).astype(np.int32)
    sid = (key // n_steps).astype(np.int32)
    del key
    return LongMetrics(t0_ms, n_steps, step, sid, value, st, form, n_dup)


def _read_one(mf: MetricFile, t0_ms: int, n_steps: int, reg: dict, st: SeriesTable, votes: dict,
              steps: list, sids: list, vals: list, errs: Errors) -> None:
    what = mf.key
    if not check_parquet_schema(mf.path, METRIC_COLUMNS, METRIC_NULLABLE, what, errs):
        return
    # String columns are read dictionary-encoded, so no per-row string is materialised.
    tbl = pq.read_table(mf.path, read_dictionary=STRING_COLUMNS)
    for name in METRIC_COLUMNS:
        if name not in METRIC_NULLABLE and tbl.column(name).null_count:
            errs.add(f"{what}: column {name} has nulls")
            return
    n = tbl.num_rows
    if n == 0:
        return
    ts = tbl.column("ts").to_numpy().astype(np.int64)
    if (ts % 10 != 0).any():
        errs.add(f"{what}: ts values that are not multiples of 10")
        return
    ts_ms = ts * 1000
    if ts_ms.min() < mf.span_lo_ms or ts_ms.max() > mf.span_hi_ms:
        errs.add(f"{what}: rows outside the export span "
                 f"[{mf.span_lo_ms // 1000}, {mf.span_hi_ms // 1000}] (11.7, 11.10)")
        return
    value = tbl.column("value").to_numpy().astype(np.float64)
    if not np.isfinite(value).all():
        errs.add(f"{what}: value holds NaN or infinity (11.1.7)")
        return
    codes = {name: _codes(tbl.column(name)) for name in STRING_COLUMNS}
    local, dict_vals = codes["series_id"]
    rank = np.empty(len(dict_vals), dtype=np.int64)
    rank[np.argsort(np.array(dict_vals, dtype=object), kind="stable")] = np.arange(len(dict_vals))
    r = rank[local]
    if n > 1:
        dt = np.diff(ts)
        dr = np.diff(r)
        if not ((dt > 0) | ((dt == 0) & (dr > 0))).all():
            errs.add(f"{what}: rows are not sorted by ts then series_id, or a (ts, series_id) pair repeats (11.8)")
            return
    # Metadata must be constant within a series (pod_uid may change when a pod restarts).
    first = np.full(len(dict_vals), -1, dtype=np.int64)
    first[local[::-1]] = np.arange(n - 1, -1, -1)
    bid = tbl.column("broker_id").to_numpy(zero_copy_only=False)
    bid = np.where(np.isnan(bid.astype(float)), -1, bid).astype(np.int64)
    meta_codes = [codes[c][0] for c in META_COLUMNS[1:] if c != "broker_id"] + [bid]
    for arr in meta_codes:
        if (arr != arr[first[local]]).any():
            bad = dict_vals[int(local[np.flatnonzero(arr != arr[first[local]])[0]])]
            errs.add(f"{what}: series {bad} has more than one metadata row")
            return
    for li, sid in enumerate(dict_vals):
        row_i = int(first[li])
        row = {"series_id": sid}
        for c in META_COLUMNS[1:]:
            if c == "broker_id":
                row[c] = None if bid[row_i] < 0 else int(bid[row_i])
            else:
                cl, cv = codes[c]
                row[c] = None if cl[row_i] < 0 else cv[cl[row_i]]
        _register_series(row, reg, st, errs, what, votes)
    if errs:
        return
    gmap = np.array([reg[s][0] for s in dict_vals], dtype=np.int32)
    stp = (ts_ms - t0_ms) // 10_000
    if stp.max() >= n_steps or stp.min() < 0:
        errs.add(f"{what}: rows fall outside the campaign timeline grid")
        return
    steps.append(stp.astype(np.int32))
    sids.append(gmap[local])
    vals.append(value)
