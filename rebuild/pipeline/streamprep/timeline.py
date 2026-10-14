"""The campaign timeline (protocol 3.1). Every second belongs to exactly one segment.

Instants are handled as integer milliseconds, so the coverage check is exact. EXCLUDED is the
default label: a segment gets another label only through the 3.3 table in contract.label_for.
"""
from __future__ import annotations

from dataclasses import dataclass

import numpy as np
import pandas as pd

from .contract import BuildError, label_for, to_ms
from .health import flagged_faults
from .loader import Campaign


@dataclass
class Timeline:
    segments: pd.DataFrame  # one row per segment, sorted by start
    start_ms: int
    end_ms: int

    def positive(self) -> pd.DataFrame:
        return self.segments[self.segments["end_ms"] > self.segments["start_ms"]]


def build_timeline(c: Campaign) -> Timeline:
    m = c.manifest
    # 2.5 with decision 22 (C23): the analysis, not the gate, flags the runs of a fault whose signature false-passed on
    # two or more shams of the campaign. The flag reaches every window of those runs through the segment flags.
    false_passing = flagged_faults(m, c.verifications)
    rows = []
    for s in m["campaign_segments"]:
        rows.append({
            "segment_type": s["segment_type"], "start_ms": to_ms(s["start_at"], "campaign segment"),
            "end_ms": to_ms(s["end_at"], "campaign segment"), "attempt_key": None, "run_id": None,
            "run_type": None, "outcome": None, "cell_key": None, "block": 0, "position": 0,
            "flags": (),
        })
    for att in m["attempts"]:
        for s in att["segments"]:
            rows.append({
                "segment_type": s["segment_type"], "start_ms": to_ms(s["start_at"], att["attempt_key"]),
                "end_ms": to_ms(s["end_at"], att["attempt_key"]), "attempt_key": att["attempt_key"],
                "run_id": att["run_id"], "run_type": att["run_type"], "outcome": att["outcome"],
                "cell_key": att["cell_key"], "block": att["block"], "position": att["position"],
                "flags": tuple(att["flags"]) + (("SIGNATURE_FALSE_PASS",) if att["run_type"] in false_passing else ()),
            })
    df = pd.DataFrame(rows)
    start_ms = to_ms(m["timeline_start_at"], "timeline_start_at")
    end_ms = to_ms(m["timeline_end_at"], "timeline_end_at")
    # Order by start, zero-length segments before the segment that starts at the same instant.
    df["_len"] = df["end_ms"] - df["start_ms"]
    df = df.sort_values(["start_ms", "_len"], kind="stable").drop(columns="_len").reset_index(drop=True)
    check_coverage(df, start_ms, end_ms)
    df["label"] = [label_for(t, rt, oc) for t, rt, oc in zip(df["segment_type"], df["run_type"], df["outcome"])]
    df.insert(0, "segment_index", np.arange(len(df), dtype=np.int32))
    tl = Timeline(df, start_ms, end_ms)
    check_per_second(tl)
    return tl


def check_coverage(df: pd.DataFrame, start_ms: int, end_ms: int) -> None:
    """Fails if any instant of [start, end) is unassigned or assigned twice (3.1)."""
    errors = []
    if end_ms <= start_ms:
        raise BuildError("timeline_end_at is not after timeline_start_at")
    cursor = start_ms
    for r in df.itertuples(index=False):
        if r.end_ms < r.start_ms:
            errors.append(f"segment {r.segment_type} of {r.attempt_key} ends before it starts")
            continue
        if r.start_ms > cursor:
            errors.append(f"unassigned time {cursor / 1000:.3f} to {r.start_ms / 1000:.3f} before {r.segment_type} of {r.attempt_key}")
        elif r.start_ms < cursor:
            errors.append(f"time {r.start_ms / 1000:.3f} to {min(cursor, r.end_ms) / 1000:.3f} assigned twice ({r.segment_type} of {r.attempt_key})")
        cursor = max(cursor, r.end_ms)
    if cursor < end_ms:
        errors.append(f"unassigned time {cursor / 1000:.3f} to {end_ms / 1000:.3f} at the end of the timeline")
    if cursor > end_ms:
        errors.append(f"segments run past timeline_end_at to {cursor / 1000:.3f}")
    if df["start_ms"].min() < start_ms:
        errors.append("a segment starts before timeline_start_at")
    if errors:
        raise BuildError(errors[:50])


def per_second_coverage(tl: Timeline) -> np.ndarray:
    """Milliseconds covered in each second of the timeline, counted segment by segment."""
    n = -((-(tl.end_ms - tl.start_ms)) // 1000)
    cov = np.zeros(n, dtype=np.int64)
    for a, b in zip(tl.segments["start_ms"].to_numpy(), tl.segments["end_ms"].to_numpy()):
        if b <= a:
            continue
        a0, b0 = a - tl.start_ms, b - tl.start_ms
        s_first, s_last = a0 // 1000, (b0 - 1) // 1000
        if s_first == s_last:
            cov[s_first] += b0 - a0
            continue
        cov[s_first] += (s_first + 1) * 1000 - a0
        cov[s_last] += b0 - s_last * 1000
        if s_last > s_first + 1:
            cov[s_first + 1:s_last] += 1000
    return cov


def check_per_second(tl: Timeline) -> None:
    cov = per_second_coverage(tl)
    expected = np.full(len(cov), 1000, dtype=np.int64)
    rem = (tl.end_ms - tl.start_ms) % 1000
    if rem:
        expected[-1] = rem
    bad = np.flatnonzero(cov != expected)
    if bad.size:
        raise BuildError([f"second {int(i)} of the timeline is covered for {int(cov[i])} ms, expected {int(expected[i])} ms"
                          for i in bad[:20]])


def per_second_labels(tl: Timeline) -> np.ndarray:
    """Label of the segment holding the start instant of each second. EXCLUDED unless 3.3 says otherwise."""
    n = -((-(tl.end_ms - tl.start_ms)) // 1000)
    out = np.full(n, "EXCLUDED", dtype=object)
    pos = tl.positive()
    instants = tl.start_ms + np.arange(n, dtype=np.int64) * 1000
    idx = segment_index_at(tl, instants)
    labels = pos["label"].to_numpy()
    ok = idx >= 0
    out[ok] = labels[idx[ok]]
    return out


def segment_index_at(tl: Timeline, instants_ms: np.ndarray) -> np.ndarray:
    """Row position in tl.positive() of the segment containing each instant, -1 outside the timeline."""
    pos = tl.positive()
    starts = pos["start_ms"].to_numpy()
    ends = pos["end_ms"].to_numpy()
    j = np.searchsorted(starts, instants_ms, side="right") - 1
    ok = (j >= 0) & (instants_ms < ends[np.clip(j, 0, len(ends) - 1)])
    return np.where(ok, j, -1)


def timeline_table(tl: Timeline):
    import pyarrow as pa
    df = tl.segments
    return pa.table({
        "segment_index": pa.array(df["segment_index"].to_numpy(), pa.int32()),
        "segment_type": pa.array(df["segment_type"].tolist(), pa.string()),
        "start_at": pa.array(df["start_ms"].to_numpy() / 1000.0, pa.float64()),
        "end_at": pa.array(df["end_ms"].to_numpy() / 1000.0, pa.float64()),
        "attempt_key": pa.array(df["attempt_key"].tolist(), pa.string()),
        "run_id": pa.array(df["run_id"].tolist(), pa.string()),
        "run_type": pa.array(df["run_type"].tolist(), pa.string()),
        "outcome": pa.array(df["outcome"].tolist(), pa.string()),
        "label": pa.array(df["label"].tolist(), pa.string()),
    })
