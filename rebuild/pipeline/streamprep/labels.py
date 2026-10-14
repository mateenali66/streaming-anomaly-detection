"""Window labels that depend on the verification channel (protocol 3.3 and 3.4, decision 24, CONTRACT_CHANGES C26).

The 3.3 table labels a window from its segment, run type and outcome alone (contract.label_for, timeline.py). One rule is
added on top, fixed before any data exist: a BASELINE or TRAILING window that fails the NORMAL-time health conditions of
streamprep.health is EXCLUDED, with the failing conditions counted per window size and segment in build_report.json. The
conditions are the ones the gate applies to every PRE_FAULT segment, so NORMAL time is healthy wherever it comes from.
"""
from __future__ import annotations

from collections import Counter

import numpy as np
import pyarrow.parquet as pq

from .contract import FLINK_JOBS
from .health import Evidence, read_verification_rows, window_health
from .loader import Campaign

PROBE_COLUMNS = ["ts", "probe", "src_pod", "dst", "tried_count", "ok_count", "value", "detail", "error"]
HEALTH_SEGMENTS = ("BASELINE", "TRAILING")
EVIDENCE_MARGIN_S = 10.0


def normal_time_evidence(c: Campaign) -> Evidence:
    """Probe samples of probes/baseline and probes/trailing, and kube-state rows of the baseline and trailing exports."""
    rows: list[dict] = []
    for p in c.probe_files:
        key = p.relative_to(c.root).as_posix()
        if key.startswith("probes/baseline/") or key.startswith("probes/trailing/"):
            rows += pq.read_table(p, columns=PROBE_COLUMNS).to_pylist()
    kube: list[dict] = []
    for mf in c.verification_files:
        if mf.kind in ("baseline", "trailing"):
            kube += [r for r in read_verification_rows(mf.path) if r["metric"] != "up"]
    return Evidence(rows, kube, list(FLINK_JOBS[c.workload]), c.thresholds["gate"]["pre_fault"])


def apply_normal_time_health(c: Campaign, windows: dict) -> dict:
    """Sets label EXCLUDED on every BASELINE and TRAILING window that fails the rule, in place. Returns the counts per
    window size: windows checked, windows excluded, and how often each condition failed."""
    ev = normal_time_evidence(c)
    report = {}
    for w, df in windows.items():
        seg = df["segment_type"].to_numpy()
        mask = np.isin(seg, HEALTH_SEGMENTS)
        idx = np.flatnonzero(mask)
        fails = window_health(ev, df["start_at"].to_numpy()[idx], df["end_at"].to_numpy()[idx], EVIDENCE_MARGIN_S)
        bad = np.asarray([bool(f) for f in fails], dtype=bool)
        if bad.any():
            col = df.columns.get_loc("label")
            df.iloc[idx[bad], col] = "EXCLUDED"
        per_seg = {}
        for s in HEALTH_SEGMENTS:
            sel = seg[idx] == s
            per_seg[s] = {"windows": int(sel.sum()), "excluded": int((bad & sel).sum()),
                          "failed_conditions": dict(sorted(Counter(x for f, k in zip(fails, sel) if k for x in f).items()))}
        report[str(w)] = per_seg
    return report
