"""Telemetry rules recomputed from the exported files.

The attempt-level rule is scrape health (decision 21, CONTRACT_CHANGES C22): an attempt fails telemetry when an untargeted
scrape target misses more than `scrape_miss_max` scrapes in one segment. The gate writes the per-target counts into the
verification record's `scrapes`, and loader._check_verifications checks result.telemetry against them. This module
recomputes the counts independently from each attempt's exported verification file (Prometheus `up` and kube-state
`kube_pod_info`), with the same rule and the campaign's registered parameters (streamprep.health), and fails the build when a
valid attempt breaks it, so the record and the exported data cannot disagree.

Task-level series presence is no longer a verdict. It stays in the record as values and the SERIES_GAP flag, and every
window keeps meta__telemetry_ok and its per-source sample counts (3.4.4).
"""
from __future__ import annotations

import numpy as np

from .contract import VALID_OUTCOMES
from .health import read_verification_rows, scrape_health_rows, scrape_index
from .loader import Campaign
from .metrics_io import LongMetrics
from .timeline import Timeline

TELEMETRY_SEGMENTS = ("PRE_FAULT", "INJECTION", "RECOVERY")


def recompute_attempt_telemetry(c: Campaign, tl: Timeline, lm: LongMetrics, fr, cfg: dict) -> tuple[list[str], list[str]]:
    """(violations, scrape targets seen). A violation is a valid attempt whose recomputed scrape rows fail the rule, or
    differ from the rows its verification record holds."""
    th_tel = c.thresholds["gate"]["telemetry"]
    miss_max = int(th_tel["scrape_miss_max"])
    files = {mf.attempt_key: mf for mf in c.verification_files if mf.kind == "attempt"}
    violations: list[str] = []
    targets: set[str] = set()
    for att in c.attempts:
        if att["outcome"] not in VALID_OUTCOMES:
            continue
        key = att["attempt_key"]
        mf = files.get(key)
        rec = c.verifications.get(key)
        if mf is None or rec is None:
            violations.append(f"{key}: no exported verification file or record to recompute scrape health from")
            continue
        alive, up_ok = scrape_index(read_verification_rows(mf.path))
        rows = scrape_health_rows(alive, up_ok, att["segments"], att["run_type"], rec["targets_expected"], th_tel)
        targets |= {r["target"] for r in rows}
        if not rows:
            violations.append(f"{key}: no scrape target in the exported verification file")
        for r in rows:
            if not r["targeted"] and r["missed_count"] > miss_max:
                violations.append(f"{key} {r['segment']}: {r['target']} missed {r['missed_count']} of {r['expected_count']} "
                                  f"scrapes on an untargeted target (more than {miss_max})")
        norm = lambda rr: sorted((x["target"], x["segment"], x["expected_count"], x["missed_count"], x["targeted"]) for x in rr)  # noqa: E731
        if norm(rows) != norm(rec["scrapes"]):
            violations.append(f"{key}: the scrape rows recomputed from the export differ from the verification record")
    return violations, sorted(targets)


def baseline_outage(windows_meta: dict[str, np.ndarray], segment_type: np.ndarray,
                    label: np.ndarray | None = None) -> tuple[int, int]:
    """Count BASELINE windows with no Flink data at all, and BASELINE windows not telemetry_ok. With `label`, only windows
    still NORMAL count: a window the NORMAL-time health rule excluded (decision 24, C26) is never training data."""
    in_base = segment_type == "BASELINE"
    if label is not None:
        in_base = in_base & (label == "NORMAL")
    outage = in_base & ((windows_meta["meta__tm_reporting_mean"] == 0)
                        | (windows_meta["meta__samples__flink"] == 0)
                        | (windows_meta["meta__samples__consumer"] == 0))
    not_ok = in_base & ~windows_meta["meta__telemetry_ok"]
    return int(outage.sum()), int(not_ok.sum())
