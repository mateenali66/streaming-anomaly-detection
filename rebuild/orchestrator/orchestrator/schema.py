"""Strict validators for the section 11 records: plan (11.3), manifest and attempts (11.4, 11.5), verification
record (11.6), completeness marker (11.10) and baseline statistics (11.12).

An unknown field is an error (11.1.6). A field is nullable only where the contract says so.
"""
from __future__ import annotations

import math
import re
from typing import Any

from . import ids
from .errors import SchemaError

OUTCOMES = ("VALID", "VALID_REMEDIATED", "GATE_FAILED", "TELEMETRY_FAILED", "ABORTED")
# SIGNATURE_FALSE_PASS stays a known flag for the windows of 11.9, which the analysis sets (C23); the orchestrator never
# writes it into the manifest. SERIES_GAP (C22) and AFTER_RESUBMIT (C28) are new.
FLAGS = ("AFTER_REDEPLOY", "AFTER_RESUBMIT", "OVERRUN", "F7_TM_LOSS", "REMEDIATED", "HEALTH_EVENT", "SERIES_GAP",
         "SIGNATURE_FALSE_PASS")
MANIFEST_FLAGS = tuple(f for f in FLAGS if f != "SIGNATURE_FALSE_PASS")
ATTEMPT_SEGMENTS = ("SETTLE", "PRE_FAULT", "APPLY", "INJECTION", "RECOVERY", "COOLDOWN", "GATE")
CAMPAIGN_SEGMENTS = ("WARMUP", "BASELINE", "TRAILING", "IDLE")
ONSET_SOURCES = ("CHAOS_APPLY_EVENT", "FIRST_PRODUCE_ACK", "PLANNED")
END_SOURCES = ("CHAOS_RECOVER_EVENT", "FIXED_SPAN", "LAST_PRODUCE_ACK", "PLANNED")
SHA_RE = re.compile(r"^[0-9a-f]{64}$")

STR, INT, NUM, BOOL, OBJ, ARR, SHA = "str", "int", "num", "bool", "obj", "arr", "sha"


def _type_ok(v: Any, t: str) -> bool:
    if t == STR:
        return isinstance(v, str)
    if t == INT:
        return isinstance(v, int) and not isinstance(v, bool)
    if t == NUM:
        return isinstance(v, (int, float)) and not isinstance(v, bool) and math.isfinite(v)
    if t == BOOL:
        return isinstance(v, bool)
    if t == OBJ:
        return isinstance(v, dict)
    if t == ARR:
        return isinstance(v, list)
    if t == SHA:
        return isinstance(v, str) and bool(SHA_RE.match(v))
    raise ValueError(t)


def check(obj: Any, spec: dict, path: str) -> None:
    """spec: name -> (type, nullable[, sub]) where sub is a nested spec (OBJ) or an element spec (ARR)."""
    if not isinstance(obj, dict):
        raise SchemaError(f"{path}: expected an object")
    unknown = set(obj) - set(spec)
    missing = set(spec) - set(obj)
    if unknown:
        raise SchemaError(f"{path}: unknown fields {sorted(unknown)}")
    if missing:
        raise SchemaError(f"{path}: missing fields {sorted(missing)}")
    for name, s in spec.items():
        t, nullable = s[0], s[1]
        sub = s[2] if len(s) > 2 else None
        v = obj[name]
        p = f"{path}.{name}"
        if v is None:
            if not nullable:
                raise SchemaError(f"{p}: null not allowed")
            continue
        if not _type_ok(v, t):
            raise SchemaError(f"{p}: expected {t}, got {type(v).__name__} ({v!r:.60})")
        if t == OBJ and isinstance(sub, dict):
            check(v, sub, p)
        if t == ARR and sub is not None:
            for i, el in enumerate(v):
                if isinstance(sub, dict):
                    check(el, sub, f"{p}[{i}]")
                elif not _type_ok(el, sub):
                    raise SchemaError(f"{p}[{i}]: expected {sub}")


def enum(v: Any, allowed, path: str) -> None:
    if v not in allowed:
        raise SchemaError(f"{path}: {v!r} not in {allowed}")


# ===================================================================================================== plan
CELL = {"position": (INT, False), "cell_key": (STR, False), "run_type": (STR, False), "settle_gap_s": (INT, False),
        "follower_rank": (INT, False), "sham_duration_s": (INT, False), "tm_kill_ranks": (ARR, False, INT)}
PLAN = {
    "schema_version": (STR, False), "workload": (STR, False), "campaign_index": (INT, False), "campaign_id": (STR, False),
    "seed": (STR, False), "seed_rule": (STR, False), "generator": (STR, False), "numpy_version": (STR, False),
    "run_types": (ARR, False, {"run_type": (STR, False), "name": (STR, False), "kind": (STR, False),
                               "duration_s": (INT, False), "params": (OBJ, False)}),
    "phase_s": (OBJ, False, {k: (INT, False) for k in ("settle_max", "pre_fault", "recovery", "cooldown", "gate_hold",
                                                       "gate_timeout", "remediation_timeout", "apply_max",
                                                       "redeploy_extra_settle")}),
    "blocks": (ARR, False, {"block": (INT, False), "cells": (ARR, False, CELL)}),
    "reserve_settle_gap_s": (ARR, False, INT), "load_generator_seed": (INT, False), "f6_payload_seed": (INT, False),
}


def validate_plan(plan: dict) -> None:
    check(plan, PLAN, "plan")
    if plan["schema_version"] != "1":
        raise SchemaError("plan.schema_version must be '1'")
    if [r["run_type"] for r in plan["run_types"]] != ["F0", "F1", "F2", "F4", "F5", "F6", "F7", "F8"]:
        raise SchemaError("plan.run_types must be in the order F0, F1, F2, F4, F5, F6, F7, F8")
    for rt in plan["run_types"]:
        for k, v in rt["params"].items():
            if not (isinstance(v, str) or (isinstance(v, int) and not isinstance(v, bool))):
                raise SchemaError(f"plan.run_types[{rt['run_type']}].params.{k} must be an integer or a string")
    smax = plan["phase_s"]["settle_max"]
    for b in plan["blocks"]:
        if [c["position"] for c in b["cells"]] != list(range(1, 9)):
            raise SchemaError(f"block {b['block']} positions must be 1 to 8")
        if sorted(c["run_type"] for c in b["cells"]) != sorted(ids.RUN_TYPES):
            raise SchemaError(f"block {b['block']} must hold each run type once")
        for c in b["cells"]:
            if c["cell_key"] != ids.cell_key(plan["workload"], plan["campaign_index"], b["block"], c["position"]):
                raise SchemaError(f"bad cell key {c['cell_key']}")
            if not 0 <= c["settle_gap_s"] <= smax or c["follower_rank"] not in (0, 1) or c["sham_duration_s"] not in (180, 300):
                raise SchemaError(f"cell {c['cell_key']} field out of range")
            if len(c["tm_kill_ranks"]) != 5 or not all(0 <= x <= 2 for x in c["tm_kill_ranks"]):
                raise SchemaError(f"cell {c['cell_key']} tm_kill_ranks must be 5 integers 0 to 2")
    if len(plan["reserve_settle_gap_s"]) != 40 or not all(0 <= x <= smax for x in plan["reserve_settle_gap_s"]):
        raise SchemaError("reserve_settle_gap_s must be 40 integers in 0..settle_max")
    for k in ("load_generator_seed", "f6_payload_seed"):
        if not 0 <= plan[k] <= 2147483646:
            raise SchemaError(f"{k} out of range")


# ===================================================================================================== manifest
SEGMENT = {"segment_type": (STR, False), "start_at": (NUM, False), "end_at": (NUM, False)}
RESOURCE = {"kind": (STR, False), "name": (STR, False), "created_at": (NUM, False), "delete_called_at": (NUM, True),
            "not_found_at": (NUM, True)}
ATTEMPT = {
    "attempt_key": (STR, False), "run_id": (STR, False), "cell_key": (STR, False),
    "block": (INT, False), "position": (INT, False), "attempt": (INT, False),
    "run_type": (STR, False), "fault_name": (STR, False), "status": (STR, False), "outcome": (STR, True),
    "failed_checks": (ARR, False, STR), "flags": (ARR, False, STR),
    "started_at": (NUM, False), "ended_at": (NUM, True), "planned_onset_at": (NUM, False),
    "apply_called_at": (NUM, True), "onset_verified_at": (NUM, True), "onset_source": (STR, True),
    "configured_duration_s": (INT, False), "labelled_end_at": (NUM, True), "end_source": (STR, True),
    "overrun_s": (NUM, True), "applied": (BOOL, False), "aborted_at": (NUM, True),
    "resources": (ARR, False, RESOURCE),
    "controller_at_onset": (OBJ, True, {"node_id": (INT, False), "pod": (STR, False)}),
    "segments": (ARR, False, SEGMENT),
    "recovery_gate": (OBJ, False, {"started_at": (NUM, True), "passed_at": (NUM, True),
                                   "remediation": (OBJ, False, {"performed": (BOOL, False), "actions": (ARR, False, STR),
                                                                "started_at": (NUM, True), "ended_at": (NUM, True)})}),
    "flink_jobs_at_end": (ARR, False, {"name": (STR, False), "job_id": (STR, False), "state": (STR, False)}),
    "verification_key": (STR, True), "verification_sha256": (STR, True),
    "export": (OBJ, False, {"features_key": (STR, False), "features_sha256": (STR, False),
                            "verification_metrics_key": (STR, False), "verification_metrics_sha256": (STR, False),
                            "probes_key": (STR, False), "probes_sha256": (STR, False), "status": (STR, False)}),
}
MANIFEST = {
    "schema_version": (STR, False), "campaign_id": (STR, False), "workload": (STR, False), "campaign_index": (INT, False),
    "cluster_name": (STR, False), "region": (STR, False), "availability_zone": (STR, False),
    "kubernetes_version": (STR, False), "node_ami_release": (STR, False), "plan_sha256": (SHA, False), "seed": (STR, False),
    "orchestrator": (OBJ, False, {"git_commit": (STR, False), "image_digest": (STR, False)}),
    "gate": (OBJ, False, {"git_commit": (STR, False), "thresholds_sha256": (SHA, False)}),
    "images_lock_sha256": (SHA, False), "feature_allowlist_sha256": (SHA, False),
    "flink_jobs_expected": (ARR, False, STR), "created_at": (NUM, False), "updated_at": (NUM, False),
    "timeline_start_at": (NUM, False), "timeline_end_at": (NUM, True), "clock_skew_s": (NUM, False),
    "campaign_segments": (ARR, False, SEGMENT),
    "baseline_stats_key": (STR, True), "baseline_stats_sha256": (STR, True),
    "attempts": (ARR, False, ATTEMPT),
    "halts": (ARR, False, {"at": (NUM, False), "reason": (STR, False), "attempt_key": (STR, True),
                           "resumed_at": (NUM, True), "resolution": (STR, True)}),
    "node_inventory": (ARR, False, {"at": (NUM, False), "instance_ids": (ARR, False, STR)}),
    # C30: one entry per AWS Health event, from the CloudWatch Logs group infra's EventBridge rule writes. event_key is the
    # first 16 hex of SHA-256 of the event ARN; start_at and end_at are the event's own times (end_at null while open).
    "health_events": (ARR, False, {"at": (NUM, False), "service": (STR, False), "event_type_code": (STR, False),
                                   "status": (STR, False), "event_key": (STR, False), "start_at": (NUM, False),
                                   "end_at": (NUM, True)}),
    "health_events_polled_until": (NUM, True),
    "status": (STR, False),
}
EXPECTED_ORDER = {
    # Allowed successors of each segment type within an attempt (11.5 table).
    None: ("SETTLE",), "SETTLE": ("PRE_FAULT",), "PRE_FAULT": ("APPLY",), "APPLY": ("INJECTION", "GATE"),
    "INJECTION": ("RECOVERY",), "RECOVERY": ("COOLDOWN",), "COOLDOWN": ("GATE",), "GATE": (),
}


def validate_attempt(a: dict, path: str, workload: str, index: int) -> None:
    check(a, ATTEMPT, path)
    k = ids.parse_attempt_key(a["attempt_key"])
    if (k["workload"], k["campaign_index"]) != (workload, index):
        raise SchemaError(f"{path}: key {a['attempt_key']} is from another campaign")
    if a["cell_key"] != k["cell_key"] or a["block"] != k["block"] or a["position"] != k["position"] or a["attempt"] != k["attempt"]:
        raise SchemaError(f"{path}: block, position, attempt or cell_key disagree with the key")
    if not ids.UUID4_RE.match(a["run_id"]):
        raise SchemaError(f"{path}: run_id must be a lower-case UUID4")
    enum(a["run_type"], ids.RUN_TYPES, f"{path}.run_type")
    enum(a["status"], ("RUNNING", "FINISHED"), f"{path}.status")
    if a["status"] == "RUNNING":
        if a["outcome"] is not None or a["verification_key"] is not None or a["verification_sha256"] is not None:
            raise SchemaError(f"{path}: a RUNNING attempt has no outcome and no verification record yet (2.7.5)")
    else:
        enum(a["outcome"], OUTCOMES, f"{path}.outcome")
        if a["ended_at"] is None or a["verification_key"] != f"attempts/{a['attempt_key']}/verification.json" \
                or not SHA_RE.match(a["verification_sha256"] or ""):
            raise SchemaError(f"{path}: a FINISHED attempt needs ended_at and its verification record")
    for f in a["flags"]:
        enum(f, MANIFEST_FLAGS, f"{path}.flags")
    if a["onset_source"] is not None:
        enum(a["onset_source"], ONSET_SOURCES, f"{path}.onset_source")
    if a["end_source"] is not None:
        enum(a["end_source"], END_SOURCES, f"{path}.end_source")
    if a["run_type"] == "F0" and (a["applied"] or a["apply_called_at"] is not None):
        raise SchemaError(f"{path}: a sham applies nothing")
    exp = a["export"]
    enum(exp["status"], ("PENDING", "DONE", "FAILED"), f"{path}.export.status")
    want = {"features_key": f"export/attempts/{a['attempt_key']}/features.parquet",
            "verification_metrics_key": f"export/attempts/{a['attempt_key']}/verification.parquet",
            "probes_key": f"attempts/{a['attempt_key']}/probes.parquet"}
    for kk, v in want.items():
        if exp[kk] != v:
            raise SchemaError(f"{path}.export.{kk} must be {v}")
    for kk in ("features_sha256", "verification_metrics_sha256", "probes_sha256"):
        if exp["status"] == "DONE" and not SHA_RE.match(exp[kk]):
            raise SchemaError(f"{path}.export.{kk} must be a SHA-256 when DONE")
        if exp["status"] != "DONE" and exp[kk] != "" and not SHA_RE.match(exp[kk]):
            raise SchemaError(f"{path}.export.{kk} must be empty or a SHA-256")
    if a["status"] == "FINISHED" and exp["status"] != "DONE":
        raise SchemaError(f"{path}: a FINISHED attempt has its export DONE")
    # Segments: in order, contiguous from started_at, no gaps or overlaps (11.5).
    prev_type, t = None, a["started_at"]
    for i, s in enumerate(a["segments"]):
        enum(s["segment_type"], ATTEMPT_SEGMENTS, f"{path}.segments[{i}]")
        if s["segment_type"] not in EXPECTED_ORDER[prev_type]:
            raise SchemaError(f"{path}.segments[{i}]: {s['segment_type']} cannot follow {prev_type}")
        if abs(s["start_at"] - t) > 1e-6 or s["end_at"] < s["start_at"]:
            raise SchemaError(f"{path}.segments[{i}]: gap, overlap or negative length")
        prev_type, t = s["segment_type"], s["end_at"]
    if a["ended_at"] is not None:
        if not a["segments"] or abs(a["segments"][-1]["end_at"] - a["ended_at"]) > 1e-6:
            raise SchemaError(f"{path}: segments must end at ended_at")
        if a["status"] == "FINISHED" and a["outcome"] != "ABORTED" and prev_type != "GATE":
            raise SchemaError(f"{path}: a finished attempt that was not aborted ends with GATE")
    types = [s["segment_type"] for s in a["segments"]]
    if a["status"] == "FINISHED" and a["outcome"] != "ABORTED":
        has_inj = "INJECTION" in types
        if has_inj != (a["applied"] or a["run_type"] == "F0"):
            raise SchemaError(f"{path}: INJECTION, RECOVERY and COOLDOWN are present iff anything was applied or F0")
        if has_inj and not ("RECOVERY" in types and "COOLDOWN" in types):
            raise SchemaError(f"{path}: once anything was applied, RECOVERY and COOLDOWN always run (1.3)")
    if a["status"] == "FINISHED" and a["outcome"] != "ABORTED" and a["recovery_gate"]["started_at"] is None:
        # recovery_gate.started_at is null only for an attempt that ended before its GATE segment (CONTRACT_CHANGES C3)
        raise SchemaError(f"{path}: a finished attempt that was not aborted records when its recovery gate started")
    if a["outcome"] in ("VALID", "VALID_REMEDIATED"):
        if a["onset_verified_at"] is None or a["labelled_end_at"] is None:
            raise SchemaError(f"{path}: a valid attempt has a verified onset and a labelled end")
        if (a["outcome"] == "VALID_REMEDIATED") != a["recovery_gate"]["remediation"]["performed"]:
            raise SchemaError(f"{path}: VALID_REMEDIATED exactly when remediation was performed")
    for i, r in enumerate(a["resources"]):
        enum(r["kind"], ("PodChaos", "NetworkChaos", "StressChaos", "Job"), f"{path}.resources[{i}].kind")
        if len(r["name"]) > 63:
            raise SchemaError(f"{path}.resources[{i}].name longer than 63")


def validate_manifest(m: dict) -> None:
    check(m, MANIFEST, "manifest")
    if m["schema_version"] != "1":
        raise SchemaError("manifest.schema_version must be '1'")
    enum(m["workload"], ids.WORKLOADS, "manifest.workload")
    if m["campaign_id"] != ids.campaign_id_for(m["workload"], m["campaign_index"]):
        raise SchemaError("manifest.campaign_id disagrees with workload and index")
    if m["cluster_name"] != ids.cluster_name(m["campaign_id"]):
        raise SchemaError("manifest.cluster_name must be streaming-anomaly-detection-<campaign_id>")
    if m["region"] != "us-east-2":
        raise SchemaError("manifest.region must be us-east-2")
    enum(m["status"], ("RUNNING", "COMPLETE", "ABANDONED"), "manifest.status")
    if m["flink_jobs_expected"] != [j[0] for j in ids.FLINK_JOBS[m["workload"]]]:
        raise SchemaError("manifest.flink_jobs_expected must be the workload's three job names")
    for k in ("baseline_stats_sha256",):
        if m[k] is not None and not SHA_RE.match(m[k]):
            raise SchemaError(f"manifest.{k} must be a SHA-256")
    if (m["baseline_stats_key"] is None) != (m["baseline_stats_sha256"] is None):
        raise SchemaError("baseline_stats_key and baseline_stats_sha256 are set together")
    for i, s in enumerate(m["campaign_segments"]):
        enum(s["segment_type"], CAMPAIGN_SEGMENTS, f"manifest.campaign_segments[{i}]")
        if s["end_at"] < s["start_at"]:
            raise SchemaError(f"manifest.campaign_segments[{i}]: negative length")
    for i, h in enumerate(m["halts"]):
        if h["resolution"] is not None:
            enum(h["resolution"], ("RESUMED", "ABANDONED"), f"manifest.halts[{i}].resolution")
    keys = set()
    for i, a in enumerate(m["attempts"]):
        validate_attempt(a, f"manifest.attempts[{i}]", m["workload"], m["campaign_index"])
        if a["attempt_key"] in keys:
            raise SchemaError(f"duplicate attempt key {a['attempt_key']}")
        keys.add(a["attempt_key"])
        if a["started_at"] < m["timeline_start_at"] - 1e-6:
            raise SchemaError(f"attempt {a['attempt_key']} starts before the timeline")
    starts = [a["started_at"] for a in m["attempts"]]
    if starts != sorted(starts):
        raise SchemaError("manifest.attempts must be in start order")
    if sum(1 for a in m["attempts"] if a["status"] == "RUNNING") > 1:
        raise SchemaError("at most one attempt is RUNNING")
    run_ids = [a["run_id"] for a in m["attempts"]]
    if len(set(run_ids)) != len(run_ids):
        raise SchemaError("run_id values are never reused")
    valid_cells = [a["cell_key"] for a in m["attempts"] if a["outcome"] in ("VALID", "VALID_REMEDIATED")]
    if len(set(valid_cells)) != len(valid_cells):
        raise SchemaError("a cell has more than one valid attempt")
    _no_overlaps(m)


def all_segments(m: dict) -> list[dict]:
    segs = [dict(s, owner=None) for s in m["campaign_segments"]]
    for a in m["attempts"]:
        segs += [dict(s, owner=a["attempt_key"]) for s in a["segments"]]
    return sorted(segs, key=lambda s: (s["start_at"], s["end_at"]))


def _no_overlaps(m: dict) -> None:
    segs = [s for s in all_segments(m) if s["end_at"] > s["start_at"]]
    for x, y in zip(segs, segs[1:]):
        if y["start_at"] < x["end_at"] - 1e-6:
            raise SchemaError(f"segments overlap: {x['segment_type']}({x['owner']}) and {y['segment_type']}({y['owner']})")


def timeline_gaps(m: dict) -> list[str]:
    """Every second from timeline_start_at to timeline_end_at belongs to exactly one segment (3.1)."""
    problems = []
    if m["timeline_end_at"] is None:
        return ["timeline_end_at is null"]
    t = m["timeline_start_at"]
    for s in all_segments(m):
        if s["end_at"] == s["start_at"]:
            continue
        if s["start_at"] > t + 1e-6:
            problems.append(f"gap {t}..{s['start_at']}")
        elif s["start_at"] < t - 1e-6:
            problems.append(f"overlap at {s['start_at']}")
        t = max(t, s["end_at"])
    if abs(t - m["timeline_end_at"]) > 1e-6:
        problems.append(f"segments end at {t}, timeline ends at {m['timeline_end_at']}")
    return problems


# ===================================================================================================== verification
CHECK = {"name": (STR, False), "layer": (STR, False), "target": (STR, False), "series": (STR, False),
         "pre_value": (NUM, True), "during_value": (NUM, True), "op": (STR, False), "threshold": (NUM, False),
         "unit": (STR, False), "pass": (BOOL, False), "reason": (STR, True)}
POD = {"pod": (STR, False), "uid": (STR, False), "node": (STR, False), "ready": (BOOL, False),
       "restart_count": (INT, False), "last_termination_reason": (STR, True), "started_at": (NUM, False)}
TARGET = {"pod": (STR, False), "uid": (STR, False), "node": (STR, False), "broker_id": (INT, True)}
COND = {"status": (STR, False), "last_transition_at": (NUM, False)}
VERIFICATION = {
    "schema_version": (STR, False), "attempt_key": (STR, False), "run_id": (STR, False), "run_type": (STR, False),
    "mode": (STR, False), "gate_git_commit": (STR, False), "thresholds_sha256": (SHA, False), "evaluated_at": (NUM, False),
    "pseudo_targets": (OBJ, True, {"follower_pod": (STR, True), "tm_pods": (ARR, False, STR),
                                   "tm_kill_ranks": (ARR, False, INT), "f6_planned_count": (INT, True)}),
    "targets_expected": (ARR, False, TARGET), "targets_selected": (ARR, False, TARGET),
    "controller": (OBJ, True, {"node_id_before_apply": (INT, True), "node_id_after_injection": (INT, True),
                               "method": (STR, False), "metric_agrees": (BOOL, False)}),
    "chaos_snapshots": (ARR, False, {"tag": (STR, False), "taken_at": (NUM, False), "kind": (STR, False),
                                     "name": (STR, False), "yaml_key": (STR, False),
                                     "conditions": (OBJ, False, {k: (OBJ, False, COND) for k in
                                                                 ("Selected", "AllInjected", "AllRecovered", "Paused")}),
                                     "records": (ARR, False, {"id": (STR, False), "phase": (STR, False),
                                                              "first_apply_at": (NUM, True), "last_recover_at": (NUM, True)})}),
    "pods_before": (ARR, False, POD), "pods_after": (ARR, False, POD),
    "f5": (OBJ, True, {"kills": (ARR, False, {"k": (INT, False), "planned_at": (NUM, False), "pod": (STR, False),
                                               "uid": (STR, False), "apply_at": (NUM, True)}),
                       "new_tm_uid_count": (INT, True), "all_jobs_running_at": (NUM, True)}),
    "f6": (OBJ, True, {"injector_image_digest": (STR, False), "pod_ready_at": (NUM, True), "rate_per_s": (NUM, False),
                       "planned_count": (INT, False), "sent_count": (INT, True), "acked_count": (INT, True),
                       "first_produce_at": (NUM, True), "last_produce_at": (NUM, True), "consumer_count": (INT, True)}),
    # C36: the sizing samples the injector read (timestamps and values), so the gate recomputes the size from them, and the
    # size recomputed from the export, recorded only (late cAdvisor samples can change it, REVIEW4 L5).
    "f8": (OBJ, True, {"per_pod": (ARR, False, {"pod": (STR, False), "limit_bytes": (INT, True), "ws_pre_bytes": (INT, True),
                                                 "stressor_bytes": (INT, True), "ws_during_bytes": (INT, True),
                                                 "increase_bytes": (INT, True),
                                                 "ws_samples": (ARR, False, {"ts": (INT, False), "value": (NUM, False)}),
                                                 "limit_samples": (ARR, False, {"ts": (INT, False), "value": (NUM, False)}),
                                                 "export_stressor_bytes": (INT, True)})}),
    "checks": (ARR, False, CHECK),
    "signatures_all_faults": (ARR, False, {"run_type": (STR, False), "pass": (BOOL, False)}),
    "telemetry": (ARR, False, {"source": (STR, False), "segment": (STR, False), "expected_count": (INT, False),
                               "present_count": (INT, False), "coverage_frac": (NUM, False), "targeted": (BOOL, False)}),
    # C22: scrape health per target and segment. target is <role>:<pod>; expected_count counts the 10 s steps at which
    # kube-state-metrics shows the pod, missed_count those without `up` equal to 1.
    "scrapes": (ARR, False, {"target": (STR, False), "segment": (STR, False), "expected_count": (INT, False),
                             "missed_count": (INT, False), "targeted": (BOOL, False)}),
    "result": (OBJ, False, {k: (BOOL, False) for k in ("mechanism", "signature", "envelope", "timing", "telemetry", "gate")}),
    "recovery_gate_conditions": (ARR, False, {"name": (STR, False), "value": (NUM, True), "threshold": (NUM, False),
                                              "pass": (BOOL, False), "at": (NUM, False)}),
}


def validate_verification(v: dict) -> None:
    check(v, VERIFICATION, "verification")
    enum(v["mode"], ("ONLINE", "OFFLINE"), "verification.mode")
    enum(v["run_type"], ids.RUN_TYPES, "verification.run_type")
    for i, c in enumerate(v["checks"]):
        enum(c["layer"], ("MECHANISM", "SIGNATURE", "ENVELOPE", "TIMING", "TELEMETRY"), f"checks[{i}].layer")
        enum(c["op"], (">=", "<=", "=="), f"checks[{i}].op")
        if (c["pre_value"] is None or c["during_value"] is None) != (c["reason"] == "MISSING"):
            raise SchemaError(f"checks[{i}]: reason is MISSING exactly when a value is null")
        if c["reason"] == "MISSING" and c["pass"]:
            raise SchemaError(f"checks[{i}]: a missing value never passes (2.4)")
    if (v["pseudo_targets"] is None) != (v["run_type"] != "F0"):
        raise SchemaError("pseudo_targets is set for F0 only")
    for s in v["chaos_snapshots"]:
        enum(s["tag"], ("APPLY_PLUS_10S", "MID", "BEFORE_DELETE", "AFTER_DELETE"), "chaos_snapshots.tag")
    if sorted(s["run_type"] for s in v["signatures_all_faults"]) != sorted(ids.FAULTS):
        raise SchemaError("signatures_all_faults covers every fault")


# ===================================================================================================== COMPLETE
COMPLETE = {"schema_version": (STR, False), "campaign_id": (STR, False), "status": (STR, False), "written_at": (NUM, False),
            "plan_sha256": (SHA, False), "manifest_sha256": (SHA, False), "checksums_key": (STR, False),
            "checksums_sha256": (SHA, False), "object_count": (INT, False), "total_bytes": (INT, False),
            "newest_object_at": (NUM, False), "checks": (ARR, False, {"name": (STR, False), "pass": (BOOL, False)})}


def validate_complete(c: dict) -> None:
    check(c, COMPLETE, "COMPLETE")
    enum(c["status"], ("COMPLETE", "ABANDONED"), "COMPLETE.status")
    if c["checksums_key"] != "checksums/SHA256SUMS":
        raise SchemaError("COMPLETE.checksums_key must be checksums/SHA256SUMS")
    if not all(x["pass"] for x in c["checks"]):
        raise SchemaError("COMPLETE is written only when every check passes")


# ===================================================================================================== baseline stats
STATS = {"schema_version": (STR, False), "campaign_id": (STR, False),
         "window": (OBJ, False, {"start_at": (NUM, False), "end_at": (NUM, False)}), "queries": (OBJ, False),
         "raw_events_in_per_s": (OBJ, False, {k: (NUM, False) for k in ("mean", "sd", "p05", "p95")}),
         "consumer_lag_max": (OBJ, False, {"mean": (NUM, False), "sd": (NUM, False)}),
         "f6_rate_per_s": (NUM, False), "f6_planned_count": (INT, False), "tm_working_set_bytes": (OBJ, False),
         "computed_at": (NUM, False)}


def validate_baseline_stats(s: dict) -> None:
    check(s, STATS, "baseline_stats")
    for k, v in s["queries"].items():
        if not isinstance(v, str):
            raise SchemaError(f"baseline_stats.queries.{k} must be a PromQL string")
    for pod, v in s["tm_working_set_bytes"].items():
        check(v, {"mean": (NUM, False), "sd": (NUM, False)}, f"baseline_stats.tm_working_set_bytes.{pod}")
