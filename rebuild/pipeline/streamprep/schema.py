"""Field-by-field schemas for the JSON files of section 11.

11.1.6: a field marked nullable may be null, every other field is always present, and an
unknown field is an error for the reader. The specs below follow the tables in 11.3 to 11.12.
Where the contract contradicts itself the spec says so in a comment and is lenient only on
that point (see README, "Contract ambiguities").
"""
from __future__ import annotations

import math
import re
from typing import Any

from .contract import (
    ATTEMPT_KEY_RE, ATTEMPT_STATUSES, CAMPAIGN_ID_RE, CAMPAIGN_SEGMENT_TYPES, CAMPAIGN_STATUSES,
    CELL_KEY_RE, END_SOURCES, EXPORT_STATUSES, HALT_RESOLUTIONS, MANIFEST_FLAGS, ONSET_SOURCES, OUTCOMES,
    PHASE_KEYS, RUN_TYPES, SHA256_RE, UUID4_RE, WORKLOADS, ALL_SEGMENT_TYPES, Errors,
)


class Spec:
    def check(self, value: Any, path: str, errs: Errors) -> None:  # pragma: no cover - interface
        raise NotImplementedError


class Str(Spec):
    def __init__(self, enum: tuple | None = None, pattern: re.Pattern | None = None,
                 const: str | None = None):
        self.enum, self.pattern, self.const = enum, pattern, const

    def check(self, value: Any, path: str, errs: Errors) -> None:
        if not isinstance(value, str):
            errs.add(f"{path}: expected string, got {type(value).__name__}")
            return
        if self.const is not None and value != self.const:
            errs.add(f"{path}: expected {self.const!r}, got {value!r}")
        if self.enum is not None and value not in self.enum:
            errs.add(f"{path}: {value!r} not one of {list(self.enum)}")
        if self.pattern is not None and not self.pattern.match(value):
            errs.add(f"{path}: {value!r} does not match {self.pattern.pattern}")


class Int(Spec):
    def __init__(self, lo: int | None = None, hi: int | None = None):
        self.lo, self.hi = lo, hi

    def check(self, value: Any, path: str, errs: Errors) -> None:
        if isinstance(value, bool) or not isinstance(value, int):
            errs.add(f"{path}: expected integer, got {type(value).__name__}")
            return
        if self.lo is not None and value < self.lo:
            errs.add(f"{path}: {value} below {self.lo}")
        if self.hi is not None and value > self.hi:
            errs.add(f"{path}: {value} above {self.hi}")


class Num(Spec):
    """A JSON number. ``instant`` adds the millisecond-precision rule of 11.1.1."""

    def __init__(self, instant: bool = False):
        self.instant = instant

    def check(self, value: Any, path: str, errs: Errors) -> None:
        if isinstance(value, bool) or not isinstance(value, (int, float)):
            errs.add(f"{path}: expected number, got {type(value).__name__}")
            return
        if not math.isfinite(value):
            errs.add(f"{path}: not finite")
            return
        if self.instant and abs(value * 1000.0 - round(value * 1000.0)) > 0.05:
            errs.add(f"{path}: {value!r} has more than millisecond precision")


class Bool(Spec):
    def check(self, value: Any, path: str, errs: Errors) -> None:
        if not isinstance(value, bool):
            errs.add(f"{path}: expected boolean, got {type(value).__name__}")


class Nullable(Spec):
    def __init__(self, inner: Spec):
        self.inner = inner

    def check(self, value: Any, path: str, errs: Errors) -> None:
        if value is not None:
            self.inner.check(value, path, errs)


class List(Spec):
    def __init__(self, item: Spec, length: int | None = None):
        self.item, self.length = item, length

    def check(self, value: Any, path: str, errs: Errors) -> None:
        if not isinstance(value, list):
            errs.add(f"{path}: expected array, got {type(value).__name__}")
            return
        if self.length is not None and len(value) != self.length:
            errs.add(f"{path}: expected {self.length} items, got {len(value)}")
        for i, v in enumerate(value):
            self.item.check(v, f"{path}[{i}]", errs)


class Obj(Spec):
    def __init__(self, fields: dict[str, Spec]):
        self.fields = fields

    def check(self, value: Any, path: str, errs: Errors) -> None:
        if not isinstance(value, dict):
            errs.add(f"{path}: expected object, got {type(value).__name__}")
            return
        for k in value:
            if k not in self.fields:
                errs.add(f"{path}: unknown field {k!r}")
        for k, spec in self.fields.items():
            if k not in value:
                errs.add(f"{path}: missing field {k!r}")
            else:
                spec.check(value[k], f"{path}.{k}", errs)


class MapOf(Spec):
    """Object with free keys and typed values (for example queries in 11.12)."""

    def __init__(self, value_spec: Spec):
        self.value_spec = value_spec

    def check(self, value: Any, path: str, errs: Errors) -> None:
        if not isinstance(value, dict):
            errs.add(f"{path}: expected object, got {type(value).__name__}")
            return
        for k, v in value.items():
            self.value_spec.check(v, f"{path}.{k}", errs)


class AnyJSON(Spec):
    def check(self, value: Any, path: str, errs: Errors) -> None:
        return


class ParamValue(Spec):
    """run_types[].params: object of integers and strings."""

    def check(self, value: Any, path: str, errs: Errors) -> None:
        if isinstance(value, bool) or not isinstance(value, (int, str)):
            errs.add(f"{path}: params values must be integers or strings")


SHA = Str(pattern=SHA256_RE)
INSTANT = Num(instant=True)
SEGMENT_REC = Obj({"segment_type": Str(enum=ALL_SEGMENT_TYPES), "start_at": INSTANT, "end_at": INSTANT})

# ---------------------------------------------------------------------------------------------
# 11.3 plan
# ---------------------------------------------------------------------------------------------
PLAN_CELL = Obj({
    "position": Int(1, 8),
    "cell_key": Str(pattern=CELL_KEY_RE),
    "run_type": Str(enum=RUN_TYPES),
    "settle_gap_s": Int(0),
    "follower_rank": Int(0, 1),
    "sham_duration_s": Int(),
    "tm_kill_ranks": List(Int(0, 2), length=5),
})
PLAN = Obj({
    "schema_version": Str(const="1"),
    "workload": Str(enum=WORKLOADS),
    "campaign_index": Int(0, 5),
    "campaign_id": Str(pattern=CAMPAIGN_ID_RE),
    "seed": Str(pattern=re.compile(r"^[0-9]+$")),
    "seed_rule": Str(),
    "generator": Str(const="numpy.random.Generator(numpy.random.PCG64(seed))"),
    "numpy_version": Str(),
    "run_types": List(Obj({
        "run_type": Str(enum=RUN_TYPES), "name": Str(), "kind": Str(),
        "duration_s": Int(0), "params": MapOf(ParamValue()),
    }), length=8),
    "phase_s": Obj({k: Int(0) for k in PHASE_KEYS}),
    "blocks": List(Obj({"block": Int(1, 10), "cells": List(PLAN_CELL, length=8)})),
    "reserve_settle_gap_s": List(Int(0), length=40),
    "load_generator_seed": Int(0, 2147483646),
    "f6_payload_seed": Int(0, 2147483646),
})

# ---------------------------------------------------------------------------------------------
# 11.5 attempt record
# ---------------------------------------------------------------------------------------------
ATTEMPT = Obj({
    "attempt_key": Str(pattern=ATTEMPT_KEY_RE),
    "run_id": Str(pattern=UUID4_RE),
    "cell_key": Str(pattern=CELL_KEY_RE),
    "block": Int(1, 10),
    "position": Int(1, 8),
    "attempt": Int(1, 9),
    "run_type": Str(enum=RUN_TYPES),
    "fault_name": Str(),
    "status": Str(enum=ATTEMPT_STATUSES),
    "outcome": Nullable(Str(enum=OUTCOMES)),
    "failed_checks": List(Str()),
    "flags": List(Str(enum=MANIFEST_FLAGS)),
    "started_at": INSTANT,
    "ended_at": Nullable(INSTANT),
    "planned_onset_at": INSTANT,
    "apply_called_at": Nullable(INSTANT),
    "onset_verified_at": Nullable(INSTANT),
    "onset_source": Nullable(Str(enum=ONSET_SOURCES)),
    "configured_duration_s": Int(0),
    "labelled_end_at": Nullable(INSTANT),
    "end_source": Nullable(Str(enum=END_SOURCES)),
    "overrun_s": Nullable(Num()),
    "applied": Bool(),
    "aborted_at": Nullable(INSTANT),
    "resources": List(Obj({
        "kind": Str(), "name": Str(), "created_at": INSTANT,
        "delete_called_at": Nullable(INSTANT), "not_found_at": Nullable(INSTANT),
    })),
    "controller_at_onset": Nullable(Obj({"node_id": Int(0), "pod": Str()})),
    "segments": List(Obj({"segment_type": Str(enum=("SETTLE", "PRE_FAULT", "APPLY", "INJECTION",
                                                     "RECOVERY", "COOLDOWN", "GATE")),
                          "start_at": INSTANT, "end_at": INSTANT})),
    # Null only for an attempt that ended (ABORTED) before its GATE segment began (CONTRACT_CHANGES C3).
    "recovery_gate": Obj({
        "started_at": Nullable(INSTANT),
        "passed_at": Nullable(INSTANT),
        "remediation": Obj({
            "performed": Bool(), "actions": List(Str()),
            "started_at": Nullable(INSTANT), "ended_at": Nullable(INSTANT),
        }),
    }),
    "flink_jobs_at_end": List(Obj({"name": Str(), "job_id": Str(), "state": Str()})),
    "verification_key": Nullable(Str()),
    "verification_sha256": Nullable(SHA),
    "export": Obj({
        "features_key": Str(), "features_sha256": SHA,
        "verification_metrics_key": Str(), "verification_metrics_sha256": SHA,
        "probes_key": Str(), "probes_sha256": SHA,
        "status": Str(enum=EXPORT_STATUSES),
    }),
})

# ---------------------------------------------------------------------------------------------
# 11.4 manifest
# ---------------------------------------------------------------------------------------------
MANIFEST = Obj({
    "schema_version": Str(const="1"),
    "campaign_id": Str(pattern=CAMPAIGN_ID_RE),
    "workload": Str(enum=WORKLOADS),
    "campaign_index": Int(0, 5),
    "cluster_name": Str(),
    "region": Str(const="us-east-2"),
    "availability_zone": Str(pattern=re.compile(r"^us-east-2[a-z]$")),
    "kubernetes_version": Str(const="1.35"),
    "node_ami_release": Str(),
    "plan_sha256": SHA,
    "seed": Str(pattern=re.compile(r"^[0-9]+$")),
    "orchestrator": Obj({"git_commit": Str(), "image_digest": Str()}),
    "gate": Obj({"git_commit": Str(), "thresholds_sha256": SHA}),
    "images_lock_sha256": SHA,
    "feature_allowlist_sha256": SHA,
    "flink_jobs_expected": List(Str(), length=3),
    "created_at": INSTANT,
    "updated_at": INSTANT,
    "timeline_start_at": INSTANT,
    "timeline_end_at": Nullable(INSTANT),
    "clock_skew_s": Num(),
    "campaign_segments": List(Obj({"segment_type": Str(enum=CAMPAIGN_SEGMENT_TYPES),
                                   "start_at": INSTANT, "end_at": INSTANT})),
    "baseline_stats_key": Nullable(Str()),
    "baseline_stats_sha256": Nullable(SHA),
    "attempts": List(ATTEMPT),
    "halts": List(Obj({
        "at": INSTANT, "reason": Str(), "attempt_key": Nullable(Str(pattern=ATTEMPT_KEY_RE)),
        "resumed_at": Nullable(INSTANT), "resolution": Nullable(Str(enum=HALT_RESOLUTIONS)),
    })),
    "node_inventory": List(Obj({"at": INSTANT, "instance_ids": List(Str())})),
    # C30: one entry per AWS Health event (the ARN only as a 16-hex hash), with the event's own start and end.
    "health_events": List(Obj({"at": INSTANT, "service": Str(), "event_type_code": Str(), "status": Str(),
                               "event_key": Str(pattern=re.compile(r"^[0-9a-f]{16}$")), "start_at": INSTANT,
                               "end_at": Nullable(INSTANT)})),
    "health_events_polled_until": Nullable(INSTANT),
    "status": Str(enum=CAMPAIGN_STATUSES),
})

# ---------------------------------------------------------------------------------------------
# 11.6 verification record
# ---------------------------------------------------------------------------------------------
TARGET = Obj({"pod": Str(), "uid": Str(), "node": Str(), "broker_id": Nullable(Int(0))})
# The contract marks condition and record timestamps as always present, but a snapshot taken at
# APPLY_PLUS_10S cannot hold a recover time yet. The reader accepts null there (README, A12).
CONDITION = Obj({"status": Str(), "last_transition_at": Nullable(INSTANT)})
POD_STATE = Obj({
    "pod": Str(), "uid": Str(), "node": Str(), "ready": Bool(), "restart_count": Int(0),
    "last_termination_reason": Nullable(Str()), "started_at": INSTANT,
})
VERIFICATION = Obj({
    "schema_version": Str(const="1"),
    "attempt_key": Str(pattern=ATTEMPT_KEY_RE),
    "run_id": Str(pattern=UUID4_RE),
    "run_type": Str(enum=RUN_TYPES),
    "mode": Str(enum=("ONLINE", "OFFLINE")),
    "gate_git_commit": Str(),
    "thresholds_sha256": SHA,
    "evaluated_at": INSTANT,
    # Values the orchestrator could not observe are null; every check that needs one fails as MISSING
    # (CONTRACT_CHANGES C4). Valid attempts are held to the non-null fields in loader._check_verifications.
    "pseudo_targets": Nullable(Obj({
        "follower_pod": Nullable(Str()), "tm_pods": List(Str()), "tm_kill_ranks": List(Int(0, 2), length=5),
        "f6_planned_count": Nullable(Int(0)),
    })),
    "targets_expected": List(TARGET),
    "targets_selected": List(TARGET),
    "controller": Nullable(Obj({
        "node_id_before_apply": Nullable(Int(0)), "node_id_after_injection": Nullable(Int(0)),
        "method": Str(const="metadata-quorum"), "metric_agrees": Bool(),
    })),
    "chaos_snapshots": List(Obj({
        "tag": Str(enum=("APPLY_PLUS_10S", "MID", "BEFORE_DELETE", "AFTER_DELETE")),
        "taken_at": INSTANT, "kind": Str(), "name": Str(), "yaml_key": Str(),
        "conditions": Obj({k: CONDITION for k in ("Selected", "AllInjected", "AllRecovered", "Paused")}),
        "records": List(Obj({"id": Str(), "phase": Str(), "first_apply_at": Nullable(INSTANT),
                             "last_recover_at": Nullable(INSTANT)})),
    })),
    "pods_before": List(POD_STATE),
    "pods_after": List(POD_STATE),
    "f5": Nullable(Obj({
        "kills": List(Obj({"k": Int(1, 5), "planned_at": INSTANT, "pod": Str(), "uid": Str(),
                           "apply_at": Nullable(INSTANT)})),
        "new_tm_uid_count": Nullable(Int(0)),
        "all_jobs_running_at": Nullable(INSTANT),
    })),
    "f6": Nullable(Obj({
        "injector_image_digest": Str(), "pod_ready_at": Nullable(INSTANT), "rate_per_s": Num(),
        "planned_count": Int(0), "sent_count": Nullable(Int(0)), "acked_count": Nullable(Int(0)),
        "first_produce_at": Nullable(INSTANT), "last_produce_at": Nullable(INSTANT),
        "consumer_count": Nullable(Int(0)),
    })),
    # C36: the sizing samples the injector read, and the size recomputed from the export (recorded only, may be null).
    "f8": Nullable(Obj({"per_pod": List(Obj({
        "pod": Str(), "limit_bytes": Nullable(Int(0)), "ws_pre_bytes": Nullable(Int(0)),
        "stressor_bytes": Nullable(Int(0)), "ws_during_bytes": Nullable(Int(0)), "increase_bytes": Nullable(Int()),
        "ws_samples": List(Obj({"ts": Int(), "value": Num()})),
        "limit_samples": List(Obj({"ts": Int(), "value": Num()})),
        "export_stressor_bytes": Nullable(Int(0)),
    }))})),
    "checks": List(Obj({
        "name": Str(),
        "layer": Str(enum=("MECHANISM", "SIGNATURE", "ENVELOPE", "TIMING", "TELEMETRY")),
        "target": Str(), "series": Str(),
        "pre_value": Nullable(Num()), "during_value": Nullable(Num()),
        "op": Str(enum=(">=", "<=", "==")), "threshold": Num(), "unit": Str(), "pass": Bool(),
        "reason": Nullable(Str()),
    })),
    "signatures_all_faults": List(Obj({"run_type": Str(enum=RUN_TYPES), "pass": Bool()})),
    "telemetry": List(Obj({
        "source": Str(), "segment": Str(enum=ALL_SEGMENT_TYPES), "expected_count": Int(0),
        "present_count": Int(0), "coverage_frac": Num(), "targeted": Bool(),
    })),
    # C22: scrape health per target (<role>:<pod>) and segment, the input of the telemetry rule (decision 21).
    "scrapes": List(Obj({
        "target": Str(), "segment": Str(enum=("PRE_FAULT", "INJECTION", "RECOVERY")), "expected_count": Int(0),
        "missed_count": Int(0), "targeted": Bool(),
    })),
    "result": Obj({k: Bool() for k in ("mechanism", "signature", "envelope", "timing", "telemetry", "gate")}),
    "recovery_gate_conditions": List(Obj({
        "name": Str(), "value": Nullable(Num()), "threshold": Num(), "pass": Bool(), "at": INSTANT,
    })),
})

# ---------------------------------------------------------------------------------------------
# 11.10 completeness marker and 11.12 baseline statistics
# ---------------------------------------------------------------------------------------------
COMPLETE = Obj({
    "schema_version": Str(const="1"),
    "campaign_id": Str(pattern=CAMPAIGN_ID_RE),
    "status": Str(enum=("COMPLETE", "ABANDONED")),
    "written_at": INSTANT,
    "plan_sha256": SHA,
    "manifest_sha256": SHA,
    "checksums_key": Str(const="checksums/SHA256SUMS"),
    "checksums_sha256": SHA,
    "object_count": Int(0),
    "total_bytes": Int(0),
    "newest_object_at": INSTANT,
    "checks": List(Obj({"name": Str(), "pass": Bool()})),
})

BASELINE_STATS = Obj({
    "schema_version": Str(const="1"),
    "campaign_id": Str(pattern=CAMPAIGN_ID_RE),
    "window": Obj({"start_at": INSTANT, "end_at": INSTANT}),
    "queries": MapOf(Str()),
    "raw_events_in_per_s": Obj({"mean": Num(), "sd": Num(), "p05": Num(), "p95": Num()}),
    "consumer_lag_max": Obj({"mean": Num(), "sd": Num()}),
    "f6_rate_per_s": Num(),
    "f6_planned_count": Int(0),
    # "mean, sd per TaskManager": the nesting is not given, so any object is accepted (README, A13).
    "tm_working_set_bytes": MapOf(AnyJSON()),
    "computed_at": INSTANT,
})


def validate(spec: Spec, value: Any, what: str) -> Errors:
    errs = Errors()
    spec.check(value, what, errs)
    return errs
