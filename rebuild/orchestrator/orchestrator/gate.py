"""Injection verification gate (protocol 2.1 to 2.5, 3.4, 11.6). One module, called online by the orchestrator and
offline by `make gate` (`orchestrator gate-offline`), on the same stored inputs (2.7.4).

Inputs are only stored objects: the manifest attempt record, the verification-record facts written by the
orchestrator, the chaos snapshot YAML, probes.parquet, verification.parquet, features.parquet (sample presence
only, for the telemetry rule), injector.log and the baseline statistics. The online orchestrator uploads these
first and then evaluates from the store, so online and offline results cannot diverge.

Rules that make the R1 and first-patch defects impossible:
  * The gate never reads an exported feature VALUE. features.parquet is used only to count samples per source
    (telemetry, 2.1 item 5, 3.4.3). Signatures use probes, container metrics, kube-state and controller metrics
    (2.3), plus the one presence-only URP exception read from the verification copy (2.1).
  * A missing pre or during value fails the check with reason MISSING (2.4). There is no default value anywhere.
  * Every check runs on every target. One target meeting the threshold never stands in for the others (2.4).
  * A check compares `during_value` with the threshold, except checks whose name ends in `_pre`, which compare
    `pre_value` (a pre-fault level check such as "probes succeed in at least 95% of pre-fault samples").
  * A sham fails every signature by construction of the thresholds (F6 uses the planned count, 2.5). A sham whose
    signature passes anyway is a gate failure and is repeated (decision 22, C23).
  * A probe row whose `kubectl exec` failed (error EXEC...) is a missing sample, never a failed connect (REVIEW4 L7, C32).
  * Every rule the preprocessing applies to a valid attempt is also a check here, so a broken record is repeated during
    the campaign instead of stopping the build afterwards (decision 23, C25).
"""
from __future__ import annotations

import json
import math
import re
import statistics
from collections import defaultdict
from dataclasses import dataclass, field
from typing import Any

import yaml

from . import SCHEMA_VERSION, ids
from .canonical import sha256_hex
from .chaos import SELECTOR_MAIN, records
from .exporter import role_of
from .f6_injector import parse_log

LAYERS = ("MECHANISM", "SIGNATURE", "ENVELOPE", "TIMING", "TELEMETRY")
MIB = 1024 * 1024
# kube-state-metrics pod metrics (context7 /kubernetes/kube-state-metrics, docs/metrics/workload/pod-metrics.md): every
# pod metric carries namespace, pod and uid; kube_pod_container_resource_limits carries resource, unit, container and node;
# kube_pod_status_ready carries condition; ..._last_terminated_reason carries reason. The cAdvisor memory-limit series is not
# used (2.3.2), so limits come from kube_pod_container_resource_limits{resource="memory"}.
WS_METRIC = "container_memory_working_set_bytes"
CPU_METRIC = "container_cpu_usage_seconds_total"
CPU_RATE_WINDOW_S = 60  # one CPU sample is the counter increase over 60 s (4.1.2, C41)
LIMIT_METRIC = "kube_pod_container_resource_limits"
READY_METRIC = "kube_pod_status_ready"
INFO_METRIC = "kube_pod_info"
REASON_METRIC = "kube_pod_container_status_last_terminated_reason"
RESTARTS_METRIC = "kube_pod_container_status_restarts_total"
OOM_METRIC = "container_oom_events_total"
UP_METRIC = "up"
FENCED_METRIC = "kafka_controller_kafkacontroller_fencedbrokercount"
TERMINAL_JOB_STATES = ("FAILED", "CANCELED", "FINISHED")
POD_ROLES = ("broker", "jobmanager", "taskmanager")


def exec_failed(row: dict) -> bool:
    """A probe whose kubectl exec failed measured nothing: the sample is missing (REVIEW4 L7). NO_POD_IP,
    NO_JOBMANAGER_IP and CONNECT_FAILED stay failures."""
    return (row.get("error") or "").startswith("EXEC")


@dataclass
class GateInputs:
    plan: dict
    cell: dict
    attempt: dict
    facts: dict
    chaos_objects: dict[str, dict]
    probes: list[dict]
    verification_rows: list[dict]
    feature_rows: list[dict]
    injector_log: str | None
    baseline_stats: dict | None
    thresholds: dict
    workload: str


@dataclass
class GateResult:
    checks: list[dict]
    signatures_all_faults: list[dict]
    telemetry: list[dict]
    result: dict
    flags: list[str] = field(default_factory=list)
    derived: dict = field(default_factory=dict)
    scrapes: list[dict] = field(default_factory=list)

    @property
    def failed_checks(self) -> list[str]:
        """11.5 failed_checks: the distinct `name` of every failing check of the record, in record order. Targets and
        values stay in the verification record. The telemetry rule is a TELEMETRY-layer check named telemetry_coverage
        (CONTRACT_CHANGES C2), so a TELEMETRY_FAILED attempt names it too."""
        out = []
        for c in self.checks:
            if not c["pass"] and c["name"] not in out:
                out.append(c["name"])
        return out


def _cmp(value: float, op: str, threshold: float) -> bool:
    if op == ">=":
        return value >= threshold
    if op == "<=":
        return value <= threshold
    if op == "==":
        return value == threshold
    raise ValueError(op)


def make_check(name: str, layer: str, target: str, series: str, pre: float | None, during: float | None, op: str,
               threshold: float, unit: str) -> dict:
    assert layer in LAYERS
    if not (isinstance(threshold, (int, float)) and math.isfinite(threshold)):
        raise ValueError(f"check {name} has a non-finite threshold")  # thresholds are constants, never derived
    compare_pre = name.endswith("_pre")
    if pre is None or during is None or (isinstance(pre, float) and math.isnan(pre)) or (
            isinstance(during, float) and math.isnan(during)):
        ok, reason = False, "MISSING"
        pre = None if pre is None or (isinstance(pre, float) and math.isnan(pre)) else float(pre)
        during = None if during is None or (isinstance(during, float) and math.isnan(during)) else float(during)
    else:
        ok, reason = _cmp(float(pre if compare_pre else during), op, float(threshold)), None
        pre, during = float(pre), float(during)
    return {"name": name, "layer": layer, "target": target, "series": series, "pre_value": pre, "during_value": during,
            "op": op, "threshold": float(threshold), "unit": unit, "pass": bool(ok), "reason": reason}


def median(values: list[float]) -> float | None:
    return statistics.median(values) if values else None


# ================================================================================================ context
class Ctx:
    def __init__(self, gi: GateInputs) -> None:
        self.gi = gi
        self.th = gi.thresholds["gate"]
        a = gi.attempt
        self.a = a
        self.run_type = a["run_type"]
        segs = {s["segment_type"]: s for s in a["segments"]}
        self.segs = segs
        self.pre_lo = segs["PRE_FAULT"]["start_at"] if "PRE_FAULT" in segs else None
        self.pre_hi = segs["PRE_FAULT"]["end_at"] if "PRE_FAULT" in segs else None
        self.onset = a.get("onset_verified_at")
        self.lend = a.get("labelled_end_at")
        self.rec_end = segs["RECOVERY"]["end_at"] if "RECOVERY" in segs else None
        self.lead = self.th["lead_s"]
        # Probe and metric indexes.
        self.tcp: dict[tuple[str, str], list[tuple[float, bool]]] = defaultdict(list)
        self.burst: dict[str, list[tuple[float, float]]] = defaultdict(list)
        self.by_probe: dict[str, list[dict]] = defaultdict(list)
        self.burst_counts: dict[str, list[tuple[float, int, int]]] = defaultdict(list)
        for r in gi.probes:
            self.by_probe[r["probe"]].append(r)
            if r["probe"] == "tcp" and not exec_failed(r):
                self.tcp[(r["src_pod"] or "", r["dst"] or "")].append((r["ts"], r["ok_count"] == r["tried_count"]))
            elif r["probe"] == "tcp_burst" and not exec_failed(r):
                self.burst[r["src_pod"] or ""].append((r["ts"], 1.0 - r["ok_count"] / r["tried_count"]))
                self.burst_counts[r["src_pod"] or ""].append((r["ts"], r["ok_count"], r["tried_count"]))
        for v in self.by_probe.values():
            v.sort(key=lambda r: r["ts"])
        self.metric: dict[tuple[str, str], list[dict]] = defaultdict(list)
        for r in gi.verification_rows:
            self.metric[(r["metric"], r["pod"])].append(r)
        for v in self.metric.values():
            v.sort(key=lambda r: (r["ts"], r["series_id"]))
        self.pods_before = {p["pod"]: p for p in gi.facts.get("pods_before") or []}
        self.pods_after = {p["pod"]: p for p in gi.facts.get("pods_after") or []}

    # ----------------------------------------------------------------------------- windows
    def pre_window(self) -> tuple[float, float] | None:
        if self.pre_lo is None or self.pre_hi is None:
            return None
        return self.pre_lo, self.pre_hi  # [lo, hi)

    def inj_window(self) -> tuple[float, float] | None:
        if self.onset is None or self.lend is None:
            return None
        return self.onset + self.lead, self.lend + 1e-9  # [onset + lead, labelled end]

    def onset_window(self, seconds: float) -> tuple[float, float] | None:
        if self.onset is None:
            return None
        return self.onset, self.onset + seconds + 1e-9

    def full_inj_window(self) -> tuple[float, float] | None:
        if self.onset is None or self.lend is None:
            return None
        return self.onset, self.lend + 1e-9

    # ----------------------------------------------------------------------------- probe helpers
    def tcp_samples(self, src: str, dst: str, w: tuple[float, float] | None) -> list[bool] | None:
        if w is None:
            return None
        return [ok for t, ok in self.tcp.get((src, dst), []) if w[0] <= t < w[1]]

    def tcp_fail_frac(self, src: str, dst: str, w) -> float | None:
        s = self.tcp_samples(src, dst, w)
        return (sum(1 for ok in s if not ok) / len(s)) if s else None

    def tcp_ok_frac(self, src: str, dst: str, w) -> float | None:
        s = self.tcp_samples(src, dst, w)
        return (sum(1 for ok in s if ok) / len(s)) if s else None

    def tcp_fail_count(self, src: str, dst: str, w) -> float | None:
        s = self.tcp_samples(src, dst, w)
        return float(sum(1 for ok in s if not ok)) if s else None

    def burst_loss_median(self, tm: str, w) -> float | None:
        if w is None:
            return None
        return median([loss for t, loss in self.burst.get(tm, []) if w[0] <= t < w[1]])

    # ----------------------------------------------------------------------------- metric helpers
    def rows(self, metric: str, pod: str, w, pred=None) -> list[dict]:
        if w is None:
            return []
        return [r for r in self.metric.get((metric, pod), []) if w[0] <= r["ts"] < w[1] and (pred is None or pred(r))]

    def gauge_by_ts(self, metric: str, pod: str, w, pred=None) -> dict[int, float]:
        """Per 10 s step, the maximum over the pod's series (a restarted container makes a second series)."""
        out: dict[int, float] = {}
        for r in self.rows(metric, pod, w, pred):
            out[r["ts"]] = max(out.get(r["ts"], -math.inf), r["value"])
        return out

    def cpu_rates(self, pod: str, w) -> list[float]:
        """Per-second CPU rate from the cumulative cAdvisor counter (4.1.2). One sample per 10 s step t is the increase
        over the 60 s before t, divided by 60, with t and t - 60 both inside the window. cAdvisor refreshes the counter
        about every 15 s, so a 10 s step reads 0 or a double step, and the median of those ran 20 to 45 percent high in the
        pilot (PILOT_LOG 14 October, C41). Per series, a decrease (a new container) drops that sample, and the maximum over
        the pod's series is kept per step."""
        if w is None:
            return []
        lo, hi = w
        span = CPU_RATE_WINDOW_S
        series: dict[str, dict[int, float]] = defaultdict(dict)
        for r in self.metric.get((CPU_METRIC, pod), []):
            if r["container"] == self.th["F4"]["container"] and lo <= r["ts"] < hi:
                series[r["series_id"]][r["ts"]] = r["value"]
        per_ts: dict[int, float] = {}
        for pts in series.values():
            for t1, v1 in pts.items():
                v0 = pts.get(t1 - span)
                if v0 is None or v1 < v0:
                    continue
                per_ts[t1] = max(per_ts.get(t1, -math.inf), (v1 - v0) / float(span))
        return list(per_ts.values())

    def ws_values(self, pod: str, w) -> list[float]:
        return list(self.gauge_by_ts(WS_METRIC, pod, w, lambda r: r["container"] == ids.FLINK_CONTAINER).values())

    def limit_values(self, pod: str, w) -> list[float]:
        pred = lambda r: r["container"] == ids.FLINK_CONTAINER and '"resource":"memory"' in r["labels"]  # noqa: E731
        return list(self.gauge_by_ts(LIMIT_METRIC, pod, w, pred).values())

    def not_ready_count(self, pod: str, w) -> float | None:
        vals = self.gauge_by_ts(READY_METRIC, pod, w, lambda r: '"condition":"true"' in r["labels"])
        if not vals:
            return None
        return float(sum(1 for v in vals.values() if v == 0))

    def urp_presence(self, w) -> float | None:
        if w is None:
            return None
        vals = [r["value"] for r in self.gi.verification_rows
                if r["source"] == "kafka_broker_copy" and w[0] <= r["ts"] < w[1]]
        if not vals:
            return None
        return float(sum(1 for v in vals if v >= 1))

    def fenced_presence(self, w) -> float | None:
        if w is None:
            return None
        vals = [r["value"] for r in self.gi.verification_rows if r["metric"] == FENCED_METRIC and w[0] <= r["ts"] < w[1]]
        if not vals:
            return None
        return float(sum(1 for v in vals if v >= self.th["F2"]["fenced_min"]))

    def tm_uids_seen(self, w) -> set[str] | None:
        if w is None:
            return None
        uids = set()
        any_row = False
        for (metric, pod), rows in self.metric.items():
            if metric != INFO_METRIC or role_of(pod, {}) != "taskmanager":
                continue
            for r in rows:
                if w[0] <= r["ts"] < w[1]:
                    any_row = True
                    if r["pod_uid"]:
                        uids.add(r["pod_uid"])
        return uids if any_row else None

    # ----------------------------------------------------------------------------- pods
    def tm_pods_at_onset(self) -> list[str]:
        return sorted(p for p, s in self.pods_before.items() if role_of(p, {}) == "taskmanager" and s["ready"])

    def controller_from_probes(self, before: float | None) -> int | None:
        if before is None:
            return None
        best = None
        for r in self.by_probe.get("controller", []):
            if r["ts"] <= before and r["ok_count"] == 1 and r["value"] is not None:
                best = int(r["value"])
        return best

    def broker_pods(self) -> list[str]:
        return sorted(p for p in self.pods_before if ids.broker_id_from_pod(p) is not None)


# ================================================================================================ signatures
def _pseudo(ctx: Ctx) -> dict:
    """The targets each fault's signature is evaluated on (2.5)."""
    gi = ctx.gi
    rt = ctx.run_type
    pt = gi.facts.get("pseudo_targets") if rt == "F0" else None
    out: dict[str, Any] = {}
    # Follower for F1 and F2.
    if rt in ("F1", "F2"):
        out["follower"] = (gi.facts.get("targets_expected") or [{}])[0].get("pod")
    elif rt == "F0":
        out["follower"] = (pt or {}).get("follower_pod")
    else:
        ctrl = ctx.controller_from_probes(ctx.onset)
        brokers = ctx.broker_pods()
        followers = sorted((ids.broker_id_from_pod(b), b) for b in brokers if ids.broker_id_from_pod(b) != ctrl)
        out["follower"] = followers[gi.cell["follower_rank"]][1] if ctrl is not None and len(followers) == 2 else None
    # TaskManagers for F4, F7, F8.
    if rt == "F0":
        out["tms"] = list((pt or {}).get("tm_pods") or [])
    elif rt in ("F4", "F7", "F8"):
        out["tms"] = sorted(t["pod"] for t in gi.facts.get("targets_expected") or [])
    else:
        out["tms"] = ctx.tm_pods_at_onset()
    # F6 planned count.
    if rt == "F6":
        out["f6_planned"] = (gi.facts.get("f6") or {}).get("planned_count")
    elif rt == "F0":
        out["f6_planned"] = (pt or {}).get("f6_planned_count")
    else:
        out["f6_planned"] = (gi.baseline_stats or {}).get("f6_planned_count")
    return out


def sig_f1(ctx: Ctx, follower: str | None) -> list[dict]:
    th = ctx.th["F1"]
    tgt = follower or "UNRESOLVED"
    w60 = ctx.onset_window(th["presence_window_s"])
    pre = ctx.pre_window()
    out = []
    before = ctx.pods_before.get(follower or "")
    after = ctx.pods_after.get(follower or "")
    changed = None if before is None or after is None else (1.0 if before["uid"] != after["uid"] else 0.0)
    out.append(make_check("f1_target_uid_changed", "SIGNATURE", tgt, "pods_before/pods_after uid",
                          1.0 if before else None, changed, "==", 1, "bool"))
    out.append(make_check("f1_target_not_ready_within_60s", "SIGNATURE", tgt, f"{READY_METRIC}{{condition=true}}",
                          ctx.not_ready_count(follower or "", pre), ctx.not_ready_count(follower or "", w60), ">=", 1, "samples"))
    port = th["probe_port"]
    for src in [b for b in ctx.broker_pods() if b != follower]:
        dst = f"{follower}:{port}"
        out.append(make_check("f1_tcp_9091_fails_within_60s", "SIGNATURE", f"{src}->{dst}", "probe tcp",
                              ctx.tcp_fail_count(src, dst, pre), ctx.tcp_fail_count(src, dst, w60), ">=", 1, "failed connects"))
    if len([b for b in ctx.broker_pods() if b != follower]) != 2:
        out.append(make_check("f1_tcp_sources_present", "SIGNATURE", tgt, "pods_before", None, None, "==", 2, "brokers"))
    if th["urp_presence_check"]:
        out.append(make_check("f1_urp_presence", "SIGNATURE", "all", "kafka_broker_copy underreplicatedpartitions",
                              ctx.urp_presence(pre), ctx.urp_presence(ctx.full_inj_window()), ">=", th["urp_min"], "samples"))
    return out


def sig_f2(ctx: Ctx, follower: str | None) -> list[dict]:
    th = ctx.th["F2"]
    tgt = follower or "UNRESOLVED"
    pre, inj = ctx.pre_window(), ctx.inj_window()
    out = []
    others = [b for b in ctx.broker_pods() if b != follower]
    if len(others) != 2 or follower is None:
        out.append(make_check("f2_targets_resolved", "SIGNATURE", tgt, "pods_before", None, None, "==", 2, "brokers"))
    for src in others:
        for port in th["probe_ports"]:
            dst = f"{follower}:{port}"
            out.append(make_check("f2_tcp_fails_during", "SIGNATURE", f"{src}->{dst}", "probe tcp",
                                  ctx.tcp_fail_frac(src, dst, pre), ctx.tcp_fail_frac(src, dst, inj), ">=",
                                  th["probe_fail_during_frac_min"], "frac"))
            out.append(make_check("f2_tcp_ok_pre", "SIGNATURE", f"{src}->{dst}", "probe tcp",
                                  ctx.tcp_ok_frac(src, dst, pre), ctx.tcp_ok_frac(src, dst, inj), ">=",
                                  th["probe_ok_pre_frac_min"], "frac"))
    out.append(make_check("f2_fenced_presence", "SIGNATURE", "all", FENCED_METRIC, ctx.fenced_presence(pre),
                          ctx.fenced_presence(ctx.full_inj_window()), ">=", 1, "samples"))
    if th["urp_presence_check"]:
        out.append(make_check("f2_urp_presence", "SIGNATURE", "all", "kafka_broker_copy underreplicatedpartitions",
                              ctx.urp_presence(pre), ctx.urp_presence(ctx.full_inj_window()), ">=", th["urp_min"], "samples"))
    return out


def sig_f4(ctx: Ctx, tms: list[str]) -> list[dict]:
    th = ctx.th["F4"]
    out = []
    if not tms:
        out.append(make_check("f4_targets_resolved", "SIGNATURE", "all", "pods_before", None, None, ">=", 1, "pods"))
    for tm in tms:
        pre = median(ctx.cpu_rates(tm, ctx.pre_window()))
        dur = median(ctx.cpu_rates(tm, ctx.inj_window()))
        ratio = (dur / pre) if (pre is not None and dur is not None and pre > 0) else None
        out.append(make_check("f4_cpu_ratio", "SIGNATURE", tm, f"rate({CPU_METRIC}) during / pre", pre, ratio, ">=",
                              th["cpu_ratio_min"], "ratio"))
        out.append(make_check("f4_cpu_cores", "SIGNATURE", tm, f"rate({CPU_METRIC})", pre, dur, ">=", th["cpu_cores_min"], "cores"))
    return out


def sig_f5(ctx: Ctx) -> list[dict]:
    th = ctx.th["F5"]
    before = {p["uid"] for p in ctx.pods_before.values() if role_of(p["pod"], {}) == "taskmanager"}
    w = None if ctx.onset is None or ctx.lend is None else (ctx.onset, ctx.lend + th["uid_window_after_end_s"] + 1e-9)
    seen = ctx.tm_uids_seen(w)
    pre_seen = ctx.tm_uids_seen(ctx.pre_window())
    new = None if seen is None else float(len(seen - before))
    pre_new = None if pre_seen is None else float(len(pre_seen - before))
    return [make_check("f5_new_tm_uids", "SIGNATURE", "all", f"{INFO_METRIC} uid", pre_new, new, ">=", th["new_uid_min"], "uids")]


def malformed_delta(ctx: Ctx, start: float | None, end: float | None) -> float | None:
    """Malformed raw-events records carrying this attempt's rebuild-attempt header (11.11) that the verification consumer
    counted between `start` and `end`.

    Attribution is by the header, as 2.3.8 and 11.11 intend: each consumer probe sample's detail holds malformed_by_attempt,
    the cumulative count per attempt key (11.8, CONTRACT_CHANGES C6). The value is that count at the last consumer sample at
    or before `end`, minus the count at the last sample at or before `start` (a key not yet seen counts 0), from samples
    that all share one started_at, so a consumer restart inside the window makes the value missing, never smaller.
    Records of any other attempt, or without the header, never count."""
    if start is None or end is None:
        return None
    akey = ctx.a["attempt_key"]
    samples = []
    for r in ctx.by_probe.get("consumer", []):
        if r["ok_count"] != 1 or r["value"] is None or not r["detail"]:
            continue
        d = json.loads(r["detail"])
        by = d.get("malformed_by_attempt")
        if not isinstance(by, dict) or "started_at" not in d:
            continue
        samples.append((r["ts"], d["started_at"], float(by.get(akey, 0))))
    before = [x for x in samples if x[0] <= start]
    upto = [x for x in samples if x[0] <= end]
    if not before or not upto:
        return None
    first, last = before[-1], upto[-1]
    if len({x[1] for x in samples if first[0] <= x[0] <= last[0]}) != 1:
        return None
    return last[2] - first[2]


def sig_f6(ctx: Ctx, planned: float | None) -> list[dict]:
    th = ctx.th["F6"]
    end = ctx.rec_end if ctx.rec_end is not None else ctx.lend
    count = malformed_delta(ctx, ctx.a["planned_onset_at"], end)
    pre_count = malformed_delta(ctx, ctx.pre_lo, ctx.pre_hi)
    frac = (count / planned) if (count is not None and planned) else None
    return [make_check("f6_consumer_malformed", "SIGNATURE", "all",
                       "probe consumer malformed_by_attempt[attempt_key], planned onset to end of RECOVERY, / planned count",
                       pre_count, frac, ">=", th["consumer_frac_min"], "frac")]


def sig_f7(ctx: Ctx, tms: list[str]) -> list[dict]:
    th = ctx.th["F7"]
    out = []
    if not tms:
        out.append(make_check("f7_targets_resolved", "SIGNATURE", "all", "pods_before", None, None, ">=", 1, "pods"))
    for tm in tms:
        pre = ctx.burst_loss_median(tm, ctx.pre_window())
        dur = ctx.burst_loss_median(tm, ctx.inj_window())
        out.append(make_check("f7_loss_during", "SIGNATURE", tm, "probe tcp_burst loss", pre, dur, ">=", th["loss_during_min"], "frac"))
        out.append(make_check("f7_loss_pre", "SIGNATURE", tm, "probe tcp_burst loss", pre, dur, "<=", th["loss_pre_max"], "frac"))
    return out


def _gauge_rows_by_ts(rows: list[dict], metric: str, pod: str, w: tuple[float, float], pred) -> dict[int, float]:
    out: dict[int, float] = {}
    for r in rows:
        if r["metric"] == metric and r["pod"] == pod and w[0] <= r["ts"] < w[1] and pred(r):
            out[r["ts"]] = max(out.get(r["ts"], -math.inf), r["value"])
    return out


def f8_size_from_rows(rows: list[dict], tm: str, planned_onset: float, th8: dict) -> tuple[float | None, float | None, int | None]:
    """Stressor size = floor((0.85 x limit - working set over the last 60 s of pre-fault) / MiB) MiB (2.4).

    The pre-fault statistic is the median, as for every pre value (2.4). The limit comes from kube-state-metrics,
    because the cAdvisor limit series is dropped by the chart's relabelling (2.3.2). Used online by the F8 injector
    on freshly queried rows and offline by the gate on the exported verification rows, so both sides compute the
    same number from the same samples."""
    w = (planned_onset - th8["sizing_window_s"], planned_onset)
    ws = median(list(_gauge_rows_by_ts(rows, WS_METRIC, tm, w, lambda r: r["container"] == th8["container"]).values()))
    lim = median(list(_gauge_rows_by_ts(rows, LIMIT_METRIC, tm, w, lambda r: r["container"] == th8["container"]
                                        and '"resource":"memory"' in r["labels"]).values()))
    if ws is None or lim is None:
        return lim, ws, None
    return lim, ws, math.floor((th8["target_limit_frac"] * lim - ws) / MIB)


def f8_rows_from_samples(tm: str, per_pod: dict) -> list[dict]:
    """The sizing samples the F8 injector read and recorded (C36), as rows f8_size_from_rows understands."""
    rows = []
    for x in per_pod.get("ws_samples") or []:
        rows.append({"metric": WS_METRIC, "pod": tm, "ts": x["ts"], "value": x["value"], "container": ids.FLINK_CONTAINER,
                     "labels": "{}"})
    for x in per_pod.get("limit_samples") or []:
        rows.append({"metric": LIMIT_METRIC, "pod": tm, "ts": x["ts"], "value": x["value"], "container": ids.FLINK_CONTAINER,
                     "labels": '{"resource":"memory"}'})
    return rows


def f8_sizing(ctx: Ctx, tm: str, from_export: bool = False) -> tuple[float | None, float | None, int | None]:
    """F8 attempts size from the samples the injector recorded (REVIEW4 L5: a late cAdvisor sample can change the export
    after the online query). Every other run type, and the recorded-only export comparison, size from the export."""
    if ctx.pre_hi is None:
        return None, None, None
    if ctx.run_type == "F8" and not from_export:
        rec = {p["pod"]: p for p in ((ctx.gi.facts.get("f8") or {}).get("per_pod") or [])}.get(tm) or {}
        return f8_size_from_rows(f8_rows_from_samples(tm, rec), tm, ctx.pre_hi, ctx.th["F8"])
    return f8_size_from_rows(ctx.gi.verification_rows, tm, ctx.pre_hi, ctx.th["F8"])


def sig_f8(ctx: Ctx, tms: list[str]) -> list[dict]:
    th = ctx.th["F8"]
    out = []
    if not tms:
        out.append(make_check("f8_targets_resolved", "SIGNATURE", "all", "pods_before", None, None, ">=", 1, "pods"))
    recorded = {p["pod"]: p for p in ((ctx.gi.facts.get("f8") or {}).get("per_pod") or [])} if ctx.run_type == "F8" else {}
    for tm in tms:
        if ctx.run_type == "F8":
            size_bytes = (recorded.get(tm) or {}).get("stressor_bytes")
        else:
            _, _, size_mib = f8_sizing(ctx, tm)
            size_bytes = size_mib * MIB if size_mib is not None and size_mib > 0 else None
        pre = median(ctx.ws_values(tm, ctx.pre_window()))
        dur = median(ctx.ws_values(tm, ctx.inj_window()))
        inc = (dur - pre) if pre is not None and dur is not None else None
        frac = (inc / size_bytes) if (inc is not None and size_bytes) else None
        out.append(make_check("f8_ws_increase", "SIGNATURE", tm, f"{WS_METRIC} (during - pre) / stressor size", pre, frac,
                              ">=", th["increase_frac_of_stressor_min"], "frac"))
    return out


def signature_checks(ctx: Ctx, fault: str, pt: dict) -> list[dict]:
    if fault == "F1":
        return sig_f1(ctx, pt["follower"])
    if fault == "F2":
        return sig_f2(ctx, pt["follower"])
    if fault == "F4":
        return sig_f4(ctx, pt["tms"])
    if fault == "F5":
        return sig_f5(ctx)
    if fault == "F6":
        return sig_f6(ctx, pt["f6_planned"])
    if fault == "F7":
        return sig_f7(ctx, pt["tms"])
    if fault == "F8":
        return sig_f8(ctx, pt["tms"])
    raise ValueError(fault)


# ================================================================================================ mechanism
def _expected_set(pods: list[str]) -> set[str]:
    return {f"{ids.NAMESPACE}/{p}" for p in pods}


def _objects_for(ctx: Ctx, kind: str | None = None) -> dict[str, dict]:
    """Latest stored snapshot object per resource name (the APPLY_PLUS_10S or later snapshot)."""
    order = {"APPLY_PLUS_10S": 0, "MID": 1, "BEFORE_DELETE": 2, "AFTER_DELETE": 3}
    best: dict[str, tuple[int, dict]] = {}
    for s in ctx.gi.facts.get("chaos_snapshots") or []:
        if kind and s["kind"] != kind:
            continue
        obj = ctx.gi.chaos_objects.get(s["yaml_key"])
        if obj is None:
            continue
        rank = order.get(s["tag"], -1)
        if s["name"] not in best or rank >= best[s["name"]][0]:
            best[s["name"]] = (rank, obj)
    return {k: v[1] for k, v in best.items()}


def _records_match(obj: dict | None, selector_sets: dict[str, set[str]]) -> float:
    if obj is None:
        return 0.0
    recs = records(obj)
    if not recs:
        return 0.0
    got: dict[str, set[str]] = defaultdict(set)
    for r in recs:
        got[r["selector_key"]].add(r["id"])
    return 1.0 if dict(got) == selector_sets else 0.0


def _applied_within(obj: dict | None, planned: float, limit: float) -> tuple[float | None, float | None]:
    """(latest per-target first Apply event, seconds after `planned`)."""
    if obj is None:
        return None, None
    recs = records(obj) or []
    if not recs or any(r["first_apply_at"] is None for r in recs):
        return None, None
    latest = max(r["first_apply_at"] for r in recs)
    return latest, latest - planned


def mechanism_checks(ctx: Ctx) -> list[dict]:
    gi, a, rt = ctx.gi, ctx.a, ctx.run_type
    out: list[dict] = []
    planned = a["planned_onset_at"]
    exp = [t["pod"] for t in gi.facts.get("targets_expected") or []]
    if rt in ("F0", "F1", "F2"):
        # The preprocessing requires controller_at_onset on a valid F0, F1 and F2 attempt (C25).
        out.append(make_check("controller_at_onset_recorded", "MECHANISM", "all", "attempt.controller_at_onset", 1.0,
                              1.0 if a.get("controller_at_onset") is not None else 0.0, "==", 1, "bool"))
    if rt == "F0":
        n_res = float(len(a.get("resources") or []) + len(gi.facts.get("chaos_snapshots") or []))
        out.append(make_check("f0_nothing_applied", "MECHANISM", "all", "attempt resources and chaos listings",
                              0.0, n_res + (1.0 if a.get("applied") else 0.0), "==", 0, "objects"))
        return out
    onset_ok = 1.0 if a.get("onset_verified_at") is not None else 0.0
    out.append(make_check("onset_verified", "MECHANISM", "all", "attempt.onset_verified_at", 1.0, onset_ok, "==", 1, "bool"))
    if rt in ("F1", "F2"):
        ctrl = gi.facts.get("controller") or {}
        before = ctrl.get("node_id_before_apply")
        after = ctrl.get("node_id_after_injection")
        # REVIEW4 CAM-2 item 1 (C25): the controller read after the labelled end is a gate check, so a failed read is a
        # repeat during the campaign, not a refused build afterwards.
        out.append(make_check("controller_read_after_injection", "MECHANISM", "all",
                              "metadata-quorum LeaderId after the labelled end",
                              float(before) if before is not None else None,
                              float(after) if after is not None else None, ">=", 0, "node id"))
        brokers = ctx.broker_pods()
        followers = sorted((ids.broker_id_from_pod(b), b) for b in brokers if ids.broker_id_from_pod(b) != before)
        rank = gi.cell["follower_rank"]
        want = followers[rank][1] if before is not None and len(followers) == 2 else None
        out.append(make_check("follower_rule", "MECHANISM", exp[0] if exp else "none", "metadata-quorum LeaderId",
                              float(before) if before is not None else None,
                              1.0 if (want is not None and exp == [want]) else 0.0, "==", 1, "bool"))
        out.append(make_check("controller_metric_agrees", "MECHANISM", "all", "activecontrollercount",
                              float(before) if before is not None else None,
                              1.0 if ctrl.get("metric_agrees") else 0.0, "==", 1, "bool"))
    if rt == "F1":
        objs = _objects_for(ctx, "PodChaos")
        obj = next(iter(objs.values()), None)
        out.append(make_check("records_match", "MECHANISM", exp[0] if exp else "none", "PodChaos records", float(len(exp)),
                              _records_match(obj, {SELECTOR_MAIN: _expected_set(exp)}), "==", 1, "bool"))
        _, lag = _applied_within(obj, planned, ctx.th["F1"]["apply_max_s"])
        out.append(make_check("apply_event_within_max", "MECHANISM", "all", "PodChaos Apply event", 0.0, lag, "<=",
                              ctx.th["F1"]["apply_max_s"], "s"))
    elif rt == "F2":
        objs = _objects_for(ctx, "NetworkChaos")
        obj = next(iter(objs.values()), None)
        others = [b for b in ctx.broker_pods() if exp and b != exp[0]]
        sets = {SELECTOR_MAIN: _expected_set(exp), ".Target": _expected_set(others)}
        out.append(make_check("records_match", "MECHANISM", exp[0] if exp else "none", "NetworkChaos records", float(len(exp)),
                              _records_match(obj, sets), "==", 1, "bool"))
        _, lag = _applied_within(obj, planned, ctx.th["F2"]["apply_max_s"])
        out.append(make_check("all_injected_within_max", "MECHANISM", "all", "NetworkChaos Apply events", 0.0, lag, "<=",
                              ctx.th["F2"]["apply_max_s"], "s"))
    elif rt in ("F4", "F7"):
        kind = "StressChaos" if rt == "F4" else "NetworkChaos"
        objs = _objects_for(ctx, kind)
        obj = next(iter(objs.values()), None)
        ready_tms = ctx.tm_pods_at_onset()
        out.append(make_check("targets_all_ready_tms", "MECHANISM", "all", "pods_before", float(len(ready_tms)),
                              1.0 if sorted(exp) == ready_tms and len(ready_tms) == ids.EXPECTED_TASKMANAGERS else 0.0,
                              "==", 1, "bool"))
        out.append(make_check("records_match", "MECHANISM", "all", f"{kind} records", float(len(exp)),
                              _records_match(obj, {SELECTOR_MAIN: _expected_set(exp)}), "==", 1, "bool"))
        _, lag = _applied_within(obj, planned, ctx.th[rt]["apply_max_s"])
        out.append(make_check("all_injected_within_max", "MECHANISM", "all", f"{kind} Apply events", 0.0, lag, "<=",
                              ctx.th[rt]["apply_max_s"], "s"))
    elif rt == "F8":
        objs = _objects_for(ctx, "StressChaos")
        ready_tms = ctx.tm_pods_at_onset()
        out.append(make_check("targets_all_ready_tms", "MECHANISM", "all", "pods_before", float(len(ready_tms)),
                              1.0 if sorted(exp) == ready_tms and len(ready_tms) == ids.EXPECTED_TASKMANAGERS else 0.0,
                              "==", 1, "bool"))
        per_pod = {p["pod"]: p for p in ((gi.facts.get("f8") or {}).get("per_pod") or [])}
        res_by_pod = {}
        for name, obj in objs.items():
            pods = sorted(((obj.get("spec") or {}).get("selector") or {}).get("pods", {}).get(ids.NAMESPACE, []))
            if len(pods) == 1:
                res_by_pod[pods[0]] = obj
        for tm in sorted(exp):
            obj = res_by_pod.get(tm)
            out.append(make_check("records_match", "MECHANISM", tm, "StressChaos records", 1.0,
                                  _records_match(obj, {SELECTOR_MAIN: _expected_set([tm])}), "==", 1, "bool"))
            lim, ws, size_mib = f8_sizing(ctx, tm)
            rec = per_pod.get(tm) or {}
            rec_bytes = rec.get("stressor_bytes")
            spec_size = None
            if obj is not None:
                s = ((((obj.get("spec") or {}).get("stressors") or {}).get("memory") or {}).get("size") or "")
                if s.endswith("MiB") and s[:-3].isdigit():
                    spec_size = int(s[:-3]) * MIB
            consistent = (size_mib is not None and rec_bytes == size_mib * MIB and spec_size == rec_bytes)
            out.append(make_check("f8_stressor_size_rule", "MECHANISM", tm, f"{LIMIT_METRIC}, {WS_METRIC}",
                                  float(size_mib * MIB) if size_mib is not None else None,
                                  1.0 if consistent else 0.0, "==", 1, "bool"))
            out.append(make_check("f8_stressor_min", "MECHANISM", tm, "stressor size",
                                  float(size_mib * MIB) if size_mib is not None else None,
                                  float(size_mib * MIB) if size_mib is not None else None, ">=",
                                  ctx.th["F8"]["min_stressor_mib"] * MIB, "bytes"))
            _, lag = _applied_within(obj, planned, ctx.th["F8"]["apply_max_s"])
            out.append(make_check("apply_event_within_max", "MECHANISM", tm, "StressChaos Apply event", 0.0, lag, "<=",
                                  ctx.th["F8"]["apply_max_s"], "s"))
    elif rt == "F5":
        th = ctx.th["F5"]
        f5 = gi.facts.get("f5") or {}
        kills = f5.get("kills") or []
        out.append(make_check("f5_kill_count", "MECHANISM", "all", "f5.kills", float(th["kills"]), float(len(kills)), "==",
                              th["kills"], "kills"))
        objs = _objects_for(ctx, "PodChaos")
        for k in kills:
            name = ids.k8s_name("pk", a["attempt_key"], k["k"])
            obj = objs.get(name)
            out.append(make_check("records_match", "MECHANISM", f"kill-{k['k']}", "PodChaos records", 1.0,
                                  _records_match(obj, {SELECTOR_MAIN: _expected_set([k["pod"]])}), "==", 1, "bool"))
            first, _ = _applied_within(obj, k["planned_at"], th["kill_apply_max_s"])
            lag = None if first is None else first - k["planned_at"]
            out.append(make_check("f5_kill_apply_within_max", "MECHANISM", f"kill-{k['k']}", "PodChaos Apply event", 0.0,
                                  lag, "<=", th["kill_apply_max_s"], "s"))
            if lag is not None:
                out.append(make_check("f5_kill_apply_not_early", "MECHANISM", f"kill-{k['k']}", "PodChaos Apply event", 0.0,
                                      lag, ">=", 0, "s"))
    elif rt == "F6":
        th = ctx.th["F6"]
        f6 = gi.facts.get("f6") or {}
        parsed = parse_log(gi.injector_log or "")
        summ = parsed.get("summary") or {}
        obs = parsed.get("orchestrator") or {}
        planned_count = f6.get("planned_count")
        ready = f6.get("pod_ready_at")
        out.append(make_check("f6_pod_ready_before_onset", "MECHANISM", "all", "injector pod Ready", 0.0,
                              (planned - ready) if ready is not None else None, ">=", th["ready_lead_s"], "s"))
        out.append(make_check("f6_exit_zero", "MECHANISM", "all", "injector container exit code", 0.0,
                              float(obs["exit_code"]) if obs.get("exit_code") is not None else None, "==", 0, "code"))
        consistent = (summ.get("acked_count") == f6.get("acked_count") and summ.get("planned_count") == planned_count
                      and summ.get("sent_count") == f6.get("sent_count") and f6.get("sent_count") is not None
                      and summ.get("first_produce_at") == f6.get("first_produce_at")
                      and summ.get("last_produce_at") == f6.get("last_produce_at"))
        out.append(make_check("f6_record_matches_log", "MECHANISM", "all", "injector.log summary", 1.0,
                              1.0 if summ and consistent else 0.0, "==", 1, "bool"))
        acked = summ.get("acked_count")
        out.append(make_check("f6_acked_frac", "MECHANISM", "all", "injector acked_count", float(planned_count or 0) or None,
                              (acked / planned_count) if acked is not None and planned_count else None, ">=",
                              th["acked_frac_min"], "frac"))
        first = summ.get("first_produce_at")
        out.append(make_check("f6_first_ack_within_max", "MECHANISM", "all", "injector first_produce_at", 0.0,
                              (first - planned) if first is not None else None, "<=", th["apply_max_s"], "s"))
    return out


# ================================================================================================ envelope
def checkpoint_path_ok(path: str, prefix: str) -> bool:
    """A checkpoint under this campaign's scratch prefix (11.7, C13). The stored probe records hold the path relative to
    s3://<results-bucket>/scratch/<campaign_id>/ (C24), so a path in this campaign starts with flink-checkpoints/ and
    any other path keeps a scheme or another prefix and fails."""
    return path.startswith(prefix) and len(path) > len(prefix) and re.match(r"^[^\s:]+$", path) is not None


def scrub_checkpoint_path(path: str | None, bucket: str, campaign_id: str) -> str | None:
    """The form a checkpoint path is stored in (C24): relative to s3://<bucket>/scratch/<campaign_id>/ when it lies there,
    otherwise with the bucket name removed (s3:///<key>), so no bucket name enters a deposited record."""
    if not path:
        return path
    own = f"s3://{bucket}/scratch/{campaign_id}/"
    if bucket and path.startswith(own):
        return path[len(own):]
    m = re.match(r"^(s3a?|s3n)://[^/]*/(.*)$", path)
    if m:
        return f"{m.group(1)}:///{m.group(2)}"
    return path


def _jobs_ok(jobs: list[dict] | None, expected: list[str]) -> bool:
    """1.6.4 and C19: the non-terminal jobs are exactly the expected names, once each, all RUNNING."""
    if jobs is None:
        return False
    live = [j for j in jobs if j["state"] not in TERMINAL_JOB_STATES]
    return sorted(j["name"] for j in live) == sorted(expected) and all(j["state"] == "RUNNING" for j in live)


def _replaced(ctx: Ctx, pod: str) -> bool | None:
    b, a = ctx.pods_before.get(pod), ctx.pods_after.get(pod)
    if b is None:
        return None
    if a is None:
        return True  # gone: replaced under another name (TaskManagers) or never came back
    return a["uid"] != b["uid"] or a["restart_count"] > b["restart_count"]


def _env_no_replacement(ctx: Ctx, name: str, pods: list[str]) -> list[dict]:
    out = []
    for p in pods:
        r = _replaced(ctx, p)
        out.append(make_check(name, "ENVELOPE", p, "pods_before/pods_after", 0.0,
                              None if r is None else (1.0 if r else 0.0), "==", 0, "bool"))
    return out


def _flink_job_samples(ctx: Ctx, w: tuple[float, float]) -> list[tuple[float, list[dict]] | tuple[float, None]]:
    out = []
    for r in ctx.by_probe.get("flink_jobs", []):
        if w[0] <= r["ts"] < w[1]:
            out.append((r["ts"], json.loads(r["detail"]) if r["ok_count"] == 1 and r["detail"] else None))
    return out


def _all_expected_running(jobs: list[dict] | None, expected: list[str]) -> bool:
    if jobs is None:
        return False
    live = [j for j in jobs if j["state"] not in ("FAILED", "CANCELED", "FINISHED")]
    for name in expected:
        if sum(1 for j in live if j["name"] == name and j["state"] == "RUNNING") != 1:
            return False
    return all(j["name"] in expected for j in live)


def pre_fault_checks(ctx: Ctx) -> list[dict]:
    """Decision 24 (REVIEW4 CAM-3, C26): NORMAL time must be healthy. Over the PRE_FAULT segment, every expected job is
    RUNNING with the same job ID, three TaskManagers are registered, no pod is replaced or restarted, and the probes are at
    their pass level (1.6.8, per pair and per TaskManager). Every attempt, the sham included, fails its envelope otherwise.
    A segment with no sample of a kind fails as MISSING."""
    th = ctx.th["pre_fault"]
    w = ctx.pre_window()
    out: list[dict] = []
    expected_jobs = [j[0] for j in ids.FLINK_JOBS[ctx.gi.workload]]
    samples = _flink_job_samples(ctx, w) if w else []
    bad, first = 0, None
    for _, js in samples:
        if not _jobs_ok(js, expected_jobs):
            bad += 1
            continue
        ids_now = sorted((j["name"], j["job_id"]) for j in js if j["state"] not in TERMINAL_JOB_STATES)
        if first is None:
            first = ids_now
        elif ids_now != first:
            bad += 1
    out.append(make_check("pre_fault_jobs_running_same_id", "ENVELOPE", "all", "probe flink_jobs over PRE_FAULT",
                          float(len(samples)) if samples else None, float(bad) if samples else None, "==", 0, "samples"))
    tms = [r for r in ctx.by_probe.get("flink_tms", []) if w and w[0] <= r["ts"] < w[1]]
    bad_tm = sum(1 for r in tms if r["ok_count"] != 1 or r["value"] is None or r["value"] != th["taskmanagers_registered"])
    out.append(make_check("pre_fault_taskmanagers_registered", "ENVELOPE", "all", "probe flink_tms over PRE_FAULT",
                          float(len(tms)) if tms else None, float(bad_tm) if tms else None, "==", 0, "samples"))
    out.append(make_check("pre_fault_no_pod_change", "ENVELOPE", "all",
                          f"{INFO_METRIC} uid and {RESTARTS_METRIC} over PRE_FAULT", *pod_changes(
                              ctx.gi.verification_rows, w, th["expected_pods"]), "==", 0, "changes"))
    worst_tcp, n_pairs = tcp_worst_pair(ctx, w, ctx.broker_pods())
    out.append(make_check("pre_fault_tcp_ok", "ENVELOPE", "all", "probe tcp ok fraction over PRE_FAULT, worst src->dst pair",
                          float(n_pairs) if worst_tcp is not None else None, worst_tcp, ">=", th["tcp_ok_frac_min"], "frac"))
    worst_loss, n_tm = burst_worst_tm(ctx, w, ctx.tm_pods_at_onset())
    out.append(make_check("pre_fault_packet_loss", "ENVELOPE", "all",
                          "probe tcp_burst loss over PRE_FAULT, worst TaskManager",
                          float(n_tm) if worst_loss is not None else None, worst_loss, "<=", th["packet_loss_frac_max"],
                          "frac"))
    return out


def pod_changes(rows: list[dict], w: tuple[float, float] | None, expected: dict[str, int]
                ) -> tuple[float | None, float | None]:
    """(pods seen, changes) over a window from kube-state rows: for each role, the distinct pod UIDs minus the expected
    count, and every restart-counter increase of a broker, JobManager or TaskManager container. (None, None) without
    kube-state data. The same rule runs in the preprocessing for BASELINE and TRAILING windows (C26)."""
    if w is None:
        return None, None
    uids: dict[str, set[str]] = defaultdict(set)
    restarts: dict[tuple[str, str], list[tuple[int, float]]] = defaultdict(list)
    seen_info = seen_restarts = False
    for r in rows:
        if not (w[0] <= r["ts"] < w[1]):
            continue
        role = role_of(r["pod"], {})
        if role not in expected:
            continue
        if r["metric"] == INFO_METRIC:
            seen_info = True
            uids[role].add(r["pod_uid"] or f"name:{r['pod']}")
        elif r["metric"] == RESTARTS_METRIC:
            seen_restarts = True
            restarts[(r["pod"], r["series_id"])].append((r["ts"], r["value"]))
    if not seen_info or not seen_restarts:
        return None, None
    changes = sum(abs(len(uids.get(role, set())) - n) for role, n in expected.items())
    for pts in restarts.values():
        pts.sort()
        changes += sum(1 for (_, v0), (_, v1) in zip(pts, pts[1:]) if v1 != v0)
    return float(sum(len(v) for v in uids.values())), float(changes)


def tcp_worst_pair(ctx: Ctx, w, brokers: list[str]) -> tuple[float | None, int]:
    """The lowest per-pair TCP ok fraction (1.6.8, REVIEW4 MIN-10) over every broker-to-broker pair (ports 9090 and 9091)
    and every load generator pair (port 9092). None if an expected broker pair or every load generator pair has no sample."""
    if w is None or len(brokers) != ids.EXPECTED_BROKERS:
        return None, 0
    pairs = [(src, f"{dst}:{port}") for src in brokers for dst in brokers if dst != src
             for port in (ids.KAFKA_PORT_CONTROLLER, ids.KAFKA_PORT_REPLICATION)]
    lg = sorted({k for k in ctx.tcp if k[1].endswith(f":{ids.KAFKA_PORT_PLAIN}") and k[0] not in brokers})
    if not lg:
        return None, 0
    fracs = []
    for src, dst in pairs + lg:
        f = ctx.tcp_ok_frac(src, dst, w)
        if f is None:
            return None, 0
        fracs.append(f)
    return min(fracs), len(fracs)


def burst_worst_tm(ctx: Ctx, w, tms: list[str]) -> tuple[float | None, int]:
    """The highest per-TaskManager packet-probe loss (pooled over the window's bursts), None if a TaskManager has none."""
    if w is None or not tms:
        return None, 0
    worst = 0.0
    for tm in tms:
        pts = [(ok, tried) for t, ok, tried in ctx.burst_counts.get(tm, []) if w[0] <= t < w[1]]
        if not pts:
            return None, 0
        worst = max(worst, 1.0 - sum(o for o, _ in pts) / sum(n for _, n in pts))
    return worst, len(tms)


def envelope_checks(ctx: Ctx, flags: list[str], derived: dict) -> list[dict]:
    rt = ctx.run_type
    gi = ctx.gi
    brokers = ctx.broker_pods()
    jms = sorted(p for p in ctx.pods_before if role_of(p, {}) == "jobmanager")
    tms = sorted(p for p in ctx.pods_before if role_of(p, {}) == "taskmanager")
    expected_jobs = [j[0] for j in ids.FLINK_JOBS[gi.workload]]
    exp = [t["pod"] for t in gi.facts.get("targets_expected") or []]
    out: list[dict] = pre_fault_checks(ctx)
    # REVIEW4 CAM-2 item 2 (C25): the preprocessing requires a valid attempt to end with exactly the expected jobs RUNNING.
    end_jobs = ctx.a.get("flink_jobs_at_end") or None
    out.append(make_check("flink_jobs_at_end_running", "ENVELOPE", "all", "attempt.flink_jobs_at_end", 1.0,
                          None if end_jobs is None else (1.0 if _jobs_ok(end_jobs, expected_jobs) else 0.0), "==", 1, "bool"))
    if rt == "F1":
        out += _env_no_replacement(ctx, "env_no_other_replacement", [p for p in brokers + jms + tms if p not in exp])
    elif rt == "F2":
        follower = exp[0] if exp else None
        out += _env_no_replacement(ctx, "env_follower_not_restarted", [follower] if follower else [])
        th = ctx.th["F2"]
        lg_src = sorted({src for (src, dst) in ctx.tcp if dst == f"{follower}:{th['client_port']}"})
        if not lg_src:
            out.append(make_check("env_clients_connected", "ENVELOPE", "all", "probe tcp loadgen", None, None, ">=",
                                  th["client_ok_during_frac_min"], "frac"))
        for src in lg_src:
            dst = f"{follower}:{th['client_port']}"
            out.append(make_check("env_clients_connected", "ENVELOPE", f"{src}->{dst}", "probe tcp",
                                  ctx.tcp_ok_frac(src, dst, ctx.pre_window()), ctx.tcp_ok_frac(src, dst, ctx.inj_window()),
                                  ">=", th["client_ok_during_frac_min"], "frac"))
    elif rt == "F4":
        out += _env_no_replacement(ctx, "env_no_tm_replacement", tms)
    elif rt == "F5":
        out += _env_no_replacement(ctx, "env_jobmanager_not_restarted", jms)
        th = ctx.th["F5"]
        kills = (gi.facts.get("f5") or {}).get("kills") or []
        last_kill = max((k["apply_at"] for k in kills if k.get("apply_at") is not None), default=None)
        recovered_at = None
        if last_kill is not None and ctx.rec_end is not None:
            samples = _flink_job_samples(ctx, (last_kill, ctx.rec_end))
            # The earliest instant from which EVERY later sample shows all expected jobs RUNNING, so a stale RUNNING
            # sample before Flink noticed the kill cannot count as recovery.
            for i, (t, jobs) in enumerate(samples):
                if all(_all_expected_running(js, expected_jobs) for _, js in samples[i:]):
                    recovered_at = t
                    break
        out.append(make_check("env_jobs_running_after_last_kill", "ENVELOPE", "all", "probe flink_jobs", 0.0,
                              (recovered_at - last_kill) if recovered_at is not None and last_kill is not None else None,
                              "<=", th["job_recovery_limit_s"], "s"))
        # Checkpoints on S3: every expected job's latest completed checkpoint path at the end of RECOVERY.
        on_s3 = None
        if ctx.rec_end is not None:
            last = [js for _, js in _flink_job_samples(ctx, (ctx.onset or ctx.rec_end, ctx.rec_end)) if js is not None]
            if last:
                js = [j for j in last[-1] if j["state"] == "RUNNING" and j["name"] in expected_jobs]
                paths = [j.get("latest_completed_path") or "" for j in js]
                on_s3 = 1.0 if len(js) == len(expected_jobs) and all(
                    checkpoint_path_ok(p, th["checkpoint_path_prefix"]) for p in paths) else 0.0
        out.append(make_check("env_checkpoints_on_s3", "ENVELOPE", "all", "probe flink_jobs latest_completed_path", 1.0,
                              on_s3, "==", 1, "bool"))
        derived["f5_all_jobs_running_at"] = None if recovered_at is None else round(recovered_at, 3)
    elif rt == "F6":
        restart = None
        if ctx.onset is not None and ctx.rec_end is not None:
            samples = _flink_job_samples(ctx, (ctx.onset, ctx.rec_end))
            if samples:
                ids_first = None
                restart = 0.0
                for _, js in samples:
                    if js is None or not _all_expected_running(js, expected_jobs):
                        restart = 1.0
                        break
                    jid = sorted((j["name"], j["job_id"]) for j in js if j["name"] in expected_jobs)
                    if ids_first is None:
                        ids_first = jid
                    elif jid != ids_first:
                        restart = 1.0
                        break
        out.append(make_check("env_no_flink_job_restart", "ENVELOPE", "all", "probe flink_jobs", 0.0, restart, "==", 0, "bool"))
    elif rt == "F7":
        out += _env_no_replacement(ctx, "env_no_broker_or_jm_replacement", brokers + jms)
        if any(_replaced(ctx, t) for t in tms):
            flags.append("F7_TM_LOSS")  # flagged, never a failure (1.2 F7)
    elif rt == "F8":
        out += _env_no_replacement(ctx, "env_no_tm_replacement", tms)
        w = (ctx.onset, ctx.rec_end) if ctx.onset is not None and ctx.rec_end is not None else None
        for tm in tms:
            pre_oom = ctx.gauge_by_ts(REASON_METRIC, tm, ctx.pre_window(), lambda r: '"reason":"OOMKilled"' in r["labels"])
            dur_oom = ctx.gauge_by_ts(REASON_METRIC, tm, w, lambda r: '"reason":"OOMKilled"' in r["labels"])
            after = ctx.pods_after.get(tm) or {}
            oom = 1.0 if (any(v >= 1 for v in dur_oom.values()) and not any(v >= 1 for v in pre_oom.values())) \
                or after.get("last_termination_reason") == "OOMKilled" and (ctx.pods_before.get(tm) or {}).get(
                    "last_termination_reason") != "OOMKilled" else 0.0
            out.append(make_check("env_no_oom_kill", "ENVELOPE", tm, f"{REASON_METRIC}{{reason=OOMKilled}}", 0.0, oom,
                                  "==", 0, "bool"))
            # REVIEW4 L8 (C38): a stressor killed by the kernel leaves the container running and no OOMKilled reason, but
            # cAdvisor counts the OOM event.
            out.append(make_check("env_no_oom_event", "ENVELOPE", tm, f"{OOM_METRIC}{{container={ids.FLINK_CONTAINER}}}",
                                  0.0, oom_increase(ctx, tm, w), "<=", ctx.th["F8"]["oom_events_increase_max"], "events"))
    return out


def oom_increase(ctx: Ctx, tm: str, w: tuple[float, float] | None) -> float | None:
    """Increase of container_oom_events_total for flink-main-container from the last sample at or before onset to the end
    of RECOVERY, summed per series (a restarted container is a new series). None without samples."""
    if w is None:
        return None
    series: dict[str, list[tuple[int, float]]] = defaultdict(list)
    for r in ctx.metric.get((OOM_METRIC, tm), []):
        if r["container"] == ids.FLINK_CONTAINER and w[0] - 10 <= r["ts"] < w[1]:
            series[r["series_id"]].append((r["ts"], r["value"]))
    if not series:
        return None
    inc = 0.0
    for pts in series.values():
        pts.sort()
        inc += sum(max(0.0, v1 - v0) for (_, v0), (_, v1) in zip(pts, pts[1:]))
    return inc


# ================================================================================================ timing
SOURCES = {"F0": ("PLANNED", "PLANNED"), "F1": ("CHAOS_APPLY_EVENT", "FIXED_SPAN"), "F5": ("CHAOS_APPLY_EVENT", "FIXED_SPAN"),
           "F6": ("FIRST_PRODUCE_ACK", "LAST_PRODUCE_ACK"), "F2": ("CHAOS_APPLY_EVENT", "CHAOS_RECOVER_EVENT"),
           "F4": ("CHAOS_APPLY_EVENT", "CHAOS_RECOVER_EVENT"), "F7": ("CHAOS_APPLY_EVENT", "CHAOS_RECOVER_EVENT"),
           "F8": ("CHAOS_APPLY_EVENT", "CHAOS_RECOVER_EVENT")}
ONE_SHOT_KINDS = ("PodChaos",)  # pod-kill is the only one-shot action the injectors create


def times_from_records(ctx: Ctx) -> tuple[float | None, float | None]:
    """REVIEW4 MIN-1 (C27): onset and labelled end recomputed from the stored records alone (the chaos snapshot YAML, the
    injector log, the plan), by the 2.4 source table, so the offline gate catches a manifest whose times disagree."""
    a, rt = ctx.a, ctx.run_type
    dur = a["configured_duration_s"]
    if rt == "F0":
        return a["planned_onset_at"], a["planned_onset_at"] + dur
    if rt == "F6":
        summ = parse_log(ctx.gi.injector_log or "").get("summary") or {}
        return summ.get("first_produce_at"), summ.get("last_produce_at")
    kind = {"F1": "PodChaos", "F5": "PodChaos", "F2": "NetworkChaos", "F7": "NetworkChaos"}.get(rt, "StressChaos")
    objs = _objects_for(ctx, kind)
    if rt in ("F1", "F5"):
        name = ids.k8s_name("pk", a["attempt_key"]) if rt == "F1" else ids.k8s_name("pk", a["attempt_key"], 1)
        recs = records(objs[name]) if name in objs else None
        if not recs or any(r["first_apply_at"] is None for r in recs):
            return None, None
        onset = max(r["first_apply_at"] for r in recs)
        return onset, onset + dur
    firsts, lasts = [], []
    for name in sorted(objs):
        recs = records(objs[name]) or []
        if not recs or any(r["first_apply_at"] is None for r in recs):
            return None, None
        firsts.append(max(r["first_apply_at"] for r in recs))
        lasts.append(None if any(r["last_recover_at"] is None for r in recs) else max(r["last_recover_at"] for r in recs))
    if not firsts:
        return None, None
    return max(firsts), (None if any(x is None for x in lasts) else max(lasts))


def timing_checks(ctx: Ctx, flags: list[str]) -> list[dict]:
    a, rt = ctx.a, ctx.run_type
    th = ctx.th["timing"]
    out: list[dict] = []
    onset, lend = a.get("onset_verified_at"), a.get("labelled_end_at")
    tol = ctx.th["record_time_tolerance_s"]
    r_onset, r_end = times_from_records(ctx)
    if onset is not None:
        out.append(make_check("onset_from_records", "TIMING", "all", "2.4 onset source in the stored records", onset,
                              None if r_onset is None else abs(r_onset - onset), "<=", tol, "s"))
    if lend is not None:
        out.append(make_check("end_from_records", "TIMING", "all", "2.4 end source in the stored records", lend,
                              None if r_end is None else abs(r_end - lend), "<=", tol, "s"))
    if onset is not None and lend is not None:
        # C25: the onset and end sources of 2.4, which the preprocessing checks on every valid attempt
        out.append(make_check("onset_end_sources", "TIMING", "all", "attempt.onset_source and end_source", 1.0,
                              1.0 if (a.get("onset_source"), a.get("end_source")) == SOURCES[rt] else 0.0, "==", 1, "bool"))
    if rt == "F0":
        out.append(make_check("f0_planned_span", "TIMING", "all", "attempt", 0.0,
                              (lend - onset - a["configured_duration_s"]) if lend is not None and onset is not None else None,
                              "==", 0, "s"))
        return out
    planned = a["planned_onset_at"]
    apply_max = ctx.th["F5"]["kill_apply_max_s"] if rt == "F5" else ctx.th[rt]["apply_max_s"]
    out.append(make_check("onset_within_apply_max", "TIMING", "all", "onset_verified_at - planned_onset_at", 0.0,
                          (onset - planned) if onset is not None else None, "<=", apply_max, "s"))
    if onset is not None:
        out.append(make_check("onset_not_before_planned", "TIMING", "all", "onset_verified_at - planned_onset_at", 0.0,
                              onset - planned, ">=", 0, "s"))
    if rt in ("F2", "F4", "F7", "F8"):
        ov = a.get("overrun_s")
        out.append(make_check("end_overrun", "TIMING", "all", "labelled_end_at - onset - duration", 0.0, ov, "<=",
                              th["overrun_fail_s"], "s"))
        # REVIEW4 CAM-2 item 3 (C25): an end more than 60 s early fails too (2.4 "within 60 s of onset plus duration")
        out.append(make_check("end_underrun", "TIMING", "all", "labelled_end_at - onset - duration", 0.0, ov, ">=",
                              th["underrun_fail_s"], "s"))
        if ov is not None and lend is not None and onset is not None:
            out.append(make_check("overrun_matches_span", "TIMING", "all", "overrun_s against the labelled span", 0.0,
                                  abs(ov - (lend - onset - a["configured_duration_s"])), "<=", ctx.th["record_time_tolerance_s"],
                                  "s"))
        if ov is not None and th["end_tolerance_s"] < ov <= th["overrun_fail_s"]:
            flags.append("OVERRUN")
    elif rt in ("F1", "F5"):
        out.append(make_check("labelled_end_present", "TIMING", "all", "labelled_end_at", 1.0,
                              1.0 if lend is not None else 0.0, "==", 1, "bool"))
        if lend is not None and onset is not None:
            out.append(make_check("fixed_span", "TIMING", "all", "labelled_end_at - onset_verified_at - 300", 0.0,
                                  abs(lend - onset - a["configured_duration_s"]), "<=", ctx.th["record_time_tolerance_s"], "s"))
    elif rt == "F6":
        out.append(make_check("labelled_end_present", "TIMING", "all", "labelled_end_at", 1.0,
                              1.0 if lend is not None else 0.0, "==", 1, "bool"))
        if lend is not None and onset is not None:
            out.append(make_check("last_ack_after_first", "TIMING", "all", "labelled_end_at - onset_verified_at", 0.0,
                                  lend - onset, ">=", 0.001, "s"))
    for res in a.get("resources") or []:
        dt = None
        if res.get("delete_called_at") is not None and res.get("not_found_at") is not None:
            dt = res["not_found_at"] - res["delete_called_at"]
        out.append(make_check("deleted_not_found_within", "TIMING", f"{res['kind']}/{res['name']}", "NotFound poll",
                              0.0, dt, "<=", th["not_found_max_s"], "s"))
        if res["kind"] in ONE_SHOT_KINDS:
            # REVIEW4 L2 (C33): Chaos Mesh keeps a one-shot experiment in Run until the object is deleted, so AllRecovered
            # never shows while it exists. The finalizer is released only once every record is Not Injected (the
            # orchestrator never sets the force-clean annotation), so a NotFound within not_found_max_s of the delete call
            # is the recovery proof.
            out.append(make_check("all_recovered_observed", "TIMING", f"{res['kind']}/{res['name']}",
                                  "one-shot: NotFound within 60 s of the delete call (finalizer released after every record is "
                                  "Not Injected)", 1.0, dt, "<=", th["not_found_max_s"], "s"))
        elif res["kind"] in ("NetworkChaos", "StressChaos"):
            rec = _recovered_observed(ctx, res["name"])
            out.append(make_check("all_recovered_observed", "TIMING", f"{res['kind']}/{res['name']}",
                                  "AllRecovered condition and records", 1.0, rec, "==", 1, "bool"))
    if a.get("aborted_at") is not None:
        out.append(make_check("not_aborted", "TIMING", "all", "attempt.aborted_at", 0.0, 1.0, "==", 0, "bool"))
    return out


def _recovered_observed(ctx: Ctx, name: str) -> float:
    for s in ctx.gi.facts.get("chaos_snapshots") or []:
        if s["name"] != name or s["tag"] not in ("BEFORE_DELETE", "AFTER_DELETE"):
            continue
        obj = ctx.gi.chaos_objects.get(s["yaml_key"])
        if s["conditions"].get("AllRecovered", {}).get("status") == "True" and obj is not None:
            recs = records(obj)
            if recs and all(r["phase"] == "Not Injected" for r in recs):
                return 1.0
    return 0.0


# ================================================================================================ telemetry
def segment_steps(seg: dict) -> list[int]:
    """The 10 s steps t with start <= t < end of one segment (the export grid)."""
    return [t for t in range(int(math.ceil(seg["start_at"] / 10.0)) * 10, int(math.ceil(seg["end_at"] / 10.0)) * 10, 10)
            if seg["start_at"] <= t < seg["end_at"]]


def targeted_scrape_pods(rt: str, targets_expected: list[dict], th_tel: dict) -> tuple[set[str], set[str]]:
    """(pods, roles) of the fault's own targets, exempt in INJECTION and RECOVERY (decision 21)."""
    pods, roles = set(), set()
    for spec in th_tel["targeted"].get(rt, []):
        if spec == "TARGET_BROKER":
            pods |= {t["pod"] for t in targets_expected if ids.broker_id_from_pod(t["pod"]) is not None}
        else:
            roles.add(spec)
    return pods, roles


def scrape_health_rows(rows: list[dict], segments: list[dict], rt: str, targets_expected: list[dict], th_tel: dict
                       ) -> list[dict]:
    """Decision 21 (C22). For every broker, JobManager and TaskManager pod and every PRE_FAULT, INJECTION and RECOVERY
    segment: the 10 s steps at which kube-state-metrics shows the pod (expected) and those among them without Prometheus
    `up` equal to 1 for the pod (missed). The preprocessing applies the same rule to the same exported rows
    (streamprep.health.scrape_health_rows)."""
    roles = set(th_tel["scrape_roles"])
    alive: dict[str, set[int]] = defaultdict(set)
    up_ok: dict[str, set[int]] = defaultdict(set)
    for r in rows:
        if r["metric"] == INFO_METRIC and r["source"] == "kube_state":
            if role_of(r["pod"], {}) in roles:
                alive[r["pod"]].add(int(r["ts"]))
        elif r["metric"] == UP_METRIC and r["source"] == "scrape" and r["value"] == 1.0:
            up_ok[r["pod"]].add(int(r["ts"]))
    t_pods, t_roles = targeted_scrape_pods(rt, targets_expected, th_tel)
    out = []
    for seg in segments:
        if seg["segment_type"] not in th_tel["segments"]:
            continue
        steps = segment_steps(seg)
        if not steps:
            continue
        exempt_here = seg["segment_type"] in th_tel["exempt_segments"]
        for pod in sorted(alive):
            exp = [t for t in steps if t in alive[pod]]
            if not exp:
                continue
            role = role_of(pod, {})
            targeted = exempt_here and (pod in t_pods or role in t_roles)
            out.append({"target": f"{role}:{pod}", "segment": seg["segment_type"], "expected_count": len(exp),
                        "missed_count": sum(1 for t in exp if t not in up_ok.get(pod, set())), "targeted": targeted})
    return out


def scrape_checks(scr: list[dict], miss_max: int) -> list[dict]:
    """One TELEMETRY check per untargeted scrape row: more than miss_max missed scrapes fail (decision 21)."""
    return [make_check("telemetry_scrape_misses", "TELEMETRY", r["target"], f"up absent or 0 while the pod exists, {r['segment']}",
                       float(r["expected_count"]), float(r["missed_count"]), "<=", miss_max, "scrapes")
            for r in scr if not r["targeted"]]


def telemetry_rows(ctx: Ctx) -> list[dict]:
    """Task-level series presence per feature source (3.4.4): recorded as values and the SERIES_GAP flag, never a gate
    (decision 21, C22). The fault's own targets are marked targeted in INJECTION and RECOVERY."""
    th = ctx.th["telemetry"]
    rt = ctx.run_type
    t_pods, t_roles = targeted_scrape_pods(rt, ctx.gi.facts.get("targets_expected") or [], th)
    t_brokers = {ids.broker_id_from_pod(p) for p in t_pods}
    presence: dict[str, dict[int, set[str]]] = defaultdict(lambda: defaultdict(set))
    for r in ctx.gi.feature_rows:
        if r["role"] == "broker" and r["broker_id"] is not None:
            key = f"{r['source']}:broker-{r['broker_id']}"
        else:
            key = f"{r['source']}:{r['role']}"
        presence[key][r["ts"]].add(r["pod"])
    out = []
    for seg_type in th["segments"]:
        seg = ctx.segs.get(seg_type)
        if seg is None:
            continue
        steps = segment_steps(seg)
        if not steps:
            continue  # a segment shorter than one 10 s step (an ABORTED attempt) expects nothing and gets no row
        exempt_here = seg_type in th["exempt_segments"]
        for spec in th["sources"]:
            keys = ([f"{spec['source']}:broker-{b}" for b in range(ids.EXPECTED_BROKERS)] if spec["per_broker"]
                    else [f"{spec['source']}:{spec['role']}"])
            for key in keys:
                expected = len(steps) * spec["pods"]
                present = sum(min(len(presence[key].get(t, ())), spec["pods"]) for t in steps)
                scope = key.split(":", 1)[1]
                is_targeted = exempt_here and ((scope.startswith("broker-") and int(scope[7:]) in t_brokers)
                                               or spec["role"] in t_roles)
                out.append({"source": key, "segment": seg_type, "expected_count": expected, "present_count": present,
                            "coverage_frac": (present / expected) if expected else 1.0, "targeted": is_targeted})
    return out


# ================================================================================================ evaluate
def evaluate(gi: GateInputs) -> GateResult:
    ctx = Ctx(gi)
    flags: list[str] = []
    rt = ctx.run_type
    pt = _pseudo(ctx)
    checks: list[dict] = []
    checks += mechanism_checks(ctx)
    sig_all = []
    own_sig: list[dict] = []
    for fault in ids.FAULTS:
        sc = signature_checks(ctx, fault, pt)
        sig_all.append({"run_type": fault, "pass": bool(sc) and all(c["pass"] for c in sc)})
        if fault == rt:
            own_sig = sc
    if rt == "F0":
        # Decision 22 (REVIEW4 CAM-1, C23): 2.4 puts "every fault's signature fails" in the gating column of F0. A sham
        # whose signature passes is GATE_FAILED and repeated. The false passes are counted over every F0 attempt by the
        # analysis, which also flags the fault's runs after two (2.5).
        own_sig = [make_check("f0_signature_false_pass", "SIGNATURE", s_["run_type"],
                              "the fault's signature on the sham's pseudo-targets", 0.0, 1.0 if s_["pass"] else 0.0,
                              "==", 0, "bool") for s_ in sig_all]
    checks += own_sig
    derived: dict = {}
    checks += envelope_checks(ctx, flags, derived)
    for c in own_sig:
        if c["name"] == "f5_new_tm_uids" and c["during_value"] is not None:
            derived["f5_new_tm_uid_count"] = int(c["during_value"])
        if c["name"] == "f6_consumer_malformed":
            n = malformed_delta(ctx, ctx.a["planned_onset_at"], ctx.rec_end if ctx.rec_end is not None else ctx.lend)
            derived["f6_consumer_count"] = None if n is None else int(n)
    if rt == "F8":
        during, export_size = {}, {}
        for tm in pt["tms"]:
            pre = median(ctx.ws_values(tm, ctx.pre_window()))
            dur = median(ctx.ws_values(tm, ctx.inj_window()))
            if pre is not None and dur is not None:
                during[tm] = (int(dur), int(dur - pre))
            _, _, mib = f8_sizing(ctx, tm, from_export=True)
            export_size[tm] = None if mib is None else int(mib) * MIB
        derived["f8_during"] = during
        derived["f8_export_size"] = export_size
    checks += timing_checks(ctx, flags)
    th_tel = gi.thresholds["gate"]["telemetry"]
    scr = scrape_health_rows(gi.verification_rows, ctx.a["segments"], rt, gi.facts.get("targets_expected") or [], th_tel)
    checks += scrape_checks(scr, th_tel["scrape_miss_max"])
    tele = telemetry_rows(ctx)
    if any(not t["targeted"] and t["coverage_frac"] < th_tel["series_gap_flag_below"] for t in tele):
        flags.append("SERIES_GAP")  # recorded, never a gate (decision 21)

    def layer_ok(layer: str) -> bool:
        return all(c["pass"] for c in checks if c["layer"] == layer)

    telemetry_ok = layer_ok("TELEMETRY") and bool(scr)
    signature_ok = bool(own_sig) and layer_ok("SIGNATURE")
    gate_ok = layer_ok("MECHANISM") and signature_ok and layer_ok("ENVELOPE") and layer_ok("TIMING") and telemetry_ok
    result = {"mechanism": layer_ok("MECHANISM"), "signature": signature_ok, "envelope": layer_ok("ENVELOPE"),
              "timing": layer_ok("TIMING"), "telemetry": telemetry_ok, "gate": gate_ok}
    return GateResult(checks=checks, signatures_all_faults=sig_all, telemetry=tele, result=result, flags=flags,
                      derived=derived, scrapes=scr)


def classify_failure(gr: GateResult, run_type: str) -> str:
    """GATE_FAILED if any mechanism, signature, envelope or timing check failed; TELEMETRY_FAILED if only the
    telemetry rule failed (3.5). A sham's signature layer gates like every other run type's (C23)."""
    r = gr.result
    other_ok = r["mechanism"] and r["envelope"] and r["timing"] and r["signature"]
    if other_ok and not r["telemetry"]:
        return "TELEMETRY_FAILED"
    return "GATE_FAILED"


# ================================================================================================ store I/O
def load_inputs(store, manifest: dict, plan: dict, akey: str, thresholds: dict, facts: dict | None = None) -> GateInputs:
    """Everything the gate reads, from the object store only."""
    from .exporter import read_parquet as read_metrics
    from .probefile import read_parquet as read_probes

    attempt = next(a for a in manifest["attempts"] if a["attempt_key"] == akey)
    if facts is None:
        facts = json.loads(store.get(f"attempts/{akey}/verification.json").decode("utf-8"))
    cell = next(c for b in plan["blocks"] for c in b["cells"] if c["cell_key"] == attempt["cell_key"])
    chaos_objects = {}
    for s in facts.get("chaos_snapshots") or []:
        chaos_objects[s["yaml_key"]] = yaml.safe_load(store.get(s["yaml_key"]).decode("utf-8"))
    exp = attempt["export"]
    probes = read_probes(store.get(exp["probes_key"]))
    vrows = read_metrics(store.get(exp["verification_metrics_key"]))
    # Presence columns only: the gate counts feature samples for the telemetry rule and never sees a feature value (2.1).
    from .exporter import FEATURE_PRESENCE_COLUMNS
    frows = read_metrics(store.get(exp["features_key"]), columns=FEATURE_PRESENCE_COLUMNS)
    injector_log = None
    if attempt["run_type"] == "F6" and any(o.key == f"attempts/{akey}/injector.log" for o in store.list(f"attempts/{akey}/")):
        injector_log = store.get(f"attempts/{akey}/injector.log").decode("utf-8")
    stats = None
    if manifest.get("baseline_stats_key"):
        stats = json.loads(store.get(manifest["baseline_stats_key"]).decode("utf-8"))
    return GateInputs(plan=plan, cell=cell, attempt=attempt, facts=facts, chaos_objects=chaos_objects, probes=probes,
                      verification_rows=vrows, feature_rows=frows, injector_log=injector_log, baseline_stats=stats,
                      thresholds=thresholds, workload=manifest["workload"])


def verification_record(gi: GateInputs, gr: GateResult, mode: str, git_commit: str, thresholds_sha256: str,
                        evaluated_at: float) -> dict:
    facts = gi.facts
    keep = ("pseudo_targets", "targets_expected", "targets_selected", "controller", "chaos_snapshots", "pods_before",
            "pods_after", "f5", "f6", "f8", "recovery_gate_conditions")
    rec = {k: facts.get(k) for k in keep}
    for k in ("targets_expected", "targets_selected", "chaos_snapshots", "pods_before", "pods_after",
              "recovery_gate_conditions"):
        if rec[k] is None:
            rec[k] = []
    rec.update({
        "schema_version": SCHEMA_VERSION, "attempt_key": gi.attempt["attempt_key"], "run_id": gi.attempt["run_id"],
        "run_type": gi.attempt["run_type"], "mode": mode, "gate_git_commit": git_commit,
        "thresholds_sha256": thresholds_sha256, "evaluated_at": evaluated_at,
        "checks": gr.checks, "signatures_all_faults": gr.signatures_all_faults, "telemetry": gr.telemetry,
        "scrapes": gr.scrapes, "result": gr.result,
    })
    return rec


def compare(online: dict, offline: dict) -> list[str]:
    """Differences between the online record and an offline re-evaluation (`make check` fails on any)."""
    diffs = []
    key = lambda c: (c["name"], c["target"], c["layer"], c["series"])  # noqa: E731
    on = {key(c): c["pass"] for c in online["checks"]}
    off = {key(c): c["pass"] for c in offline["checks"]}
    for k in sorted(set(on) | set(off)):
        if on.get(k) != off.get(k):
            diffs.append(f"check {k}: online={on.get(k)} offline={off.get(k)}")
    for k in ("mechanism", "signature", "envelope", "timing", "telemetry", "gate"):
        if online["result"][k] != offline["result"][k]:
            diffs.append(f"result.{k}: online={online['result'][k]} offline={offline['result'][k]}")
    on_sig = {s["run_type"]: s["pass"] for s in online["signatures_all_faults"]}
    off_sig = {s["run_type"]: s["pass"] for s in offline["signatures_all_faults"]}
    if on_sig != off_sig:
        diffs.append(f"signatures_all_faults: online={on_sig} offline={off_sig}")
    return diffs


def record_sha(rec: dict) -> str:
    from .canonical import canonical_bytes
    return sha256_hex(canonical_bytes(rec))
