"""Rules computed from the verification channel (protocol 2.3, 3.4), never from a feature value.

1. Scrape health (decision 21, CONTRACT_CHANGES C22). The same rule as the orchestrator's gate.scrape_health_rows, on the
   same exported rows: for every broker, JobManager and TaskManager pod and every PRE_FAULT, INJECTION and RECOVERY segment,
   the 10 s steps at which kube-state-metrics shows the pod (expected) and those among them without Prometheus `up` equal
   to 1 (missed). More than `scrape_miss_max` misses of an untargeted target fail the attempt. The fault's own targets are
   exempt in INJECTION and RECOVERY. The parameters are the campaign's registered thresholds (run/thresholds.json,
   gate.telemetry), so the two builders cannot apply different numbers.

2. NORMAL-time health (decision 24, C26). BASELINE and TRAILING windows must meet the conditions the gate applies to
   PRE_FAULT: every expected Flink job RUNNING with an unchanged job ID, three TaskManagers registered, no broker,
   JobManager or TaskManager pod replaced or restarted, and the probes at their pass level (TCP connects at least 95% per
   source and destination pair, packet-probe loss at most 2% per TaskManager). The evidence of a window is every sample
   from 10 s before its start to 10 s after its end, so a change between two samples is seen by the windows on both sides.
   A window that fails, or has no sample of a kind, is EXCLUDED, and the counts are reported (build_report.json).

3. Sham false passes (2.5, decision 22, C23). Counted over every F0 attempt that has a verification record. The fault runs
   of a signature that false-passed on two or more shams carry SIGNATURE_FALSE_PASS in the windows (timeline.py).

A probe whose kubectl exec failed (error starting with EXEC) is a missing sample, never a failed connect (REVIEW4 L7).
"""
from __future__ import annotations

import json
import math
from collections import defaultdict

import numpy as np
import pyarrow.parquet as pq

INFO_METRIC = "kube_pod_info"
RESTARTS_METRIC = "kube_pod_container_status_restarts_total"
UP_METRIC = "up"
TERMINAL = ("FAILED", "CANCELED", "FINISHED")
BROKER_PREFIX = "streaming-kafka-brokers-"
KAFKA_PORTS_BROKER = (9090, 9091)
KAFKA_PORT_PLAIN = 9092
HEALTH_CONDITIONS = ("jobs_running_same_id", "taskmanagers_registered", "no_pod_change", "tcp_ok", "packet_loss")


def role_of(pod: str) -> str:
    """The exporter's role rule (orchestrator exporter.role_of) for rows without a flink_component label."""
    if pod.startswith(BROKER_PREFIX) and pod[len(BROKER_PREFIX):].isdigit():
        return "broker"
    if "-taskmanager-" in pod:
        return "taskmanager"
    if pod.startswith("streaming-flink-"):
        return "jobmanager"
    return "other"


def broker_id(pod: str) -> int | None:
    if pod.startswith(BROKER_PREFIX) and pod[len(BROKER_PREFIX):].isdigit():
        return int(pod[len(BROKER_PREFIX):])
    return None


def exec_failed(error) -> bool:
    return isinstance(error, str) and error.startswith("EXEC")


def segment_steps(start_s: float, end_s: float) -> list[int]:
    """The 10 s steps t with start <= t < end (the orchestrator's gate.segment_steps)."""
    return [t for t in range(int(math.ceil(start_s / 10.0)) * 10, int(math.ceil(end_s / 10.0)) * 10, 10)
            if start_s <= t < end_s]


# ============================================================================================ 1. scrape health
def scrape_index(rows) -> tuple[dict[str, set[int]], dict[str, set[int]]]:
    """(pod -> steps kube-state shows it, pod -> steps with up == 1) from verification rows (dicts with metric, source,
    pod, ts, value)."""
    alive: dict[str, set[int]] = defaultdict(set)
    up_ok: dict[str, set[int]] = defaultdict(set)
    for r in rows:
        if r["metric"] == INFO_METRIC and r["source"] == "kube_state":
            alive[r["pod"]].add(int(r["ts"]))
        elif r["metric"] == UP_METRIC and r["source"] == "scrape" and r["value"] == 1.0:
            up_ok[r["pod"]].add(int(r["ts"]))
    return alive, up_ok


def targeted_pods(run_type: str, targets_expected: list[dict], th_tel: dict) -> tuple[set[str], set[str]]:
    pods, roles = set(), set()
    for spec in th_tel["targeted"].get(run_type, []):
        if spec == "TARGET_BROKER":
            pods |= {t["pod"] for t in targets_expected if broker_id(t["pod"]) is not None}
        else:
            roles.add(spec)
    return pods, roles


def scrape_health_rows(alive: dict[str, set[int]], up_ok: dict[str, set[int]], segments: list[dict], run_type: str,
                       targets_expected: list[dict], th_tel: dict) -> list[dict]:
    """The verification record's `scrapes` rows, recomputed (the gate's rule, see the module docstring)."""
    roles = set(th_tel["scrape_roles"])
    t_pods, t_roles = targeted_pods(run_type, targets_expected, th_tel)
    out = []
    for seg in segments:
        if seg["segment_type"] not in th_tel["segments"]:
            continue
        steps = segment_steps(seg["start_at"], seg["end_at"])
        if not steps:
            continue
        exempt_here = seg["segment_type"] in th_tel["exempt_segments"]
        for pod in sorted(alive):
            role = role_of(pod)
            if role not in roles:
                continue
            exp = [t for t in steps if t in alive[pod]]
            if not exp:
                continue
            targeted = exempt_here and (pod in t_pods or role in t_roles)
            out.append({"target": f"{role}:{pod}", "segment": seg["segment_type"], "expected_count": len(exp),
                        "missed_count": sum(1 for t in exp if t not in up_ok.get(pod, set())), "targeted": targeted})
    return out


def scrape_verdict(rows: list[dict], miss_max: int) -> bool:
    """result.telemetry under decision 21: at least one row, and no untargeted row with more than miss_max misses."""
    return bool(rows) and all(r["targeted"] or r["missed_count"] <= miss_max for r in rows)


def read_verification_rows(path, lo_s: float | None = None, hi_s: float | None = None) -> list[dict]:
    """The rows of one exported verification file the two rules above read (kube-state info and restarts, up)."""
    t = pq.read_table(path, columns=["ts", "metric", "source", "pod", "pod_uid", "series_id", "value"])
    metric = np.asarray(t.column("metric").to_pylist(), dtype=object)
    keep = np.isin(metric, [INFO_METRIC, RESTARTS_METRIC, UP_METRIC])
    if lo_s is not None:
        ts = t.column("ts").to_numpy()
        keep &= (ts >= lo_s) & (ts < hi_s)
    idx = np.flatnonzero(keep)
    if not idx.size:
        return []
    sub = t.take(idx).to_pylist()
    return sub


# ============================================================================================ 2. NORMAL-time health
class Evidence:
    """Verification-channel samples over the BASELINE and TRAILING stretches, as sorted arrays for window queries."""

    def __init__(self, probe_rows: list[dict], kube_rows: list[dict], expected_jobs: list[str], th_pre: dict) -> None:
        self.th = th_pre
        self.expected_jobs = sorted(expected_jobs)
        # flink_jobs: a sample is bad when the job set is not exactly the expected jobs RUNNING; a job ID change between
        # consecutive samples makes both samples bad.
        jobs = sorted((r for r in probe_rows if r["probe"] == "flink_jobs"), key=lambda r: r["ts"])
        jt, jbad, prev_ids, prev_t = [], [], None, None
        for r in jobs:
            if prev_t is not None and r["ts"] - prev_t > 30.0:
                prev_ids = None  # a new stretch (TRAILING after the blocks): job IDs are compared inside a stretch only
            prev_t = r["ts"]
            ids_now, ok = None, False
            if r["ok_count"] == 1 and r["detail"]:
                try:
                    js = json.loads(r["detail"])
                except json.JSONDecodeError:
                    js = None
                if isinstance(js, list):
                    live = [j for j in js if isinstance(j, dict) and j.get("state") not in TERMINAL]
                    ok = sorted(j.get("name") for j in live) == self.expected_jobs and \
                        all(j.get("state") == "RUNNING" for j in live)
                    ids_now = sorted((j.get("name"), j.get("job_id")) for j in live)
            bad = not ok
            if ok and prev_ids is not None and ids_now != prev_ids and jbad:
                bad = True
                jbad[-1] = True
            jt.append(r["ts"])
            jbad.append(bad)
            prev_ids = ids_now if ok else prev_ids
        self.jobs_t, self.jobs_bad = np.asarray(jt, float), np.asarray(jbad, bool)
        tms = sorted((r for r in probe_rows if r["probe"] == "flink_tms"), key=lambda r: r["ts"])
        self.tms_t = np.asarray([r["ts"] for r in tms], float)
        self.tms_bad = np.asarray([not (r["ok_count"] == 1 and r["value"] == th_pre["taskmanagers_registered"])
                                   for r in tms], bool)
        # kube-state: the number of distinct pod UIDs per role at each step, and restart-counter changes
        expected = th_pre["expected_pods"]
        uid_at: dict[int, dict[str, set[str]]] = defaultdict(lambda: defaultdict(set))
        restarts: dict[str, list[tuple[int, float]]] = defaultdict(list)
        for r in kube_rows:
            role = role_of(r["pod"])
            if role not in expected:
                continue
            if r["metric"] == INFO_METRIC:
                uid_at[int(r["ts"])][role].add(r["pod_uid"] or f"name:{r['pod']}")
            elif r["metric"] == RESTARTS_METRIC:
                restarts[r["series_id"]].append((int(r["ts"]), float(r["value"])))
        steps = sorted(uid_at)
        self.kube_t = np.asarray(steps, float)
        self.kube_uids = [uid_at[t] for t in steps]
        changes = []
        for pts in restarts.values():
            pts.sort()
            changes += [t1 for (t0, v0), (t1, v1) in zip(pts, pts[1:]) if v1 != v0 and t1 - t0 <= 30]
        self.restart_t = np.asarray(sorted(changes), float)
        self.has_restarts = bool(restarts)
        # TCP per pair and packet probes per TaskManager, EXEC rows dropped
        self.pairs: dict[tuple, tuple[np.ndarray, np.ndarray]] = {}
        acc: dict[tuple, list[tuple[float, int]]] = defaultdict(list)
        bursts: dict[str, list[tuple[float, int, int]]] = defaultdict(list)
        for r in probe_rows:
            if exec_failed(r.get("error")):
                continue
            if r["probe"] == "tcp":
                acc[(r["src_pod"], r["dst"])].append((r["ts"], int(r["ok_count"] == r["tried_count"])))
            elif r["probe"] == "tcp_burst":
                bursts[r["src_pod"]].append((r["ts"], int(r["ok_count"]), int(r["tried_count"])))
        for k, v in acc.items():
            v.sort()
            self.pairs[k] = (np.asarray([x[0] for x in v], float), np.cumsum([0] + [x[1] for x in v]))
        self.bursts = {}
        for k, v in bursts.items():
            v.sort()
            self.bursts[k] = (np.asarray([x[0] for x in v], float), np.cumsum([0] + [x[1] for x in v]),
                              np.cumsum([0] + [x[2] for x in v]))
        brokers = sorted({p for p, _ in acc if broker_id(p or "") is not None})
        self.broker_pairs = [(s, f"{d}:{port}") for s in brokers for d in brokers if d != s for port in KAFKA_PORTS_BROKER]
        self.lg_pairs = sorted(k for k in acc if (k[1] or "").endswith(f":{KAFKA_PORT_PLAIN}") and broker_id(k[0] or "") is None)

    @staticmethod
    def _span(arr: np.ndarray, lo: float, hi: float) -> tuple[int, int]:
        return int(np.searchsorted(arr, lo, side="left")), int(np.searchsorted(arr, hi, side="left"))

    def failing(self, lo: float, hi: float) -> list[str]:
        """The conditions a window with evidence span [lo, hi) fails. No sample of a kind fails that condition."""
        bad = []
        a, b = self._span(self.jobs_t, lo, hi)
        if b <= a or self.jobs_bad[a:b].any():
            bad.append("jobs_running_same_id")
        a, b = self._span(self.tms_t, lo, hi)
        if b <= a or self.tms_bad[a:b].any():
            bad.append("taskmanagers_registered")
        a, b = self._span(self.kube_t, lo, hi)
        r0, r1 = self._span(self.restart_t, lo, hi)
        if b <= a or not self.has_restarts:
            bad.append("no_pod_change")
        else:
            seen: dict[str, set[str]] = defaultdict(set)
            for d in self.kube_uids[a:b]:
                for role, u in d.items():
                    seen[role] |= u
            if any(len(seen.get(role, set())) != n for role, n in self.th["expected_pods"].items()) or r1 > r0:
                bad.append("no_pod_change")
        if not self.lg_pairs or not self.broker_pairs:
            bad.append("tcp_ok")
        else:
            for k in self.broker_pairs + self.lg_pairs:
                t, cum = self.pairs.get(k, (np.empty(0), np.zeros(1)))
                i, j = self._span(t, lo, hi)
                if j <= i or (cum[j] - cum[i]) / (j - i) < self.th["tcp_ok_frac_min"]:
                    bad.append("tcp_ok")
                    break
        tm_seen = 0
        worst = 0.0
        for k, (t, ok, tried) in self.bursts.items():
            i, j = self._span(t, lo, hi)
            if j <= i:
                continue
            tm_seen += 1
            worst = max(worst, 1.0 - (ok[j] - ok[i]) / (tried[j] - tried[i]))
        if tm_seen < self.th["expected_pods"]["taskmanager"] or worst > self.th["packet_loss_frac_max"]:
            bad.append("packet_loss")
        return bad


def window_health(evidence: Evidence, starts_s: np.ndarray, ends_s: np.ndarray, margin_s: float = 10.0) -> list[list[str]]:
    return [evidence.failing(float(a) - margin_s, float(b) + margin_s) for a, b in zip(starts_s, ends_s)]


# ============================================================================================ 3. sham false passes
def false_pass_counts(manifest: dict, verifications: dict[str, dict]) -> dict[str, int]:
    """2.5 with decision 22: per fault, the number of F0 attempts on which its signature passed, over every F0 attempt
    that has a verification record (GATE_FAILED and ABORTED shams included)."""
    counts: dict[str, int] = {}
    for a in manifest["attempts"]:
        if a["run_type"] != "F0" or a["attempt_key"] not in verifications:
            continue
        for sig in verifications[a["attempt_key"]]["signatures_all_faults"]:
            if sig["pass"]:
                counts[sig["run_type"]] = counts.get(sig["run_type"], 0) + 1
    return counts


def flagged_faults(manifest: dict, verifications: dict[str, dict], min_false_passes: int = 2) -> set[str]:
    return {f for f, n in false_pass_counts(manifest, verifications).items() if n >= min_false_passes}
