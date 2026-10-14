"""Fault injectors, one class per run type (protocol 1.2, 2.4).

Rules for every injector (2.7.1 to 2.7.3):
  * Targets come from the live pod list by the 11.11 labels at apply time. No pod name is hard-coded.
  * The resource is recorded in the attempt BEFORE the create call, and `applied` is set before it, so a crash
    between the call and the manifest write can never hide a live fault.
  * Any error raises. Nothing returns quietly, nothing marks an injection done: success is decided only by gate.py.
  * Deletion is always confirmed: the resource is polled until NotFound and the time is recorded. A resource still
    present 300 s after the delete call raises; the recovery gate then blocks the next attempt.
"""
from __future__ import annotations

import json
import logging

import yaml

from . import chaos, ids
from .clock import ceil10
from .errors import HaltRequested, InjectionError, KubeError, OrchestratorError
from .exporter import fetch_rows
from .f6_injector import parse_log
from .gate import _gauge_rows_by_ts, f8_size_from_rows
from .kube import pod_ready, pod_summary
from .probe_service import QUORUM_CMD, parse_leader_id
from .timeutil import parse_rfc3339

log = logging.getLogger("orchestrator.injectors")
MIB = 1024 * 1024
POLL_S = 1.0
STUCK_AFTER_S = 300.0


def empty_facts(run_type: str) -> dict:
    return {"pseudo_targets": None, "targets_expected": [], "targets_selected": [], "controller": None,
            "chaos_snapshots": [], "pods_before": [], "pods_after": [],
            "f5": {"kills": [], "new_tm_uid_count": None, "all_jobs_running_at": None} if run_type == "F5" else None,
            "f6": None, "f8": {"per_pod": []} if run_type == "F8" else None, "recovery_gate_conditions": []}


def target_obj(pod: dict) -> dict:
    name = pod["metadata"]["name"]
    return {"pod": name, "uid": pod["metadata"]["uid"], "node": (pod.get("spec") or {}).get("nodeName") or "",
            "broker_id": ids.broker_id_from_pod(name)}


def selected_from(recs: list[dict], known: dict[str, dict]) -> list[dict]:
    """targets_selected (11.6): the pods Chaos Mesh's records name. A pod the orchestrator did not target is still
    listed (uid unknown), so a wrong selection is visible in the record and fails the mechanism check."""
    out = []
    for r in sorted(recs, key=lambda r: r["id"]):
        pod = r["id"].split("/", 1)[1]
        if pod in known:
            out.append(target_obj(known[pod]))
        else:
            out.append({"pod": pod, "uid": "", "node": "", "broker_id": ids.broker_id_from_pod(pod)})
    return out


class AttemptCtx:
    def __init__(self, camp, rec: dict, cell: dict, facts: dict) -> None:
        self.camp, self.rt, self.rec, self.cell, self.facts = camp, camp.rt, rec, cell, facts
        self.akey = rec["attempt_key"]
        self.first_seen: dict[str, dict] = {}
        self.entry = next(r for r in self.rt.plan["run_types"] if r["run_type"] == rec["run_type"])

    # ------------------------------------------------------------------------------------ persistence
    def save(self) -> None:
        self.camp.save()
        self.rt.manifest_store.save_facts(self.akey, self.facts)

    def now(self) -> float:
        return self.rt.clock.now()

    def sleep_until(self, t: float) -> None:
        self.rt.clock.sleep_until(t)

    # ------------------------------------------------------------------------------------ pods
    def pods(self, selector: str) -> list[dict]:
        return sorted(self.rt.kube.list_pods(selector), key=lambda p: p["metadata"]["name"])

    def ready_pods(self, selector: str) -> list[dict]:
        return [p for p in self.pods(selector) if pod_ready(p)]

    def pod_summaries(self) -> list[dict]:
        out = []
        for sel, container in ((ids.BROKER_SELECTOR, ids.BROKER_CONTAINER), (ids.JM_SELECTOR, ids.FLINK_CONTAINER),
                               (ids.TM_SELECTOR, ids.FLINK_CONTAINER)):
            out += [pod_summary(p, container) for p in self.pods(sel)]
        return out

    def read_controller(self) -> tuple[int, str, bool]:
        """kafka-metadata-quorum.sh describe --status in a broker, cross-checked with the active-controller metric."""
        leader = None
        errors = []
        for b in self.ready_pods(ids.BROKER_SELECTOR):
            res = self.rt.kube.exec(b["metadata"]["name"], ids.BROKER_CONTAINER, QUORUM_CMD, timeout_s=30)
            if res.rc == 0:
                leader = parse_leader_id(res.stdout)
                break
            errors.append(f"{b['metadata']['name']} rc={res.rc}")
        if leader is None:
            raise InjectionError(f"controller identity unreadable: {errors}")
        pod = f"{ids.BROKER_POD_PREFIX}{leader}"
        agrees = False
        try:
            res = self.rt.prom.instant(self.rt.thresholds["queries"]["active_controller_by_pod"])
            by_pod = {lab.get("pod"): v for lab, v in res}
            agrees = len(by_pod) == ids.EXPECTED_BROKERS and by_pod.get(pod) == 1 and sum(by_pod.values()) == 1
        except OrchestratorError as e:
            log.warning("active-controller metric unreadable: %s", e)
        return leader, pod, agrees

    def followers(self, leader: int) -> list[dict]:
        brokers = self.pods(ids.BROKER_SELECTOR)
        if len(brokers) != ids.EXPECTED_BROKERS or not all(pod_ready(b) for b in brokers):
            raise InjectionError("F1/F2 need three Ready brokers at apply time")
        fol = sorted((b for b in brokers if ids.broker_id_from_pod(b["metadata"]["name"]) != leader),
                     key=lambda b: ids.broker_id_from_pod(b["metadata"]["name"]))
        if len(fol) != 2:
            raise InjectionError(f"controller {leader} is not one of the brokers")
        return fol

    # ------------------------------------------------------------------------------------ resources
    def create(self, obj: dict) -> dict:
        kind, name = obj["kind"], obj["metadata"]["name"]
        res = {"kind": kind, "name": name, "created_at": round(self.now(), 3), "delete_called_at": None, "not_found_at": None}
        self.rec["applied"] = True
        self.rec["resources"].append(res)
        self.save()
        self.rt.kube.create(obj)
        return res

    def snapshot(self, tag: str, kind: str, name: str) -> dict | None:
        obj = self.rt.kube.get(kind, name)
        if obj is None:
            return None
        key = f"attempts/{self.akey}/chaos/{tag}-{name}.yaml"
        self.rt.uploader.put_blocking(key, yaml.safe_dump(obj, sort_keys=True).encode("utf-8"), "application/yaml")
        fs = self.first_seen.setdefault(name, {})
        self.facts["chaos_snapshots"].append(chaos.snapshot_entry(tag, round(self.now(), 3), obj, key, fs))
        self.save()
        return obj

    def wait_applied(self, kind: str, name: str, selector_sets: dict[str, set[str]], deadline: float) -> dict | None:
        while True:
            obj = self.rt.kube.get(kind, name)
            if obj is not None:
                recs = chaos.records(obj)
                if recs:
                    got: dict[str, set[str]] = {}
                    for r in recs:
                        got.setdefault(r["selector_key"], set()).add(r["id"])
                    if got == selector_sets and all(r["first_apply_at"] is not None for r in recs):
                        return obj
                    if any(r["failed_events"] for r in recs):
                        log.warning("%s/%s reports failed events", kind, name)
            if self.now() >= deadline:
                return None
            self.rt.clock.sleep(POLL_S)

    def wait_recovered(self, kind: str, name: str, deadline: float) -> dict | None:
        while True:
            obj = self.rt.kube.get(kind, name)
            if obj is None:
                return None
            if chaos.all_recovered(chaos.records(obj)) and chaos.conditions(obj).get("AllRecovered") == "True":
                return obj
            if self.now() >= deadline:
                return None
            self.rt.clock.sleep(POLL_S)

    def delete_confirmed(self, res: dict, snapshots: bool = True, stuck_after_s: float = STUCK_AFTER_S,
                         halting: bool = False) -> None:
        if res["not_found_at"] is not None:
            return
        kind, name = res["kind"], res["name"]
        is_chaos = kind in ("PodChaos", "NetworkChaos", "StressChaos")
        if snapshots and is_chaos:
            self.snapshot("BEFORE_DELETE", kind, name)
        if kind == "Job" and name.startswith("poison-"):
            # C25: the pod log goes with the Job, so it is stored before the delete on every path (halt, abort, failure)
            self.ensure_injector_log("deleted by the orchestrator", blocking=not halting)
        res["delete_called_at"] = round(self.now(), 3)
        self.save()
        self.rt.kube.delete(kind, name, cascade="foreground" if kind == "Job" else "background")
        took_after = False
        while True:
            obj = self.rt.kube.get(kind, name)
            if obj is None:
                res["not_found_at"] = round(self.now(), 3)
                self.save()
                return
            if snapshots and is_chaos and not took_after:
                self.snapshot("AFTER_DELETE", kind, name)
                took_after = True
            if self.now() - res["delete_called_at"] > stuck_after_s:
                raise KubeError(f"{kind}/{name} still present {stuck_after_s:.0f} s after delete (stuck finalizer?)")
            self.rt.clock.sleep(POLL_S)

    # ------------------------------------------------------------------------------------ F6 injector log
    def injector_log_key(self) -> str:
        return f"attempts/{self.akey}/injector.log"

    def has_injector_log(self) -> bool:
        key = self.injector_log_key()
        return any(o.key == key for o in self.rt.store.list(f"attempts/{self.akey}/"))

    def ensure_injector_log(self, reason: str, blocking: bool = True) -> bool:
        """REVIEW4 CAM-2 item 4 (C25): every F6 attempt that created its injector Job stores attempts/<key>/injector.log, the
        pod log if it can still be read, plus one orchestrator_observed line. On abort the observed line may stand alone.
        Returns True when the log is stored."""
        if self.rec["run_type"] != "F6" or not any(r["kind"] == "Job" for r in self.rec["resources"]):
            return False
        try:
            if self.has_injector_log():
                return True
        except OrchestratorError as e:
            log.warning("cannot list %s: %s", self.injector_log_key(), e)
        pod, logs = None, ""
        try:
            pods = self.rt.kube.list_pods(f"job-name={ids.k8s_name('poison', self.akey)}")
            pod = pods[0] if pods else None
            logs = self.rt.kube.logs(pod["metadata"]["name"]) if pod else ""
        except OrchestratorError as e:
            log.warning("injector pod log unreadable for %s: %s", self.akey, e)
        text = injector_log_text(logs, pod, reason, self.now())
        if blocking:
            self.rt.uploader.put_blocking(self.injector_log_key(), text.encode("utf-8"), "text/plain")
            return True
        return self.rt.uploader.put_deferred(self.injector_log_key(), text.encode("utf-8"), "text/plain")

    def cleanup_all(self, snapshots: bool = True) -> list[str]:
        """Delete every resource of this attempt that is not confirmed gone. Returns errors, never hides them.
        The halt path passes snapshots=False: deletion is still confirmed and timed, but no snapshot upload can
        hold it up during an S3 outage."""
        errors = []
        for res in self.rec["resources"]:
            if res["not_found_at"] is None:
                try:
                    # The halt path (snapshots=False) must finish inside the Job's 180 s termination grace period
                    # (rebuild/infra README section 5), so it waits at most 60 s per resource.
                    self.delete_confirmed(res, snapshots=snapshots and res["kind"] != "Job",
                                          stuck_after_s=STUCK_AFTER_S if snapshots else 60.0, halting=not snapshots)
                except OrchestratorError as e:
                    errors.append(f"{res['kind']}/{res['name']}: {e}")
        return errors

    def campaign_objects(self) -> list[tuple[str, dict]]:
        sel = f"{ids.LABEL_CAMPAIGN}={self.rt.env.campaign_id}"
        found = []
        for kind in ids.CHAOS_KINDS:
            found += [(kind, o) for o in self.rt.kube.list(kind, sel)]
        found += [("Job", j) for j in self.rt.kube.list("Job", sel) if j["metadata"]["name"] != ids.ORCHESTRATOR_JOB]
        return found


# ================================================================================================ injectors
class Injector:
    def __init__(self, ctx: AttemptCtx) -> None:
        self.ctx, self.rec, self.facts = ctx, ctx.rec, ctx.facts
        self.duration = ctx.entry["duration_s"]
        self.params = ctx.entry["params"]
        self.planned = ctx.rec["planned_onset_at"]
        self.apply_max = ctx.rt.plan["phase_s"]["apply_max"]

    def prepare_at(self) -> float | None:
        return None

    def prepare(self) -> None:
        return None

    def apply(self) -> None:
        raise NotImplementedError

    def hold(self) -> None:
        raise NotImplementedError

    def set_onset(self, t: float, source: str) -> None:
        if t < self.planned:
            raise InjectionError(f"verified onset {t} is before the planned onset {self.planned} (clock disagreement)")
        self.rec["onset_verified_at"] = round(t, 3)
        self.rec["onset_source"] = source
        self.ctx.save()

    def set_end(self, t: float, source: str) -> None:
        self.rec["labelled_end_at"] = round(t, 3)
        self.rec["end_source"] = source
        self.ctx.save()

    def controller_before(self, required: bool) -> tuple[int, str, bool] | None:
        """2.3.4 and REVIEW4 L6 (C37): the controller is read immediately before every apply, for every run type, so the
        admin requests of the read are in every run type's windows at onset. F0, F1 and F2 need it (follower rule,
        pseudo-targets), so a failed read fails the attempt; for F4 to F8 it is recorded only."""
        try:
            leader, pod, agrees = self.ctx.read_controller()
        except OrchestratorError as e:
            if required:
                raise
            log.warning("controller before apply unreadable: %s", e)
            self.facts["controller"] = {"node_id_before_apply": None, "node_id_after_injection": None,
                                        "method": "metadata-quorum", "metric_agrees": False}
            self.ctx.save()
            return None
        self.facts["controller"] = {"node_id_before_apply": leader, "node_id_after_injection": None,
                                    "method": "metadata-quorum", "metric_agrees": agrees}
        self.rec["controller_at_onset"] = {"node_id": leader, "pod": pod}
        self.ctx.save()
        return leader, pod, agrees

    def controller_after(self, tries: int = 3, pause_s: float = 10.0) -> None:
        """The same read after the labelled end, for every run type (2.3.4, C37). Retried, because for F1 and F2 a missing
        value fails the gate (C25)."""
        if self.facts.get("controller") is None:
            return
        for i in range(tries):
            try:
                leader, _, _ = self.ctx.read_controller()
                self.facts["controller"]["node_id_after_injection"] = leader
                break
            except OrchestratorError as e:
                log.warning("controller after injection unreadable (try %d of %d): %s", i + 1, tries, e)
                if i + 1 < tries:
                    self.ctx.rt.clock.sleep(pause_s)
        self.ctx.save()


class Sham(Injector):
    """F0: nothing is applied. Every fault's checks run against seeded pseudo-targets (2.5)."""

    def apply(self) -> None:
        ctx = self.ctx
        leader, pod, agrees = self.controller_before(required=True)
        fol = ctx.followers(leader)
        stats = ctx.camp.baseline_stats()
        self.facts["pseudo_targets"] = {
            "follower_pod": fol[ctx.cell["follower_rank"]]["metadata"]["name"],
            "tm_pods": [p["metadata"]["name"] for p in ctx.ready_pods(ids.TM_SELECTOR)],
            "tm_kill_ranks": list(ctx.cell["tm_kill_ranks"]),
            "f6_planned_count": stats["f6_planned_count"] if stats else None,
        }
        self.assert_nothing()
        self.set_onset(self.planned, "PLANNED")

    def assert_nothing(self) -> None:
        found = self.ctx.campaign_objects()
        if found:
            for kind, obj in found:
                if kind in ("PodChaos", "NetworkChaos", "StressChaos"):
                    self.ctx.snapshot("MID", kind, obj["metadata"]["name"])
            raise HaltRequested("SHAM_SHOWED_CAMPAIGN_RESOURCE", self.rec["attempt_key"])

    def hold(self) -> None:
        onset = self.rec["onset_verified_at"]
        end = onset + self.ctx.cell["sham_duration_s"]
        for t in (onset + 10, onset + (end - onset) / 2, end):
            self.ctx.sleep_until(t)
            self.assert_nothing()
        self.controller_after()
        self.set_end(end, "PLANNED")


class BrokerKill(Injector):
    """F1: PodChaos pod-kill, gracePeriod 0, on the follower at the seeded rank; the controller is never the target."""

    def apply(self) -> None:
        ctx = self.ctx
        leader, pod, agrees = self.controller_before(required=True)
        target = ctx.followers(leader)[ctx.cell["follower_rank"]]
        tname = target["metadata"]["name"]
        self.facts["targets_expected"] = [target_obj(target)]
        self.name = ids.k8s_name("pk", ctx.akey)
        self.rec["apply_called_at"] = round(ctx.now(), 3)
        self.res = ctx.create(chaos.pod_kill(self.name, ctx.rt.env.campaign_id, ctx.akey, "F1", tname))
        obj = ctx.wait_applied("PodChaos", self.name, {chaos.SELECTOR_MAIN: {f"{ids.NAMESPACE}/{tname}"}},
                               self.planned + self.apply_max)
        if obj is None:
            raise InjectionError(f"F1: no Apply event for {tname} within {self.apply_max} s")
        recs = chaos.records(obj)
        self.facts["targets_selected"] = selected_from(recs, {tname: target})
        self.set_onset(max(r["first_apply_at"] for r in recs), "CHAOS_APPLY_EVENT")

    def hold(self) -> None:
        ctx = self.ctx
        onset = self.rec["onset_verified_at"]
        ctx.sleep_until(onset + 10)
        ctx.snapshot("APPLY_PLUS_10S", "PodChaos", self.name)
        # pod-kill is one-shot: Chaos Mesh keeps it in Run, records Injected, until the object is deleted, so there is no
        # AllRecovered to wait for (REVIEW4 L2). The confirmed NotFound is the recovery proof (C33).
        ctx.delete_confirmed(self.res)
        end = onset + 300  # FIXED_SPAN (section 10 decision 9, protocol default)
        ctx.sleep_until(end)
        self.controller_after()
        self.set_end(end, "FIXED_SPAN")


class DurationChaos(Injector):
    """Faults whose labelled end is the latest per-target Recover event: F2, F4, F7, F8."""

    kind = ""
    resources: list[dict]

    def selector_sets(self, res_name: str) -> dict[str, set[str]]:
        raise NotImplementedError

    def wait_all_applied(self) -> list[dict]:
        objs = []
        for res in self.resources:
            obj = self.ctx.wait_applied(self.kind, res["name"], self.selector_sets(res["name"]), self.planned + self.apply_max)
            if obj is None:
                raise InjectionError(f"{self.rec['run_type']}: {res['name']} not applied on every target within "
                                     f"{self.apply_max} s")
            objs.append(obj)
        return objs

    def hold(self) -> None:
        ctx = self.ctx
        onset = self.rec["onset_verified_at"]
        ctx.sleep_until(onset + 10)
        for res in self.resources:
            ctx.snapshot("APPLY_PLUS_10S", self.kind, res["name"])
        ctx.sleep_until(onset + self.duration / 2)
        for res in self.resources:
            ctx.snapshot("MID", self.kind, res["name"])
        deadline = onset + self.duration + ctx.rt.thresholds["gate"]["timing"]["overrun_fail_s"]
        ctx.sleep_until(onset + self.duration)
        ends = []
        for res in self.resources:
            obj = ctx.wait_recovered(self.kind, res["name"], deadline)
            if obj is None:
                self.rec["aborted_at"] = round(ctx.now(), 3)
                ctx.save()
                raise InjectionError(f"{self.rec['run_type']}: {res['name']} not recovered {deadline - onset - self.duration:.0f} s "
                                     f"after its planned end")
            ends.append(max(r["last_recover_at"] for r in chaos.records(obj)))
        end = max(ends)
        for res in self.resources:
            ctx.delete_confirmed(res)
        self.rec["overrun_s"] = round(end - onset - self.duration, 3)
        self.controller_after()
        self.set_end(end, "CHAOS_RECOVER_EVENT")


class Partition(DurationChaos):
    """F2: NetworkChaos partition, both directions, the seeded follower against the other two brokers only."""
    kind = "NetworkChaos"

    def apply(self) -> None:
        ctx = self.ctx
        leader, pod, agrees = self.controller_before(required=True)
        fol = ctx.followers(leader)
        target = fol[ctx.cell["follower_rank"]]
        self.follower = target["metadata"]["name"]
        self.others = sorted(b["metadata"]["name"] for b in ctx.pods(ids.BROKER_SELECTOR) if b["metadata"]["name"] != self.follower)
        self.facts["targets_expected"] = [target_obj(target)]
        name = ids.k8s_name("nc", ctx.akey)
        self.rec["apply_called_at"] = round(ctx.now(), 3)
        self.resources = [ctx.create(chaos.partition(name, ctx.rt.env.campaign_id, ctx.akey, self.follower, self.others,
                                                     self.duration))]
        objs = self.wait_all_applied()
        recs = chaos.records(objs[0])
        self.facts["targets_selected"] = selected_from([r for r in recs if r["selector_key"] == chaos.SELECTOR_MAIN],
                                                       {self.follower: target})
        self.set_onset(max(r["first_apply_at"] for r in recs), "CHAOS_APPLY_EVENT")

    def selector_sets(self, res_name: str) -> dict[str, set[str]]:
        return {chaos.SELECTOR_MAIN: {f"{ids.NAMESPACE}/{self.follower}"},
                ".Target": {f"{ids.NAMESPACE}/{o}" for o in self.others}}


class TmChaos(DurationChaos):
    """F4 (StressChaos CPU) and F7 (NetworkChaos loss): one resource on all three Ready TaskManagers."""

    def tm_targets(self) -> list[dict]:
        tms = self.ctx.pods(ids.TM_SELECTOR)
        ready = [p for p in tms if pod_ready(p)]
        if len(tms) != ids.EXPECTED_TASKMANAGERS or len(ready) != ids.EXPECTED_TASKMANAGERS:
            raise InjectionError(f"{self.rec['run_type']} needs three Ready TaskManagers at apply, found {len(ready)}/{len(tms)}")
        return ready

    def selector_sets(self, res_name: str) -> dict[str, set[str]]:
        return {chaos.SELECTOR_MAIN: {f"{ids.NAMESPACE}/{p}" for p in self.tm_names}}

    def build(self, name: str) -> dict:
        raise NotImplementedError

    def apply(self) -> None:
        ctx = self.ctx
        tms = self.tm_targets()
        self.tm_names = [p["metadata"]["name"] for p in tms]
        self.facts["targets_expected"] = [target_obj(p) for p in tms]
        self.controller_before(required=False)
        name = ids.k8s_name("sc" if self.kind == "StressChaos" else "nc", ctx.akey)
        self.rec["apply_called_at"] = round(ctx.now(), 3)
        self.resources = [ctx.create(self.build(name))]
        objs = self.wait_all_applied()
        recs = chaos.records(objs[0])
        self.facts["targets_selected"] = selected_from(recs, {p["metadata"]["name"]: p for p in tms})
        self.set_onset(max(r["first_apply_at"] for r in recs), "CHAOS_APPLY_EVENT")


class CpuStress(TmChaos):
    kind = "StressChaos"

    def build(self, name: str) -> dict:
        return chaos.cpu_stress(name, self.ctx.rt.env.campaign_id, self.ctx.akey, self.tm_names, self.params["workers"],
                                self.params["load_percent"], self.duration)


class PacketLoss(TmChaos):
    kind = "NetworkChaos"

    def build(self, name: str) -> dict:
        return chaos.loss(name, self.ctx.rt.env.campaign_id, self.ctx.akey, self.tm_names, self.params["loss_percent"],
                          self.duration)


class MemoryPressure(TmChaos):
    """F8: one StressChaos memory resource per TaskManager, sized per pod from its live working set and limit."""
    kind = "StressChaos"

    def selector_sets(self, res_name: str) -> dict[str, set[str]]:
        return {chaos.SELECTOR_MAIN: {f"{ids.NAMESPACE}/{self.by_name[res_name]}"}}

    def sizing_rows(self) -> list[dict]:
        rt = self.ctx.rt
        lo = ceil10(self.planned - rt.thresholds["gate"]["F8"]["sizing_window_s"])
        hi = ceil10(self.planned) - 10
        rows = []
        for source, metric in (("cadvisor", "container_memory_working_set_bytes"),
                               ("kube_state", "kube_pod_container_resource_limits")):
            rows += fetch_rows(rt.prom, rt.cfg, source, metric, lo, hi, {}, _NoUids())
        return rows

    def apply(self) -> None:
        ctx = self.ctx
        th8 = ctx.rt.thresholds["gate"]["F8"]
        tms = self.tm_targets()
        self.tm_names = [p["metadata"]["name"] for p in tms]
        self.facts["targets_expected"] = [target_obj(p) for p in tms]
        rows = self.sizing_rows()
        per_pod, sizes = [], {}
        w = (float("-inf"), float("inf"))
        for tm in self.tm_names:
            lim, ws, size_mib = f8_size_from_rows(rows, tm, self.planned, th8)
            # REVIEW4 L5 (C36): the samples this size came from, so the gate recomputes it from them and not from an
            # export that a late cAdvisor sample may have changed
            ws_s = _gauge_rows_by_ts(rows, "container_memory_working_set_bytes", tm, w,
                                     lambda r: r["container"] == th8["container"])
            lim_s = _gauge_rows_by_ts(rows, "kube_pod_container_resource_limits", tm, w,
                                      lambda r: r["container"] == th8["container"] and '"resource":"memory"' in r["labels"])
            per_pod.append({"pod": tm, "limit_bytes": None if lim is None else int(lim),
                            "ws_pre_bytes": None if ws is None else int(ws),
                            "stressor_bytes": None if size_mib is None else int(size_mib) * MIB,
                            "ws_during_bytes": None, "increase_bytes": None,
                            "ws_samples": [{"ts": int(t), "value": float(v)} for t, v in sorted(ws_s.items())],
                            "limit_samples": [{"ts": int(t), "value": float(v)} for t, v in sorted(lim_s.items())],
                            "export_stressor_bytes": None})
            sizes[tm] = size_mib
        self.facts["f8"] = {"per_pod": per_pod}
        ctx.save()
        bad = [tm for tm, s in sizes.items() if s is None or s < th8["min_stressor_mib"]]
        if bad:
            raise InjectionError(f"F8: stressor under {th8['min_stressor_mib']} MiB or unsizeable for {bad}; nothing applied")
        self.controller_before(required=False)
        self.rec["apply_called_at"] = round(ctx.now(), 3)
        self.resources, self.by_name = [], {}
        for n, tm in enumerate(self.tm_names, start=1):
            name = ids.k8s_name("sc", ctx.akey, n)
            self.by_name[name] = tm
            self.resources.append(ctx.create(chaos.memory_stress(name, ctx.rt.env.campaign_id, ctx.akey, tm, sizes[tm],
                                                                 self.duration)))
        objs = self.wait_all_applied()
        firsts, all_recs = [], []
        for obj in objs:
            recs = chaos.records(obj)
            firsts.append(max(r["first_apply_at"] for r in recs))
            all_recs += recs
        self.facts["targets_selected"] = selected_from(all_recs, {p["metadata"]["name"]: p for p in tms})
        self.set_onset(max(firsts), "CHAOS_APPLY_EVENT")


class _NoUids:
    def get(self, pod, ts):
        return None


class TmLoss(Injector):
    """F5: five PodChaos pod-kills, gracePeriod 0, one Ready TaskManager each at onset + 0, 60, 120, 180, 240 s,
    chosen by the seeded ranks among the Ready TaskManagers sorted by name at the kill time, modulo their count."""

    def kill(self, k: int, planned_at: float) -> dict:
        ctx = self.ctx
        ctx.sleep_until(planned_at)
        ready = ctx.ready_pods(ids.TM_SELECTOR)
        if not ready:
            raise InjectionError(f"F5 kill {k}: no Ready TaskManager")
        target = ready[ctx.cell["tm_kill_ranks"][k - 1] % len(ready)]
        tname = target["metadata"]["name"]
        name = ids.k8s_name("pk", ctx.akey, k)
        kill = {"k": k, "planned_at": round(planned_at, 3), "pod": tname, "uid": target["metadata"]["uid"], "apply_at": None}
        self.facts["f5"]["kills"].append(kill)
        self.facts["targets_expected"].append(target_obj(target))
        if k == 1:
            self.controller_before(required=False)
            self.rec["apply_called_at"] = round(ctx.now(), 3)
        res = ctx.create(chaos.pod_kill(name, ctx.rt.env.campaign_id, ctx.akey, "F5", tname))
        obj = ctx.wait_applied("PodChaos", name, {chaos.SELECTOR_MAIN: {f"{ids.NAMESPACE}/{tname}"}},
                               planned_at + self.ctx.rt.thresholds["gate"]["F5"]["kill_apply_max_s"])
        if obj is None:
            raise InjectionError(f"F5 kill {k}: no Apply event for {tname} in time")
        recs = chaos.records(obj)
        kill["apply_at"] = max(r["first_apply_at"] for r in recs)
        self.facts["targets_selected"] += selected_from(recs, {tname: target})
        ctx.save()
        self.pending = (res, name, kill["apply_at"])
        return kill

    def settle_kill(self) -> None:
        res, name, apply_at = self.pending
        ctx = self.ctx
        ctx.sleep_until(apply_at + 10)
        ctx.snapshot("APPLY_PLUS_10S", "PodChaos", name)
        ctx.delete_confirmed(res)  # one-shot: no AllRecovered while the object exists (REVIEW4 L2, C33)

    def apply(self) -> None:
        kill = self.kill(1, self.planned)
        self.set_onset(kill["apply_at"], "CHAOS_APPLY_EVENT")

    def hold(self) -> None:
        onset = self.rec["onset_verified_at"]
        interval = self.params["interval_s"]
        self.settle_kill()
        for k in range(2, self.params["kills"] + 1):
            self.kill(k, onset + interval * (k - 1))
            self.settle_kill()
        end = onset + 300
        self.ctx.sleep_until(end)
        self.controller_after()
        self.set_end(end, "FIXED_SPAN")


def injector_log_text(logs: str, pod: dict | None, reason: str, now: float) -> str:
    """The stored injector.log (11.7, C6): the pod log, then one orchestrator_observed line with the container's exit code."""
    exit_code, term_reason = None, None
    for cs in ((pod or {}).get("status") or {}).get("containerStatuses") or []:
        term = (cs.get("state") or {}).get("terminated")
        if term:
            exit_code, term_reason = term.get("exitCode"), term.get("reason")
    observed = json.dumps({"event": "orchestrator_observed", "exit_code": exit_code, "reason": term_reason or reason,
                           "pod": pod["metadata"]["name"] if pod else None, "at": round(now, 3)}, sort_keys=True)
    body = logs.rstrip("\n")
    return (body + "\n" if body else "") + observed + "\n"


class PoisonPill(Injector):
    """F6: the prebuilt injector Job starts before onset, sends exactly the planned count over 180 s, and logs its
    first and last produce acks, which are the labelled span (2.4)."""

    LEAD_S = 90  # created 90 s before the planned onset so the pod is Ready at least 30 s before it

    def prepare_at(self) -> float | None:
        return self.planned - self.LEAD_S

    def prepare(self) -> None:
        ctx = self.ctx
        stats = ctx.camp.baseline_stats()
        if stats is None:
            raise InjectionError("F6 needs the baseline statistics (f6_planned_count)")
        image = ctx.rt.env.f6_injector_image or "f6-injector:dry-run"
        digest = image.split("@", 1)[1] if "@" in image else ""
        planned_count = int(stats["f6_planned_count"])
        self.facts["f6"] = {"injector_image_digest": digest, "pod_ready_at": None, "rate_per_s": float(stats["f6_rate_per_s"]),
                            "planned_count": planned_count, "sent_count": None, "acked_count": None,
                            "first_produce_at": None, "last_produce_at": None, "consumer_count": None}
        self.job_name = ids.k8s_name("poison", ctx.akey)
        labels = ids.campaign_labels(ctx.rt.env.campaign_id, ctx.akey, "F6")
        rate = float(stats["f6_rate_per_s"])
        # The injector environment of rebuild/infra/NAMES.md section 4 (f6_injector.py, the one implementation, C6).
        # planned = round(INJECT_RATE * INJECT_DURATION), which is f6_planned_count of 11.12 by construction. ATTEMPT_KEY
        # is the value of the rebuild-attempt header on every record (11.11).
        env = {"KAFKA_BOOTSTRAP": ctx.rt.env.kafka_bootstrap, "KAFKA_TOPIC": "raw-events", "INJECT_RATE": repr(rate),
               "INJECT_DURATION": str(self.duration), "INJECT_SEED": str(ctx.rt.plan["f6_payload_seed"]),
               "START_AT_UNIX": f"{self.planned:.3f}", "LATE_TOLERANCE_SEC": str(ctx.rt.thresholds["gate"]["F6"]["apply_max_s"]),
               "MIN_DELIVERED_FRACTION": str(ctx.rt.thresholds["gate"]["F6"]["acked_frac_min"]), "ATTEMPT_KEY": ctx.akey}
        if int(round(rate * self.duration)) != planned_count:
            raise InjectionError("INJECT_RATE x INJECT_DURATION does not round to f6_planned_count")
        job = {"apiVersion": "batch/v1", "kind": "Job",
               "metadata": {"name": self.job_name, "namespace": ids.NAMESPACE, "labels": labels},
               "spec": {"backoffLimit": 0, "activeDeadlineSeconds": self.LEAD_S + self.duration + 300,
                        "template": {"metadata": {"labels": labels}, "spec": {
                            "restartPolicy": "Never", "automountServiceAccountToken": False,
                            "nodeSelector": {"workload": "monitoring"},
                            "tolerations": [{"key": "workload", "operator": "Exists", "effect": "NoSchedule"}],
                            "securityContext": {"runAsNonRoot": True, "runAsUser": 10001, "runAsGroup": 10001},
                            "containers": [{"name": "injector", "image": image,
                                            "env": [{"name": k, "value": v} for k, v in env.items()],
                                            "resources": {"requests": {"cpu": "250m", "memory": "256Mi"},
                                                          "limits": {"memory": "512Mi"}},
                                            "securityContext": {"allowPrivilegeEscalation": False,
                                                                "readOnlyRootFilesystem": True,
                                                                "capabilities": {"drop": ["ALL"]}},
                                            "volumeMounts": [{"name": "tmp", "mountPath": "/tmp"}]}],
                            "volumes": [{"name": "tmp", "emptyDir": {"sizeLimit": "64Mi"}}]}}}}
        self.rec["apply_called_at"] = round(ctx.now(), 3)
        self.res = ctx.create(job)
        # Wait for the pod to be Ready, at the latest 30 s before the planned onset; the gate checks the lead.
        while ctx.now() < self.planned:
            pod = self.pod()
            if pod is not None and pod_ready(pod):
                self.facts["f6"]["pod_ready_at"] = self.ready_time(pod)
                ctx.save()
                return
            ctx.rt.clock.sleep(1)

    def pod(self) -> dict | None:
        pods = self.ctx.rt.kube.list_pods(f"job-name={self.job_name}")
        return pods[0] if pods else None

    def ready_time(self, pod: dict) -> float:
        for c in (pod.get("status") or {}).get("conditions") or []:
            if c.get("type") == "Ready" and c.get("status") == "True" and c.get("lastTransitionTime"):
                return parse_rfc3339(c["lastTransitionTime"])
        return round(self.ctx.now(), 3)

    def apply(self) -> None:
        ctx = self.ctx
        if not hasattr(self, "res"):
            raise InjectionError("F6 injector was not created")
        self.controller_before(required=False)  # the injector produces on its own from START_AT; this read delays nothing
        deadline = self.planned + self.duration + 120
        pod = None
        while True:
            job = ctx.rt.kube.get("Job", self.job_name)
            st = (job or {}).get("status") or {}
            if job is None:
                raise InjectionError("F6 injector Job disappeared")
            if st.get("succeeded", 0) >= 1 or st.get("failed", 0) >= 1:
                break
            if ctx.now() > deadline:
                raise InjectionError("F6 injector did not finish in time")
            ctx.rt.clock.sleep(5)
        pod = self.pod()
        if pod is not None and self.facts["f6"]["pod_ready_at"] is None:
            for c in (pod.get("status") or {}).get("conditions") or []:
                if c.get("type") == "Ready" and c.get("lastTransitionTime") and c.get("status") == "True":
                    self.facts["f6"]["pod_ready_at"] = parse_rfc3339(c["lastTransitionTime"])
        logs = ctx.rt.kube.logs(pod["metadata"]["name"]) if pod else ""
        text = injector_log_text(logs, pod, "finished", ctx.now())
        exit_code = (parse_log(text).get("orchestrator") or {}).get("exit_code")
        ctx.rt.uploader.put_blocking(ctx.injector_log_key(), text.encode("utf-8"), "text/plain")
        summ = parse_log(text).get("summary") or {}
        for k in ("sent_count", "acked_count", "first_produce_at", "last_produce_at"):
            self.facts["f6"][k] = summ.get(k)
        ctx.save()
        if summ.get("first_produce_at") is None or summ.get("last_produce_at") is None:
            raise InjectionError(f"F6 injector reported no produce acks (exit={exit_code})")
        self.set_onset(summ["first_produce_at"], "FIRST_PRODUCE_ACK")
        self.end = summ["last_produce_at"]

    def hold(self) -> None:
        self.ctx.delete_confirmed(self.res, snapshots=False)
        self.controller_after()
        self.set_end(self.end, "LAST_PRODUCE_ACK")


INJECTORS = {"F0": Sham, "F1": BrokerKill, "F2": Partition, "F4": CpuStress, "F5": TmLoss, "F6": PoisonPill,
             "F7": PacketLoss, "F8": MemoryPressure}
