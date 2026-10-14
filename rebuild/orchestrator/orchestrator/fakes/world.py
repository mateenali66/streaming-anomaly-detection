"""A deterministic simulation of one campaign cluster: three Strimzi brokers, a Flink session cluster with three
TaskManagers and three jobs, the load generator, the verification consumer, Chaos Mesh and the F6 injector.

Every observable is a pure function of simulated time and of the recorded history (pod lifetimes, chaos resources,
job state changes), so Prometheus range queries and probe samples for any past instant are reproducible.

Fault behaviour is set per run type through `knobs`, which the tests use to make an effect realistic, absent,
partial, late or applied to the wrong target, to make a resource stick on deletion, and so on.
"""
from __future__ import annotations

import heapq
import itertools
import math
import uuid
import zlib
from dataclasses import dataclass, field

from .. import ids
from ..timeutil import to_rfc3339

GIB = 1024 ** 3
MIB = 1024 ** 2
# What /jobs/<id>/config and /jobs/<id>/checkpoints/config answer for a job submitted by run-job.sh (REVIEW4 L1): the settings
# of rebuild/infra/images/flink-jobs/run-job.sh. A job submitted with plain `flink run` would get the distribution defaults.
RUN_JOB_SETTINGS = {"parallelism": 4, "checkpoint_mode": "exactly_once", "checkpoint_interval_ms": 60000,
                    "checkpoint_timeout_ms": 120000, "checkpoint_min_pause_ms": 30000, "externalization_enabled": True,
                    "delete_on_cancellation": False}
DISTRIBUTION_DEFAULT_SETTINGS = {"parallelism": 1, "checkpointing": False}
DEFAULT_KNOBS = {
    "apply_delay_s": 2.0,      # create -> Apply event
    "effect": "full",          # full | none | partial | weak
    "effect_delay_s": 0.0,     # Apply event -> effect starts (a late effect)
    "wrong_target": False,     # Chaos Mesh records (and acts on) another pod
    "stuck": False,            # finalizer never released: the resource stays after delete
    "no_recover": False,       # the resource never reports Recover (overrun)
    "recover_delay_s": 0.5,
    "fail_first_n": 0,         # the first n resources of this run type have no effect at all
    "missing_record": False,   # Chaos Mesh records omit the last target (a fault on 2 of 3 pods)
    "apply_only_first_n": None,  # only the first n resources of this run type ever get an Apply event
    "oom_at_frac": None,       # F8: the kernel kills the stressor (not the container) at this fraction of the duration
}


@dataclass
class FakePod:
    name: str
    role: str
    labels: dict
    created_at: float
    ready_at: float | None
    node: str
    ip: str
    uid: str = field(default_factory=lambda: "")
    deleted_at: float | None = None
    restart_count: int = 0
    last_reason: str | None = None
    container: str = "main"
    exit_code: int | None = None
    terminated_at: float | None = None
    log: str = ""
    not_ready: list = field(default_factory=list)  # [(start, end)] readiness dips

    def alive(self, t: float) -> bool:
        return self.created_at <= t and (self.deleted_at is None or t < self.deleted_at)

    def ready(self, t: float) -> bool:
        if not self.alive(t) or self.ready_at is None or t < self.ready_at:
            return False
        if self.terminated_at is not None and t >= self.terminated_at:
            return False
        return not any(a <= t < b for a, b in self.not_ready)


@dataclass
class FakeChaos:
    kind: str
    name: str
    labels: dict
    spec: dict
    created_at: float
    run_type: str
    targets: dict  # selector key -> [pod names]
    apply_at: dict = field(default_factory=dict)
    recover_at: dict = field(default_factory=dict)
    effect: dict = field(default_factory=dict)  # pod -> (start, end or None)
    delete_at: float | None = None
    gone_at: float | None = None
    duration: float | None = None
    one_shot: bool = False


@dataclass
class FakeJobRun:
    name: str
    jid: str
    submitted_at: float
    states: list  # [(t, state)]
    restored_from: str | None = None
    removed_at: float | None = None
    settings: dict = field(default_factory=dict)  # what /jobs/<id>/config and /checkpoints/config answer

    def state(self, t: float) -> str | None:
        if t < self.submitted_at or (self.removed_at is not None and t >= self.removed_at):
            return None
        s = None
        for ts, st in self.states:
            if ts <= t:
                s = st
        return s


class FakeWorld:
    def __init__(self, clock, workload: str, campaign_id: str, start: float | None = None) -> None:
        self.clock = clock
        clock.world = self
        self.workload = workload
        self.campaign_id = campaign_id
        self.t0 = clock.t if start is None else start
        self.dead = False
        self.seq = itertools.count(1)
        self.events: list = []
        self.pods: list[FakePod] = []
        self.chaos: list[FakeChaos] = []
        self.jobs_k8s: dict[str, dict] = {}
        self.flink_jobs: list[FakeJobRun] = []
        self.knobs: dict[str, dict] = {}
        self.global_knobs = {"controller": 2, "tm_scrape_drop_under_loss": True, "checkpoint_scheme": "s3",
                             "f5_restore_fails": False, "export_fail_until": None, "probes_down": [],
                             "consumer_restart_at": None, "node_replaced_at": None, "broker_ready_delay_s": 45.0,
                             "f2_breaks_clients": False, "ws_base_bytes": 1.5 * GIB, "f6_ready": True,
                             "f6_ack_delay_s": 0.02, "f6_send_frac": 1.0, "f6_exit_code": 0, "loadgen_mismatch": 0.0,
                             "imbalance_after_broker_restart": False, "submit_fails_until_redeploy": False,
                             "extra_series": [], "prom_clock_offset": 0.0, "drop_series": [],
                             # scenario knobs of rebuild/integration/dump_dry_run.py (end-to-end runs)
                             "f5_restore_fails_until_redeploy": False, "fail_job_after_recover_once": None,
                             "telemetry_gap_once": None, "checkpoint_campaign_id": None,
                             # REVIEW4 realism (orchestrator README section 6): how long a job stays RESTARTING after a
                             # TaskManager loss; cAdvisor samples keep their own timestamps and arrive late (None: a
                             # deterministic delay of 0 to 11 s per pod and step; a number: that delay for every sample)
                             "job_restart_s": 30.0, "cadvisor_arrival_delay_s": None,
                             # slotmanager.number-of-slots.min of infra's session config (12: three TaskManagers stay up
                             # with no job)
                             "slots_min": 12,
                             # kubectl exec failures of the probe service in these [start, end) intervals (REVIEW4 L7)
                             "exec_fail": [],
                             # 50% packet-probe loss on every TaskManager in these intervals, from no campaign fault: a real
                             # disturbance during a sham makes its F7 signature pass (REVIEW4 CAM-1)
                             "background_loss": [],
                             # scenario knobs that react to the orchestrator's own attempts (FakeWorld.on_attempt)
                             "sham_disturbance": [],          # F0 attempt ordinals (1, 2, ...) that get background loss
                             "prefault_job_restart_once": None,  # run type whose first attempt sees a job restart in PRE_FAULT
                             "job_restart_at_offset_s": [],   # every job restarts at world.t0 plus these offsets
                             # AWS Health events, as EventBridge events published at world.t0 plus offset (C30)
                             "health_events": [], "health_poll_fail_until": None,
                             # kafka-metadata-quorum.sh fails in every broker in these intervals; the next n Flink job
                             # listings fail (a REST hiccup)
                             "quorum_fail": [], "flink_snapshot_fail_next": 0,
                             # once-only scenario knobs for the CAM-2 end-to-end runs (run type whose first attempt is hit):
                             # the controller read after the labelled end fails, the job list right after the recovery
                             # gate fails twice, the fault recovers 70 s early
                             "quorum_fail_after_end_once": None, "flink_snapshot_fail_after_gate_once": None,
                             "early_recover_once": None}
        self.tm_counter = itertools.count(1)
        self.tm_pending = 0
        self.jm_pending = False
        self.base_rate = 1000.0 if workload == "ecommerce" else 11000.0
        self.f6_runs: list[dict] = []
        self.sentinels: list[float] = []
        self.imbalance_until: float = -math.inf
        self.leader_elections: list[float] = []
        self.alerts: list = []
        self.redeploys: list[float] = []
        self.created_by_type: dict[str, int] = {}
        self.results_bucket = "dry-run-results"  # the harness's RESULTS_BUCKET, so checkpoint paths carry it like Flink's
        self.seen_attempts: set[str] = set()
        self.kill_log: list[tuple[float, str, str]] = []
        self.f6_connects: list[tuple[str, float]] = []
        self.sham_count = 0
        self.disturbances: list[dict] = []     # ground truth of every disturbance the world created (dump_dry_run.py)
        from .series import SeriesModel
        self.series = SeriesModel(self)
        self.cache = self.series  # .clear() drops the cached steps whenever the world changes
        self.snap_cache: dict = {}
        self.exporter_hook = None
        self.nodes = [{"name": f"ip-10-0-0-{i}.ec2.internal", "iid": f"i-0{i:016x}"} for i in range(1, 7)]
        self._bootstrap()

    # ---------------------------------------------------------------------------------------------- setup
    def _uid(self) -> str:
        return str(uuid.UUID(int=next(self.seq) * 7919 + 10 ** 12, version=4))

    def _ip(self) -> str:
        n = next(self.seq)
        return f"10.0.{(n // 250) % 250}.{n % 250 + 2}"

    def add_pod(self, name: str, role: str, labels: dict, t: float, ready_delay: float | None, container: str,
                node: str | None = None) -> FakePod:
        p = FakePod(name=name, role=role, labels=labels, created_at=t, ready_at=None if ready_delay is None else t + ready_delay,
                    node=node or self.nodes[len(self.pods) % 6]["name"], ip=self._ip(), uid=self._uid(), container=container)
        self.pods.append(p)
        self.cache.clear()
        self.snap_cache.clear()
        return p

    def _bootstrap(self) -> None:
        t = self.t0 - 7200
        for i in range(3):
            self.add_pod(f"{ids.BROKER_POD_PREFIX}{i}", "broker",
                         {"strimzi.io/cluster": ids.KAFKA_CLUSTER, "strimzi.io/pool-name": ids.KAFKA_POOL}, t, 30, "kafka",
                         node=self.nodes[i]["name"])
        self.add_jm(t)
        for _ in range(3):
            self.add_tm(t)
        self.add_pod("load-generator-7c9d5-x2k4p", "loadgen", {"app": "load-generator"}, t, 5, "load-generator",
                     node=self.nodes[5]["name"])
        self.add_pod("verification-consumer-6b8f-q9z", "consumer", {"app": "verification-consumer"}, t, 5, "consumer",
                     node=self.nodes[5]["name"])
        for name, _, _ in ids.FLINK_JOBS[self.workload]:
            self.submit_job(name, t + 120, None)

    def add_jm(self, t: float) -> FakePod:
        return self.add_pod(f"streaming-flink-{uuid.UUID(int=next(self.seq)).hex[:9]}-{next(self.seq) % 99999:05d}",
                            "jobmanager", {"app": "streaming-flink", "component": "jobmanager"}, t, 25,
                            ids.FLINK_CONTAINER, node=self.nodes[3]["name"])

    def add_tm(self, t: float, ready_delay: float = 20.0) -> FakePod:
        n = next(self.tm_counter)
        return self.add_pod(f"streaming-flink-taskmanager-1-{n}", "taskmanager",
                            {"app": "streaming-flink", "component": "taskmanager"}, t, ready_delay, ids.FLINK_CONTAINER,
                            node=self.nodes[3 + n % 2]["name"])

    def submit_job(self, name: str, t: float, restored_from: str | None, settings: dict | None = None) -> FakeJobRun:
        states = [(t, "CREATED"), (t + 10, "RUNNING")]
        if getattr(self, "global_knobs", {}).get("submit_fails_until_redeploy") and not self.redeploys:
            states = [(t, "CREATED"), (t + 10, "FAILED")]
        j = FakeJobRun(name=name, jid=uuid.UUID(int=next(self.seq) * 104729).hex, submitted_at=t,
                       states=states, restored_from=restored_from, settings=dict(settings or RUN_JOB_SETTINGS))
        self.flink_jobs.append(j)
        self.ensure_slots(t)
        return j

    def ensure_slots(self, t: float) -> None:
        """A native session cluster starts TaskManagers for the slots jobs request (each job its parallelism, 4 slots per
        TaskManager) and keeps at least slotmanager.number-of-slots.min slots (REVIEW4 L1, L4). With slots_min 0 a cluster
        with no job starts no TaskManager. Pods already starting count."""
        if not hasattr(self, "tm_counter"):
            return  # bootstrap: the three TaskManagers are added explicitly
        if not any(p.role == "jobmanager" and (p.deleted_at is None or p.deleted_at > t) for p in self.pods) \
                and not self.jm_pending:
            return  # no JobManager, no ResourceManager to ask for pods
        slots = sum(int(j.settings.get("parallelism", 4)) for j in self.flink_jobs
                    if j.removed_at is None and j.state(t + 1e-6) not in ("FAILED", "CANCELED", "FINISHED"))
        need = -(-max(slots, int(self.global_knobs["slots_min"])) // ids.SLOTS_PER_TASKMANAGER)
        have = self.tm_pending + sum(1 for p in self.pods if p.role == "taskmanager" and (p.deleted_at is None or p.deleted_at > t))
        for _ in range(max(0, need - have)):
            self.tm_pending += 1

            def start(tt):
                self.tm_pending -= 1
                self.add_tm(tt)
            self.at(t + 3, start)

    def knob(self, run_type: str, key: str):
        return self.knobs.get(run_type, {}).get(key, DEFAULT_KNOBS[key])

    # ---------------------------------------------------------------------------------------------- events
    def at(self, t: float, fn) -> None:
        heapq.heappush(self.events, (t, next(self.seq), fn))

    def advance(self, t: float) -> None:
        while self.events and self.events[0][0] <= t:
            et, _, fn = heapq.heappop(self.events)
            fn(et)
            self.cache.clear()
            self.snap_cache.clear()

    # ---------------------------------------------------------------------------------------------- pods
    def pods_at(self, t: float, role: str | None = None) -> list[FakePod]:
        return [p for p in self.pods if p.alive(t) and (role is None or p.role == role)]

    def pod_by_name(self, name: str, t: float) -> FakePod | None:
        for p in reversed(self.pods):
            if p.name == name and p.alive(t):
                return p
        return None

    def kill_pod(self, name: str, t: float) -> None:
        p = self.pod_by_name(name, t)
        if p is None:
            return
        p.deleted_at = t
        p.last_reason = "Error"
        self.kill_log.append((t, p.name, p.role))
        self.cache.clear()
        if p.role == "broker":
            delay = self.global_knobs["broker_ready_delay_s"]
            self.at(t + 2, lambda tt, n=p.name: self.add_pod(n, "broker", dict(p.labels), tt, delay, "kafka", node=p.node))
            if self.global_knobs["imbalance_after_broker_restart"]:
                self.imbalance_until = math.inf
        elif p.role == "taskmanager":
            self.at(t + 5, lambda tt: self.add_tm(tt))
            self.jobs_disrupted(t)

    def jobs_disrupted(self, t: float) -> None:
        gk = self.global_knobs
        fails = gk["f5_restore_fails"] or (gk["f5_restore_fails_until_redeploy"] and not self.redeploys)
        for j in self.flink_jobs:
            if j.state(t) == "RUNNING":
                j.states.append((t + 1, "RESTARTING"))
                j.states.append((t + 1 + gk["job_restart_s"], "FAILED" if fails else "RUNNING"))

    def redeploy(self, t: float) -> None:
        self.redeploys.append(t)
        for p in self.pods_at(t):
            if p.role in ("jobmanager", "taskmanager"):
                p.deleted_at = t + 1
        for j in self.flink_jobs:
            if j.state(t) is not None:
                j.removed_at = t + 1
        self.jm_pending = True

        def jm_up(tt):
            self.jm_pending = False
            self.add_jm(tt)
            self.ensure_slots(tt)  # with slots_min 12, three TaskManagers start with no job; with 0 none do (L4)
        self.at(t + 5, jm_up)
        self.cache.clear()

    def tm_names_ready(self, t: float) -> list[str]:
        return sorted(p.name for p in self.pods_at(t, "taskmanager") if p.ready(t))

    # ---------------------------------------------------------------------------------------------- chaos
    def create_chaos(self, obj: dict, t: float) -> FakeChaos:
        kind, md, spec = obj["kind"], obj["metadata"], obj["spec"]
        rt = md["labels"].get(ids.LABEL_RUN_TYPE, "")
        sel = {".": list(spec["selector"]["pods"][ids.NAMESPACE])}
        if "target" in spec:
            sel[".Target"] = list(spec["target"]["selector"]["pods"][ids.NAMESPACE])
        if self.knob(rt, "wrong_target"):
            sel["."] = [self.wrong_pod_for(sel["."], t)] + sel["."][1:]
        dur = None
        if spec.get("duration"):
            dur = float(spec["duration"].rstrip("s"))
        if self.knob(rt, "missing_record") and len(sel["."]) > 1:
            sel["."] = sel["."][:-1]
        c = FakeChaos(kind=kind, name=md["name"], labels=dict(md["labels"]), spec=spec, created_at=t, run_type=rt,
                      targets=sel, duration=dur, one_shot=spec.get("action") == "pod-kill")
        n = self.created_by_type.get(rt, 0) + 1
        self.created_by_type[rt] = n
        c.nth = n
        self.chaos.append(c)
        only = self.knob(rt, "apply_only_first_n")
        if only is not None and n > only:
            return c  # never applied: no Apply event ever appears
        delay = self.knob(rt, "apply_delay_s")
        self.at(t + delay, lambda tt, c=c: self._apply(c, tt))
        return c

    def wrong_pod_for(self, pods: list[str], t: float) -> str:
        p0 = pods[0]
        if ids.broker_id_from_pod(p0) is not None:
            return f"{ids.BROKER_POD_PREFIX}{self.global_knobs['controller']}" if p0 != f"{ids.BROKER_POD_PREFIX}{self.global_knobs['controller']}" else f"{ids.BROKER_POD_PREFIX}0"
        jm = self.pods_at(t, "jobmanager")
        return jm[0].name if jm else p0

    def _apply(self, c: FakeChaos, t: float) -> None:
        if c.gone_at is not None or (c.delete_at is not None and c.delete_at <= t):
            return
        all_pods = [p for k in c.targets for p in c.targets[k]]
        for p in all_pods:
            c.apply_at[p] = t
        effect = self.knob(c.run_type, "effect")
        if getattr(c, "nth", 1) <= self.knob(c.run_type, "fail_first_n"):
            effect = "none"
        delay = self.knob(c.run_type, "effect_delay_s")
        main = c.targets["."]
        affected = main if effect in ("full", "weak") else main[:1] if effect == "partial" else []
        if c.one_shot and effect == "partial" and c.run_type == "F5" and getattr(c, "nth", 1) % 2 == 0:
            affected = []  # partial F5: every second kill is reported applied but kills nothing
        if c.one_shot:
            # Chaos Mesh keeps a one-shot experiment in Run until the object is deleted, so its records stay Injected and
            # AllRecovered stays False while it exists (REVIEW4 L2). delete_chaos recovers the records, then it is gone.
            for p in affected:
                self.at(t + delay, lambda tt, p=p: self.kill_pod(p, tt))
        else:
            for p in affected:
                c.effect[p] = (t + delay, None)
            if c.duration is not None and not self.knob(c.run_type, "no_recover"):
                self.at(t + c.duration + self.knob(c.run_type, "recover_delay_s"), lambda tt, c=c: self._recover(c, tt))

    def _recover(self, c: FakeChaos, t: float) -> None:
        once = self.global_knobs["fail_job_after_recover_once"]
        if once is not None and c.run_type == once:
            # one job fails right after the fault recovers, once: the recovery gate then needs the scripted remediation
            self.global_knobs["fail_job_after_recover_once"] = None
            j = next((j for j in self.flink_jobs if j.state(t) == "RUNNING"), None)
            if j is not None:
                j.states.append((t + 1, "FAILED"))
        for p in [p for k in c.targets for p in c.targets[k]]:
            if p in c.apply_at and p not in c.recover_at:
                c.recover_at[p] = t
        for p, (s, e) in list(c.effect.items()):
            if e is None:
                c.effect[p] = (s, t)
        self.cache.clear()

    def delete_chaos(self, c: FakeChaos, t: float) -> None:
        if c.delete_at is not None:
            return
        c.delete_at = t
        if c.apply_at and len(c.recover_at) < len(c.apply_at) and not self.knob(c.run_type, "stuck"):
            self.at(t + 0.5, lambda tt, c=c: self._recover(c, tt))
        if not self.knob(c.run_type, "stuck"):
            self.at(t + 1.0, lambda tt, c=c: setattr(c, "gone_at", tt))

    def chaos_by_name(self, kind: str, name: str, t: float) -> FakeChaos | None:
        for c in reversed(self.chaos):
            if c.kind == kind and c.name == name and (c.gone_at is None or t < c.gone_at):
                return c
        return None

    def chaos_obj(self, c: FakeChaos, t: float) -> dict:
        recs = []
        for key, pods in c.targets.items():
            for p in pods:
                evs = []
                if p in c.apply_at and c.apply_at[p] <= t:
                    evs.append({"type": "Succeeded", "operation": "Apply", "timestamp": to_rfc3339(c.apply_at[p])})
                rec_t = c.recover_at.get(p)
                if rec_t is not None and rec_t <= t:
                    evs.append({"type": "Succeeded", "operation": "Recover", "timestamp": to_rfc3339(rec_t)})
                injected = p in c.apply_at and c.apply_at[p] <= t and not (rec_t is not None and rec_t <= t)
                recs.append({"id": f"{ids.NAMESPACE}/{p}" + (f"/{ids.FLINK_CONTAINER}" if c.kind == "StressChaos" else ""),
                             "selectorKey": key, "phase": "Injected" if injected else "Not Injected",
                             "injectedCount": 1 if p in c.apply_at and c.apply_at[p] <= t else 0,
                             "recoveredCount": 1 if rec_t is not None and rec_t <= t else 0, "events": evs})
        all_inj = bool(recs) and all(r["phase"] == "Injected" for r in recs)
        all_rec = bool(recs) and all(r["phase"] == "Not Injected" for r in recs)
        md = {"name": c.name, "namespace": ids.NAMESPACE, "labels": c.labels, "creationTimestamp": to_rfc3339(c.created_at)}
        if c.delete_at is not None and c.delete_at <= t:
            md["deletionTimestamp"] = to_rfc3339(c.delete_at)
        return {"apiVersion": "chaos-mesh.org/v1alpha1", "kind": c.kind, "metadata": md, "spec": c.spec,
                "status": {"conditions": [{"type": "Selected", "status": "True"},
                                          {"type": "AllInjected", "status": "True" if all_inj else "False"},
                                          {"type": "AllRecovered", "status": "True" if all_rec else "False"},
                                          {"type": "Paused", "status": "False"}],
                           "experiment": {"desiredPhase": "Run", "containerRecords": recs}}}

    def effect_active(self, run_type: str, pod: str, t: float) -> bool:
        for c in self.chaos:
            if c.run_type != run_type:
                continue
            e = c.effect.get(pod)
            if e and e[0] <= t and (e[1] is None or t < e[1]):
                return True
        return False

    def oom_at(self, c: FakeChaos, pod: str) -> float | None:
        frac = self.knob("F8", "oom_at_frac")
        e = c.effect.get(pod)
        if frac is None or not e or c.duration is None:
            return None
        return e[0] + frac * c.duration

    def oom_events(self, pod: str, t: float) -> float:
        """cAdvisor container_oom_events_total of the TaskManager's flink-main-container."""
        return float(sum(1 for c in self.chaos if c.run_type == "F8" and (self.oom_at(c, pod) or math.inf) <= t))

    def stress_bytes(self, pod: str, t: float) -> float:
        total = 0.0
        for c in self.chaos:
            if c.run_type != "F8":
                continue
            e = c.effect.get(pod)
            oom = self.oom_at(c, pod)
            if oom is not None and t >= oom:
                continue  # the kernel killed the stressor; the container keeps running
            if e and e[0] <= t and (e[1] is None or t < e[1]):
                size = c.spec["stressors"]["memory"]["size"]
                b = int(size[:-3]) * MIB
                total += b * (0.3 if self.knob("F8", "effect") == "weak" else 1.0)
        return total

    def partition_active(self, a: str, b: str, t: float) -> bool:
        for c in self.chaos:
            if c.run_type != "F2":
                continue
            fol = c.targets["."][0]
            others = c.targets.get(".Target", [])
            if self.knob("F2", "effect") == "partial":
                others = others[:1]  # partial: cut off from one of the two other brokers only
            e = c.effect.get(fol)
            if e and e[0] <= t and (e[1] is None or t < e[1]):
                if (a == fol and b in others) or (b == fol and a in others):
                    return True
        return False

    def any_partition(self, t: float) -> tuple[bool, float]:
        for c in self.chaos:
            if c.run_type == "F2":
                e = c.effect.get(c.targets["."][0])
                if e and e[0] <= t and (e[1] is None or t < e[1]):
                    return True, t - e[0]
        return False, 0.0

    # ---------------------------------------------------------------------------------------------- signals
    def controller(self) -> int:
        return self.global_knobs["controller"]

    def loadgen_rate(self, t: float) -> float:
        return self.base_rate * (1 + 0.3 * math.sin(2 * math.pi * t / 1800.0))

    def loadgen_count(self, t: float) -> float:
        k = 2 * math.pi / 1800.0
        a, b = self.t0 - 7200, t
        return self.base_rate * ((b - a) - 0.3 / k * (math.cos(k * b) - math.cos(k * a)))

    def f6_count(self, t: float, delay: float = 0.0, akey: str | None = None) -> float:
        n = 0.0
        for r in self.f6_runs:
            if akey is not None and r["akey"] != akey:
                continue
            tt = t - delay
            if tt < r["first"]:
                continue
            if tt >= r["last"]:
                n += r["count"]
            else:
                n += 1 + (r["count"] - 1) * (tt - r["first"]) / max(1e-9, r["last"] - r["first"])
        return math.floor(n)

    def raw_events_count(self, t: float) -> float:
        return self.loadgen_count(t) + self.f6_count(t)

    def dead_letter_count(self, t: float) -> float:
        return self.f6_count(t, delay=3.0) + sum(1 for s in self.sentinels if s <= t)

    def dead_letter_meter(self, broker: str, t: float) -> float | None:
        """Kafka creates a topic's MessagesInPerSec meter on a broker at the first append to a partition it leads, and closes
        it when the broker stops leading the topic (REVIEW4 L3). dead-letter has three partitions led by brokers 0, 1 and 2;
        the sentinel lands on partition 0 and every F6 attempt writes to all three. A restarted or partitioned broker loses
        its leadership, so its meter is gone until the next append."""
        p = self.pod_by_name(broker, t)
        if p is None:
            return None
        epoch = p.created_at
        for c in self.chaos:
            if c.run_type == "F2" and c.targets["."][0] == broker:
                e = c.effect.get(broker)
                if e and e[0] <= t:
                    epoch = max(epoch, t if e[1] is None else min(e[1], t))
        appends = [x for x in self.sentinels if broker == f"{ids.BROKER_POD_PREFIX}0"]
        appends += [max(r["first"], epoch) for r in self.f6_runs if r["last"] >= epoch and r["first"] <= t]
        opened = [x for x in appends if epoch <= x <= t]
        return min(opened) if opened else None  # when the current meter was created, None if it does not exist

    def broker_down(self, t: float) -> tuple[list[str], float]:
        """Brokers that are not Ready, and the longest time since one became unavailable or came back (catch-up)."""
        down, since = [], 0.0
        for i in range(3):
            name = f"{ids.BROKER_POD_PREFIX}{i}"
            p = self.pod_by_name(name, t)
            if p is None or not p.ready(t):
                down.append(name)
                gone = [q.deleted_at for q in self.pods if q.name == name and q.deleted_at is not None and q.deleted_at <= t]
                since = max(since, t - max(gone)) if gone else since
        return down, since

    def broker_catching_up(self, t: float) -> bool:
        for p in self.pods_at(t, "broker"):
            if p.ready_at is not None and p.created_at > self.t0 - 3600 and 0 <= t - p.ready_at < 60:
                return True
        return False

    def urp(self, broker: str, t: float) -> float:
        down, _ = self.broker_down(t)
        part, _ = self.any_partition(t)
        if broker in down:
            return 0.0
        if down or part or self.broker_catching_up(t):
            return 30.0
        return 0.0

    def fenced(self, t: float) -> float:
        down, since = self.broker_down(t)
        part, elapsed = self.any_partition(t)
        return 1.0 if (down and since >= 9) or (part and elapsed >= 9) else 0.0

    def tm_cpu_rate(self, pod: str, t: float) -> float:
        base = 0.45 + 0.05 * math.sin(t / 47.0)
        if self.effect_active("F4", pod, t):
            base += 1.5 * (0.5 if self.knob("F4", "effect") == "weak" else 1.0)
        return base

    def effect_intervals(self, run_type: str, pod: str, t: float) -> list[tuple[float, float]]:
        out = []
        for c in self.chaos:
            if c.run_type == run_type and pod in c.effect:
                s0, e0 = c.effect[pod]
                out.append((s0, t if e0 is None else min(e0, t)))
        return out

    def tm_cpu_counter(self, p: FakePod, t: float) -> float:
        """Closed-form integral of tm_cpu_rate from the pod start (exactly reproducible for any past t)."""
        a, b = p.created_at, t
        total = 0.45 * (b - a) - 0.05 * 47.0 * (math.cos(b / 47.0) - math.cos(a / 47.0))
        factor = 0.5 if self.knob("F4", "effect") == "weak" else 1.0
        for s0, e0 in self.effect_intervals("F4", p.name, t):
            lo, hi = max(a, s0), min(b, e0)
            if hi > lo:
                total += 1.5 * factor * (hi - lo)
        return total

    def tm_ws(self, pod: str, t: float) -> float:
        # a smooth cycle plus per-step noise, so the order statistics of a few samples change from step to step as they do
        # in a real working set
        noise = ((zlib.crc32(pod.encode()) ^ (int(t) // 10) * 2654435761) % 1000 / 1000.0 - 0.5) * 16 * MIB
        return self.global_knobs["ws_base_bytes"] + 20 * MIB * math.sin(t / 61.0) + noise + self.stress_bytes(pod, t)

    def lag(self, t: float) -> float:
        base = 40.0 + 10.0 * math.sin(t / 37.0)
        extra = 0.0
        for c in self.chaos:
            if c.run_type in ("F4", "F7") and c.effect:
                s = min(e[0] for e in c.effect.values())
                ends = [e[1] for e in c.effect.values()]
                e = None if any(x is None for x in ends) else max(ends)
                if s <= t and (e is None or t < e):
                    extra = max(extra, 30000.0)
                elif e is not None and e <= t < e + 120:
                    extra = max(extra, 30000.0 * (1 - (t - e) / 120.0))
        for p in self.pods:
            if p.role == "taskmanager" and p.deleted_at is not None and p.deleted_at <= t < p.deleted_at + 90:
                extra = max(extra, 8000.0 * (1 - (t - p.deleted_at) / 90.0))
        return base + extra

    def loss_frac(self, pod: str, t: float) -> float:
        if any(a <= t < b for a, b in self.global_knobs["background_loss"]):
            return 0.5
        return 0.5 if self.effect_active("F7", pod, t) else 0.0

    # ---------------------------------------------------------------------------------------------- flink
    def loss_intervals(self, t: float) -> list[tuple[float, float]]:
        out = []
        for c in self.chaos:
            if c.run_type == "F7":
                for s0, e0 in c.effect.values():
                    out.append((s0, math.inf if e0 is None else e0))
        return out

    def ckpt_times(self, j: FakeJobRun, t: float) -> tuple[int, int, list[float], list[float]]:
        """Checkpoints every 60 s while RUNNING; a checkpoint fails while any TaskManager is under packet loss.
        Returns (completed count, failed count, last completed instants, last failed instants)."""
        runs, cur = [], None
        for ts, st in j.states + [(j.removed_at if j.removed_at is not None else math.inf, "END")]:
            if st == "RUNNING" and cur is None:
                cur = ts
            elif st != "RUNNING" and cur is not None:
                runs.append((cur, ts))
                cur = None
        if cur is not None:
            runs.append((cur, math.inf))
        loss = self.loss_intervals(t)
        n_done = n_fail = 0
        recent_done: list[float] = []
        recent_fail: list[float] = []
        for a, b in runs:
            end = min(b, t)
            n = int(math.floor((end - a) / 60.0)) if end > a else 0
            if n <= 0:
                continue
            failed = set()
            for s0, e0 in loss:
                k_lo = max(1, int(math.ceil((s0 - a) / 60.0)))
                k_hi = min(n, int(math.ceil((min(e0, end + 1e-9) - a) / 60.0)) - 1)
                failed.update(range(k_lo, k_hi + 1))
            n_fail += len(failed)
            n_done += n - len(failed)
            for k in range(n, max(0, n - 12), -1):
                (recent_fail if k in failed else recent_done).append(a + 60 * k)
        return n_done, n_fail, sorted(recent_done)[-10:], sorted(recent_fail)[-10:]

    def flink_snapshot(self, t: float) -> list[dict]:
        key = ("snap", round(t, 3))
        if key in self.snap_cache:
            return self.snap_cache[key]
        out = []
        for j in self.flink_jobs:
            st = j.state(t)
            if st is None:
                continue
            n_done, n_fail, done, failed = self.ckpt_times(j, t)
            scheme = self.global_knobs["checkpoint_scheme"]
            cp_cid = self.global_knobs["checkpoint_campaign_id"] or self.campaign_id
            path = (f"s3://{self.results_bucket}/scratch/{cp_cid}/flink-checkpoints/{j.jid}/chk-{n_done}" if scheme == "s3"
                    else f"file:///opt/flink/checkpoints/{j.jid}/chk-{n_done}") if n_done else None
            entry = {"name": j.name, "job_id": j.jid, "state": st, "completed_count": None, "failed_count": None,
                     "latest_completed_ack_at": None, "latest_completed_path": None, "latest_failed_at": None,
                     "completed_ack_at": [], "failed_at": []}
            if st not in ("FAILED", "CANCELED", "FINISHED"):
                entry.update({"completed_count": n_done, "failed_count": n_fail,
                              "latest_completed_ack_at": done[-1] if done else None, "latest_completed_path": path,
                              "latest_failed_at": failed[-1] if failed else None,
                              "completed_ack_at": done[-10:], "failed_at": failed[-10:]})
            out.append(entry)
        out = sorted(out, key=lambda e: (e["name"], e["job_id"]))
        if t <= self.clock.t - 1:
            self.snap_cache[key] = out
        return out

    def flink_overview(self, t: float) -> dict:
        n = sum(1 for p in self.pods_at(t, "taskmanager") if p.ready(t - 2))
        jm_ok = any(p.ready(t) for p in self.pods_at(t, "jobmanager"))
        if not jm_ok:
            return {}
        return {"taskmanagers": n, "slots-total": 4 * n, "slots-available": 0,
                "jobs-running": sum(1 for j in self.flink_jobs if j.state(t) == "RUNNING")}

    # ---------------------------------------------------------------------------------------------- scrape
    def series_at(self, t: int):
        """Every series Prometheus would hold at the 10 s step t (fakes/series.py), as a Step: name -> [(labels, value)]."""
        return self.series.step(t)

    # ---------------------------------------------------------------------------------------------- probes
    def tcp_ok(self, src: str, dst: str, port: int, t: float) -> bool:
        d = self.pod_by_name(dst, t)
        if d is None or not d.ready(t):
            return False
        if port in (9090, 9091) and self.partition_active(src, dst, t):
            return False
        if port == 9092 and self.global_knobs["f2_breaks_clients"] and self.any_partition(t)[0]:
            return False
        return True

    def probe_rows(self, t: int) -> list[dict]:
        from ..canonical import canonical_str
        from ..probefile import row
        if any(a <= t < b for a, b in self.global_knobs["probes_down"]):
            return []
        rows = []
        exec_down = any(a <= t < b for a, b in self.global_knobs["exec_fail"])
        brokers = sorted((p for p in self.pods_at(t, "broker")), key=lambda p: p.name)
        names = [f"{ids.BROKER_POD_PREFIX}{i}" for i in range(3)]
        for b in brokers:
            if not b.ready(t):
                continue
            k = 0
            for o in names:
                if o == b.name:
                    continue
                for port in (9090, 9091):
                    ok = self.tcp_ok(b.name, o, port, t)
                    if exec_down:
                        rows.append(row(t + 0.2 + 0.01 * k, "tcp", src_pod=b.name, dst=f"{o}:{port}", tried=1, ok=0,
                                        timeout_s=0.5, error="EXEC rc=1 error: unable to upgrade connection"))
                    else:
                        rows.append(row(t + 0.2 + 0.01 * k, "tcp", src_pod=b.name, dst=f"{o}:{port}", tried=1, ok=int(ok),
                                        timeout_s=0.5, latency_s=0.002 if ok else None, error=None if ok else "CONNECT_FAILED"))
                    k += 1
        lg = next((p for p in self.pods_at(t, "loadgen") if p.ready(t)), None)
        if lg is not None:
            for i, o in enumerate(names):
                ok = self.tcp_ok(lg.name, o, 9092, t)
                rows.append(row(t + 0.3 + 0.01 * i, "tcp", src_pod=lg.name, dst=f"{o}:9092", tried=1, ok=int(ok),
                                timeout_s=0.5, latency_s=0.002 if ok else None, error=None if ok else "CONNECT_FAILED"))
        jm = next((p for p in self.pods_at(t, "jobmanager") if p.ready(t)), None)
        for tm in sorted(self.pods_at(t, "taskmanager"), key=lambda p: p.name):
            if not tm.ready(t):
                continue
            if jm is None:
                rows.append(row(t + 0.5, "tcp_burst", src_pod=tm.name, dst="jobmanager:6123", tried=10, ok=0, timeout_s=0.5,
                                error="NO_JOBMANAGER_IP"))
                continue
            if exec_down:
                rows.append(row(t + 0.5, "tcp_burst", src_pod=tm.name, dst=f"{jm.name}:6123", tried=10, ok=0, timeout_s=0.5,
                                error="EXEC rc=1 lines=0 error: unable to upgrade connection"))
                continue
            loss = self.loss_frac(tm.name, t)
            failed = int(round(10 * loss)) if loss else 0
            if loss and (t // 10) % 2:
                failed = min(10, failed + 1)
            rows.append(row(t + 0.5, "tcp_burst", src_pod=tm.name, dst=f"{jm.name}:6123", tried=10, ok=10 - failed,
                            timeout_s=0.5, latency_s=0.001, error=None if not failed else f"{failed} connects failed"))
        if t % 60 == 0:
            src = next((b for b in brokers if b.ready(t)), None)
            if src is not None:
                rows.append(row(t + 0.1, "controller", src_pod=src.name, tried=1, ok=1, value=float(self.controller()),
                                detail=canonical_str({"method": "metadata-quorum"})))
        ov = self.flink_overview(t)
        if ov:
            snap = self.flink_snapshot(t)
            rows.append(row(t + 0.3, "flink_jobs", dst="streaming-flink-rest:8081", tried=1, ok=1,
                            value=float(sum(1 for j in snap if j["state"] == "RUNNING")), detail=canonical_str(snap)))
            rows.append(row(t + 0.35, "flink_tms", dst="streaming-flink-rest:8081", tried=1, ok=1,
                            value=float(ov["taskmanagers"]),
                            detail=canonical_str({"slots_total": ov["slots-total"], "slots_available": 0})))
        else:
            rows.append(row(t + 0.3, "flink_jobs", dst="streaming-flink-rest:8081", tried=1, ok=0, error="REST down"))
        if lg is not None:
            acked = math.floor(self.loadgen_count(t) * (1 + self.global_knobs["loadgen_mismatch"]))
            status = {"acked_count": acked, "commanded_rate_per_s": self.loadgen_rate(t),
                      "cycle_phase_frac": (t % 1800) / 1800.0, "started_at": self.t0 - 7200}
            rows.append(row(t + 0.4, "loadgen", dst=f"{lg.name}:8000", tried=1, ok=1, value=float(acked),
                            detail=canonical_str(status)))
        cons = next((p for p in self.pods_at(t, "consumer") if p.ready(t)), None)
        if cons is not None:
            started = self.t0 - 7200
            ra = self.global_knobs["consumer_restart_at"]
            if ra is not None and t >= ra:
                started = ra
            by, dl_by = {}, {}
            for r in self.f6_runs:
                n = self.f6_count(t, delay=2.0, akey=r["akey"]) if ra is None or t < ra else max(
                    0.0, self.f6_count(t, 2.0, r["akey"]) - self.f6_count(ra, 2.0, r["akey"]))
                if n:
                    by[r["akey"]] = int(n)
                # the enrichment job routes each malformed record to dead-letter, where the consumer counts it again
                d = self.f6_count(t, delay=5.0, akey=r["akey"]) if ra is None or t < ra else max(
                    0.0, self.f6_count(t, 5.0, r["akey"]) - self.f6_count(ra, 5.0, r["akey"]))
                if d:
                    dl_by[r["akey"]] = int(d)
            status = {"started_at": started, "malformed_count": sum(by.values()), "malformed_by_attempt": by,
                      "raw_events_count": 0, "dead_letter_count": sum(dl_by.values()), "dead_letter_by_attempt": dl_by,
                      "last_poll_at": t}
            rows.append(row(t + 0.45, "consumer", dst=f"{cons.name}:8000", tried=1, ok=1, value=float(sum(by.values())),
                            detail=canonical_str(status)))
        return rows

    # ---------------------------------------------------------------------------------------------- scenario reactions
    def on_manifest(self, m: dict) -> None:
        """Called by the harness on every manifest upload. Scenario knobs that need an attempt's own times (a disturbance
        during a sham, a job restart inside one attempt's PRE_FAULT) are scheduled when the attempt first appears."""
        gk = self.global_knobs
        for a in m.get("attempts") or []:
            if a["attempt_key"] in self.seen_attempts or a["status"] != "RUNNING":
                continue
            self.seen_attempts.add(a["attempt_key"])
            if a["run_type"] == "F0":
                self.sham_count += 1
                if self.sham_count in gk["sham_disturbance"]:
                    span = (a["planned_onset_at"], a["planned_onset_at"] + a["configured_duration_s"])
                    gk["background_loss"] = list(gk["background_loss"]) + [span]
                    self.disturbances.append({"kind": "background_loss", "run_type": "F0", "pod": "taskmanager",
                                              "start": span[0], "end": span[1], "attempt_key": a["attempt_key"]})
                    self.cache.clear()
            if gk["prefault_job_restart_once"] == a["run_type"]:
                gk["prefault_job_restart_once"] = None
                at = a["planned_onset_at"] - 300.0
                self.at(at, lambda tt, k=a["attempt_key"]: self.restart_jobs(tt, k))
            if gk["quorum_fail_after_end_once"] == a["run_type"]:
                gk["quorum_fail_after_end_once"] = None
                end = a["planned_onset_at"] + max(a["configured_duration_s"], 1)
                gk["quorum_fail"] = list(gk["quorum_fail"]) + [(end - 5.0, end + 120.0)]
            rt = a["run_type"]
            if gk["early_recover_once"] == rt:
                gk["early_recover_once"] = f"done:{rt}"
                self.knobs.setdefault(rt, {})["recover_delay_s"] = -70.0
            elif gk["early_recover_once"] == f"done:{rt}":
                gk["early_recover_once"] = None
                self.knobs.setdefault(rt, {}).pop("recover_delay_s", None)
        for a in m.get("attempts") or []:
            arm = gk["flink_snapshot_fail_after_gate_once"]
            if arm == a["run_type"] and a["status"] == "RUNNING" and (a.get("recovery_gate") or {}).get("passed_at"):
                gk["flink_snapshot_fail_after_gate_once"] = None
                gk["flink_snapshot_fail_next"] = 2
        if gk["job_restart_at_offset_s"] and not getattr(self, "_restarts_scheduled", False):
            self._restarts_scheduled = True
            for off in gk["job_restart_at_offset_s"]:
                self.at(self.t0 + off, lambda tt: self.restart_jobs(tt, None))

    def restart_jobs(self, t: float, attempt_key: str | None) -> None:
        """Every RUNNING job restarts (RESTARTING for job_restart_s), as after a checkpoint failure or a JobManager
        hiccup: not a campaign fault. Its task metrics vanish meanwhile (fakes/series.py)."""
        self.jobs_disrupted(t)
        self.disturbances.append({"kind": "job_restart", "run_type": None, "pod": "jobs", "start": t + 1,
                                  "end": t + 1 + self.global_knobs["job_restart_s"], "attempt_key": attempt_key})

    # ---------------------------------------------------------------------------------------------- ground truth
    def ground_truth(self, t_end: float) -> list[dict]:
        """REVIEW4 MIN-1 (C27): every interval in which the simulated cluster was disturbed, from the world's own state,
        independent of any Chaos Mesh record or orchestrator time. dump_dry_run.py writes it; check_e2e.py asserts that no
        NORMAL, RECOVERY or SHAM window overlaps one and that every ANOMALOUS window lies inside its attempt's interval.

        kind "effect": the fault acting (a duration fault's effect on each target, a killed pod until its replacement is
        Ready plus 60 s of catch-up, an F6 run's first to last record); kind "span": for the one-shot kills of F1 and F5,
        from the first kill to that instant plus the fixed 300 s span of 2.4, which is what the fault's label covers."""
        out = []
        for c in self.chaos:
            label = c.labels.get(ids.LABEL_ATTEMPT)
            if c.one_shot:
                continue
            for pod, (a, b) in c.effect.items():
                out.append({"kind": "effect", "run_type": c.run_type, "pod": pod, "start": a,
                            "end": t_end if b is None else b, "attempt_key": label})
        kills: dict[str, list[float]] = {}
        matched = set()
        for c in self.chaos:
            if not c.one_shot or not c.apply_at:
                continue
            label = c.labels.get(ids.LABEL_ATTEMPT)
            for i, (kt, name, role) in enumerate(self.kill_log):
                if i in matched or name not in c.apply_at or not (c.apply_at[name] <= kt <= c.apply_at[name] + 5.0):
                    continue
                matched.add(i)
                kills.setdefault(label, []).append(kt)
                out.append({"kind": "effect", "run_type": c.run_type, "pod": name, "start": kt,
                            "end": min(t_end, self._back_at(kt, name, role, t_end)), "attempt_key": label})
        for label, ts in kills.items():
            rt = next(c.run_type for c in self.chaos if c.labels.get(ids.LABEL_ATTEMPT) == label)
            out.append({"kind": "span", "run_type": rt, "pod": "all", "start": min(ts), "end": min(ts) + 300.0,
                        "attempt_key": label})
        for i, (kt, name, role) in enumerate(self.kill_log):
            if i not in matched and role in ("broker", "taskmanager"):  # a pod lost to no campaign fault (a test's own kill)
                out.append({"kind": "effect", "run_type": None, "pod": name, "start": kt,
                            "end": min(t_end, self._back_at(kt, name, role, t_end)), "attempt_key": None})
        for r in self.f6_runs:
            out.append({"kind": "effect", "run_type": "F6", "pod": "raw-events", "start": r["first"], "end": r["last"],
                        "attempt_key": r["akey"]})
        out += [dict(d, kind="effect") for d in self.disturbances]
        return sorted(out, key=lambda x: (x["start"], x["end"]))

    def _back_at(self, kt: float, name: str, role: str, t_end: float) -> float:
        """When a killed pod's effect ends: a broker when its replacement (same name) is Ready plus the 60 s of catch-up
        the fake models (URP until then), a TaskManager when its replacement is Ready and the tasks are placed again."""
        if role == "broker":
            rep = [p.ready_at for p in self.pods if p.name == name and p.created_at >= kt and p.ready_at is not None]
            return (min(rep) + 60.0) if rep else t_end
        rep = [p.ready_at for p in self.pods if p.role == "taskmanager" and p.created_at >= kt and p.ready_at is not None]
        return max(kt + 1 + self.global_knobs["job_restart_s"], min(rep)) if rep else t_end
