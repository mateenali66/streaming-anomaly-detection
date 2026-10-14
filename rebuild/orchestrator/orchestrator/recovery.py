"""Recovery gate between attempts, health gate before every settle gap, and the scripted remediation (protocol 1.6).

Every value comes from the orchestrator's own queries and probes and is returned as a condition object
{name, value, threshold, pass, at} for the verification record (11.6 recovery_gate_conditions). A query that fails
or returns nothing gives value null and the condition fails; nothing is ever read as zero.
"""
from __future__ import annotations

import json
import logging
import math
from dataclasses import dataclass, field

from . import ids
from .errors import HaltRequested, OrchestratorError
from .flinkrest import TERMINAL_STATES
from .gate import exec_failed, scrub_checkpoint_path
from .kube import pod_ready

log = logging.getLogger("orchestrator.recovery")


def cond(name: str, value: float | None, op: str, threshold: float, at: float) -> dict:
    ok = False
    if value is not None and not (isinstance(value, float) and math.isnan(value)):
        ok = {"==": value == threshold, "<=": value <= threshold, ">=": value >= threshold}[op]
    return {"name": name, "value": None if value is None else float(value), "threshold": float(threshold),
            "pass": bool(ok), "at": round(at, 3)}


@dataclass
class GateOutcome:
    passed: bool
    passed_at: float | None
    conditions: list[dict]
    remediation: dict = field(default_factory=lambda: {"performed": False, "actions": [], "started_at": None, "ended_at": None})
    redeployed: bool = False
    resubmitted: bool = False  # a job was resubmitted without a full redeploy (REVIEW4 MIN-2, C28)


class RecoveryGate:
    def __init__(self, rt) -> None:
        self.rt = rt
        self.th = rt.thresholds["recovery_gate"]
        self.q = rt.thresholds["queries"]

    # ------------------------------------------------------------------------------------------- values
    def _prom_scalar(self, name: str, at: float | None = None) -> float | None:
        try:
            res = self.rt.prom.instant(self.q[name], at=at)
        except OrchestratorError as e:
            log.warning("query %s failed: %s", name, e)
            return None
        if len(res) != 1:
            return None
        v = res[0][1]
        return None if math.isnan(v) else v

    def leftover_objects(self) -> int | None:
        sel = f"{ids.LABEL_CAMPAIGN}={self.rt.env.campaign_id}"
        try:
            n = 0
            for kind in ids.CHAOS_KINDS:
                n += len(self.rt.kube.list(kind, sel))
            n += sum(1 for j in self.rt.kube.list("Job", sel) if j["metadata"]["name"] != ids.ORCHESTRATOR_JOB)
            n += len(self.rt.kube.list_pods(f"{ids.LABEL_ATTEMPT}"))
            return n
        except OrchestratorError as e:
            log.warning("leftover listing failed: %s", e)
            return None

    def evaluate(self, reference_at: float, resources: list[dict] | None, stats: dict | None,
                 health: bool = False) -> list[dict]:
        rt, th = self.rt, self.th
        now = rt.clock.now()
        out: list[dict] = []
        out.append(cond("no_campaign_chaos_or_jobs", self.leftover_objects(), "==", 0, now))
        unconfirmed = sum(1 for r in (resources or []) if r.get("not_found_at") is None)
        out.append(cond("deleted_resources_not_found", float(unconfirmed), "==", 0, now))
        try:
            brokers = rt.kube.list_pods(ids.BROKER_SELECTOR)
            jms = rt.kube.list_pods(ids.JM_SELECTOR)
            tms = rt.kube.list_pods(ids.TM_SELECTOR)
            nb = float(sum(1 for p in brokers if pod_ready(p))) if len(brokers) == th["brokers_ready"] else -1.0
            nj = float(sum(1 for p in jms if pod_ready(p))) if len(jms) == th["jobmanagers_ready"] else -1.0
            nt = float(sum(1 for p in tms if pod_ready(p))) if len(tms) == th["taskmanagers_ready"] else -1.0
        except OrchestratorError:
            nb = nj = nt = None
        out.append(cond("brokers_ready", nb, "==", th["brokers_ready"], now))
        out.append(cond("jobmanager_ready", nj, "==", th["jobmanagers_ready"], now))
        out.append(cond("taskmanagers_ready", nt, "==", th["taskmanagers_ready"], now))
        for name in ("under_replicated_sum", "offline_partitions_max", "under_min_isr_sum", "preferred_imbalance_max",
                     "fenced_brokers_max"):
            out.append(cond(name, self._prom_scalar(name), "==", 0, now))
        out += self.flink_conditions(reference_at, now)
        if stats is not None:
            lag_thr = stats["consumer_lag_max"]["mean"] + th["lag_sd_multiplier"] * stats["consumer_lag_max"]["sd"]
            out.append(cond("consumer_lag_below_baseline", self._prom_scalar("consumer_lag_max"), "<=", lag_thr, now))
        out += self.probe_conditions(now)
        if health:
            out += self.health_conditions(now)
        return out

    def flink_conditions(self, reference_at: float, now: float) -> list[dict]:
        th = self.th
        expected = [j[0] for j in ids.FLINK_JOBS[self.rt.env.workload]]
        try:
            snap = self.rt.flink.snapshot()
            ov = self.rt.flink.overview()
        except OrchestratorError as e:
            log.warning("Flink REST failed: %s", e)
            return [cond(n, None, op, t, now) for n, op, t in (
                ("flink_jobs_running_by_name", "==", len(expected)), ("flink_no_extra_jobs", "==", 0),
                ("flink_checkpoints_since_end", ">=", th["checkpoints_since_end_min"]),
                ("flink_no_failed_checkpoint_recent", "==", 0),
                ("flink_registered_taskmanagers", "==", th["registered_taskmanagers"]),
                ("flink_task_slots", "==", th["task_slots"]))]
        live = [j for j in snap if j["state"] not in TERMINAL_STATES]
        running = {n: [j for j in live if j["name"] == n and j["state"] == "RUNNING"] for n in expected}
        ok_names = sum(1 for n in expected if len(running[n]) == 1)
        extra = sum(1 for j in live if j["name"] not in expected) + sum(max(0, len([j for j in live if j["name"] == n]) - 1)
                                                                        for n in expected)
        cps = []
        for n in expected:
            if len(running[n]) == 1:
                cps.append(sum(1 for t in running[n][0]["completed_ack_at"] if t > reference_at))
        min_cp = float(min(cps)) if len(cps) == len(expected) else None
        failed_recent = 0
        for j in live:
            times = list(j.get("failed_at") or [])
            if j.get("latest_failed_at"):
                times.append(j["latest_failed_at"])
            failed_recent += sum(1 for t in set(times) if t > now - th["failed_checkpoint_window_s"])
        return [
            cond("flink_jobs_running_by_name", float(ok_names), "==", len(expected), now),
            cond("flink_no_extra_jobs", float(extra), "==", 0, now),
            cond("flink_checkpoints_since_end", min_cp, ">=", th["checkpoints_since_end_min"], now),
            cond("flink_no_failed_checkpoint_recent", float(failed_recent), "==", 0, now),
            cond("flink_registered_taskmanagers", float(ov["taskmanagers"]), "==", th["registered_taskmanagers"], now),
            cond("flink_task_slots", float(ov["slots-total"]), "==", th["task_slots"], now),
        ]

    def probe_conditions(self, now: float) -> list[dict]:
        th = self.th
        try:
            _, rows = self.rt.probes.samples(now - th["probe_window_s"], now + 1)
        except OrchestratorError as e:
            log.warning("probe service failed: %s", e)
            rows = None
        if rows is None:
            return [cond(n, None, op, t, now) for n, op, t in (
                ("probe_tcp_ok_frac", ">=", th["tcp_ok_frac_min"]), ("probe_packet_loss_frac", "<=", th["packet_loss_frac_max"]),
                ("probe_consumer_age_s", "<=", th["probe_max_age_s"]), ("probe_controller_age_s", "<=", th["probe_window_s"]),
                ("raw_events_rate_matches_loadgen", "<=", th["rate_match_tolerance_frac"]),
                ("dead_letter_rate_zero", "==", 0))]
        # 1.6.8 per source and destination pair and per TaskManager (REVIEW4 MIN-10, C32): the value is the worst pair's
        # ok fraction and the worst TaskManager's loss. A failed kubectl exec is a missing sample (REVIEW4 L7).
        tcp: dict[tuple, list[dict]] = {}
        burst: dict[str, list[dict]] = {}
        for r in rows:
            if exec_failed(r):
                continue
            if r["probe"] == "tcp":
                tcp.setdefault((r["src_pod"], r["dst"]), []).append(r)
            elif r["probe"] == "tcp_burst":
                burst.setdefault(r["src_pod"], []).append(r)
        tcp_ok = min(sum(r["ok_count"] for r in v) / sum(r["tried_count"] for r in v) for v in tcp.values()) if tcp else None
        loss = max(1 - sum(r["ok_count"] for r in v) / sum(r["tried_count"] for r in v) for v in burst.values()) if burst else None
        cons = [r for r in rows if r["probe"] == "consumer" and r["ok_count"] == 1]
        ctrl = [r for r in rows if r["probe"] == "controller" and r["ok_count"] == 1]
        out = [
            cond("probe_tcp_ok_frac", tcp_ok, ">=", th["tcp_ok_frac_min"], now),
            cond("probe_packet_loss_frac", loss, "<=", th["packet_loss_frac_max"], now),
            cond("probe_consumer_age_s", (now - max(r["ts"] for r in cons)) if cons else None, "<=", th["probe_max_age_s"], now),
            cond("probe_controller_age_s", (now - max(r["ts"] for r in ctrl)) if ctrl else None, "<=", th["probe_window_s"], now),
        ]
        out.append(cond("raw_events_rate_matches_loadgen", self.rate_mismatch(rows), "<=", th["rate_match_tolerance_frac"], now))
        out.append(cond("dead_letter_rate_zero", self.dead_letter_rate(rows), "==", 0, now))
        return out

    def dead_letter_rate(self, rows: list[dict]) -> float | None:
        """1.6 item 7 from the verification consumer (REVIEW4 L3, C34): the change of its dead_letter_count (the sentinel
        excluded) over the last 60 s, per second. Two samples of one consumer process are needed, so a restart or a
        missing sample gives None, which fails the condition."""
        cons = sorted((r for r in rows if r["probe"] == "consumer" and r["ok_count"] == 1 and r["detail"]),
                      key=lambda r: r["ts"])
        if len(cons) < 2:
            return None
        last = cons[-1]
        target = last["ts"] - self.th["dead_letter_window_s"]
        first = min(cons[:-1], key=lambda r: abs(r["ts"] - target))
        if abs(first["ts"] - target) > 5:
            return None
        try:
            d0, d1 = json.loads(first["detail"]), json.loads(last["detail"])
        except (TypeError, json.JSONDecodeError):
            return None
        if d0.get("started_at") != d1.get("started_at") or "dead_letter_count" not in d0 or "dead_letter_count" not in d1:
            return None
        return (float(d1["dead_letter_count"]) - float(d0["dead_letter_count"])) / (last["ts"] - first["ts"])

    def rate_mismatch(self, rows: list[dict]) -> float | None:
        """|broker messages-in rate on raw-events - load generator acked rate| / acked rate, over the same 60 s (1.6.6)."""
        lg = sorted((r for r in rows if r["probe"] == "loadgen" and r["ok_count"] == 1 and r["value"] is not None),
                    key=lambda r: r["ts"])
        if len(lg) < 2:
            return None
        last = lg[-1]
        target = last["ts"] - self.th["rate_window_s"]
        first = min(lg[:-1], key=lambda r: abs(r["ts"] - target))
        if abs(first["ts"] - target) > 5:
            return None
        try:
            if json.loads(first["detail"]).get("started_at") != json.loads(last["detail"]).get("started_at"):
                return None  # the load generator restarted: its counter is not continuous
        except (TypeError, json.JSONDecodeError):
            return None
        lg_rate = (last["value"] - first["value"]) / (last["ts"] - first["ts"])
        broker = self._prom_scalar("raw_events_in_per_s", at=last["ts"])
        if broker is None or lg_rate <= 0:
            return None
        return abs(broker - lg_rate) / lg_rate

    def health_conditions(self, now: float) -> list[dict]:
        out = []
        try:
            current = sorted(instance_ids(self.rt.kube.list_nodes()))
        except OrchestratorError:
            current = None
        recorded = self.rt.recorded_instance_ids()
        out.append(cond("ec2_instance_ids_match", None if current is None else (1.0 if current == recorded else 0.0),
                        "==", 1, now))
        out.append(cond("clock_skew_s", self.rt.clock_skew_abs(), "<=", self.rt.thresholds["health_gate"]["clock_skew_max_s"], now))
        return out

    # ------------------------------------------------------------------------------------------- loops
    def hold(self, reference_at: float, resources: list[dict] | None, stats: dict | None, deadline: float,
             health: bool = False) -> tuple[bool, float | None, list[dict]]:
        clock = self.rt.clock
        hold_start = None
        last: list[dict] = []
        while True:
            last = self.evaluate(reference_at, resources, stats, health=health)
            now = clock.now()
            if all(c["pass"] for c in last):
                if hold_start is None:
                    hold_start = now
                if now - hold_start >= self.th["hold_s"]:
                    return True, now, last
            else:
                hold_start = None
                failing = [c["name"] for c in last if not c["pass"]]
                log.info("recovery gate failing: %s", failing)
            if now >= deadline:
                return False, None, last
            clock.sleep(self.th["poll_s"])

    def run(self, reference_at: float, resources: list[dict] | None, stats: dict | None,
            on_remediation_start=None) -> GateOutcome:
        clock = self.rt.clock
        start = clock.now()
        ok, at, conds = self.hold(reference_at, resources, stats, start + self.th["timeout_s"])
        if ok:
            return GateOutcome(True, at, conds)
        rem = {"performed": True, "actions": [], "started_at": clock.now(), "ended_at": None}
        if on_remediation_start:
            on_remediation_start(rem)
        remed = Remediation(self.rt)
        redeployed = remed.run(rem, reference_at, resources, stats, self)
        rem["ended_at"] = clock.now()
        ok, at, conds = self.hold(reference_at, resources, stats, rem["ended_at"] + self.th["remediation_timeout_s"])
        return GateOutcome(ok, at, conds, rem, redeployed, resubmitted=remed.resubmitted and not redeployed)


def instance_ids(nodes: list[dict]) -> list[str]:
    out = []
    for n in nodes:
        pid = (n.get("spec") or {}).get("providerID") or ""
        iid = pid.rsplit("/", 1)[-1]
        if not iid.startswith("i-"):
            raise OrchestratorError(f"node {n['metadata']['name']} has no EC2 instance providerID ({pid!r})")
        out.append(iid)
    return out


class Remediation:
    """One scripted remediation (1.6): preferred leader election, resubmit missing jobs by name from their latest S3
    checkpoint, and as the last step a full Flink redeploy, which flags the next attempt AFTER_REDEPLOY."""

    # REVIEW4 L1 (C40): the one submission path, the same script the flink-submit Job runs. It passes -p 4 and every
    # checkpoint and restart setting itself, so a resubmitted job runs exactly like the first submission.
    RUN_JOB = "/opt/flink/bin/run-job.sh"
    LEADER_ELECTION = ["/opt/kafka/bin/kafka-leader-election.sh", "--bootstrap-server", "localhost:9092",
                       "--election-type", "PREFERRED", "--all-topic-partitions"]

    def __init__(self, rt) -> None:
        self.rt = rt
        self.resubmitted = False

    def _exec_broker(self, argv: list[str]) -> bool:
        for b in sorted(self.rt.kube.list_pods(ids.BROKER_SELECTOR), key=lambda p: p["metadata"]["name"]):
            if pod_ready(b):
                res = self.rt.kube.exec(b["metadata"]["name"], ids.BROKER_CONTAINER, argv, timeout_s=120)
                return res.rc == 0
        return False

    def latest_checkpoint_paths(self) -> dict[str, str]:
        now = self.rt.clock.now()
        paths: dict[str, str] = {}
        _, rows = self.rt.probes.samples(now - 6 * 3600, now + 1)
        for r in sorted((r for r in rows if r["probe"] == "flink_jobs" and r["ok_count"] == 1 and r["detail"]),
                        key=lambda r: r["ts"]):
            for j in json.loads(r["detail"]):
                if j.get("latest_completed_path"):
                    paths[j["name"]] = j["latest_completed_path"]
        return paths

    def resubmit_missing(self, actions: list[str]) -> None:
        rt = self.rt
        jobs = {j[0]: j for j in ids.FLINK_JOBS[rt.env.workload]}
        snap = rt.flink.snapshot()
        live = [j for j in snap if j["state"] not in TERMINAL_STATES]
        paths = self.latest_checkpoint_paths()
        jm = next((p for p in rt.kube.list_pods(ids.JM_SELECTOR) if pod_ready(p)), None)
        if jm is None:
            actions.append("RESUBMIT_SKIPPED_NO_READY_JOBMANAGER")
            return
        for name, (_, cls, jar) in jobs.items():
            mine = [j for j in live if j["name"] == name]
            if len(mine) == 1 and mine[0]["state"] == "RUNNING":
                continue
            for j in mine:
                rt.flink.cancel(j["job_id"])
                actions.append(f"CANCEL {name} {j['job_id']}")
            argv = [self.RUN_JOB, cls, f"{ids.FLINK_JAR_DIR}/{jar}"] + ([self.full_path(paths[name])] if name in paths else [])
            res = rt.kube.exec(jm["metadata"]["name"], ids.FLINK_CONTAINER, argv, timeout_s=180)
            self.resubmitted = True
            # The action text enters the manifest, so the path is stored without the bucket name (C24).
            src = scrub_checkpoint_path(paths[name], rt.env.results_bucket, rt.env.campaign_id) if name in paths else "NO_CHECKPOINT"
            actions.append(f"RESUBMIT {name} from={src} rc={res.rc}")

    def full_path(self, path: str) -> str:
        """The restore path for run-job.sh. Live probe samples carry Flink's full URI. A path in the stored, relative form
        (C24, flink-checkpoints/...) is rebuilt under this campaign's scratch prefix."""
        if "://" in path:
            return path
        return f"s3://{self.rt.env.results_bucket}/scratch/{self.rt.env.campaign_id}/{path}"

    def check_job_settings(self, actions: list[str]) -> None:
        """REVIEW4 L1 (C40): after a resubmission every job must run with the registered settings, or the campaign halts."""
        from .flinkrest import job_settings_problems
        probs = job_settings_problems(self.rt.flink, [j[0] for j in ids.FLINK_JOBS[self.rt.env.workload]],
                                      self.rt.thresholds["flink_job_settings"])
        if probs:
            actions.append(f"JOB_SETTINGS_WRONG {probs[0]}"[:200])
            raise HaltRequested(f"FLINK_JOB_SETTINGS_WRONG {probs}"[:300])
        actions.append("JOB_SETTINGS_OK")

    def jobs_running_by_name(self) -> bool:
        expected = [j[0] for j in ids.FLINK_JOBS[self.rt.env.workload]]
        try:
            live = [j for j in self.rt.flink.snapshot() if j["state"] not in TERMINAL_STATES]
        except OrchestratorError:
            return False
        return all(sum(1 for j in live if j["name"] == n and j["state"] == "RUNNING") == 1 for n in expected) and \
            all(j["name"] in expected for j in live)

    def wait_jobs_running(self, timeout_s: float) -> bool:
        deadline = self.rt.clock.now() + timeout_s
        while True:
            if self.jobs_running_by_name():
                return True
            if self.rt.clock.now() >= deadline:
                return False
            self.rt.clock.sleep(10)

    def redeploy(self, actions: list[str]) -> None:
        """Full Flink redeploy: change spec.restartNonce of the FlinkDeployment. Flink Kubernetes Operator docs
        (context7 /apache/flink-kubernetes-operator, "Triggering a Restart with restartNonce"; FlinkDeploymentSpec:
        "Nonce used to manually trigger restart ... change the number to a different non-null value"). The session
        cluster (no spec.job) comes back without jobs, so every expected job is resubmitted by name afterwards."""
        rt = self.rt
        old_uids = {p["metadata"]["uid"] for p in rt.kube.list_pods(ids.JM_SELECTOR) + rt.kube.list_pods(ids.TM_SELECTOR)}
        nonce = int(rt.clock.now())
        rt.kube.patch_merge("FlinkDeployment", ids.FLINK_DEPLOYMENT, {"spec": {"restartNonce": nonce}})
        actions.append(f"FLINK_REDEPLOY restartNonce={nonce}")
        # REVIEW4 L4 (C35): a native session cluster with no jobs requests no TaskManagers. So wait for the NEW JobManager to
        # be Ready, the old pods gone and REST answering, resubmit every job, and only then wait for three new Ready
        # TaskManagers registered with 12 slots, all within 600 s. The old pods can look Ready for a while after the patch.
        deadline = rt.clock.now() + 600
        while True:
            jms = [p for p in rt.kube.list_pods(ids.JM_SELECTOR) if p["metadata"]["uid"] not in old_uids and pod_ready(p)]
            old_left = [p for p in rt.kube.list_pods(ids.JM_SELECTOR) + rt.kube.list_pods(ids.TM_SELECTOR)
                        if p["metadata"]["uid"] in old_uids]
            if len(jms) == 1 and not old_left:
                try:
                    rt.flink.overview()
                    break
                except OrchestratorError:
                    pass
            if rt.clock.now() >= deadline:
                raise OrchestratorError("the redeployed JobManager did not become Ready in 600 s")
            rt.clock.sleep(10)
        self.resubmit_missing(actions)
        while True:
            tms = [p for p in rt.kube.list_pods(ids.TM_SELECTOR) if p["metadata"]["uid"] not in old_uids and pod_ready(p)]
            if len(tms) == ids.EXPECTED_TASKMANAGERS:
                try:
                    ov = rt.flink.overview()
                    if ov["taskmanagers"] == ids.EXPECTED_TASKMANAGERS and \
                            ov["slots-total"] == ids.EXPECTED_TASKMANAGERS * ids.SLOTS_PER_TASKMANAGER:
                        return
                except OrchestratorError:
                    pass
            if rt.clock.now() >= deadline:
                raise OrchestratorError("the redeployed Flink cluster did not start three TaskManagers in 600 s")
            rt.clock.sleep(10)

    def run(self, rem: dict, reference_at: float, resources, stats, gate: RecoveryGate) -> bool:
        actions = rem["actions"]
        try:
            ok = self._exec_broker(self.LEADER_ELECTION)
            actions.append(f"PREFERRED_LEADER_ELECTION ok={ok}")
        except OrchestratorError as e:
            actions.append(f"PREFERRED_LEADER_ELECTION error={type(e).__name__}")
        try:
            self.resubmit_missing(actions)
        except OrchestratorError as e:
            actions.append(f"RESUBMIT error={type(e).__name__}: {e}"[:200])
        # Escalate to a full redeploy only if the jobs are still not RUNNING by name after 300 s. Everything else
        # (checkpoints, lag, probes) gets the remediation timeout of the gate itself.
        if self.wait_jobs_running(300):
            if self.resubmitted:
                self.check_job_settings(actions)
            return False
        try:
            self.redeploy(actions)
        except OrchestratorError as e:
            actions.append(f"FLINK_REDEPLOY error={type(e).__name__}: {e}"[:200])
            raise HaltRequested("REMEDIATION_REDEPLOY_FAILED")
        self.wait_jobs_running(300)
        self.check_job_settings(actions)  # a job still not RUNNING here is a problem too, and halts
        return True
