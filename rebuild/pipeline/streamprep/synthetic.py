"""Synthetic campaign generator. Writes a contract-conformant campaign directory (11.3 to 11.10).

The "full" scenario covers what the preprocessing must handle:
  valid fault and sham attempts in every block, a failed attempt that applied nothing, failed
  attempts that applied a fault, a TELEMETRY_FAILED attempt, an ABORTED attempt with a halt and an
  IDLE cleanup segment, a stuck-resource overrun that fails timing, a 90 s overrun that stays
  valid with the OVERRUN flag, a remediated attempt with a full Flink redeploy and an
  AFTER_REDEPLOY successor, a health event, an F7 TaskManager loss, broker restarts with counter
  resets, TaskManager kills with job restarts and scrape gaps, an in-place counter reset and a
  scrape gap in the baseline, an IDLE gap between blocks, and a trailing baseline.

Truth about the generated data (gap intervals, kills) is written outside the campaign directory,
so it never enters SHA256SUMS.
"""
from __future__ import annotations

import gzip
import hashlib
import io
import json
import uuid
from dataclasses import dataclass, field
from pathlib import Path

import numpy as np
import pyarrow as pa
import pyarrow.parquet as pq

from .contract import (
    FLINK_JOBS, RUN_TYPES, campaign_id_for, canonical_json_bytes, ceil10, cluster_name_for,
    floor10, series_id_for, sha256_bytes, sha256_file,
)
from .health import scrape_health_rows, scrape_index
from .plan import generate_plan

RUN_TYPE_SPECS = [
    {"run_type": "F0", "name": "Sham", "kind": "SHAM", "duration_s": 0, "params": {}},
    {"run_type": "F1", "name": "Broker pod kill", "kind": "PodChaos", "duration_s": 300,
     "params": {"action": "pod-kill", "grace_period_s": 0}},
    {"run_type": "F2", "name": "Broker network partition", "kind": "NetworkChaos", "duration_s": 180,
     "params": {"action": "partition", "direction": "both"}},
    {"run_type": "F4", "name": "TaskManager CPU stress", "kind": "StressChaos", "duration_s": 300,
     "params": {"workers": 4, "load_pct": 90}},
    {"run_type": "F5", "name": "TaskManager loss", "kind": "PodChaos", "duration_s": 300,
     "params": {"action": "pod-kill", "kills": 5, "interval_s": 60}},
    {"run_type": "F6", "name": "Poison pill", "kind": "Injector", "duration_s": 180,
     "params": {"rate_pct_of_baseline": 5}},
    {"run_type": "F7", "name": "TaskManager packet loss", "kind": "NetworkChaos", "duration_s": 300,
     "params": {"action": "loss", "loss_pct": 50}},
    {"run_type": "F8", "name": "TaskManager memory pressure", "kind": "StressChaos", "duration_s": 180,
     "params": {"fill_frac_milli": 850, "min_stressor_mib": 256}},
]
PHASE_S = {"settle_max": 300, "pre_fault": 600, "recovery": 300, "cooldown": 300, "gate_hold": 120,
           "gate_timeout": 900, "remediation_timeout": 900, "apply_max": 30, "redeploy_extra_settle": 600}

# The gate's registered parameters for the verification-channel rules (orchestrator thresholds.py, gate.telemetry and
# gate.pre_fault), written into the synthetic run/thresholds.json so the preprocessing reads them as from a real campaign.
SYNTH_GATE = {
    "telemetry": {"segments": ["PRE_FAULT", "INJECTION", "RECOVERY"], "scrape_miss_max": 3,
                  "scrape_roles": ["broker", "jobmanager", "taskmanager"], "exempt_segments": ["INJECTION", "RECOVERY"],
                  "targeted": {"F1": ["TARGET_BROKER"], "F2": ["TARGET_BROKER"], "F4": ["taskmanager"],
                               "F5": ["taskmanager"], "F7": ["taskmanager"], "F8": ["taskmanager"]},
                  "series_gap_flag_below": 0.95},
    "pre_fault": {"taskmanagers_registered": 3, "tcp_ok_frac_min": 0.95, "packet_loss_frac_max": 0.02,
                  "expected_pods": {"broker": 3, "jobmanager": 1, "taskmanager": 3}},
}
BROKERS = [f"streaming-kafka-brokers-{i}" for i in range(3)]
BROKER_NODES = [f"ip-10-0-1-{11 + i}.us-east-2.compute.internal" for i in range(3)]
FLINK_NODES = [f"ip-10-0-2-{21 + i}.us-east-2.compute.internal" for i in range(3)]
CONTROLLER_NODE_ID = 0


@dataclass
class AttemptSpec:
    outcome: str = "VALID"
    kind: str = "normal"  # normal, fail_before_apply, fail_applied, stuck, aborted, telemetry
    overrun_ms: int = 412
    remediated: bool = False
    after_redeploy: bool = False
    flags: list[str] = field(default_factory=list)
    health_event: bool = False
    tm_loss: bool = False
    idle_before_ms: int = 0


@dataclass
class SynthConfig:
    workload: str = "ecommerce"
    campaign_index: int = 1
    t0_ms: int = 1_790_000_003_217
    warmup_s: int = 3600
    baseline_s: int = 7200
    trailing_s: int = 3600
    scenario: str = "full"  # full, clean
    seed: int = 7


def full_scenario(plan: dict) -> dict[str, list[AttemptSpec]]:
    """cell_key -> attempt specs. Cells are picked by run type so the plan permutation is respected."""
    by = {}
    for block in plan["blocks"]:
        for cell in block["cells"]:
            by[(block["block"], cell["run_type"])] = cell["cell_key"]
    sc: dict[str, list[AttemptSpec]] = {}
    sc[by[(1, "F1")]] = [AttemptSpec("GATE_FAILED", "fail_applied"), AttemptSpec()]
    sc[by[(2, "F4")]] = [AttemptSpec("TELEMETRY_FAILED", "telemetry", flags=["SERIES_GAP"]), AttemptSpec()]
    sc[by[(3, "F2")]] = [AttemptSpec("GATE_FAILED", "fail_before_apply"), AttemptSpec()]
    sc[by[(3, "F8")]] = [AttemptSpec(overrun_ms=90_000, flags=["OVERRUN"])]
    sc[by[(4, "F7")]] = [AttemptSpec("GATE_FAILED", "stuck", overrun_ms=130_000), AttemptSpec()]
    sc[by[(5, "F5")]] = [AttemptSpec("ABORTED", "aborted"), AttemptSpec()]
    sc[by[(6, "F7")]] = [AttemptSpec("VALID_REMEDIATED", remediated=True, flags=["REMEDIATED"])]
    sc[by[(7, "F6")]] = [AttemptSpec(health_event=True, flags=["HEALTH_EVENT"])]
    sc[by[(9, "F7")]] = [AttemptSpec(tm_loss=True, flags=["F7_TM_LOSS"])]
    first_b8 = plan["blocks"][7]["cells"][0]["cell_key"]
    sc.setdefault(first_b8, [AttemptSpec()])[0].idle_before_ms = 200_000
    return sc


class _Rng:
    def __init__(self, seed: int):
        self.g = np.random.Generator(np.random.PCG64(seed))

    def uuid4(self) -> str:
        b = bytearray(self.g.bytes(16))
        return str(uuid.UUID(bytes=bytes(b), version=4))

    def hex(self, n: int) -> str:
        return self.g.bytes((n + 1) // 2).hex()[:n]


# ----------------------------------------------------------------------------------------------
# Schedule
# ----------------------------------------------------------------------------------------------

@dataclass
class Sched:
    spec: AttemptSpec
    cell: dict
    block: int
    attempt: int
    key: str
    run_type: str
    started: int
    segments: list[tuple[str, int, int]]
    planned: int
    onset: int | None
    labelled_end: int | None
    aborted_at: int | None
    ended: int
    applied: bool
    flags: list[str]
    settle_s: int
    crash_at: int | None = None


def _schedule(plan: dict, cfg: SynthConfig, scenario: dict[str, list[AttemptSpec]]):
    t = cfg.t0_ms
    camp_segs = [("WARMUP", t, t + cfg.warmup_s * 1000)]
    t += cfg.warmup_s * 1000
    camp_segs.append(("BASELINE", t, t + cfg.baseline_s * 1000))
    t += cfg.baseline_s * 1000
    camp_segs.append(("IDLE", t, t + 45_250))  # baseline export and statistics before block 1
    t += 45_250
    rts = {r["run_type"]: r for r in plan["run_types"]}
    reserve = list(plan["reserve_settle_gap_s"])
    scheds: list[Sched] = []
    pending_redeploy = False
    halts = []
    for block in plan["blocks"]:
        for cell in block["cells"]:
            specs = scenario.get(cell["cell_key"], [AttemptSpec()])
            for a_i, spec in enumerate(specs, start=1):
                if spec.idle_before_ms:
                    camp_segs.append(("IDLE", t, t + spec.idle_before_ms))
                    t += spec.idle_before_ms
                rt = cell["run_type"]
                key = f"{cell['cell_key']}-a{a_i}"
                settle_s = cell["settle_gap_s"] if a_i == 1 else reserve.pop(0)
                flags = list(spec.flags)
                if pending_redeploy:
                    flags.append("AFTER_REDEPLOY")
                    pending_redeploy = False
                settle_ms = settle_s * 1000 + (PHASE_S["redeploy_extra_settle"] * 1000 if "AFTER_REDEPLOY" in flags else 0)
                started = t
                segs = [("SETTLE", t, t + settle_ms)]
                t += settle_ms
                segs.append(("PRE_FAULT", t, t + 600_000))
                t += 600_000
                planned = t
                onset = labelled_end = aborted_at = crash_at = None
                applied = rt != "F0" and spec.kind != "fail_before_apply"
                if spec.kind == "fail_before_apply":
                    segs.append(("APPLY", t, t + 1_532))
                    t += 1_532
                    gate = 150_000
                    segs.append(("GATE", t, t + gate))
                    t += gate
                else:
                    apply_ms = 0 if rt == "F0" else (1_405 if rt == "F6" else 1_000 + (int(hashlib.sha256(key.encode()).hexdigest(), 16) % 2_000))
                    onset = t + apply_ms
                    segs.append(("APPLY", t, onset))
                    t = onset
                    dur = cell["sham_duration_s"] if rt == "F0" else rts[rt]["duration_s"]
                    if rt == "F0" or rt in ("F1", "F5"):
                        inj = dur * 1000
                    elif rt == "F6":
                        inj = 179_600
                    else:
                        inj = dur * 1000 + spec.overrun_ms
                    if spec.kind == "aborted":
                        crash_at = onset + 100_000
                        restart = crash_at + 117_000
                        segs.append(("INJECTION", t, restart))
                        t = restart
                        aborted_at = restart
                        halts.append({"at": crash_at / 1000, "reason": "orchestrator pod stopped (node pressure)",
                                      "attempt_key": key, "resumed_at": restart / 1000, "resolution": "RESUMED"})
                    else:
                        labelled_end = onset + inj
                        segs.append(("INJECTION", t, labelled_end))
                        t = labelled_end
                        segs.append(("RECOVERY", t, t + 300_000))
                        t += 300_000
                        segs.append(("COOLDOWN", t, t + 300_000))
                        t += 300_000
                        gate = 1_200_000 if spec.remediated else 152_731
                        segs.append(("GATE", t, t + gate))
                        t += gate
                ended = t
                sc = Sched(spec, cell, block["block"], a_i, key, rt, started, segs, planned, onset,
                           labelled_end, aborted_at, ended, applied, flags, settle_s, crash_at)
                scheds.append(sc)
                if spec.kind == "aborted":
                    camp_segs.append(("IDLE", t, t + 335_000))  # delete, confirm NotFound, wait 300 s, recovery gate
                    t += 335_000
                if spec.remediated:
                    pending_redeploy = True
    camp_segs.append(("IDLE", t, t + 30_000))
    t += 30_000
    camp_segs.append(("TRAILING", t, t + cfg.trailing_s * 1000))
    t += cfg.trailing_s * 1000
    return camp_segs, scheds, halts, t


# ----------------------------------------------------------------------------------------------
# Series instances and values
# ----------------------------------------------------------------------------------------------

@dataclass
class Inst:
    metric: str
    metric_type: str
    source: str
    role: str
    pod: str
    broker_id: int | None
    container: str | None
    labels: dict
    life: tuple[int, int]  # [start_ms, end_ms)
    kind: str  # value generator key
    counter: bool = False
    gaps: list[tuple[int, int]] = field(default_factory=list)
    resets_at: list[int] = field(default_factory=list)  # ms instants where the counter restarts low
    uids: list[tuple[int, str]] = field(default_factory=list)  # (from_ms, uid)
    params: dict = field(default_factory=dict)

    @property
    def labels_str(self) -> str:
        return canonical_json_bytes(self.labels).decode("utf-8")

    @property
    def series_id(self) -> str:
        return series_id_for(self.metric, self.pod, self.labels)


class World:
    """Pods, Flink epochs and fault intervals over the campaign."""

    def __init__(self, cfg: SynthConfig, plan: dict, scheds: list[Sched], end_ms: int, rng: _Rng):
        self.cfg, self.plan, self.scheds, self.end_ms, self.rng = cfg, plan, scheds, end_ms, rng
        self.insts: list[Inst] = []
        self.truth = {"gaps": [], "kills": [], "baseline_counter_reset": None, "tm_events": []}
        self.jobs = list(FLINK_JOBS[cfg.workload])
        self.effects: dict[str, list[tuple[int, int]]] = {rt: [] for rt in RUN_TYPES}
        for s in scheds:
            if s.applied and s.onset is not None:
                inj = [x for x in s.segments if x[0] == "INJECTION"][0]
                self.effects[s.run_type].append((inj[1], inj[2]))
        self.f5_kill_victims: dict[str, list[tuple[int, str, str]]] = {}
        self.f1_targets: dict[str, int] = {}
        self.pods_at: dict[str, list[str]] = {}
        self._build()

    # -- helpers ------------------------------------------------------------------------------
    def _flink_events(self):
        """Ordered Flink events: (time_ms, kind, attempt key, extra)."""
        ev = []
        for s in self.scheds:
            if s.run_type == "F5" and s.applied and s.onset is not None:
                inj_end = [x for x in s.segments if x[0] == "INJECTION"][0][2]
                for k in range(5):
                    tk = s.onset + 60_000 * k
                    if tk < inj_end and (s.crash_at is None or tk < s.crash_at):
                        ev.append((tk, "kill", s.key, s.cell["tm_kill_ranks"][k]))
            if s.spec.tm_loss and s.onset is not None:
                ev.append((s.onset + 120_000, "kill", s.key, 1))
            if s.spec.remediated:
                gate = [x for x in s.segments if x[0] == "GATE"][0]
                ev.append((gate[1] + 905_000, "redeploy", s.key, None))
        return sorted(ev)

    def _build(self):
        cfg, rng = self.cfg, self.rng
        t0, t_end = cfg.t0_ms, self.end_ms
        b0 = cfg.t0_ms + cfg.warmup_s * 1000  # BASELINE start
        global_gaps = [(b0 + 3_000_000, b0 + 3_020_000)]
        self.truth["gaps"].append({"scope": "all", "start_ms": global_gaps[0][0], "end_ms": global_gaps[0][1]})
        # Brokers -------------------------------------------------------------------------------
        broker_gaps = {i: list(global_gaps) for i in range(3)}
        broker_resets = {i: [] for i in range(3)}
        broker_uids = {i: [(t0 - 3_600_000, rng.uuid4())] for i in range(3)}
        for s in self.scheds:
            if s.run_type == "F1" and s.applied and s.onset is not None:
                followers = sorted(i for i in range(3) if i != CONTROLLER_NODE_ID)
                tgt = followers[s.cell["follower_rank"]]
                self.f1_targets[s.key] = tgt
                g = (s.onset + 3_000, s.onset + 63_000)
                broker_gaps[tgt].append(g)
                broker_resets[tgt].append(g[1])
                broker_uids[tgt].append((g[1], rng.uuid4()))
                self.truth["gaps"].append({"scope": f"broker-{tgt}", "start_ms": g[0], "end_ms": g[1], "attempt": s.key})
            if s.spec.kind == "telemetry":
                g = (s.onset + 100_000, s.onset + 160_000)
                broker_gaps[2].append(g)
                self.truth["gaps"].append({"scope": "broker-2", "start_ms": g[0], "end_ms": g[1], "attempt": s.key})
        self.broker_uids, self.broker_gaps, self.global_gaps = broker_uids, broker_gaps, global_gaps
        reset_at = b0 + 5_000_000
        self.truth["baseline_counter_reset"] = {"broker": 1, "metric": "kafka_server_brokertopicmetrics_messagesinpersec_total",
                                                "topic": "raw-events", "at_ms": reset_at}
        for i, pod in enumerate(BROKERS):
            common = {"kafka_broker": pod, "node": BROKER_NODES[i]}
            def add(metric, mtype, source, labels, kind, counter=False, extra_resets=()):
                self.insts.append(Inst(metric, mtype, source, "broker", pod, i, "kafka", {**common, **labels},
                                       (t0, t_end), kind, counter, list(broker_gaps[i]),
                                       sorted(list(broker_resets[i]) + list(extra_resets)) if counter else [],
                                       list(broker_uids[i]), {"broker": i}))
            for topic in ("raw-events", "enriched-events"):
                extra = (reset_at,) if (i == 1 and topic == "raw-events") else ()
                add("kafka_server_brokertopicmetrics_messagesinpersec_total", "counter", "broker", {"topic": topic},
                    f"msgs:{topic}", True, extra)
            add("kafka_server_brokertopicmetrics_messagesinpersec_total", "counter", "broker", {}, "msgs:all", True)
            add("kafka_server_brokertopicmetrics_bytesinpersec_total", "counter", "broker", {"topic": "raw-events"}, "bytes", True)
            add("kafka_server_replicamanager_isrshrinks_total", "counter", "broker", {}, "isr_shrink", True)
            add("kafka_server_replicamanager_isrexpands_total", "counter", "broker", {}, "isr_expand", True)
            add("kafka_server_replicamanager_partitioncount", "gauge", "broker", {}, "partitions")
            add("kafka_server_replicamanager_underreplicatedpartitions", "gauge", "broker", {}, "urp")
            add("jvm_memory_used_bytes", "gauge", "jvm", {"area": "heap"}, "bheap")
            add("jvm_memory_used_bytes", "gauge", "jvm", {"area": "nonheap"}, "bnonheap")
            add("jvm_gc_collection_seconds_count", "summary", "jvm", {"gc": "G1 Young Generation"}, "bgc", True)
            add("jvm_threads_current", "gauge", "jvm", {}, "bthreads")
        # A series with no pod (cluster scope).
        self.insts.append(Inst("kafka_topic_partitions", "unknown", "broker", "other", "", None, None,
                               {"topic": "raw-events"}, (t0, t_end), "const12", False, list(global_gaps)))
        # Flink: pods, epochs, series --------------------------------------------------------------
        tm_seq = [0]
        def new_tm(start, gen):
            tm_seq[0] += 1
            n = tm_seq[0]
            return {"pod": f"streaming-flink-taskmanager-{gen}-{n}", "node": FLINK_NODES[(n - 1) % 3],
                    "uid": rng.uuid4(), "start": start, "end": t_end}
        gen = 1
        tms = [new_tm(t0, gen) for _ in range(3)]
        all_tms = list(tms)
        jm = {"pod": f"streaming-flink-{rng.hex(10)}-{rng.hex(5)}", "node": FLINK_NODES[0], "uid": rng.uuid4(),
              "start": t0, "end": t_end, "job_ids": {j: rng.hex(32) for j in self.jobs}}
        jms = [jm]
        epochs = []  # task epochs: (start, end, attempt_num, assignment pods, job_ids)
        ep_start, attempt_num = t0, 0
        events = self._flink_events()
        cur_assign = [p["pod"] for p in sorted(tms, key=lambda x: x["pod"])]
        for tk, kind, akey, rank in events:
            if kind == "kill":
                alive = sorted([p for p in tms if p["start"] <= tk < p["end"]], key=lambda x: x["pod"])
                victim = alive[rank % len(alive)]
                victim["end"] = tk + 1_000
                rep = new_tm(tk + 25_000, gen)
                tms.append(rep)
                all_tms.append(rep)
                self.f5_kill_victims.setdefault(akey, []).append((tk, victim["pod"], victim["uid"]))
                self.truth["kills"].append({"attempt": akey, "at_ms": tk, "victim": victim["pod"], "replacement": rep["pod"]})
                epochs.append((ep_start, tk + 1_000, attempt_num, cur_assign, dict(jms[-1]["job_ids"])))
                self.truth["gaps"].append({"scope": "tm_tasks", "start_ms": tk + 1_000, "end_ms": tk + 31_000, "attempt": akey})
                ep_start, attempt_num = tk + 31_000, attempt_num + 1
                live = sorted([p for p in tms if p["start"] <= ep_start < p["end"]], key=lambda x: x["pod"])
                cur_assign = [p["pod"] for p in live][:3]
            else:
                r = tk
                for p in tms:
                    if p["end"] > r:
                        p["end"] = r
                jms[-1]["end"] = r
                epochs.append((ep_start, r, attempt_num, cur_assign, dict(jms[-1]["job_ids"])))
                gen += 1
                new = [new_tm(r + 60_000, gen) for _ in range(3)]
                tms.extend(new)
                all_tms.extend(new)
                jms.append({"pod": f"streaming-flink-{rng.hex(10)}-{rng.hex(5)}", "node": FLINK_NODES[1],
                            "uid": rng.uuid4(), "start": r + 60_000, "end": t_end,
                            "job_ids": {j: rng.hex(32) for j in self.jobs}})
                self.truth["gaps"].append({"scope": "flink_all", "start_ms": r, "end_ms": r + 70_000, "attempt": akey})
                ep_start, attempt_num = r + 70_000, 0
                cur_assign = [p["pod"] for p in sorted(new, key=lambda x: x["pod"])]
        epochs.append((ep_start, t_end, attempt_num, cur_assign, dict(jms[-1]["job_ids"])))
        self.all_tms, self.jms = all_tms, jms
        tm_by_pod = {p["pod"]: p for p in all_tms}
        for p in all_tms:
            lab = {"flink_component": "taskmanager", "node": p["node"]}
            for metric, kind, counter in (("flink_taskmanager_Status_JVM_CPU_Load", "cpu", False),
                                          ("flink_taskmanager_Status_JVM_Memory_Heap_Used", "heap", False),
                                          ("flink_taskmanager_Status_JVM_GarbageCollector_G1_Young_Generation_Count", "gc", True)):
                self.insts.append(Inst(metric, "gauge", "jvm", "taskmanager", p["pod"], None, "flink-main-container",
                                       dict(lab), (p["start"], p["end"]), kind, counter, list(global_gaps), [],
                                       [(p["start"], p["uid"])], {"pod": p["pod"]}))
        for j in jms:
            lab = {"flink_component": "jobmanager", "node": j["node"]}
            self.insts.append(Inst("flink_jobmanager_numRegisteredTaskManagers", "gauge", "flink", "jobmanager", j["pod"],
                                   None, "flink-main-container", dict(lab), (j["start"], j["end"]), "ntm", False,
                                   list(global_gaps), [], [(j["start"], j["uid"])]))
            self.insts.append(Inst("flink_jobmanager_Status_JVM_CPU_Load", "gauge", "jvm", "jobmanager", j["pod"],
                                   None, "flink-main-container", dict(lab), (j["start"], j["end"]), "jmcpu", False,
                                   list(global_gaps), [], [(j["start"], j["uid"])]))
            for jn, jid in j["job_ids"].items():
                jl = {**lab, "job_id": jid, "job_name": jn}
                self.insts.append(Inst("flink_jobmanager_job_lastCheckpointDuration", "gauge", "flink", "jobmanager",
                                       j["pod"], None, "flink-main-container", dict(jl), (j["start"], j["end"]),
                                       "ckpt", False, list(global_gaps), [], [(j["start"], j["uid"])], {"job": jn}))
                self.insts.append(Inst("flink_jobmanager_job_numberOfCompletedCheckpoints", "gauge", "flink", "jobmanager",
                                       j["pod"], None, "flink-main-container", dict(jl), (j["start"], j["end"]),
                                       "nckpt", True, list(global_gaps), [], [(j["start"], j["uid"])], {"job": jn}))
        task_metrics = [
            ("flink_taskmanager_job_task_numRecordsInPerSecond", "flink", "rin", False, False),
            ("flink_taskmanager_job_task_numRecordsOutPerSecond", "flink", "rout", False, False),
            ("flink_taskmanager_job_task_busyTimeMsPerSecond", "flink", "busy", False, False),
            ("flink_taskmanager_job_task_idleTimeMsPerSecond", "flink", "idle", False, False),
            ("flink_taskmanager_job_task_backPressuredTimeMsPerSecond", "flink", "bp", False, False),
            ("flink_taskmanager_job_task_numRecordsIn", "flink", "nrin", True, False),
            ("flink_taskmanager_job_task_operator_currentInputWatermark", "flink", "wm", False, True),
            ("flink_taskmanager_job_task_operator_KafkaSourceReader_KafkaConsumer_records_lag_max", "consumer", "lag", False, True),
            ("flink_taskmanager_job_task_operator_KafkaSourceReader_KafkaConsumer_records_consumed_total", "consumer", "consumed", True, True),
            ("flink_taskmanager_job_task_operator_KafkaSourceReader_KafkaConsumer_rebalance_total", "consumer", "rebal", True, True),
        ]
        for (ea, eb, anum, assign, job_ids) in epochs:
            if eb <= ea:
                continue
            for jn in self.jobs:
                task_id = hashlib.sha256(f"{self.cfg.workload}/{jn}/task".encode()).hexdigest()[:32]
                op_id = hashlib.sha256(f"{self.cfg.workload}/{jn}/op".encode()).hexdigest()[:32]
                for sub, pod in enumerate(assign):
                    p = tm_by_pod[pod]
                    life = (max(ea, p["start"]), min(eb, p["end"]))
                    if life[1] <= life[0]:
                        continue
                    att_id = hashlib.sha256(f"{jn}/{sub}/{anum}/{ea}".encode()).hexdigest()[:32]
                    base_l = {"flink_component": "taskmanager", "node": p["node"], "job_id": job_ids[jn], "job_name": jn,
                              "task_id": task_id, "task_name": f"Source: {jn} -> Map", "subtask_index": str(sub),
                              "task_attempt_num": str(anum), "task_attempt_id": att_id}
                    for metric, source, kind, counter, operator in task_metrics:
                        lab = dict(base_l)
                        if operator:
                            lab.update({"operator_id": op_id, "operator_name": "Source: raw-events"})
                        self.insts.append(Inst(metric, "gauge", source, "taskmanager", pod, None, "flink-main-container",
                                               lab, life, kind, counter, list(global_gaps), [], [(p["start"], p["uid"])],
                                               {"job": jn, "sub": sub}))
        self.epochs = epochs

    # -- values --------------------------------------------------------------------------------
    def active(self, rt: str, tt_ms: np.ndarray) -> np.ndarray:
        m = np.zeros(len(tt_ms), dtype=bool)
        for a, b in self.effects[rt]:
            m |= (tt_ms >= a) & (tt_ms < b)
        return m

    def series_rows(self, inst: Inst, t0_grid: int, rng: np.random.Generator):
        """Return (ts_ms array, value array, true_rate array) for one instance."""
        a, b = inst.life
        i0 = -((-(a - t0_grid)) // 10_000)
        i1 = -((-(b - t0_grid)) // 10_000)
        steps = np.arange(i0, i1, dtype=np.int64)
        tt = t0_grid + steps * 10_000
        keep = np.ones(len(tt), dtype=bool)
        for ga, gb in inst.gaps:
            keep &= ~((tt >= ga) & (tt < gb))
        load = 1.0 + 0.3 * np.sin(2 * np.pi * (tt / 1000.0) / 1800.0)
        noise = rng.normal(0.0, 1.0, len(tt))
        act = {rt: self.active(rt, tt) for rt in ("F1", "F2", "F4", "F5", "F6", "F7", "F8")}
        k = inst.kind
        rate = None
        if k.startswith("msgs") or k == "bytes":
            per = 3000.0 * load * (1 + 0.02 * noise)
            per = np.where(act["F4"] | act["F5"], per * 0.7, per)
            if k == "msgs:all":
                per = per * 2.0
            if k == "bytes":
                per = per * 512.0
            rate = np.round(per, 3)
        elif k in ("isr_shrink", "isr_expand"):
            rate = np.where(act["F1"] | act["F2"], 0.1, 0.0)
        elif k == "bgc":
            rate = np.round(0.2 * load, 4)
        elif k == "gc":
            rate = np.round(np.where(act["F8"], 3.0, 0.5) * load, 4)
        elif k == "nrin":
            rate = np.round(1000.0 * load * (1 + 0.01 * noise), 3)
        elif k == "consumed":
            rate = np.round(1000.0 * load, 3)
        elif k == "rebal":
            rate = np.zeros(len(tt))
            rate[::360] = 0.1
        elif k == "nckpt":
            rate = np.where((steps % 6) == 0, 0.1, 0.0)
        if rate is not None:
            inc = rate * 10.0
            vals = np.empty(len(tt))
            resets = sorted({int(np.searchsorted(tt, r)) for r in inst.resets_at if np.searchsorted(tt, r) < len(tt)})
            true_rate = rate.astype(np.float64).copy()
            start, acc0 = 0, 1_000_000.0
            for r in resets + [len(tt)]:
                vals[start:r] = acc0 + np.cumsum(inc[start:r])
                if r < len(tt):
                    acc0 = 3.0 + rate[r] * 10.0 * 0.37 - inc[r]
                    true_rate[r] = np.nan
                    start = r
            # A rate exists only where the previous step is also present and no reset happened.
            prev = np.r_[False, keep[:-1]]
            true_rate = np.where(keep & prev, true_rate, np.nan)
            return tt[keep], vals[keep], true_rate[keep]
        if k == "partitions":
            v = np.full(len(tt), 12.0)
        elif k == "urp":
            v = np.where(act["F1"] | act["F2"], 4.0, 0.0)
        elif k == "bheap":
            v = np.round(4e8 * load + 1e7 * noise)
        elif k == "bnonheap":
            v = np.round(1.2e8 + 1e6 * noise)
        elif k == "bthreads":
            v = np.round(120 + 2 * noise)
        elif k == "const12":
            v = np.full(len(tt), 12.0)
        elif k == "cpu":
            v = np.round(np.where(act["F4"], 0.95, 0.3 * load + 0.02 * noise), 4)
        elif k == "heap":
            v = np.round(np.where(act["F8"], 3.0e9, 1.0e9 * load) + 1e7 * noise)
        elif k == "ntm":
            v = np.full(len(tt), 3.0)
        elif k == "jmcpu":
            v = np.round(0.1 + 0.01 * noise, 4)
        elif k == "ckpt":
            v = np.round(np.where(act["F7"], 30000.0, np.where(act["F8"], 3000.0, 800.0)) + 20 * noise)
        elif k == "rin":
            v = np.round(1000.0 * load * (1 + 0.01 * noise), 3)
        elif k == "rout":
            v = np.round(np.where(act["F4"], 600.0, 980.0) * load, 3)
        elif k == "busy":
            v = np.round(np.where(act["F4"], 900.0, np.where(act["F7"], 450.0, 300.0 * load)) + 5 * noise, 3)
        elif k == "bp":
            v = np.round(np.where(act["F7"], 400.0, np.where(act["F4"], 80.0, 20.0)) + noise, 3)
        elif k == "idle":
            v = np.round(np.clip(1000.0 - 300.0 * load - 20.0 + 5 * noise, 0, None), 3)
        elif k == "wm":
            v = np.round(tt - np.where(act["F4"], 30000.0, 2000.0) - 100 * np.abs(noise))
        elif k == "lag":
            v = np.round(np.where(act["F4"], 20000.0, np.where(act["F7"], 8000.0, np.where(act["F5"], 15000.0, 500.0 * load))) + 10 * np.abs(noise))
        else:
            raise ValueError(k)
        return tt[keep], v[keep].astype(np.float64), None


# ----------------------------------------------------------------------------------------------
# Writers
# ----------------------------------------------------------------------------------------------

METRIC_SCHEMA = pa.schema([
    ("ts", pa.int64()), ("series_id", pa.string()), ("metric", pa.string()), ("metric_type", pa.string()),
    ("source", pa.string()), ("role", pa.string()), ("pod", pa.string()), ("pod_uid", pa.string()),
    ("broker_id", pa.int32()), ("container", pa.string()), ("labels", pa.string()), ("value", pa.float64()),
])
PROBE_SCHEMA = pa.schema([
    ("ts", pa.float64()), ("probe", pa.string()), ("src_pod", pa.string()), ("dst", pa.string()),
    ("tried_count", pa.int32()), ("ok_count", pa.int32()), ("timeout_s", pa.float64()),
    ("latency_s", pa.float64()), ("value", pa.float64()), ("detail", pa.string()), ("error", pa.string()),
])


class _Rows:
    """All feature samples of the campaign as arrays, with per-instance string metadata."""

    def __init__(self, insts: list[Inst], world: World, t0_grid: int, seed: int):
        g = np.random.Generator(np.random.PCG64(seed + 1))
        ts, inst, val = [], [], []
        self.truth_rates = []
        for i, it in enumerate(insts):
            t, v, tr = world.series_rows(it, t0_grid, g)
            if len(t) == 0:
                continue
            ts.append(t)
            inst.append(np.full(len(t), i, dtype=np.int64))
            val.append(v)
            if tr is not None:
                self.truth_rates.append((it.series_id, t, tr))
        self.ts = np.concatenate(ts)
        self.inst = np.concatenate(inst)
        self.val = np.concatenate(val)
        self.insts = insts
        sids = [it.series_id for it in insts]
        if len(set(sids)) != len(sids):
            raise RuntimeError("generator produced two instances with one series_id")
        order = np.argsort(np.array(sids, dtype=object), kind="stable")
        self.rank = np.empty(len(sids), dtype=np.int64)
        self.rank[order] = np.arange(len(sids))
        self.sids = sids

    def table(self, lo_ms: int, hi_ms: int) -> pa.Table:
        m = (self.ts >= lo_ms) & (self.ts <= hi_ms)
        ts, inst, val = self.ts[m], self.inst[m], self.val[m]
        o = np.lexsort((self.rank[inst], ts))
        ts, inst, val = ts[o], inst[o], val[o]
        its = self.insts
        uniq, inv = np.unique(inst, return_inverse=True)
        inv32 = pa.array(inv.astype(np.int32), pa.int32())

        def col(values: list, typ=pa.string()) -> pa.Array:
            return pa.DictionaryArray.from_arrays(inv32, pa.array(values, typ)).cast(typ)

        uid_col = np.empty(len(ts), dtype=object)
        for k, i in enumerate(uniq):
            rows_k = np.flatnonzero(inv == k)
            uids = its[i].uids
            if len(uids) == 1:
                uid_col[rows_k] = uids[0][1]
            else:
                starts = np.array([u[0] for u in uids])
                j = np.searchsorted(starts, ts[rows_k], side="right") - 1
                uid_col[rows_k] = [uids[x][1] if x >= 0 else None for x in j]
        return pa.table({
            "ts": pa.array(ts // 1000, pa.int64()),
            "series_id": col([self.sids[i] for i in uniq]),
            "metric": col([its[i].metric for i in uniq]),
            "metric_type": col([its[i].metric_type for i in uniq]),
            "source": col([its[i].source for i in uniq]),
            "role": col([its[i].role for i in uniq]),
            "pod": col([its[i].pod for i in uniq]),
            "pod_uid": pa.array(list(uid_col), pa.string()),
            "broker_id": col([its[i].broker_id for i in uniq], pa.int32()),
            "container": col([its[i].container for i in uniq]),
            "labels": col([its[i].labels_str for i in uniq]),
            "value": pa.array(val, pa.float64()),
        }, schema=METRIC_SCHEMA)


def _pods_at(world: "World", t_ms: int) -> list[tuple[str, str, str, int | None, bool]]:
    """(pod, uid, role, broker id, scraped) of every broker, JobManager and TaskManager pod alive at t_ms."""
    out = []
    in_gap = lambda gaps: any(a <= t_ms < b for a, b in gaps)  # noqa: E731
    for i, pod in enumerate(BROKERS):
        uid = [u for since, u in world.broker_uids[i] if since <= t_ms][-1]
        out.append((pod, uid, "broker", i, not in_gap(world.broker_gaps[i])))
    for j in world.jms:
        if j["start"] <= t_ms < j["end"]:
            out.append((j["pod"], j["uid"], "jobmanager", None, not in_gap(world.global_gaps)))
    for p in world.all_tms:
        if p["start"] <= t_ms < p["end"]:
            out.append((p["pod"], p["uid"], "taskmanager", None, not in_gap(world.global_gaps)))
    return out


def _verification_table(lo_ms: int, hi_ms: int, world: "World") -> pa.Table:
    """Controller and URP copies, plus kube-state pod info and restarts and Prometheus `up` per scrape target, the
    inputs of the scrape-health and NORMAL-time rules (C22, C26)."""
    t = np.arange(ceil10(lo_ms), floor10(hi_ms) + 1, 10_000, dtype=np.int64)
    cols = {k: [] for k in ("ts", "series_id", "metric", "metric_type", "source", "role", "pod", "pod_uid", "broker_id",
                            "container", "labels", "value")}

    def add(tt, metric, mtype, source, role, pod, uid, bid, container, lab, val):
        cols["ts"].append(int(tt // 1000)); cols["series_id"].append(series_id_for(metric, pod, lab))
        cols["metric"].append(metric); cols["metric_type"].append(mtype); cols["source"].append(source)
        cols["role"].append(role); cols["pod"].append(pod); cols["pod_uid"].append(uid); cols["broker_id"].append(bid)
        cols["container"].append(container); cols["labels"].append(canonical_json_bytes(lab).decode()); cols["value"].append(val)
    for tt in t:
        add(tt, "kafka_controller_kafkacontroller_activecontrollercount", "gauge", "kafka_controller", "broker", BROKERS[0],
            None, 0, "kafka", {}, 1.0)
        add(tt, "kafka_server_replicamanager_underreplicatedpartitions", "gauge", "kafka_broker_copy", "broker", BROKERS[0],
            None, 0, "kafka", {}, 0.0)
        for pod, uid, role, bid, scraped in _pods_at(world, int(tt)):
            container = "kafka" if role == "broker" else "flink-main-container"
            add(tt, "kube_pod_info", "gauge", "kube_state", role, pod, uid, bid, None, {"uid_key": uid[:8]}, 1.0)
            add(tt, "kube_pod_container_status_restarts_total", "counter", "kube_state", role, pod, uid, bid, container,
                {"container": container, "uid_key": uid[:8]}, 0.0)
            lab = {"kafka_broker": pod} if role == "broker" else {"flink_component": role}
            add(tt, "up", "gauge", "scrape", role, pod, None, bid, None, lab, 1.0 if scraped else 0.0)
    order = sorted(range(len(cols["ts"])), key=lambda i: (cols["ts"][i], cols["series_id"][i]))
    return pa.table({k: pa.array([v[i] for i in order], METRIC_SCHEMA.field(k).type) for k, v in cols.items()},
                    schema=METRIC_SCHEMA)


def _probe_table(lo_ms: int, hi_ms: int, world: "World") -> pa.Table:
    """Every 10 s: TCP probes between the brokers (9090, 9091) and from the load generator (9092), a packet probe per
    TaskManager, the Flink job list with the live JobManager's job IDs, the registered TaskManagers, the load generator and
    the consumer, and the controller every 60 s. All healthy: the scenarios put their faults into the feature series."""
    rows = {k: [] for k in PROBE_SCHEMA.names}

    def add(ts, probe, src, dst, tried, ok, lat, val, detail):
        rows["ts"].append(ts); rows["probe"].append(probe); rows["src_pod"].append(src); rows["dst"].append(dst)
        rows["tried_count"].append(tried); rows["ok_count"].append(ok); rows["timeout_s"].append(0.5)
        rows["latency_s"].append(lat); rows["value"].append(val); rows["detail"].append(detail)
        rows["error"].append(None)
    t = ceil10(lo_ms)
    while t < hi_ms:
        ts = t / 1000.0 + 0.137
        for src in BROKERS:
            for dst in BROKERS:
                if dst != src:
                    for port in (9090, 9091):
                        add(ts, "tcp", src, f"{dst}:{port}", 1, 1, 0.0012, None, None)
        for dst in BROKERS:
            add(ts, "tcp", "load-generator-7c9d5-x2k4p", f"{dst}:9092", 1, 1, 0.0011, None, None)
        jm = next((j for j in world.jms if j["start"] <= t < j["end"]), None)
        for p in world.all_tms:
            if p["start"] <= t < p["end"]:
                add(ts, "tcp_burst", p["pod"], f"{jm['pod'] if jm else 'jobmanager'}:6123", 10, 10, 0.0009, None, None)
        if jm is not None:
            jobs = [{"name": n, "job_id": jid, "state": "RUNNING"} for n, jid in sorted(jm["job_ids"].items())]
            add(ts, "flink_jobs", None, "streaming-flink-rest:8081", 1, 1, 0.01, 3.0, canonical_json_bytes(jobs).decode())
            add(ts, "flink_tms", None, "streaming-flink-rest:8081", 1, 1, 0.01, 3.0,
                canonical_json_bytes({"slots_total": 12, "slots_available": 0}).decode())
        add(ts, "loadgen", None, "load-generator:9102", 1, 1, 0.003, float(30 * (t - lo_ms)), None)
        add(ts, "consumer", None, "verification-consumer:9103", 1, 1, 0.003, 0.0,
            canonical_json_bytes({"started_at": world.cfg.t0_ms / 1000, "dead_letter_count": 0, "malformed_count": 0,
                                  "malformed_by_attempt": {}}).decode())
        if (t // 10_000) % 6 == 0:
            add(ts, "controller", BROKERS[0], None, 1, 1, None, float(CONTROLLER_NODE_ID), None)
        t += 10_000
    return pa.table({k: pa.array(v, PROBE_SCHEMA.field(k).type) for k, v in rows.items()}, schema=PROBE_SCHEMA)


def _write_table(table: pa.Table, path: Path) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    pq.write_table(table.replace_schema_metadata(None), path, compression="zstd")


def _write_json(obj, path: Path, canonical: bool = False) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    data = canonical_json_bytes(obj) if canonical else (json.dumps(obj, indent=1, sort_keys=True) + "\n").encode()
    path.write_bytes(data)


def _chunks(a: int, b: int) -> list[tuple[int, int]]:
    out, c = [], a
    while c < b:
        e = min(c + 3_600_000, b)
        out.append((c, e))
        c = e
    return out


def rehash_campaign(root: Path) -> None:
    """Rewrite checksums/SHA256SUMS and COMPLETE after editing a campaign (used by tests)."""
    root = Path(root)
    complete_path = root / "COMPLETE"
    old = json.loads(complete_path.read_text()) if complete_path.exists() else None
    keys = []
    for p in sorted(root.rglob("*")):
        if p.is_file():
            rel = p.relative_to(root).as_posix()
            if rel in ("COMPLETE", "checksums/SHA256SUMS"):
                continue
            keys.append(rel)
    keys.sort()
    lines = [f"{sha256_file(root / k)}  {k}\n" for k in keys]
    sums = "".join(lines).encode()
    (root / "checksums").mkdir(exist_ok=True)
    (root / "checksums" / "SHA256SUMS").write_bytes(sums)
    total = sum((root / k).stat().st_size for k in keys)
    manifest = json.loads((root / "manifest" / "manifest.json").read_text())
    upd = manifest["updated_at"]
    complete = {
        "schema_version": "1", "campaign_id": manifest["campaign_id"], "status": "COMPLETE",
        "written_at": round(upd + 60.0, 3), "plan_sha256": sha256_file(root / "plan" / "plan.json"),
        "manifest_sha256": sha256_file(root / "manifest" / "manifest.json"),
        "checksums_key": "checksums/SHA256SUMS", "checksums_sha256": sha256_bytes(sums),
        "object_count": len(keys), "total_bytes": total, "newest_object_at": round(upd + 30.0, 3),
        "checks": (old or {}).get("checks") or [
            {"name": "every FINISHED attempt has its records", "pass": True},
            {"name": "baseline and trailing chunks cover their segments", "pass": True},
            {"name": "attempt export rows lie inside their span", "pass": True},
            {"name": "timeline covers every second", "pass": True},
        ],
    }
    complete_path.write_bytes(canonical_json_bytes(complete))


def generate_campaign(out_dir: Path, cfg: SynthConfig, truth_dir: Path | None = None) -> dict:
    """Write one campaign under out_dir (the local mirror of campaign/<campaign_id>/). Returns truth."""
    out_dir = Path(out_dir)
    rng = _Rng(cfg.seed)
    plan = generate_plan(cfg.workload, cfg.campaign_index, RUN_TYPE_SPECS, PHASE_S)
    cid = plan["campaign_id"]
    scenario = full_scenario(plan) if cfg.scenario == "full" else {}
    camp_segs, scheds, halts, end_ms = _schedule(plan, cfg, scenario)
    world = World(cfg, plan, scheds, end_ms, rng)
    t0_grid = floor10(cfg.t0_ms)
    rows = _Rows(world.insts, world, t0_grid, cfg.seed)

    plan_bytes = canonical_json_bytes(plan)
    (out_dir / "plan").mkdir(parents=True, exist_ok=True)
    (out_dir / "plan" / "plan.json").write_bytes(plan_bytes)
    plan_sha = sha256_bytes(plan_bytes)
    (out_dir / "plan" / "plan.json.sha256").write_bytes((plan_sha + "\n").encode())
    _write_json({"kubernetes": "1.35", "ami_release": "1.35.0-20260901", "charts": {"strimzi": "0.45.2"}},
                out_dir / "run" / "versions.json")
    (out_dir / "run" / "images.lock.yaml").write_bytes(b"images:\n  - name: kafka\n    digest: sha256:00\n")
    (out_dir / "run" / "feature_allowlist.yaml").write_bytes(
        b"broker: ['kafka_server_.*', 'kafka_topic_.*']\nconsumer: ['.*KafkaConsumer.*']\nflink: ['flink_.*']\njvm: ['jvm_.*', '.*Status_JVM.*']\n")
    thresholds = {"F1": {"probe_fail_min": 1}, "F4": {"cpu_ratio_min": 2.0, "cpu_cores_min": 1.6}, "gate": SYNTH_GATE}
    _write_json(thresholds, out_dir / "run" / "thresholds.json")
    thr_sha = sha256_file(out_dir / "run" / "thresholds.json")

    seg = {t: (a, b) for t, a, b in camp_segs if t in ("WARMUP", "BASELINE", "TRAILING")}
    base_a, base_b = seg["BASELINE"]
    stats = {
        "schema_version": "1", "campaign_id": cid,
        "window": {"start_at": base_a / 1000, "end_at": base_b / 1000},
        "queries": {"raw_events_in_per_s": "sum(rate(kafka_server_brokertopicmetrics_messagesinpersec_total{topic=\"raw-events\"}[60s]))",
                    "consumer_lag_max": "max(flink_taskmanager_job_task_operator_KafkaSourceReader_KafkaConsumer_records_lag_max)"},
        "raw_events_in_per_s": {"mean": 3000.0, "sd": 600.0, "p05": 2200.0, "p95": 3800.0},
        "consumer_lag_max": {"mean": 520.0, "sd": 110.0},
        "f6_rate_per_s": 150.0, "f6_planned_count": 27000,
        "tm_working_set_bytes": {"streaming-flink-taskmanager-1-1": {"mean": 2.1e9, "sd": 1.0e8}},
        "computed_at": (base_b + 30_000) / 1000,
    }
    _write_json(stats, out_dir / "stats" / "baseline_stats.json")
    stats_sha = sha256_file(out_dir / "stats" / "baseline_stats.json")

    # Lead and trailing chunks: features, verification and probes.
    spans: list[tuple[int, int]] = []
    for typ, sub_f, sub_v, sub_p in (("lead", "baseline/features", "baseline/verification", "probes/baseline"),
                                     ("trail", "export/trailing/features", "export/trailing/verification", "probes/trailing")):
        segs = [seg["WARMUP"], seg["BASELINE"]] if typ == "lead" else [seg["TRAILING"]]
        for a, b in segs:
            for ca, cb in _chunks(a, b):
                name = f"chunk-{ca // 1000}.parquet"
                _write_table(rows.table(floor10(ca), ceil10(cb)), out_dir / sub_f / name)
                spans.append((floor10(ca), ceil10(cb)))
                _write_table(_verification_table(floor10(ca), ceil10(cb), world), out_dir / sub_v / name)
                _write_table(_probe_table(ca, cb, world), out_dir / sub_p / name)

    gate_commit, orch_commit = "1f2e3d4c5b6a7980", "a1b2c3d4e5f60718"
    attempts = []
    health_events = []
    for s in scheds:
        lo, hi = floor10(s.started), ceil10(s.ended)
        fpath = out_dir / "export" / "attempts" / s.key / "features.parquet"
        vpath = out_dir / "export" / "attempts" / s.key / "verification.parquet"
        ppath = out_dir / "attempts" / s.key / "probes.parquet"
        _write_table(rows.table(lo, hi), fpath)
        spans.append((lo, hi))
        vtab = _verification_table(lo, hi, world)
        _write_table(vtab, vpath)
        _write_table(_probe_table(s.started, s.ended, world), ppath)
        rec, att = _attempt_records(s, world, plan, cid, gate_commit, thr_sha, stats, rng, out_dir, vtab)
        if s.spec.health_event:
            ev_start = s.onset + 50_000
            health_events.append({"at": (ev_start + 600_000) / 1000, "service": "EC2",
                                  "event_type_code": "AWS_EC2_OPERATIONAL_ISSUE", "status": "closed",
                                  "event_key": hashlib.sha256(f"health-{s.key}".encode()).hexdigest()[:16],
                                  "start_at": ev_start / 1000, "end_at": (ev_start + 60_000) / 1000})
        vjson = out_dir / "attempts" / s.key / "verification.json"
        _write_json(rec, vjson)
        att["verification_key"] = f"attempts/{s.key}/verification.json"
        att["verification_sha256"] = sha256_file(vjson)
        att["export"] = {
            "features_key": f"export/attempts/{s.key}/features.parquet", "features_sha256": sha256_file(fpath),
            "verification_metrics_key": f"export/attempts/{s.key}/verification.parquet",
            "verification_metrics_sha256": sha256_file(vpath),
            "probes_key": f"attempts/{s.key}/probes.parquet", "probes_sha256": sha256_file(ppath),
            "status": "DONE",
        }
        attempts.append(att)

    log = io.BytesIO()
    with gzip.GzipFile(fileobj=log, mode="wb", mtime=0) as gz:
        gz.write(f"orchestrator log for {cid}\n".encode())
    (out_dir / "logs").mkdir(parents=True, exist_ok=True)
    (out_dir / "logs" / f"orchestrator-{cfg.t0_ms // 1000}.log.gz").write_bytes(log.getvalue())

    instance_ids = sorted(f"i-0{h}" for h in ("1a2b3c4d5e6f7a8b9", "2b3c4d5e6f7a8b9c0", "3c4d5e6f7a8b9c0d1",
                                               "4d5e6f7a8b9c0d1e2", "5e6f7a8b9c0d1e2f3", "6f7a8b9c0d1e2f3a4"))
    manifest = {
        "schema_version": "1", "campaign_id": cid, "workload": cfg.workload, "campaign_index": cfg.campaign_index,
        "cluster_name": cluster_name_for(cid), "region": "us-east-2", "availability_zone": "us-east-2b",
        "kubernetes_version": "1.35", "node_ami_release": "1.35.0-20260901", "plan_sha256": plan_sha,
        "seed": plan["seed"], "orchestrator": {"git_commit": orch_commit, "image_digest": "sha256:" + "ab" * 32},
        "gate": {"git_commit": gate_commit, "thresholds_sha256": thr_sha},
        "images_lock_sha256": sha256_file(out_dir / "run" / "images.lock.yaml"),
        "feature_allowlist_sha256": sha256_file(out_dir / "run" / "feature_allowlist.yaml"),
        "flink_jobs_expected": list(FLINK_JOBS[cfg.workload]),
        "created_at": (cfg.t0_ms - 600_000) / 1000, "updated_at": (end_ms + 120_000) / 1000,
        "timeline_start_at": cfg.t0_ms / 1000, "timeline_end_at": end_ms / 1000, "clock_skew_s": 0.012,
        "campaign_segments": [{"segment_type": t, "start_at": a / 1000, "end_at": b / 1000} for t, a, b in camp_segs],
        "baseline_stats_key": "stats/baseline_stats.json", "baseline_stats_sha256": stats_sha,
        "attempts": attempts, "halts": halts,
        "node_inventory": [{"at": (cfg.t0_ms - 300_000) / 1000, "instance_ids": instance_ids},
                           {"at": (end_ms - 60_000) / 1000, "instance_ids": instance_ids}],
        "health_events": health_events, "health_events_polled_until": (end_ms + 60_000) / 1000, "status": "COMPLETE",
    }
    _write_json(manifest, out_dir / "manifest" / "manifest.json")
    rehash_campaign(out_dir)

    truth = {
        "campaign_id": cid, "t0_grid_ms": t0_grid, "gaps": world.truth["gaps"], "kills": world.truth["kills"],
        "baseline_counter_reset": world.truth["baseline_counter_reset"], "f1_targets": world.f1_targets,
    }
    if truth_dir is not None:
        truth_dir = Path(truth_dir)
        truth_dir.mkdir(parents=True, exist_ok=True)
        _write_json(truth, truth_dir / "truth.json")
        merged: list[list[int]] = []
        for lo_, hi_ in sorted(spans):
            if merged and lo_ <= merged[-1][1]:
                merged[-1][1] = max(merged[-1][1], hi_)
            else:
                merged.append([lo_, hi_])
        m_lo = np.array([m[0] for m in merged], dtype=np.int64)
        m_hi = np.array([m[1] for m in merged], dtype=np.int64)

        def exported(t: np.ndarray) -> np.ndarray:
            j = np.searchsorted(m_lo, t, side="right") - 1
            return (j >= 0) & (t <= m_hi[np.clip(j, 0, None)])
        sid_c, ts_c, r_c = [], [], []
        for sid, t, tr in rows.truth_rates:
            # Only exported samples exist for the pipeline, and a rate needs the previous step exported too.
            keep = exported(t)
            prev_ok = exported(t - 10_000) & np.isin(t - 10_000, t)
            tr = np.where(prev_ok, tr, np.nan)
            t, tr = t[keep], tr[keep]
            sid_c.extend([sid] * len(t)); ts_c.append(t // 1000); r_c.append(tr)
        pq.write_table(pa.table({"series_id": pa.array(sid_c, pa.string()),
                                 "ts": pa.array(np.concatenate(ts_c), pa.int64()),
                                 "expected_rate": pa.array(np.concatenate(r_c), pa.float64())}),
                       truth_dir / "counter_truth.parquet")
    return truth


def _target_broker(s: Sched, world: "World") -> int:
    """The follower at the cell's rank, the F1 and F2 target (and the sham's pseudo-target)."""
    followers = sorted(i for i in range(3) if i != CONTROLLER_NODE_ID)
    return followers[s.cell["follower_rank"]]


def _attempt_records(s: Sched, world: World, plan: dict, cid: str, gate_commit: str, thr_sha: str,
                     stats: dict, rng: _Rng, out_dir: Path, vtab: pa.Table) -> tuple[dict, dict]:
    rt = s.run_type
    rts = {r["run_type"]: r for r in plan["run_types"]}
    segs = {t: (a, b) for t, a, b in s.segments}
    valid = s.spec.outcome in ("VALID", "VALID_REMEDIATED")
    dur = s.cell["sham_duration_s"] if rt == "F0" else rts[rt]["duration_s"]
    resources = []
    kills_rec = None
    if s.applied:
        inj = segs.get("INJECTION")
        end_ref = s.labelled_end if s.labelled_end is not None else s.ended
        delete_at = end_ref + 500
        nf_delay = 75_000 if s.spec.kind == "stuck" else 2_000
        if s.spec.kind == "aborted":
            delete_at, nf_delay = s.ended + 2_000, 2_000
        names = {"F1": [("PodChaos", f"pk-{s.key}")], "F2": [("NetworkChaos", f"nc-{s.key}")],
                 "F4": [("StressChaos", f"sc-{s.key}")], "F7": [("NetworkChaos", f"nc-{s.key}")],
                 "F8": [("StressChaos", f"sc-{s.key}-{n}") for n in (1, 2, 3)],
                 "F6": [("Job", f"poison-{s.key}")]}
        if rt == "F5":
            kills = world.f5_kill_victims.get(s.key, [])
            for k, (tk, pod, uid) in enumerate(kills, start=1):
                resources.append({"kind": "PodChaos", "name": f"pk-{s.key}-{k}", "created_at": (tk - 800) / 1000,
                                  "delete_called_at": delete_at / 1000, "not_found_at": (delete_at + nf_delay) / 1000})
            kills_rec = [{"k": k, "planned_at": (s.onset + 60_000 * (k - 1)) / 1000, "pod": pod, "uid": uid,
                          "apply_at": tk / 1000} for k, (tk, pod, uid) in enumerate(kills, start=1)]
        else:
            created = (s.planned - 45_000) if rt == "F6" else (s.planned + 50)
            for kind, name in names[rt]:
                resources.append({"kind": kind, "name": name, "created_at": created / 1000,
                                  "delete_called_at": delete_at / 1000, "not_found_at": (delete_at + nf_delay) / 1000})
    if rt == "F0":
        onset_source, end_source = "PLANNED", "PLANNED"
    elif rt == "F6":
        onset_source, end_source = "FIRST_PRODUCE_ACK", "LAST_PRODUCE_ACK"
    elif rt in ("F1", "F5"):
        onset_source, end_source = "CHAOS_APPLY_EVENT", "FIXED_SPAN"
    else:
        onset_source, end_source = "CHAOS_APPLY_EVENT", "CHAOS_RECOVER_EVENT"
    if s.onset is None:
        onset_source = None
    if s.labelled_end is None:
        end_source = None
    overrun = None
    if rt in ("F2", "F4", "F7", "F8") and s.labelled_end is not None:
        overrun = round((s.labelled_end - s.onset) / 1000 - dur, 3)
    failed_checks: list[str] = []
    checks = []
    result = {"mechanism": True, "signature": True, "envelope": True, "timing": True, "telemetry": True, "gate": True}

    def check(name, layer, passed, pre=1.0, during=1.0, thr=1.0, op=">="):
        checks.append({"name": name, "layer": layer, "target": "all", "series": "probe", "pre_value": pre,
                       "during_value": during, "op": op, "threshold": thr, "unit": "count", "pass": passed,
                       "reason": None})
        if not passed:
            failed_checks.append(name)

    check(f"{rt}_mechanism", "MECHANISM", s.spec.kind != "fail_before_apply")
    if s.spec.kind != "fail_before_apply":
        check(f"{rt}_signature", "SIGNATURE", s.spec.kind != "fail_applied")
        check(f"{rt}_envelope", "ENVELOPE", True)
        check(f"{rt}_timing_end", "TIMING", s.spec.kind != "stuck")
        check(f"{rt}_timing_notfound", "TIMING", s.spec.kind != "stuck")
    if s.spec.kind == "fail_before_apply":
        result.update(mechanism=False, signature=False, envelope=False, timing=False)
    if s.spec.kind == "fail_applied":
        result["signature"] = False
    if s.spec.kind == "stuck":
        result["timing"] = False
    telemetry = []
    for segname in ("PRE_FAULT", "INJECTION", "RECOVERY"):
        if segname not in segs:
            continue
        a, b = segs[segname]
        steps = np.arange(-((-(a - floor10(world.cfg.t0_ms))) // 10_000), -((-(b - floor10(world.cfg.t0_ms))) // 10_000))
        tt = floor10(world.cfg.t0_ms) + steps * 10_000
        for i, pod in enumerate(BROKERS):
            miss = np.zeros(len(tt), dtype=bool)
            for g in world.truth["gaps"]:
                if g["scope"] in ("all", f"broker-{i}"):
                    miss |= (tt >= g["start_ms"]) & (tt < g["end_ms"])
            present = int((~miss).sum())
            # C22: the fault's own targets are exempt in INJECTION and RECOVERY
            targeted = rt in ("F1", "F2") and _target_broker(s, world) == i and segname in ("INJECTION", "RECOVERY")
            telemetry.append({"source": f"broker:{pod}", "segment": segname, "expected_count": len(tt),
                              "present_count": present, "coverage_frac": present / len(tt) if len(tt) else 0.0,
                              "targeted": targeted})
    sc_alive, sc_up = scrape_index(vtab.to_pylist())
    tgt_exp = ([{"pod": BROKERS[_target_broker(s, world)]}] if rt in ("F1", "F2") else
               [{"pod": p["pod"]} for p in world.all_tms if p["start"] <= s.planned < p["end"]])
    scrapes = scrape_health_rows(sc_alive, sc_up, [{"segment_type": t, "start_at": a / 1000, "end_at": b / 1000}
                                                for t, a, b in s.segments], rt, tgt_exp, SYNTH_GATE["telemetry"])
    if s.spec.kind == "telemetry":
        # C22: one TELEMETRY-layer check per untargeted scrape row; the broker-2 scrape gap fails it
        result["telemetry"] = False
        check("telemetry_scrape_misses", "TELEMETRY", False, thr=3.0, op="<=")
    if s.spec.kind == "aborted":
        # C2 and C22: no scrape row at all means no telemetry verdict, so result.telemetry is false
        result = {"mechanism": False, "signature": False, "envelope": False, "timing": False, "telemetry": False,
                  "gate": False}
        checks, failed_checks, telemetry, scrapes = [], [], [], []
    result["gate"] = all(result[k] for k in ("mechanism", "signature", "envelope", "timing", "telemetry"))
    alive = sorted([p for p in world.all_tms if p["start"] <= s.planned < p["end"]], key=lambda x: x["pod"])
    tm_targets = [{"pod": p["pod"], "uid": p["uid"], "node": p["node"], "broker_id": None} for p in alive]
    if rt in ("F1", "F2", "F0"):
        followers = sorted(i for i in range(3) if i != CONTROLLER_NODE_ID)
        tgt = followers[s.cell["follower_rank"]]
        targets = [{"pod": BROKERS[tgt], "uid": "u-" + BROKERS[tgt], "node": BROKER_NODES[tgt], "broker_id": tgt}]
        controller = {"node_id_before_apply": CONTROLLER_NODE_ID, "node_id_after_injection": CONTROLLER_NODE_ID,
                      "method": "metadata-quorum", "metric_agrees": True}
    else:
        targets, controller = tm_targets, None
    pods = [{"pod": p["pod"], "uid": p["uid"], "node": p["node"], "ready": True, "restart_count": 0,
             "last_termination_reason": None, "started_at": p["start"] / 1000} for p in alive]
    snaps = []
    if s.applied and rt in ("F1", "F2", "F4", "F7", "F8"):
        name = resources[0]["name"]
        ykey = f"attempts/{s.key}/chaos/APPLY_PLUS_10S-{name}.yaml"
        (out_dir / ykey).parent.mkdir(parents=True, exist_ok=True)
        (out_dir / ykey).write_bytes(f"kind: {resources[0]['kind']}\nmetadata:\n  name: {name}\n".encode())
        cond = {k: {"status": "True", "last_transition_at": (s.planned + 2_000) / 1000} for k in ("Selected", "AllInjected")}
        cond.update({k: {"status": "False", "last_transition_at": None} for k in ("AllRecovered", "Paused")})
        snaps.append({"tag": "APPLY_PLUS_10S", "taken_at": (s.planned + 10_000) / 1000, "kind": resources[0]["kind"],
                      "name": name, "yaml_key": ykey, "conditions": cond,
                      "records": [{"id": f"streaming/{targets[0]['pod']}", "phase": "Injected",
                                   "first_apply_at": (s.onset or s.planned) / 1000, "last_recover_at": None}]})
    if rt == "F6" and s.applied:
        (out_dir / "attempts" / s.key).mkdir(parents=True, exist_ok=True)
        (out_dir / "attempts" / s.key / "injector.log").write_bytes(b"sent 26940 acked 26940\n")
    f6 = None
    if rt == "F6" and s.onset is not None:
        f6 = {"injector_image_digest": "sha256:" + "cd" * 32, "pod_ready_at": (s.planned - 35_000) / 1000,
              "rate_per_s": stats["f6_rate_per_s"], "planned_count": stats["f6_planned_count"],
              "sent_count": 26940, "acked_count": 26940, "first_produce_at": s.onset / 1000,
              "last_produce_at": (s.labelled_end or s.ended) / 1000, "consumer_count": 26900}
    f8 = None
    if rt == "F8" and s.onset is not None:
        f8 = {"per_pod": [{"pod": p["pod"], "limit_bytes": 8 * 2 ** 30, "ws_pre_bytes": 2 * 2 ** 30,
                           "stressor_bytes": 5 * 2 ** 30, "ws_during_bytes": 6 * 2 ** 30,
                           "increase_bytes": 4 * 2 ** 30,
                           "ws_samples": [{"ts": (s.planned - k * 10_000) // 1000, "value": 2.0 * 2 ** 30} for k in (6, 5, 4, 3, 2, 1)],
                           "limit_samples": [{"ts": (s.planned - k * 10_000) // 1000, "value": 8.0 * 2 ** 30} for k in (6, 5, 4, 3, 2, 1)],
                           "export_stressor_bytes": 5 * 2 ** 30} for p in alive]}
    f5 = None
    if rt == "F5" and s.onset is not None:
        f5 = {"kills": kills_rec or [], "new_tm_uid_count": len(kills_rec or []),
              "all_jobs_running_at": None if s.spec.kind == "aborted" else (s.onset + 280_000) / 1000}
    pseudo = None
    if rt == "F0":
        followers = sorted(i for i in range(3) if i != CONTROLLER_NODE_ID)
        pseudo = {"follower_pod": BROKERS[followers[s.cell["follower_rank"]]], "tm_pods": [p["pod"] for p in alive],
                  "tm_kill_ranks": list(s.cell["tm_kill_ranks"]), "f6_planned_count": stats["f6_planned_count"]}
    sigs = [{"run_type": r, "pass": (r == rt) if rt != "F0" else False} for r in ("F1", "F2", "F4", "F5", "F6", "F7", "F8")]
    rec = {
        "schema_version": "1", "attempt_key": s.key, "run_id": None, "run_type": rt, "mode": "ONLINE",
        "gate_git_commit": gate_commit, "thresholds_sha256": thr_sha, "evaluated_at": s.ended / 1000,
        "pseudo_targets": pseudo, "targets_expected": targets, "targets_selected": targets if s.applied or rt == "F0" else [],
        "controller": controller, "chaos_snapshots": snaps, "pods_before": pods, "pods_after": pods,
        "f5": f5, "f6": f6, "f8": f8, "checks": checks, "signatures_all_faults": sigs, "telemetry": telemetry,
        "scrapes": scrapes, "result": result,
        "recovery_gate_conditions": [{"name": "no_campaign_resources", "value": 0.0, "threshold": 0.0, "pass": True,
                                      "at": s.ended / 1000}],
    }
    run_id = rng.uuid4()
    rec["run_id"] = run_id
    gate_seg = segs.get("GATE")
    remediation = {"performed": False, "actions": [], "started_at": None, "ended_at": None}
    if s.spec.remediated:
        remediation = {"performed": True, "actions": ["preferred_leader_election", "resubmit_missing_jobs", "full_flink_redeploy"],
                       "started_at": (gate_seg[0] + 900_000) / 1000, "ended_at": (gate_seg[0] + 990_000) / 1000}
    last_jm = [j for j in world.jms if j["start"] <= s.ended][-1]
    att = {
        "attempt_key": s.key, "run_id": run_id, "cell_key": s.cell["cell_key"], "block": s.block,
        "position": s.cell["position"], "attempt": s.attempt, "run_type": rt, "fault_name": rts[rt]["name"],
        "status": "FINISHED", "outcome": s.spec.outcome, "failed_checks": failed_checks, "flags": s.flags,
        "started_at": s.started / 1000, "ended_at": s.ended / 1000, "planned_onset_at": s.planned / 1000,
        "apply_called_at": None if (rt == "F0" or not s.applied) else (((s.planned - 45_000) if rt == "F6" else (s.planned + 50)) / 1000),
        "onset_verified_at": None if s.onset is None else s.onset / 1000, "onset_source": onset_source,
        "configured_duration_s": dur,
        "labelled_end_at": None if s.labelled_end is None else s.labelled_end / 1000, "end_source": end_source,
        "overrun_s": overrun, "applied": s.applied,
        "aborted_at": None if s.aborted_at is None else s.aborted_at / 1000, "resources": resources,
        "controller_at_onset": {"node_id": CONTROLLER_NODE_ID, "pod": BROKERS[CONTROLLER_NODE_ID]} if rt in ("F0", "F1", "F2") and s.onset is not None else None,
        "segments": [{"segment_type": t, "start_at": a / 1000, "end_at": b / 1000} for t, a, b in s.segments],
        "recovery_gate": {"started_at": (gate_seg[0] if gate_seg else s.ended) / 1000,
                          "passed_at": None if s.spec.kind == "aborted" else s.ended / 1000,
                          "remediation": remediation},
        "flink_jobs_at_end": [{"name": jn, "job_id": jid, "state": "RUNNING"} for jn, jid in last_jm["job_ids"].items()],
        "verification_key": None, "verification_sha256": None, "export": None,
    }
    return rec, att
