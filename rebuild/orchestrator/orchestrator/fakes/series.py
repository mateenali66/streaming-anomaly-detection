"""The series Prometheus would hold for the simulated cluster, with the label sets and metric names the real exporters and
scrape jobs produce (rebuild/infra NAMES.md section 6, k8s/base/kafka/kafka-metrics-configmap.yaml,
k8s/base/helm-values/prometheus-values.yaml, the Flink 1.19 Prometheus reporter and kube-prometheus-stack defaults).

Why this matters: the preprocessing must see what it will see in the pilot, so its counter, naming, aggregation, gap and
collision rules run on the real shapes, not on a tidy stand-in. What is modelled:

  * Prometheus target labels: job, instance and pod (the pod name), namespace, node, plus kafka_broker on broker series and
    flink_component on Flink series (prometheus-values.yaml relabel_configs). cAdvisor and kube-state-metrics series carry the
    chart's endpoint, service and instance labels.
  * Kafka JMX exporter: BrokerTopicMetrics counters per topic and for all topics (`_total`, type counter), request counters
    whose `request` label also holds the version tag ("Produce, version=9": the R1 rule's `request=(.+)` is greedy over the
    bean's key list; pilot check), ISR shrink and expand counters, ReplicaManager and KafkaController gauges, and the JVM
    collectors (jvm_memory_used_bytes{area}, the jvm_gc_collection_seconds summary with gc="G1 Young Generation", threads,
    buffer pools). Counters restart from zero when a broker pod restarts, inside one series (the pod keeps its name).
  * Flink Prometheus reporter: label values filtered to [A-Za-z0-9:_] ("Event_Enrichment_Job"), every Flink counter typed
    gauge (numRecordsIn, numberOfCompletedCheckpoints, GarbageCollector Count and Time, CPU Time, commitsSucceeded), task
    metrics with job_id, task_id, task_attempt_id, task_attempt_num and subtask_index, operator metrics with operator_id and
    operator_name, TaskManager JVM and Netty metrics with host and tm_id, JobManager job metrics with job_id and job_name.
    A job restart gives every subtask a new task_attempt_id and task_attempt_num; a resubmission (remediation) gives a new
    job_id; a replaced TaskManager is a new pod and a redeploy a new JobManager pod.
  * Scrape gaps: a killed TaskManager's series stop and its replacement's start; while a job restarts its task metrics vanish
    on every TaskManager (also the survivors), and tasks are placed again once three TaskManagers are Ready; during F7 one
    TaskManager scrape in three fails (the reply crosses the lossy link); a broker that is not Ready is not scraped.
  * Prometheus' own target health (REVIEW4 PIL-1): `up` and `scrape_samples_scraped` for every kafka-brokers and flink-metrics
    target, 0 when the pod is Running but its scrape failed, absent when the pod is gone.
  * Lazy Kafka meters (REVIEW4 L3): a broker's dead-letter topic series exist only after an append to a partition it leads,
    and vanish when it loses leadership (world.dead_letter_meter).
  * cAdvisor image labels carry the ECR registry host, which holds an account ID (REVIEW4 C1); the exporter strips it.

Values are deterministic functions of time and of the recorded history, as everywhere in the fake world.
"""
from __future__ import annotations

import math
import zlib
from collections import OrderedDict
from functools import lru_cache

from .. import ids

GIB = 1024 ** 3
MIB = 1024 ** 2
NS = ids.NAMESPACE
TOPICS = ("raw-events", "enriched-events", "aggregated-metrics", "fraud-alerts", "dead-letter", "__consumer_offsets")

# Prometheus metadata API: metric family -> type. Flink reports every counter as a gauge (Flink 1.19 reporter).
METRIC_TYPES = {
    **{f"kafka_server_brokertopicmetrics_{m}_total": "counter" for m in (
        "messagesinpersec", "bytesinpersec", "bytesoutpersec", "totalproducerequestspersec", "totalfetchrequestspersec")},
    "kafka_network_requestmetrics_requestspersec_total": "counter",
    "kafka_server_replicamanager_isrshrinks_total": "counter", "kafka_server_replicamanager_isrexpands_total": "counter",
    "kafka_log_logflushstats_logflushrateandtimems_total": "counter",
    "kafka_server_replicamanager_underreplicatedpartitions": "gauge",
    "kafka_server_replicamanager_underminisrpartitioncount": "gauge", "kafka_server_replicamanager_partitioncount": "gauge",
    "kafka_server_replicamanager_leadercount": "gauge",
    "kafka_controller_kafkacontroller_activecontrollercount": "gauge", "kafka_controller_kafkacontroller_fencedbrokercount": "gauge",
    "kafka_controller_kafkacontroller_offlinepartitionscount": "gauge",
    "kafka_controller_kafkacontroller_preferredreplicaimbalancecount": "gauge",
    "kafka_network_requestmetrics_totaltimems_mean": "gauge",
    "jvm_memory_used_bytes": "gauge", "jvm_gc_collection_seconds": "summary", "jvm_threads_current": "gauge",
    "jvm_buffer_pool_used_bytes": "gauge",
    **{f"flink_taskmanager_Status_JVM_{m}": "gauge" for m in (
        "Memory_Heap_Used", "Memory_Heap_Committed", "Memory_NonHeap_Used", "GarbageCollector_G1_Young_Generation_Count",
        "GarbageCollector_G1_Young_Generation_Time", "CPU_Load", "CPU_Time", "Threads_Count")},
    "flink_taskmanager_Status_Shuffle_Netty_UsedMemory": "gauge",
    **{f"flink_taskmanager_job_task_{m}": "gauge" for m in (
        "numRecordsIn", "numRecordsInPerSecond", "numRecordsOut", "numRecordsOutPerSecond", "busyTimeMsPerSecond",
        "idleTimeMsPerSecond", "backPressuredTimeMsPerSecond")},
    **{f"flink_taskmanager_job_task_operator_KafkaSourceReader_{m}": "gauge" for m in (
        "KafkaConsumer_records_lag_max", "KafkaConsumer_records_consumed_rate", "KafkaConsumer_assigned_partitions",
        "KafkaConsumer_rebalance_total", "commitsSucceeded")},
    "flink_taskmanager_job_task_operator_currentInputWatermark": "gauge",
    "flink_taskmanager_job_task_operator_numRecordsInPerSecond": "gauge",
    **{f"flink_jobmanager_job_{m}": "gauge" for m in (
        "numberOfCompletedCheckpoints", "numberOfFailedCheckpoints", "lastCheckpointDuration", "lastCheckpointSize",
        "uptime")},
    "flink_jobmanager_Status_JVM_Memory_Heap_Used": "gauge",
    "container_cpu_usage_seconds_total": "counter", "container_memory_working_set_bytes": "gauge",
    "container_oom_events_total": "counter",
    "kube_pod_info": "gauge", "kube_pod_status_ready": "gauge", "kube_pod_container_resource_limits": "gauge",
    "kube_pod_container_status_restarts_total": "counter", "kube_pod_container_status_last_terminated_reason": "gauge",
    "up": "gauge", "scrape_samples_scraped": "gauge",
}

# Flink job topology per workload: (task name, source operator, downstream operator) as the job graph names them.
TASKS = {
    "ecommerce": {"Event Enrichment Job": ("Source: raw-events -> Enrich -> Sink: enriched-events", "Source: raw-events", "Enrich"),
                  "Windowed Aggregation Job": ("Source: enriched-events -> Window(TumblingEventTime) -> Sink", "Source: enriched-events",
                                               "Window(TumblingEventTime)"),
                  "Fraud Detection Job": ("Source: enriched-events -> FraudPattern -> Sink: fraud-alerts", "Source: enriched-events",
                                          "FraudPattern")},
    "iot": {"IoT Event Enrichment Job": ("Source: raw-events -> IoTEnrich -> Sink: enriched-events", "Source: raw-events", "IoTEnrich"),
            "IoT Per-Location Aggregation Job": ("Source: enriched-events -> LocationWindow -> Sink", "Source: enriched-events",
                                                 "LocationWindow"),
            "IoT Threshold Alert Job": ("Source: enriched-events -> ThresholdAlert -> Sink: fraud-alerts", "Source: enriched-events",
                                        "ThresholdAlert")},
}
ALL_TASKS = {**TASKS["ecommerce"], **TASKS["iot"]}  # a test may run one workload's jobs on the other's cluster
JOB_ORDER = list(ALL_TASKS)
PARALLELISM = 4
EVENT_BYTES = {"ecommerce": 360.0, "iot": 210.0}


@lru_cache(maxsize=None)
def flink_label_value(v: str) -> str:
    """The Flink reporter's label value filter: anything outside [A-Za-z0-9:_] becomes '_'."""
    return "".join(c if (c.isalnum() and c.isascii()) or c in ":_" else "_" for c in v)


@lru_cache(maxsize=None)
def _crc(s: str) -> int:
    return zlib.crc32(s.encode("utf-8"))


def _jitter(seed: int, t: int) -> float:
    """Deterministic noise in [-0.5, 0.5), stable across processes (no Python hash randomisation)."""
    x = (seed ^ (t * 2654435761)) & 0xFFFFFFFF
    x = (x ^ (x >> 15)) * 2246822519 & 0xFFFFFFFF
    x ^= x >> 13
    return (x % 10000) / 10000.0 - 0.5


@lru_cache(maxsize=None)
def _hex32(*parts) -> str:
    h = 0
    out = ""
    for i in range(4):
        h = zlib.crc32(("|".join(map(str, parts)) + f"#{i}").encode()) & 0xFFFFFFFF
        out += f"{h:08x}"
    return out


class Step:
    """One 10 s step of the TSDB: metric name -> [(labels, value)]. Label dicts are shared per series and never mutated."""

    __slots__ = ("by_name", "names")

    def __init__(self, names: set | None = None) -> None:
        self.by_name: dict[str, list[tuple[dict, float]]] = {}
        self.names = names  # the model's set of every metric name ever produced (FakeProm matches names once)

    def add(self, labels: dict, value: float) -> None:
        name = labels["__name__"]
        rows = self.by_name.get(name)
        if rows is None:
            rows = self.by_name[name] = []
            if self.names is not None and name not in self.names:
                self.names.add(name)
        rows.append((labels, float(value)))

    def __iter__(self):
        for rows in self.by_name.values():
            for labels, v in rows:
                yield {"labels": labels, "value": v}

    def values_of(self, metric: str, job: str | None = None) -> list[tuple[dict, float]]:
        rows = self.by_name.get(metric, [])
        return rows if job is None else [(lab, v) for lab, v in rows if lab.get("job") == job]


class SeriesModel:
    CACHE_STEPS = 1500

    def __init__(self, world) -> None:
        self.w = world
        self.labels: dict[tuple, dict] = {}
        self.cache: OrderedDict[int, Step] = OrderedDict()
        self.placements: dict[tuple, dict[int, str] | None] = {}
        self.total_steps = 0
        self.names: set[str] = set()
        self.subtask_labels: dict[tuple, dict] = {}

    # ------------------------------------------------------------------------------------------- label registry
    def lab(self, key: tuple, **labels) -> dict:
        d = self.labels.get(key)
        if d is None:
            d = labels
            self.labels[key] = d
        return d

    def clear(self) -> None:
        self.cache.clear()

    # ------------------------------------------------------------------------------------------- one step
    def step(self, t: int) -> Step:
        hit = self.cache.get(t)
        if hit is not None:
            self.cache.move_to_end(t)
            return hit
        st = Step(self.names)
        self.total_steps += 1
        w = self.w
        self._brokers(t, st)
        self._flink(t, st)
        self._containers(t, st)
        gap = w.global_knobs["telemetry_gap_once"]
        jm_gap = False
        if gap is not None:
            # The JobManager's flink-metrics scrape fails during the first injection of run type `gap`: an untargeted scrape
            # target misses scrapes, so that attempt is TELEMETRY_FAILED (decision 21) and repeated.
            first = next((c for c in w.chaos if c.run_type == gap), None)
            spans = [e for e in (first.effect.values() if first else []) if e[0] <= t and (e[1] is None or t < e[1])]
            if spans:
                jm_gap = True
                for name in list(st.by_name):
                    st.by_name[name] = [(lab, v) for lab, v in st.by_name[name]
                                        if not (lab.get("job") == "flink-metrics" and lab.get("flink_component") == "jobmanager")]
        self._scrape(t, st, jm_gap)
        drops = w.global_knobs["drop_series"]
        if drops:
            for name in list(st.by_name):
                st.by_name[name] = [(lab, v) for lab, v in st.by_name[name] if not _dropped(lab, t, drops)]
        for extra in w.global_knobs["extra_series"]:
            st.add(self.lab(("extra",) + tuple(sorted(extra["labels"].items())), **extra["labels"]), extra["value"])
        if t <= w.clock.t - 1:
            self.cache[t] = st
            if len(self.cache) > self.CACHE_STEPS:
                self.cache.popitem(last=False)
        return st

    # ------------------------------------------------------------------------------------------- Kafka
    def _isr_events(self, broker: str, since: float, t: float) -> int:
        """ISR shrink (and later expand) events this broker saw since its pod started: one per outage or partition of
        another broker that began in that interval, 12 partitions each."""
        n = 0
        for p in self.w.pods:
            if p.role == "broker" and p.name != broker and p.deleted_at is not None and since <= p.deleted_at <= t:
                n += 1
        for c in self.w.chaos:
            if c.run_type == "F2":
                fol = c.targets["."][0]
                e = c.effect.get(fol)
                if e and fol != broker and since <= e[0] <= t:
                    n += 1
        return 12 * n

    def _brokers(self, t: int, st: Step) -> None:
        w = self.w
        wl = w.workload
        size = EVENT_BYTES[wl]
        counts = {"raw-events": w.raw_events_count(t), "enriched-events": 0.98 * w.loadgen_count(t),
                  "aggregated-metrics": 3.0 * (t - w.t0), "fraud-alerts": 0.2 * (t - w.t0),
                  "dead-letter": w.dead_letter_count(t), "__consumer_offsets": 2.0 * (t - w.t0)}
        for p in w.pods_at(t, "broker"):
            if not p.ready(t):
                continue  # a broker that is not Ready is not scraped
            c0 = p.created_at
            at_start = {"raw-events": w.raw_events_count(c0), "enriched-events": 0.98 * w.loadgen_count(c0),
                        "aggregated-metrics": 3.0 * (c0 - w.t0), "fraud-alerts": 0.2 * (c0 - w.t0),
                        "dead-letter": w.dead_letter_count(c0), "__consumer_offsets": 2.0 * (c0 - w.t0)}
            base = dict(job="kafka-brokers", namespace=NS, pod=p.name, instance=p.name, kafka_broker=p.name, node=p.node)
            bid = ids.broker_id_from_pod(p.name)
            seed = _crc(p.name)
            tot = {"messagesinpersec": 0.0, "bytesinpersec": 0.0, "bytesoutpersec": 0.0, "totalproducerequestspersec": 0.0,
                   "totalfetchrequestspersec": 0.0}
            for topic in TOPICS:
                opened = None
                if topic == "dead-letter":
                    # the lazily created topic meter (REVIEW4 L3); the all-topics meters count the topic either way
                    opened = w.dead_letter_meter(p.name, t)
                msgs = max(0.0, (counts[topic] - at_start[topic]) / 3.0)
                fetch = 25.0 * (t - c0) if topic != "dead-letter" else 4.0 * (t - c0)
                vals = {"messagesinpersec": msgs, "bytesinpersec": msgs * size, "bytesoutpersec": msgs * size * 2.0,
                        "totalproducerequestspersec": msgs / 40.0, "totalfetchrequestspersec": fetch}
                for m, v in vals.items():
                    tot[m] += v
                if topic == "dead-letter":
                    if opened is None:
                        continue
                    # a reopened meter starts again from zero, like Kafka's
                    d = max(0.0, (counts[topic] - w.dead_letter_count(opened)) / 3.0)
                    vals = {"messagesinpersec": d, "bytesinpersec": d * size, "bytesoutpersec": d * size * 2.0,
                            "totalproducerequestspersec": d / 40.0, "totalfetchrequestspersec": 4.0 * (t - opened)}
                for m, v in vals.items():
                    name = f"kafka_server_brokertopicmetrics_{m}_total"
                    st.add(self.lab((name, p.name, topic), __name__=name, topic=topic, **base), math.floor(v))
            for m, v in tot.items():
                name = f"kafka_server_brokertopicmetrics_{m}_total"
                st.add(self.lab((name, p.name), __name__=name, **base), math.floor(v))
            name = "kafka_network_requestmetrics_requestspersec_total"
            for req, v in (("Produce, version=9", tot["totalproducerequestspersec"]), ("Fetch, version=16", 150.0 * (t - c0)),
                           ("Metadata, version=12", 0.4 * (t - c0))):
                st.add(self.lab((name, p.name, req), __name__=name, request=req, **base), math.floor(v))
            shrinks = self._isr_events(p.name, c0, t)
            for name, v in (("kafka_server_replicamanager_isrshrinks_total", shrinks),
                            ("kafka_server_replicamanager_isrexpands_total", self._isr_events(p.name, c0, t - 60)),
                            ("kafka_log_logflushstats_logflushrateandtimems_total", math.floor(0.2 * (t - c0)))):
                st.add(self.lab((name, p.name), __name__=name, **base), v)
            gauges = (("kafka_server_replicamanager_underreplicatedpartitions", w.urp(p.name, t)),
                      ("kafka_server_replicamanager_underminisrpartitioncount", 0.0),
                      ("kafka_server_replicamanager_partitioncount", 53.0),
                      ("kafka_server_replicamanager_leadercount", 18.0 + (1.0 if bid == 0 else 0.0)),
                      ("kafka_controller_kafkacontroller_activecontrollercount", 1.0 if bid == w.controller() else 0.0),
                      ("kafka_controller_kafkacontroller_fencedbrokercount", w.fenced(t)),
                      ("kafka_controller_kafkacontroller_offlinepartitionscount", 0.0),
                      ("kafka_controller_kafkacontroller_preferredreplicaimbalancecount", 5.0 if t < w.imbalance_until else 0.0))
            for name, v in gauges:
                st.add(self.lab((name, p.name), __name__=name, **base), v)
            st.add(self.lab(("kafka_network_requestmetrics_totaltimems_mean", p.name), request="Produce",
                            __name__="kafka_network_requestmetrics_totaltimems_mean", **base), 2.0 + _jitter(seed, t))
            # JVM collectors of the JMX exporter agent
            heap = 900 * MIB + 300 * MIB * (((t - c0) % 47) / 47.0) + 20 * MIB * _jitter(seed + 1, t)
            st.add(self.lab(("jvm_memory_used_bytes", p.name, "heap"), __name__="jvm_memory_used_bytes", area="heap", **base), heap)
            st.add(self.lab(("jvm_memory_used_bytes", p.name, "nonheap"), __name__="jvm_memory_used_bytes", area="nonheap",
                            **base), 140 * MIB + 2 * MIB * _jitter(seed + 2, t))
            young = math.floor((t - c0) / 47.0)
            for gc, n in (("G1 Young Generation", young), ("G1 Old Generation", 0)):
                st.add(self.lab(("jvm_gc_collection_seconds_count", p.name, gc), __name__="jvm_gc_collection_seconds_count",
                                gc=gc, **base), n)
                st.add(self.lab(("jvm_gc_collection_seconds_sum", p.name, gc), __name__="jvm_gc_collection_seconds_sum",
                                gc=gc, **base), round(n * 0.011, 6))
            st.add(self.lab(("jvm_threads_current", p.name), __name__="jvm_threads_current", **base),
                   112 + math.floor(4 * _jitter(seed + 3, t)))
            for pool, b in (("direct", 64 * MIB), ("mapped", 2 * GIB)):
                st.add(self.lab(("jvm_buffer_pool_used_bytes", p.name, pool), __name__="jvm_buffer_pool_used_bytes",
                                pool=pool, **base), b + MIB * _jitter(seed + 4, t))

    # ------------------------------------------------------------------------------------------- Flink
    def _scraped(self, p, t: int) -> bool:
        """A TaskManager scrape crosses the lossy link during F7 and fails one step in three (3.4.3)."""
        w = self.w
        if not p.ready(t):
            return False
        if w.global_knobs["tm_scrape_drop_under_loss"] and p.role == "taskmanager" and w.effect_active("F7", p.name, t):
            return (_crc(p.name) + t // 10) % 3 != 0
        return True

    def _attempt(self, j, t: float) -> tuple[int, float] | None:
        """(task attempt number, start of the RUNNING stretch) of job run j at t, or None when it is not RUNNING."""
        if j.state(t) != "RUNNING":
            return None
        n, start, prev = -1, None, None
        for ts, s in sorted(j.states, key=lambda x: x[0]):
            if ts > t:
                break
            if s == "RUNNING" and prev != "RUNNING":
                n, start = n + 1, ts
            prev = s
        return n, start

    def _placement(self, j, n: int, start: float, t: int) -> dict[int, str] | None:
        """Subtask -> TaskManager pod for attempt n of job run j, which became RUNNING at `start`. The scheduler places
        the tasks at the first instant from `start` on at which three TaskManagers are Ready (12 slots for three jobs of
        parallelism 4); until then the job's task metrics are absent. The instant is found from the pods' Ready times, so
        the answer does not depend on which step is asked first."""
        key = (j.jid, n)
        got = self.placements.get(key)
        if got is not None:
            return got
        w = self.w
        cands = sorted({start} | {p.ready_at for p in w.pods if p.role == "taskmanager" and p.ready_at is not None
                                  and start <= p.ready_at <= t})
        at = next((c for c in cands if len(w.tm_names_ready(c)) >= ids.EXPECTED_TASKMANAGERS), None)
        if at is None:
            return None
        ready = w.tm_names_ready(at)
        ji = JOB_ORDER.index(j.name) % 3
        place = {s: ready[(ji * PARALLELISM + s) % len(ready)] for s in range(PARALLELISM)}
        self.placements[key] = place
        return place

    def _flink(self, t: int, st: Step) -> None:
        w = self.w
        wl = w.workload
        tms = {p.name: p for p in w.pods_at(t, "taskmanager")}
        scraped = {name: p for name, p in tms.items() if self._scraped(p, t)}
        rate = w.loadgen_rate(t)
        lag_all = w.lag(t)
        for name, p in scraped.items():
            base = dict(job="flink-metrics", namespace=NS, pod=name, instance=name, flink_component="taskmanager", node=p.node,
                        host=flink_label_value(p.ip), tm_id=flink_label_value(name))
            seed = _crc(name)
            age = t - p.created_at
            ws = w.tm_ws(name, t)
            cpu = w.tm_cpu_rate(name, t)
            young = math.floor(age / 11.0)
            vals = (("Memory_Heap_Used", ws * 0.55 + 200 * MIB * ((age % 37) / 37.0)), ("Memory_Heap_Committed", 4.0 * GIB),
                    ("Memory_NonHeap_Used", 180 * MIB + 3 * MIB * _jitter(seed, t)),
                    ("GarbageCollector_G1_Young_Generation_Count", young),
                    ("GarbageCollector_G1_Young_Generation_Time", young * 8),
                    ("CPU_Load", min(1.0, cpu / 2.0)), ("CPU_Time", math.floor(w.tm_cpu_counter(p, t) * 1e9)),
                    ("Threads_Count", 84 + math.floor(6 * _jitter(seed + 1, t))))
            for m, v in vals:
                mn = f"flink_taskmanager_Status_JVM_{m}"
                st.add(self.lab((mn, name), __name__=mn, **base), v)
            mn = "flink_taskmanager_Status_Shuffle_Netty_UsedMemory"
            st.add(self.lab((mn, name), __name__=mn, **base), 64 * MIB + 8 * MIB * _jitter(seed + 2, t))
        jm = next((p for p in w.pods_at(t, "jobmanager") if p.ready(t)), None)
        tasks = ALL_TASKS
        for j in w.flink_jobs:
            state = j.state(t)
            if state is None:
                continue
            jname = flink_label_value(j.name)
            task_name, src_op, down_op = tasks[j.name]
            if jm is not None and state not in ("FAILED", "CANCELED", "FINISHED"):
                base = dict(job="flink-metrics", namespace=NS, pod=jm.name, instance=jm.name, flink_component="jobmanager",
                            node=jm.node, host=flink_label_value(jm.ip), job_id=j.jid, job_name=jname)
                n_done, n_fail, done, _ = w.ckpt_times(j, t)
                loss = any(s0 <= t < e0 for s0, e0 in w.loss_intervals(t))
                seed = _crc(j.jid)
                att = self._attempt(j, t)
                vals = (("numberOfCompletedCheckpoints", n_done), ("numberOfFailedCheckpoints", n_fail),
                        ("lastCheckpointDuration", (900.0 + 300 * _jitter(seed, t)) * (8.0 if loss else 1.0)),
                        ("lastCheckpointSize", (60 if wl == "iot" else 35) * MIB * (1.0 + 0.2 * _jitter(seed + 1, t))),
                        ("uptime", 1000.0 * (t - att[1]) if att else 0.0))
                for m, v in vals:
                    mn = f"flink_jobmanager_job_{m}"
                    st.add(self.lab((mn, j.jid, jm.name), __name__=mn, **base), v)
            att = self._attempt(j, t)
            if att is None:
                continue
            n, start = att
            place = self._placement(j, n, start, t)
            if place is None:
                continue
            task_id = _hex32(j.name, "task")
            attempt_seconds = max(0.0, t - start)
            for s, tm in place.items():
                p = scraped.get(tm)
                if p is None:
                    continue
                share = rate / PARALLELISM
                # Flink's numRecordsIn and numRecordsOut are cumulative: the records since the task attempt started, which
                # fall only when the job restarts (a new attempt and series). Integrated from the load, never rate x time.
                done_in = (w.loadgen_count(t) - w.loadgen_count(start)) / PARALLELISM
                key = (j.jid, s, n, tm)
                common = self.subtask_labels.get(key)
                if common is None:
                    common = self.subtask_labels[key] = dict(
                        job="flink-metrics", namespace=NS, pod=tm, instance=tm, flink_component="taskmanager", node=p.node,
                        host=flink_label_value(p.ip), tm_id=flink_label_value(tm), job_id=j.jid, job_name=jname,
                        task_id=task_id, task_name=flink_label_value(task_name), task_attempt_id=_hex32(j.jid, s, n),
                        task_attempt_num=str(n), subtask_index=str(s))
                seed = _crc(f"{j.jid}{s}{n}")
                cpu = w.tm_cpu_rate(tm, t)
                busy = 280.0 + (600.0 if cpu > 1.0 else 0.0) + 40 * _jitter(seed, t)
                bp = 15.0 + (400.0 if w.effect_active("F7", tm, t) else 0.0) + 10 * _jitter(seed + 1, t)
                task_vals = (("numRecordsIn", math.floor(done_in)), ("numRecordsInPerSecond", share * (1 + 0.05 * _jitter(seed + 2, t))),
                             ("numRecordsOut", math.floor(0.98 * done_in)),
                             ("numRecordsOutPerSecond", 0.98 * share * (1 + 0.05 * _jitter(seed + 3, t))),
                             ("busyTimeMsPerSecond", busy), ("backPressuredTimeMsPerSecond", bp),
                             ("idleTimeMsPerSecond", max(0.0, 1000.0 - busy - bp)))
                for m, v in task_vals:
                    mn = f"flink_taskmanager_job_task_{m}"
                    st.add(self.lab((mn,) + key, __name__=mn, **common), v)
                src = dict(common, operator_id=_hex32(j.name, src_op), operator_name=flink_label_value(src_op))
                lag = lag_all * (0.6 + 0.4 * ((s + 1) / PARALLELISM))
                op_vals = (("KafkaConsumer_records_lag_max", math.floor(lag)),
                           ("KafkaConsumer_records_consumed_rate", share * (1 + 0.05 * _jitter(seed + 4, t))),
                           ("KafkaConsumer_assigned_partitions", 3.0), ("KafkaConsumer_rebalance_total", 0.0),
                           ("commitsSucceeded", math.floor(attempt_seconds / 60.0)))
                for m, v in op_vals:
                    mn = f"flink_taskmanager_job_task_operator_KafkaSourceReader_{m}"
                    st.add(self.lab((mn,) + key, __name__=mn, **src), v)
                down = dict(common, operator_id=_hex32(j.name, down_op), operator_name=flink_label_value(down_op))
                mn = "flink_taskmanager_job_task_operator_currentInputWatermark"
                st.add(self.lab((mn,) + key, __name__=mn, **down), math.floor((t - 3.0 - lag / max(rate, 1.0)) * 1000.0))
                mn = "flink_taskmanager_job_task_operator_numRecordsInPerSecond"
                st.add(self.lab((mn,) + key, __name__=mn, **down), share * (1 + 0.05 * _jitter(seed + 5, t)))
        if jm is not None:
            mn = "flink_jobmanager_Status_JVM_Memory_Heap_Used"
            st.add(self.lab((mn, jm.name), __name__=mn, job="flink-metrics", namespace=NS, pod=jm.name, instance=jm.name,
                            flink_component="jobmanager", node=jm.node, host=flink_label_value(jm.ip)),
                   700 * MIB + 50 * MIB * _jitter(_crc(jm.name), t))

    # ------------------------------------------------------------------------------------------- cAdvisor, kube-state
    def _containers(self, t: int, st: Step) -> None:
        w = self.w
        node_ip = {n["name"]: f"10.0.1.{i + 10}" for i, n in enumerate(w.nodes)}
        for p in w.pods_at(t):
            if p.role in ("broker", "taskmanager", "jobmanager"):
                cid = f"containerd://{p.uid[:12]}"
                kub = dict(job="kubelet", namespace=NS, pod=p.name, instance=f"{node_ip[p.node]}:10250", node=p.node,
                           endpoint="https-metrics", service="prometheus-kube-prometheus-kubelet",
                           metrics_path="/metrics/cadvisor", container=p.container, id=f"/kubepods/pod{p.uid}/{cid}",
                           name=cid, image=f"000000000000.dkr.ecr.us-east-2.amazonaws.com/streaming/{p.role}"
                                           f"@sha256:{_hex32(p.role)}{_hex32(p.role, 2)}")
                if p.role == "taskmanager":
                    cpu, ws = w.tm_cpu_counter(p, t), w.tm_ws(p.name, t)
                elif p.role == "broker":
                    cpu, ws = 0.8 * (t - p.created_at), 6 * GIB
                else:
                    cpu, ws = 0.3 * (t - p.created_at), 1.5 * GIB
                oom = w.oom_events(p.name, t) if p.role == "taskmanager" else 0.0
                for mn, v in (("container_cpu_usage_seconds_total", cpu), ("container_memory_working_set_bytes", ws),
                              ("container_oom_events_total", oom)):
                    st.add(self.lab((mn, p.uid), __name__=mn, **kub), v)
            if p.role not in ("broker", "taskmanager", "jobmanager", "loadgen", "consumer"):
                continue
            ks = dict(job="kube-state-metrics", namespace=NS, pod=p.name, uid=p.uid, instance="10.0.1.40:8080",
                      endpoint="http", service="prometheus-kube-state-metrics")
            ready = 1.0 if p.ready(t) else 0.0
            st.add(self.lab(("kube_pod_info", p.uid), __name__="kube_pod_info", node=p.node, pod_ip=p.ip,
                            host_ip=node_ip[p.node], created_by_kind="StatefulSet" if p.role == "broker" else "ReplicaSet",
                            priority_class="", host_network="false", **ks), 1.0)
            st.add(self.lab(("kube_pod_status_ready", p.uid, "true"), __name__="kube_pod_status_ready", condition="true", **ks),
                   ready)
            st.add(self.lab(("kube_pod_status_ready", p.uid, "false"), __name__="kube_pod_status_ready", condition="false",
                            **ks), 1.0 - ready)
            limit = 4 * GIB if p.role == "taskmanager" else 16 * GIB if p.role == "broker" else 1 * GIB
            st.add(self.lab(("kube_pod_container_resource_limits", p.uid), __name__="kube_pod_container_resource_limits",
                            node=p.node, container=p.container, resource="memory", unit="byte", **ks), limit)
            st.add(self.lab(("kube_pod_container_status_restarts_total", p.uid), __name__="kube_pod_container_status_restarts_total",
                            container=p.container, **ks), p.restart_count)
            if p.last_reason:
                st.add(self.lab(("kube_pod_container_status_last_terminated_reason", p.uid, p.last_reason),
                                __name__="kube_pod_container_status_last_terminated_reason", container=p.container,
                                reason=p.last_reason, **ks), 1.0)


    # ------------------------------------------------------------------------------------------- scrape health
    def _scrape(self, t: int, st: Step, jm_gap: bool) -> None:
        """`up` and `scrape_samples_scraped` per target of the two feature scrape jobs. A target exists while its pod
        exists (the jobs keep Running pods). up is 1 when the scrape succeeded, which in this model is when the pod's
        series were produced at this step."""
        w = self.w
        counts: dict[tuple[str, str], int] = {}
        for rows in st.by_name.values():
            for lab, _ in rows:
                k = (lab.get("job", ""), lab.get("pod", ""))
                counts[k] = counts.get(k, 0) + 1
        drops = w.global_knobs["drop_series"]
        for p in w.pods_at(t):
            if p.role == "broker":
                job, base = "kafka-brokers", dict(job="kafka-brokers", namespace=NS, pod=p.name, instance=p.name,
                                                  kafka_broker=p.name, node=p.node)
                ok = p.ready(t)
            elif p.role in ("jobmanager", "taskmanager"):
                job, base = "flink-metrics", dict(job="flink-metrics", namespace=NS, pod=p.name, instance=p.name,
                                                  flink_component=p.role, node=p.node)
                ok = self._scraped(p, t) and not (jm_gap and p.role == "jobmanager")
            else:
                continue
            up_lab = self.lab(("up", p.uid), __name__="up", **base)
            if drops and _dropped(up_lab, t, drops):
                ok = False
            st.add(up_lab, 1.0 if ok else 0.0)
            st.add(self.lab(("scrape_samples_scraped", p.uid), __name__="scrape_samples_scraped", **base),
                   float(counts.get((job, p.name), 0)) if ok else 0.0)


def _dropped(lab: dict, t: int, drops: list[dict]) -> bool:
    for d in drops:
        if d.get("metric") not in (None, lab["__name__"]) or d.get("pod") not in (None, lab.get("pod")):
            continue
        if d.get("job") not in (None, lab.get("job")):
            continue
        if d.get("start", -math.inf) <= t < d.get("end", math.inf):
            return True
    return False
