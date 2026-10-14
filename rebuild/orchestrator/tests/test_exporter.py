"""The metric exporter (11.8): series ids, removed labels, NaN, repeated pairs, empty sources, probe-job exclusion,
write-role limits, and loud failure (Review 2 items 6 and 9)."""
import json

import pytest

from orchestrator import queries
from orchestrator.canonical import canonical_str, sha256_hex
from orchestrator.config import load_allowlist, load_config, validate_allowlist
from orchestrator.errors import ConfigError, OrchestratorError, QueryError, StoreError
from orchestrator.exporter import (collect, export_span, fetch_rows, finalize, main, metric_type_of, read_parquet,
                                   role_of, series_id)
from orchestrator.fakes.store import ExporterView

CFG = load_config()
ALLOW, ALLOW_BYTES = load_allowlist()


class StubProm:
    def __init__(self, series, meta=None):
        self.series = series
        self.meta = meta or {}
        self.queries = []

    def range(self, q, start, end, step):
        import re
        self.queries.append(q)
        rx = re.compile("^(?:" + q.split('__name__=~"')[1].split('"')[0] + ")$")
        return [(labels, [(t, v) for t, v in values if start <= t <= end])
                for labels, values in self.series if rx.match(labels["__name__"])]

    def metadata(self):
        return self.meta


def test_series_id_formula():
    labels = {"topic": "raw-events", "kafka_broker": "streaming-kafka-brokers-0"}
    want = sha256_hex(("m|" + canonical_str({"pod": "p", "labels": labels})).encode())[:16]
    assert series_id("m", "p", labels) == want and len(want) == 16


def test_removed_labels_nan_and_types():
    lab = {"__name__": "kafka_server_replicamanager_underreplicatedpartitions", "job": "kafka-brokers", "namespace": "streaming",
           "pod": "streaming-kafka-brokers-1", "instance": "streaming-kafka-brokers-1", "pod_ip": "10.0.0.1", "node": "n1",
           "uid": "u1", "endpoint": "e", "service": "s", "prometheus": "x", "host": "h", "tm_id": "t",
           "kubernetes_pod_name": "k", "container": "kafka"}
    prom = StubProm([(lab, [(100.0, 1.0), (110.0, float("nan")), (120.0, 3.0)])],
                    meta={"kafka_server_replicamanager_underreplicatedpartitions": [{"type": "gauge"}]})
    rows = fetch_rows(prom, CFG, "broker", "kafka_server_replicamanager_underreplicatedpartitions", 100, 120, prom.metadata(),
                      type("U", (), {"get": lambda self, p, t: None})())
    assert [r["ts"] for r in rows] == [100, 120]  # NaN dropped: an absent sample has no row
    r = rows[0]
    assert json.loads(r["labels"]) == {"node": "n1", "container": "kafka"}
    assert r["pod"] == "streaming-kafka-brokers-1" and r["broker_id"] == 1 and r["role"] == "broker"
    assert r["pod_uid"] == "u1" and r["container"] == "kafka" and r["metric_type"] == "gauge"


def test_metric_type_from_metadata_handles_total_suffix():
    meta = {"foo": [{"type": "counter"}], "bar_total": [{"type": "counter"}], "baz": [{"type": "gauge"}, {"type": "counter"}]}
    assert metric_type_of("foo_total", meta) == "counter"
    assert metric_type_of("bar_total", meta) == "counter"
    assert metric_type_of("baz", meta) == "unknown"
    assert metric_type_of("nope", meta) == "unknown"


def test_roles():
    assert role_of("streaming-kafka-brokers-2", {}) == "broker"
    assert role_of("streaming-flink-taskmanager-1-7", {}) == "taskmanager"
    assert role_of("streaming-flink-6d9f8c7b5-abcde", {"flink_component": "jobmanager"}) == "jobmanager"
    assert role_of("load-generator-7c9d5-x2k4p", {}) == "loadgen"
    assert role_of("", {}) == "other"


def test_repeated_pair_with_different_values_fails():
    rows = [{"ts": 10, "series_id": "a", "metric": "m", "value": 1.0}, {"ts": 10, "series_id": "a", "metric": "m", "value": 2.0}]
    with pytest.raises(OrchestratorError):
        finalize(rows)


def test_empty_source_fails_the_export():
    prom = StubProm([])
    with pytest.raises(QueryError):
        collect(prom, CFG, ALLOW, ("broker",), 0, 100, meta={}, uids=type("U", (), {"get": lambda s, p, t: None})())


def test_feature_queries_exclude_the_probe_job_and_restrict_scrape_jobs():
    for src in ("broker", "consumer", "flink", "jvm"):
        m = queries.source_matchers(CFG, src)
        assert 'job!="rebuild-verification"' in m
        assert any(x.startswith("job=") or x.startswith("job=~") for x in m)
    with pytest.raises(ConfigError):
        validate_allowlist({**ALLOW, "jvm": ALLOW["jvm"] + ["rebuild_.*"]})


def test_probe_job_series_never_reach_the_feature_file(make):
    h = make()
    h.world.global_knobs["extra_series"] = [
        {"labels": {"__name__": "jvm_memory_probe_bytes", "job": "rebuild-verification", "namespace": "streaming",
                    "pod": "verification-probes-x"}, "value": 1.0},
        {"labels": {"__name__": "jvm_memory_consumer_bytes", "job": "verification-consumer", "namespace": "streaming",
                    "pod": "verification-consumer-x"}, "value": 1.0}]
    assert h.run(only_fault="F0") == 0
    a = h.manifest()["attempts"][0]
    rows = read_parquet(h.s3.get(a["export"]["features_key"]))
    assert not any(r["metric"] in ("jvm_memory_probe_bytes", "jvm_memory_consumer_bytes") for r in rows)


def test_exporter_role_cannot_write_outside_baseline_and_export(make):
    h = make()
    view = ExporterView(h.s3)
    with pytest.raises(StoreError):
        view.put("COMPLETE", b"x")
    with pytest.raises(StoreError):
        view.put("attempts/x/verification.json", b"x")
    view.put("export/attempts/x/features.parquet", b"x")


def test_exporter_job_exits_non_zero_on_any_error(tmp_path, capsys, monkeypatch):
    monkeypatch.setenv("FEATURE_ALLOWLIST_SHA256", sha256_hex(ALLOW_BYTES))

    class Boom:
        def metadata(self):
            raise QueryError("Prometheus down")
    rc = main(["--kind", "attempt", "--key", "ecommerce-c0-b01-p1-a1", "--start-at", "100", "--end-at", "200"],
              prom_factory=lambda: Boom(), store_factory=lambda: None)
    assert rc == 1 and '"ok": false' in capsys.readouterr().out
    monkeypatch.setenv("FEATURE_ALLOWLIST_SHA256", "0" * 64)
    rc = main(["--kind", "attempt", "--key", "k", "--start-at", "1", "--end-at", "2"], prom_factory=lambda: Boom(),
              store_factory=lambda: None)
    assert rc == 1


def test_export_spans():
    assert export_span("attempt", 1003.2, 2001.0) == (1000, 2010)
    lo, hi = export_span("chunk", 1000.0, 4600.0)
    assert (lo, hi) == (1000, 4590)  # adjacent chunks never repeat a row


def test_split_range_queries_for_long_spans(make):
    h = make()
    seen = []
    orig = h.prom.range

    def spy(q, s, e, step):
        seen.append((s, e))
        return orig(q, s, e, step)
    h.prom.range = spy
    t = h.clock.t
    h.clock.advance_to(t + 8 * 3600)
    rows = fetch_rows(h.prom, h.cfg, "broker", "kafka_server_replicamanager_underreplicatedpartitions", int(t // 10 * 10),
                      int((t + 7 * 3600) // 10 * 10), {}, type("U", (), {"get": lambda s, p, t: None})())
    assert len(seen) == 2 and all((e - s) / 10 <= 2160 for s, e in seen)
    assert len({(r["ts"], r["series_id"]) for r in rows}) == len(rows)


def test_simulated_exporters_produce_the_real_shapes(make):
    """The fake cluster's series carry what the real exporters and scrape jobs produce, so the preprocessing runs its real
    paths in the end-to-end run: Prometheus target labels, Flink label filtering and gauge-typed counters, task-attempt
    labels that change on a job restart, scrape gaps during F5 and F7, and a new JobManager pod after a redeploy."""
    import json as _json
    h = make(world={"f5_restore_fails_until_redeploy": True, "submit_fails_until_redeploy": True})
    assert h.run() == 0
    m = h.manifest()
    rows = []
    for a in m["attempts"]:
        for r in read_parquet(h.s3.get(a["export"]["features_key"])):
            r["attempt"] = a
            rows.append(r)
    by_metric = {}
    for r in rows:
        by_metric.setdefault(r["metric"], []).append(r)
    # types from the metadata API: Kafka counters are counters, Flink's counters are gauges, the JMX GC summary is a summary
    assert {r["metric_type"] for r in by_metric["kafka_server_brokertopicmetrics_messagesinpersec_total"]} == {"counter"}
    assert {r["metric_type"] for r in by_metric["flink_taskmanager_job_task_numRecordsIn"]} == {"gauge"}
    assert {r["metric_type"] for r in by_metric["jvm_gc_collection_seconds_count"]} == {"summary"}
    # labels: Flink filters label values, the scrape job adds node and flink_component, the exporter removes pod, instance,
    # host and tm_id; the R1 request rule keeps the version tag inside the request label
    task = _json.loads(by_metric["flink_taskmanager_job_task_busyTimeMsPerSecond"][0]["labels"])
    assert {"job_id", "job_name", "task_id", "task_name", "task_attempt_id", "task_attempt_num", "subtask_index", "node",
            "flink_component"} <= set(task) and not {"host", "tm_id", "pod", "instance"} & set(task)
    assert " " not in task["job_name"] and "-" not in task["task_name"]
    reqs = {_json.loads(r["labels"])["request"] for r in by_metric["kafka_network_requestmetrics_requestspersec_total"]}
    assert "Produce, version=9" in reqs
    # a job restart (F5) gives the subtasks a new task_attempt_id and task_attempt_num
    f5 = next(a for a in m["attempts"] if a["run_type"] == "F5" and a["outcome"] == "VALID")
    nums = {_json.loads(r["labels"])["task_attempt_num"] for r in by_metric["flink_taskmanager_job_task_busyTimeMsPerSecond"]
            if r["attempt"]["attempt_key"] == f5["attempt_key"]}
    assert len(nums) >= 2
    # scrape gaps: during F7 a TaskManager misses steps; during F5 task metrics vanish on every TaskManager
    f7 = next(a for a in m["attempts"] if a["run_type"] == "F7" and a["outcome"] == "VALID")
    inj = next(s for s in f7["segments"] if s["segment_type"] == "INJECTION")
    tm_steps = {}
    for r in by_metric["flink_taskmanager_Status_JVM_Memory_Heap_Used"]:
        if r["attempt"]["attempt_key"] == f7["attempt_key"] and inj["start_at"] <= r["ts"] < inj["end_at"]:
            tm_steps.setdefault(r["pod"], set()).add(r["ts"])
    n_steps = len(range(int(inj["start_at"] // 10 * 10 + 10), int(inj["end_at"]), 10))
    assert tm_steps and all(len(v) < 0.8 * n_steps for v in tm_steps.values())
    inj5 = next(s for s in f5["segments"] if s["segment_type"] == "INJECTION")
    task_ts = {r["ts"] for r in by_metric["flink_taskmanager_job_task_busyTimeMsPerSecond"]
               if r["attempt"]["attempt_key"] == f5["attempt_key"] and inj5["start_at"] <= r["ts"] < inj5["end_at"]}
    assert len(task_ts) < len(range(int(inj5["start_at"] // 10 * 10 + 10), int(inj5["end_at"]), 10))
    # the redeploy: a new JobManager pod, and the jobs come back with new job ids
    jm_pods = {r["pod"] for r in by_metric["flink_jobmanager_job_lastCheckpointDuration"]}
    job_ids = {_json.loads(r["labels"])["job_id"] for r in by_metric["flink_jobmanager_job_lastCheckpointDuration"]}
    assert h.world.redeploys and len(jm_pods) == 2 and len(job_ids) > 3
