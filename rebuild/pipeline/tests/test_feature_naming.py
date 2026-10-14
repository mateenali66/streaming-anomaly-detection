"""Review 4 MIN-11: the labels that enter a feature name come from a fixed per-metric list in config/pipeline.json
(labels.naming), never from which labels happen to vary inside one campaign. So one series has one name in every
campaign, and the cross-campaign (4.3.1) and shared-feature (C6) analyses never lose a column silently."""
from __future__ import annotations

import re
import zlib

import numpy as np
import pytest

from streamprep.config import load_config
from streamprep.contract import BuildError, ContractError, canonical_json_bytes
from streamprep.features import counter_mask, map_columns
from streamprep.metrics_io import LongMetrics, SeriesTable


def _lm(series: list[dict]) -> LongMetrics:
    st = SeriesTable()
    for i, s in enumerate(series):
        st.series_id.append(f"{i:016x}")
        st.metric.append(s["metric"])
        st.metric_type.append(s.get("metric_type", "gauge"))
        st.source.append(s["source"])
        st.role.append(s["role"])
        st.pod.append(s["pod"])
        st.broker_id.append(s.get("broker_id"))
        st.container.append(None)
        st.labels.append(dict(s["labels"]))
        st.labels_str.append(canonical_json_bytes(s["labels"]).decode())
    n = len(series)
    return LongMetrics(t0_ms=0, n_steps=2, step=np.repeat(np.arange(2, dtype=np.int32), n),
                       sid=np.tile(np.arange(n, dtype=np.int32), 2), value=np.ones(2 * n), series=st,
                       series_id_form="string")


def _names(series: list[dict], cfg=None) -> dict[str, str]:
    cfg = cfg or load_config()
    lm = _lm(series)
    cols, gid, _u, _units = map_columns(lm, counter_mask(lm, cfg), cfg)
    return {canonical_json_bytes({"m": s["metric"], "l": s["labels"], "p": s["pod"]}).decode():
            f"f__{cols[gid[i]].source}__{cols[gid[i]].name}__{cols[gid[i]].scope_class}" for i, s in enumerate(series)}


def _gc(gc: str) -> dict:
    return {"metric": "jvm_gc_collection_seconds_count", "metric_type": "summary", "source": "jvm", "role": "broker",
            "pod": "streaming-kafka-brokers-0", "broker_id": 0,
            "labels": {"gc": gc, "kafka_broker": "streaming-kafka-brokers-0"}}


def _task(job: str, sub: str, pod: str) -> dict:
    return {"metric": "flink_taskmanager_job_task_busyTimeMsPerSecond", "source": "flink", "role": "taskmanager",
            "pod": pod, "labels": {"flink_component": "taskmanager", "job_name": job, "task_name": f"Source: {job}",
                                   "task_id": f"{zlib.crc32(job.encode()):032d}", "subtask_index": sub}}


def test_a_series_has_the_same_name_whether_or_not_its_label_varies_in_the_campaign():
    young = _gc("G1 Young Generation")
    a = _names([young])  # campaign A: only the young collector reported
    b = _names([young, _gc("G1 Old Generation")])  # campaign B: both
    key = next(iter(a))
    assert a[key] == b[key] == "f__jvm__jvm_gc_collection_seconds_count_per_s.gc-G1_Young_Generation__b0"
    one = _names([_task("Event_Enrichment_Job", "0", "tm-1")])
    three = _names([_task(j, "0", "tm-1") for j in ("Event_Enrichment_Job", "Fraud_Detection_Job",
                                                    "Windowed_Aggregation_Job")])
    k = next(iter(one))
    assert one[k] == three[k]
    assert one[k] == ("f__flink__flink_taskmanager_job_task_busyTimeMsPerSecond.job_name-Event_Enrichment_Job"
                      ".task_name-Source__Event_Enrichment_Job__tm")


def test_an_unlisted_label_that_varies_fails_the_build():
    base = {"metric": "kafka_server_replicamanager_underreplicatedpartitions", "source": "broker", "role": "broker",
            "pod": "streaming-kafka-brokers-0", "broker_id": 0}
    with pytest.raises(BuildError, match="one name"):
        _names([{**base, "labels": {"shard": "1"}}, {**base, "labels": {"shard": "2"}}])


def test_a_feature_metric_without_a_naming_rule_fails_the_build():
    s = {"metric": "made_up_broker_gauge", "source": "broker", "role": "broker", "pod": "streaming-kafka-brokers-0",
         "broker_id": 0, "labels": {}}
    with pytest.raises((BuildError, ContractError), match="naming"):
        _names([s])


def test_names_in_the_synthetic_campaign_follow_the_fixed_list(full):
    cfg = load_config()
    cols = [c for c in full.windows[30].columns if c.startswith("f__")]
    task = [c for c in cols if c.startswith("f__flink__flink_taskmanager_job_task_busyTimeMsPerSecond")]
    assert task and all(".job_name-" in c and ".task_name-" in c and "task_id" not in c for c in task)
    assert any(c.startswith("f__jvm__jvm_gc_collection_seconds_count_per_s.gc-G1_Young_Generation__b1__") for c in cols)
    assert any(c.startswith("f__broker__kafka_server_brokertopicmetrics_messagesinpersec_per_s__b0__") for c in cols)
    rules = [re.compile(r["metric_regex"]) for r in cfg["labels"]["naming"]]
    computed = ("__tm_aggregates_missing_frac__", "__broker_series_missing_frac__", "__brokers_reporting__",
                "__taskmanagers_reporting__")
    for m in {c.split("__")[2].split(".")[0] for c in cols if not c.startswith("f__derived__")
              and not any(k in c for k in computed)}:
        base = m[: -len("_per_s")] if m.endswith("_per_s") else m
        assert any(r.search(base) or r.search(base + "_total") for r in rules), m
