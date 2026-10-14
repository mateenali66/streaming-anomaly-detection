"""Section 11 schemas: the manifest, attempt records, verification records, baseline statistics and the exported
metric and probe files (11.4 to 11.8, 11.12)."""
import copy
import json

import pyarrow.parquet as pq
import io
import pytest

from orchestrator import ids, schema
from orchestrator.canonical import canonical_bytes, canonical_str, sha256_hex
from orchestrator.errors import SchemaError
from orchestrator.exporter import METRIC_SCHEMA, REMOVED_LABELS, series_id
from orchestrator.probefile import PROBE_SCHEMA


@pytest.fixture(scope="module")
def done(tmp_path_factory):
    from orchestrator.fakes.harness import Harness
    h = Harness("iot", 0, root=tmp_path_factory.mktemp("schema"))
    assert h.run() == 0
    return h


def test_complete_pilot_manifest_and_records_validate(done):
    m = done.manifest()
    schema.validate_manifest(m)
    assert schema.timeline_gaps(m) == []
    assert m["status"] == "COMPLETE" and m["timeline_end_at"] is not None
    assert m["flink_jobs_expected"] == [j[0] for j in ids.FLINK_JOBS["iot"]]
    assert m["cluster_name"] == "streaming-anomaly-detection-pilot-iot"
    stats = json.loads(done.s3.get(m["baseline_stats_key"]))
    schema.validate_baseline_stats(stats)
    assert sha256_hex(done.s3.get(m["baseline_stats_key"])) == m["baseline_stats_sha256"]
    assert stats["f6_planned_count"] == round(stats["f6_rate_per_s"] * 180)
    assert stats["f6_rate_per_s"] == pytest.approx(0.05 * stats["raw_events_in_per_s"]["mean"])
    for a in m["attempts"]:
        v = json.loads(done.s3.get(a["verification_key"]))
        schema.validate_verification(v)
        assert v["mode"] == "ONLINE" and v["thresholds_sha256"] == m["gate"]["thresholds_sha256"]
        assert sha256_hex(done.s3.get(a["verification_key"])) == a["verification_sha256"]
        assert ids.ATTEMPT_KEY_RE.match(a["attempt_key"]) and ids.UUID4_RE.match(a["run_id"])


def test_manifest_is_canonical_json_on_the_volume_and_in_s3(done):
    vol = (done.volume / "manifest.json").read_bytes()
    assert vol == canonical_bytes(json.loads(vol))
    assert done.s3.get("manifest/manifest.json") == vol


def test_segments_cover_each_attempt_and_the_timeline(done):
    m = done.manifest()
    for a in m["attempts"]:
        assert a["segments"][0]["start_at"] == a["started_at"] and a["segments"][-1]["end_at"] == a["ended_at"]
        types = [s["segment_type"] for s in a["segments"]]
        if a["run_type"] == "F0":
            assert types == ["SETTLE", "PRE_FAULT", "APPLY", "INJECTION", "RECOVERY", "COOLDOWN", "GATE"]
            apply = a["segments"][2]
            assert apply["start_at"] == apply["end_at"]  # zero-length APPLY for the sham
            assert a["onset_source"] == "PLANNED" and a["end_source"] == "PLANNED"
        pre = next(s for s in a["segments"] if s["segment_type"] == "PRE_FAULT")
        assert pre["end_at"] - pre["start_at"] == 600 and pre["end_at"] == a["planned_onset_at"]
        if "RECOVERY" in types:
            rec = next(s for s in a["segments"] if s["segment_type"] == "RECOVERY")
            cool = next(s for s in a["segments"] if s["segment_type"] == "COOLDOWN")
            assert rec["end_at"] - rec["start_at"] == 300 and cool["end_at"] - cool["start_at"] == 300
            if a["labelled_end_at"] is not None:
                assert rec["start_at"] == a["labelled_end_at"]  # an overrunning fault never leaks into RECOVERY


def _broken(m, path, value):
    m = copy.deepcopy(m)
    cur = m
    for p in path[:-1]:
        cur = cur[p]
    if value is KeyError:
        del cur[path[-1]]
    else:
        cur[path[-1]] = value
    return m


@pytest.mark.parametrize("path,value", [
    (("surprise",), 1),                                       # unknown field (11.1.6)
    (("seed",), KeyError),                                    # missing field
    (("attempts", 0, "outcome"), "MAYBE"),
    (("attempts", 0, "status"), "RUNNING"),                   # RUNNING with an outcome
    (("attempts", 0, "export", "features_sha256"), ""),       # DONE without a hash
    (("attempts", 0, "segments", 1, "start_at"), 1.0),        # a gap
    (("attempts", 0, "run_id"), "not-a-uuid"),
    (("attempts", 0, "flags"), ["LUCKY"]),
    (("campaign_id",), "ec-a"),
    (("region",), "us-east-1"),
    (("timeline_end_at",), "x"),
])
def test_invalid_manifests_are_rejected(done, path, value):
    m = done.manifest()
    if path == ("surprise",):
        m["surprise"] = 1
        bad = m
    else:
        bad = _broken(m, path, value)
    with pytest.raises(SchemaError):
        schema.validate_manifest(bad)


def test_two_valid_attempts_for_one_cell_are_rejected(done):
    m = done.manifest()
    a = copy.deepcopy(m["attempts"][0])
    a["attempt_key"] = a["cell_key"] + "-a2"
    a["attempt"] = 2
    a["run_id"] = "11111111-1111-4111-8111-111111111111"
    a["export"] = {k: v.replace("-a1", "-a2") if isinstance(v, str) else v for k, v in a["export"].items()}
    a["verification_key"] = a["verification_key"].replace("-a1", "-a2")
    off = m["attempts"][-1]["ended_at"] + 10 - a["started_at"]
    a["started_at"] += off
    a["ended_at"] += off
    a["planned_onset_at"] += off
    for s in a["segments"]:
        s["start_at"] += off
        s["end_at"] += off
    m["attempts"].append(a)
    with pytest.raises(SchemaError, match="more than one valid"):
        schema.validate_manifest(m)


def test_metric_files_follow_11_8(done):
    m = done.manifest()
    a = m["attempts"][0]
    for key in (a["export"]["features_key"], a["export"]["verification_metrics_key"]):
        t = pq.read_table(io.BytesIO(done.s3.get(key)))
        assert t.schema.equals(METRIC_SCHEMA, check_metadata=False)
        rows = t.to_pylist()
        assert rows == sorted(rows, key=lambda r: (r["ts"], r["series_id"]))
        assert len({(r["ts"], r["series_id"]) for r in rows}) == len(rows)
        for r in rows[:2000]:
            assert r["ts"] % 10 == 0
            labels = json.loads(r["labels"])
            assert not (set(labels) & REMOVED_LABELS)
            assert r["labels"] == canonical_str(labels)
            assert r["series_id"] == series_id(r["metric"], r["pod"], labels)
            assert not r["pod"].count(".") == 3  # a pod name, never an IP
            assert r["value"] == r["value"]  # no NaN
        sources = {r["source"] for r in rows}
        if key.endswith("features.parquet"):
            assert sources == {"broker", "consumer", "flink", "jvm"}
        else:
            assert sources == {"cadvisor", "kube_state", "kafka_controller", "kafka_broker_copy", "scrape"}
    span_lo = int(a["started_at"] // 10 * 10)
    ts = [r["ts"] for r in pq.read_table(io.BytesIO(done.s3.get(a["export"]["features_key"]))).to_pylist()]
    assert min(ts) >= span_lo


def test_feature_labels_keep_job_and_node_style_labels_raw(done):
    """Coordinator note A1: node, job_id and task attempt labels are exported unchanged in the raw files; the
    preprocessing drops them before naming a feature."""
    a = done.manifest()["attempts"][0]
    rows = pq.read_table(io.BytesIO(done.s3.get(a["export"]["features_key"]))).to_pylist()
    assert any("node" in json.loads(r["labels"]) for r in rows)


def test_probe_files_follow_11_8(done):
    a = done.manifest()["attempts"][0]
    t = pq.read_table(io.BytesIO(done.s3.get(a["export"]["probes_key"])))
    assert t.schema.equals(PROBE_SCHEMA, check_metadata=False)
    rows = t.to_pylist()
    kinds = {r["probe"] for r in rows}
    assert kinds == {"tcp", "tcp_burst", "controller", "flink_jobs", "flink_tms", "loadgen", "consumer"}
    for r in rows:
        assert r["tried_count"] == (10 if r["probe"] == "tcp_burst" else 1)
        if r["probe"] in ("tcp", "tcp_burst"):
            assert r["timeout_s"] == 0.5


def test_s3_layout_matches_11_7(done):
    keys = set(done.s3.objects)
    m = done.manifest()
    for k in ("plan/plan.json", "plan/plan.json.sha256", "run/versions.json", "run/images.lock.yaml",
              "run/feature_allowlist.yaml", "run/thresholds.json", "manifest/manifest.json", "stats/baseline_stats.json",
              "checksums/SHA256SUMS", "COMPLETE"):
        assert k in keys, k
    for a in m["attempts"]:
        akey = a["attempt_key"]
        for k in (f"export/attempts/{akey}/features.parquet", f"export/attempts/{akey}/verification.parquet",
                  f"attempts/{akey}/probes.parquet", f"attempts/{akey}/verification.json"):
            assert k in keys, k
        if a["run_type"] == "F6":
            assert f"attempts/{akey}/injector.log" in keys
        if a["resources"] and a["run_type"] != "F6":
            assert any(k.startswith(f"attempts/{akey}/chaos/") for k in keys)
    for s in m["campaign_segments"]:
        if s["segment_type"] in ("WARMUP", "BASELINE"):
            start = int(s["start_at"])
            for k in (f"baseline/features/chunk-{start}.parquet", f"baseline/verification/chunk-{start}.parquet",
                      f"probes/baseline/chunk-{start}.parquet"):
                assert k in keys, k
        if s["segment_type"] == "TRAILING":
            start = int(s["start_at"])
            for k in (f"export/trailing/features/chunk-{start}.parquet", f"export/trailing/verification/chunk-{start}.parquet",
                      f"probes/trailing/chunk-{start}.parquet"):
                assert k in keys, k


LAYOUT_11_7 = [r"^plan/plan\.json$", r"^plan/plan\.json\.sha256$", r"^run/versions\.json$", r"^run/images\.lock\.yaml$",
               r"^run/feature_allowlist\.yaml$", r"^run/thresholds\.json$", r"^manifest/manifest\.json$",
               r"^stats/baseline_stats\.json$", r"^baseline/(features|verification)/chunk-\d+\.parquet$",
               r"^export/attempts/[a-z0-9-]+/(features|verification)\.parquet$",
               r"^export/trailing/(features|verification)/chunk-\d+\.parquet$", r"^probes/(baseline|trailing)/chunk-\d+\.parquet$",
               r"^attempts/[a-z0-9-]+/(probes\.parquet|verification\.json|injector\.log)$",
               r"^attempts/[a-z0-9-]+/chaos/(APPLY_PLUS_10S|MID|BEFORE_DELETE|AFTER_DELETE)-[^/]+\.yaml$",
               r"^logs/orchestrator-\d+\.log\.gz$", r"^checksums/SHA256SUMS$", r"^COMPLETE$"]


def test_every_object_is_an_11_7_object_and_resume_state_stays_on_the_volume(done):
    """CONTRACT_CHANGES C1: only manifest.json leaves the manifest volume. The resume state under /manifest/state/ is never
    uploaded, never listed in SHA256SUMS and never counted in COMPLETE."""
    import re
    keys = sorted(done.s3.objects)
    off = [k for k in keys if not any(re.match(p, k) for p in LAYOUT_11_7)]
    assert not off, off[:5]
    assert [k for k in keys if k.startswith("manifest/")] == ["manifest/manifest.json"]
    sums = done.s3.get("checksums/SHA256SUMS").decode().splitlines()
    assert [ln.split("  ", 1)[1] for ln in sums] == [k for k in keys if k not in ("COMPLETE", "checksums/SHA256SUMS")]
    marker = json.loads(done.s3.get("COMPLETE"))
    assert marker["object_count"] == len(sums)
    assert marker["total_bytes"] == sum(len(done.s3.get(k)) for k in keys if k not in ("COMPLETE", "checksums/SHA256SUMS"))
    vol = sorted(p.relative_to(done.volume).as_posix() for p in done.volume.rglob("*") if p.is_file())
    assert "manifest.json" in vol and all(f == "manifest.json" or f.startswith("state/") for f in vol)
    assert "state/plan.json" in vol and any(f.endswith("/facts.json") for f in vol)


def test_kubernetes_object_names_follow_11_11(done):
    names = [c[2] for c in done.kube.calls if c[0] == "create"]
    for n in names:
        assert len(n) <= 63
        assert n.split("-")[0] in ("pk", "nc", "sc", "poison", "export"), n
    for c in done.world.chaos:
        assert c.labels[ids.LABEL_CAMPAIGN] == done.campaign_id and ids.LABEL_ATTEMPT in c.labels
        assert c.labels[ids.LABEL_RUN_TYPE] in ids.RUN_TYPES
