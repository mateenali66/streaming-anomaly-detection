"""Configuration and the infra interface: WORKLOAD has no default, the environment that
rebuild/infra/k8s/base/orchestrator/orchestrator-job.yaml sets is exactly the one interface of 11.11 (CONTRACT_CHANGES C5),
retired spellings are refused, and every mounted input is checked against what this code regenerates."""
import json
from pathlib import Path

import pytest

from orchestrator import ids, thresholds
from orchestrator.config import RETIRED_ENV, environment_from, load_config, real_run_preconditions, validate_config
from orchestrator.errors import ConfigError
from orchestrator.plan import generate_plan, plan_bytes
from orchestrator.canonical import sha256_hex

CFG = load_config()
SHA = sha256_hex(plan_bytes(generate_plan("iot", 2, CFG)))
INFRA_ENV = {  # rebuild/infra/k8s/base/orchestrator/orchestrator-job.yaml, rendered
    "WORKLOAD": "IOT", "FLINK_EXPECTED_JOBS": "IoT Event Enrichment Job,IoT Per-Location Aggregation Job,IoT Threshold Alert Job",
    "LOADGEN_BASE_RATE": "10000", "CAMPAIGN_ID": "iot-b", "CAMPAIGN_INDEX": "2", "PLAN_SHA256": SHA,
    "PLAN_PATH": "/config/plan.json", "THRESHOLDS_PATH": "/config/thresholds.json", "VERSIONS_FILE": "/config/versions.json",
    "IMAGES_LOCK_FILE": "/config/images.lock.yaml",
    "RESULTS_BUCKET": "bucket", "RESULTS_PREFIX": "campaign/iot-b", "AWS_REGION": "us-east-2", "SNS_TOPIC_ARN": "arn:x",
    "PROMETHEUS_URL": ids.PROMETHEUS_URL, "KAFKA_BOOTSTRAP": ids.KAFKA_BOOTSTRAP, "FLINK_REST_URL": ids.FLINK_REST,
    "LOADGEN_STATUS_URL": ids.LOADGEN_STATUS_URL, "PROBES_URL": ids.PROBES_SERVICE_URL, "NAMESPACE": "streaming",
    "MANIFEST_DIR": "/manifest", "F6_INJECTOR_IMAGE": "reg/f6@sha256:" + "a" * 64, "KUBECTL_BIN": "/usr/local/bin/kubectl",
    "HOME": "/tmp",
}


def infra_job_env() -> dict:
    """The env: block of the infra Job manifest, read from the file (literal values and ConfigMap references)."""
    import yaml
    path = Path(__file__).resolve().parents[2] / "infra" / "k8s" / "base" / "orchestrator" / "orchestrator-job.yaml"
    docs = [d for d in yaml.safe_load_all(path.read_text()) if d and d["kind"] == "Job"]
    c = next(c for c in docs[0]["spec"]["template"]["spec"]["containers"] if c["name"] == "orchestrator")
    return {e["name"]: e.get("value", "<configmap>") for e in c["env"]}


def test_infra_environment_is_the_contract_environment():
    e = environment_from(INFRA_ENV)
    assert (e.workload, e.campaign_id, e.campaign_index) == ("iot", "iot-b", 2)
    assert e.plan_sha256 == SHA and e.results_prefix == "s3://bucket/campaign/iot-b/"
    assert e.results_bucket == "bucket" and e.results_key_prefix == "campaign/iot-b"
    assert e.manifest_dir == "/manifest" and e.alerts_topic_arn == "arn:x" and e.kubectl_bin == "/usr/local/bin/kubectl"
    assert (e.plan_path, e.thresholds_path, e.versions_file, e.images_lock_file) == (
        "/config/plan.json", "/config/thresholds.json", "/config/versions.json", "/config/images.lock.yaml")


def test_infra_job_manifest_sets_exactly_these_names():
    """The Job manifest and this code agree name for name: every required name is set, no retired name is."""
    env = infra_job_env()
    for k in ("WORKLOAD", "CAMPAIGN_ID", "CAMPAIGN_INDEX", "PLAN_SHA256", "RESULTS_BUCKET", "RESULTS_PREFIX", "PLAN_PATH",
              "THRESHOLDS_PATH", "VERSIONS_FILE", "IMAGES_LOCK_FILE", "F6_INJECTOR_IMAGE", "LOADGEN_STATUS_URL", "PROBES_URL",
              "NAMESPACE", "MANIFEST_DIR"):
        assert k in env, k
    assert not set(env) & set(RETIRED_ENV)
    assert env["RESULTS_PREFIX"] == "campaign/${CAMPAIGN_ID}" and env["LOADGEN_STATUS_URL"] == ids.LOADGEN_STATUS_URL
    assert env["PROBES_URL"] == ids.PROBES_SERVICE_URL and env["PLAN_PATH"] == "/config/plan.json"


@pytest.mark.parametrize("change", [
    {"WORKLOAD": None}, {"WORKLOAD": "iot"}, {"WORKLOAD": "ECOM"}, {"CAMPAIGN_ID": "ec-b"}, {"CAMPAIGN_INDEX": "3"},
    {"CAMPAIGN_INDEX": None}, {"CAMPAIGN_INDEX": "x"}, {"PLAN_SHA256": None}, {"PLAN_SHA256": "B" * 64},
    {"RESULTS_BUCKET": None}, {"RESULTS_BUCKET": "s3://bucket"}, {"RESULTS_PREFIX": "campaign/iot-c"},
    {"RESULTS_PREFIX": "s3://bucket/campaign/iot-b/"}, {"RESULTS_PREFIX": "campaign/iot-b/"}, {"NAMESPACE": "default"},
    {"FLINK_EXPECTED_JOBS": "Event Enrichment Job,Windowed Aggregation Job,Fraud Detection Job"},
    {"SCHEDULE_SHA256": SHA}, {"SCHEDULE_PATH": "/config/schedule.json"}, {"CAMPAIGN_SEED": "1"},
    {"LOADGEN_METRICS_URL": "http://x:9102/metrics"},
])
def test_bad_environment_is_refused(change):
    env = dict(INFRA_ENV)
    for k, v in change.items():
        if v is None:
            env.pop(k, None)
        else:
            env[k] = v
    with pytest.raises(ConfigError):
        environment_from(env)


def test_workload_has_no_default_anywhere():
    env = dict(INFRA_ENV)
    del env["WORKLOAD"]
    with pytest.raises(ConfigError, match="WORKLOAD"):
        environment_from(env)
    import yaml
    from orchestrator.config import DEFAULT_CONFIG
    assert "workload" not in yaml.safe_load(DEFAULT_CONFIG.read_text())


def test_config_rejects_unknown_and_missing_keys_and_bad_values():
    import copy
    raw = copy.deepcopy(CFG)
    raw["surprise"] = 1
    with pytest.raises(ConfigError):
        validate_config(raw)
    raw = copy.deepcopy(CFG)
    del raw["halt_total_failed_gt"]
    with pytest.raises(ConfigError):
        validate_config(raw)
    for k, v in (("pre_fault_s", 450), ("settle_max_s", 200), ("f1_labelled_span", "X"), ("halt_consecutive_failed", 0)):
        raw = copy.deepcopy(CFG)
        raw[k] = v
        with pytest.raises(ConfigError):
            validate_config(raw)


def test_real_run_refuses_unpinned_images_shortened_phases_and_not_ready_span(tmp_path):
    env = environment_from(INFRA_ENV)
    env.orchestrator_image = "reg/orch:latest"
    env.git_commit = "sha256-tree:" + "d" * 64
    probs = real_run_preconditions(env, CFG)
    assert any("own image" in p for p in probs)
    assert sum("does not exist" in p for p in probs) == 4  # the four mounted inputs are not on this machine
    env.orchestrator_image = "reg/orch@sha256:" + "e" * 64
    for attr in ("plan_path", "thresholds_path", "versions_file", "images_lock_file"):
        f = tmp_path / attr
        f.write_text("x")
        setattr(env, attr, str(f))
    assert real_run_preconditions(env, CFG) == []
    env.versions_file = None
    assert any("VERSIONS_FILE is not set" in p for p in real_run_preconditions(env, CFG))
    env.versions_file = str(tmp_path / "versions_file")
    short = load_config(overrides={"fixed": {"baseline_s": 600}})
    assert any("baseline_s" in p for p in real_run_preconditions(env, short))
    nr = load_config(overrides={"f1_labelled_span": "NOT_READY"})
    assert any("NOT_READY" in p for p in real_run_preconditions(env, nr))


def test_thresholds_file_is_what_the_code_registers(tmp_path):
    from orchestrator.cli import main
    out = tmp_path / "thresholds.json"
    assert main(["thresholds", "--out", str(out)]) == 0
    assert out.read_bytes() == thresholds.thresholds_bytes(thresholds.build(CFG))
    th = json.loads(out.read_bytes())
    assert th["gate"]["F8"]["target_limit_frac"] == 0.85 and th["campaign_rules"]["halt_total_failed_gt"] == 8
    assert th["prometheus_jobs"]["controller"] == "kafka-brokers"


def test_mounted_plan_must_equal_the_regenerated_plan(make):
    h = make()
    h.plan_file_bytes = h.plan_bytes.replace(b'"settle_gap_s":', b'"settle_gap_s": ', 1)
    assert h.run() == 2
    assert not (h.volume / "manifest.json").exists()


def test_load_generator_seed_must_be_the_plan_seed(make):
    h = make(world={"loadgen_seed": 12345})
    assert h.run() == 2  # preflight refuses before the warm-up
    assert not (h.volume / "manifest.json").exists()


def test_exporter_and_injector_jobs_use_the_infra_environment(make):
    h = make()
    assert h.run(only_fault="F6") == 0
    rt = h.runtime()
    from orchestrator.runtime import JobRunner
    job = JobRunner(rt).export_job("export-x", "attempt", "x", 1.0, 2.0, "x")
    spec = job["spec"]["template"]["spec"]
    env = {e["name"]: e["value"] for e in spec["containers"][0]["env"]}
    assert spec["serviceAccountName"] == "metric-exporter" and spec["automountServiceAccountToken"] is False
    assert env["RESULTS_PREFIX"] == f"campaign/{h.campaign_id}" and env["RESULTS_BUCKET"] == "dry-run-results"
    assert env["WORKLOAD"] == "ECOMMERCE" and spec["containers"][0]["securityContext"]["readOnlyRootFilesystem"]
    log = h.s3.get(f"attempts/{h.manifest()['attempts'][0]['attempt_key']}/injector.log").decode()
    assert "INJECTED sent=" in log and "orchestrator_observed" in log
