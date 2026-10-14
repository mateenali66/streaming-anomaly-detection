"""Configuration: the one config file plus the required environment (protocol 11.11, Orchestrator row).

Unknown keys, missing keys and wrong types are errors. The workload has no default anywhere.
"""
from __future__ import annotations

import copy
import os
import re
from dataclasses import dataclass, field
from pathlib import Path
from typing import Any

import yaml

from . import ids
from .errors import ConfigError

PACKAGE_ROOT = Path(__file__).resolve().parent.parent
DEFAULT_CONFIG = PACKAGE_ROOT / "config" / "campaign_config.yaml"
DEFAULT_ALLOWLIST = PACKAGE_ROOT / "config" / "feature_allowlist.yaml"

PROTOCOL_FIXED = {
    "warmup_s": 3600, "baseline_s": 86400, "pilot_baseline_s": 7200, "trailing_s": 3600, "chunk_s": 3600,
    "recovery_s": 300, "cooldown_s": 300, "gate_hold_s": 120, "gate_timeout_s": 900,
    "remediation_timeout_s": 900, "apply_max_s": 30, "redeploy_extra_settle_s": 600,
    "pause_abandon_s": 7200, "s3_failure_halt_s": 900,
}
PROMETHEUS_JOB_KEYS = ("brokers", "flink", "loadgen", "controller", "kubelet", "kube_state", "probes")
METRIC_NAME_KEYS = ("messages_in", "under_replicated", "under_min_isr", "offline_partitions", "preferred_imbalance",
                    "fenced_brokers", "active_controller", "consumer_lag_max", "tm_pod_regex")
TOP_KEYS = {
    "schema_version": str, "pre_fault_s": int, "settle_max_s": int, "trailing_baseline": bool,
    "f5_job_recovery_limit_s": int, "f8_target_limit_permille": int, "f8_min_stressor_mib": int,
    "halt_consecutive_failed": int, "halt_total_failed_gt": int, "f1_labelled_span": str,
    "f1_f2_urp_presence_check": bool, "fixed": dict, "prometheus_jobs": dict, "metric_names": dict,
    "expected_kubernetes_version": str,
}
FEATURE_SOURCES = ("broker", "consumer", "flink", "jvm")
VERIFICATION_SOURCES = ("cadvisor", "kube_state", "kafka_controller", "kafka_broker_copy", "scrape")
# The orchestrator environment (protocol 11.11 as recorded in rebuild/CONTRACT_CHANGES.md, item C5). One spelling only:
# rebuild/infra/k8s/base/orchestrator/orchestrator-job.yaml sets exactly these names, and nothing has a default.
#   WORKLOAD        ECOMMERCE or IOT, the upper-case form of the 11.2 token (the R1 load generator and submit-jobs.sh read the
#                   same ConfigMap value); every record carries the lower-case token
#   CAMPAIGN_ID     11.2
#   CAMPAIGN_INDEX  11.2, must agree with CAMPAIGN_ID
#   PLAN_SHA256     the committed hash of plan/plan.json (11.3)
#   RESULTS_BUCKET  the results bucket of the shared root
#   RESULTS_PREFIX  campaign/<campaign_id>, no s3:// and no slash at either end
# A real run also needs the mounted files PLAN_PATH, THRESHOLDS_PATH, VERSIONS_FILE and IMAGES_LOCK_FILE (real_run_preconditions).
REQUIRED_ENV = ("WORKLOAD", "CAMPAIGN_ID", "CAMPAIGN_INDEX", "PLAN_SHA256", "RESULTS_BUCKET", "RESULTS_PREFIX")
# Names an earlier draft of the interface used. Setting one is a stale manifest, so it is refused, never silently mapped.
RETIRED_ENV = {"SCHEDULE_PATH": "PLAN_PATH", "SCHEDULE_SHA256": "PLAN_SHA256", "CAMPAIGN_SEED": "(none: the plan holds the seed)",
               "LOADGEN_METRICS_URL": "LOADGEN_STATUS_URL"}
WORKLOAD_ENV = {"ECOMMERCE": "ecommerce", "IOT": "iot"}
MOUNTED_FILES = ("plan_path", "thresholds_path", "versions_file", "images_lock_file")


def _is_int(v: Any) -> bool:
    return isinstance(v, int) and not isinstance(v, bool)


def load_config(path: str | os.PathLike | None = None, overrides: dict | None = None) -> dict:
    p = Path(path) if path else DEFAULT_CONFIG
    try:
        raw = yaml.safe_load(p.read_text(encoding="utf-8"))
    except FileNotFoundError as e:
        raise ConfigError(f"config file not found: {p}") from e
    if overrides:
        raw = deep_merge(raw, overrides)
    return validate_config(raw)


def deep_merge(base: dict, over: dict) -> dict:
    out = copy.deepcopy(base)
    for k, v in over.items():
        if isinstance(v, dict) and isinstance(out.get(k), dict):
            out[k] = deep_merge(out[k], v)
        else:
            out[k] = copy.deepcopy(v)
    return out


def validate_config(cfg: Any) -> dict:
    if not isinstance(cfg, dict):
        raise ConfigError("config must be a mapping")
    unknown = set(cfg) - set(TOP_KEYS)
    missing = set(TOP_KEYS) - set(cfg)
    if unknown:
        raise ConfigError(f"unknown config keys: {sorted(unknown)}")
    if missing:
        raise ConfigError(f"missing config keys: {sorted(missing)}")
    for k, t in TOP_KEYS.items():
        v = cfg[k]
        ok = _is_int(v) if t is int else isinstance(v, t)
        if not ok:
            raise ConfigError(f"config key {k} must be {t.__name__}, got {type(v).__name__}")
    if cfg["schema_version"] != "1":
        raise ConfigError("config schema_version must be '1'")
    if cfg["pre_fault_s"] not in (300, 600):
        raise ConfigError("pre_fault_s must be 600 (protocol default) or 300 (cost option C)")
    if cfg["settle_max_s"] not in (120, 300):
        raise ConfigError("settle_max_s must be 300 (protocol default) or 120 (cost option C)")
    if cfg["f1_labelled_span"] not in ("FIXED_300", "NOT_READY"):
        raise ConfigError("f1_labelled_span must be FIXED_300 or NOT_READY")
    for k in ("f5_job_recovery_limit_s", "f8_target_limit_permille", "f8_min_stressor_mib",
              "halt_consecutive_failed", "halt_total_failed_gt"):
        if cfg[k] <= 0:
            raise ConfigError(f"{k} must be positive")
    if not 0 < cfg["f8_target_limit_permille"] < 1000:
        raise ConfigError("f8_target_limit_permille must be between 1 and 999")
    fixed = cfg["fixed"]
    if set(fixed) != set(PROTOCOL_FIXED):
        raise ConfigError(f"fixed must have exactly the keys {sorted(PROTOCOL_FIXED)}")
    for k, v in fixed.items():
        if not _is_int(v) or v < 0:
            raise ConfigError(f"fixed.{k} must be a non-negative integer")
    if fixed["chunk_s"] <= 0 or fixed["chunk_s"] % 10:
        raise ConfigError("fixed.chunk_s must be a positive multiple of 10")
    if set(cfg["prometheus_jobs"]) != set(PROMETHEUS_JOB_KEYS):
        raise ConfigError(f"prometheus_jobs must have exactly {PROMETHEUS_JOB_KEYS}")
    if set(cfg["metric_names"]) != set(METRIC_NAME_KEYS):
        raise ConfigError(f"metric_names must have exactly {METRIC_NAME_KEYS}")
    for section in ("prometheus_jobs", "metric_names"):
        for k, v in cfg[section].items():
            if not isinstance(v, str) or not v:
                raise ConfigError(f"{section}.{k} must be a non-empty string")
            if '"' in v or "\\" in v:
                raise ConfigError(f"{section}.{k} must not contain quotes or backslashes")
    return cfg


def protocol_fixed_violations(cfg: dict) -> list[str]:
    """Differences from the protocol-fixed values. A real run refuses to start if this is non-empty."""
    return [f"fixed.{k}={cfg['fixed'][k]} (protocol {v})" for k, v in PROTOCOL_FIXED.items() if cfg["fixed"][k] != v]


def load_allowlist(path: str | os.PathLike | None = None) -> tuple[dict, bytes]:
    p = Path(path) if path else DEFAULT_ALLOWLIST
    data = p.read_bytes()
    allow = yaml.safe_load(data.decode("utf-8"))
    validate_allowlist(allow)
    return allow, data


def validate_allowlist(allow: Any) -> None:
    if not isinstance(allow, dict):
        raise ConfigError("feature allowlist must be a mapping of source to pattern list")
    expected = set(FEATURE_SOURCES) | set(VERIFICATION_SOURCES)
    if set(allow) != expected:
        raise ConfigError(f"feature allowlist must have exactly the sources {sorted(expected)}")
    for src, pats in allow.items():
        if not isinstance(pats, list) or not pats or not all(isinstance(p, str) and p for p in pats):
            raise ConfigError(f"allowlist source {src} must be a non-empty list of patterns")
        for pat in pats:
            if '"' in pat or "\\" in pat:
                raise ConfigError(f"allowlist pattern {pat!r} must not contain quotes or backslashes")
            try:
                rx = re.compile(f"^(?:{pat})$")
            except re.error as e:
                raise ConfigError(f"allowlist pattern {pat!r} is not a regex: {e}") from e
            # The probe metrics (job rebuild-verification) must never become features (2.3, 4.1.6).
            if src in FEATURE_SOURCES and (rx.match("rebuild_probe_tcp_ok") or rx.match("rebuild_consumer_malformed")):
                raise ConfigError(f"feature pattern {pat!r} would match verification-channel probe metrics")


@dataclass
class Environment:
    workload: str
    campaign_id: str
    campaign_index: int
    plan_sha256: str
    results_bucket: str                    # RESULTS_BUCKET
    results_key_prefix: str                # RESULTS_PREFIX, campaign/<campaign_id>
    manifest_dir: str = "/manifest"
    plan_path: str | None = None           # PLAN_PATH, the committed plan.json (ConfigMap campaign-config)
    thresholds_path: str | None = None     # THRESHOLDS_PATH, the registered thresholds.json (ConfigMap campaign-config)
    versions_file: str | None = None       # VERSIONS_FILE, versions.json (ConfigMap campaign-config)
    images_lock_file: str | None = None    # IMAGES_LOCK_FILE, images.lock.yaml (ConfigMap campaign-config)
    orchestrator_image: str = ""           # taken from this pod's own spec when not set
    f6_injector_image: str = ""
    git_commit: str = ""
    alerts_topic_arn: str | None = None
    prometheus_url: str = ids.PROMETHEUS_URL
    kafka_bootstrap: str = ids.KAFKA_BOOTSTRAP
    flink_rest_url: str = ids.FLINK_REST
    loadgen_status_url: str = ids.LOADGEN_STATUS_URL
    probes_url: str = ids.PROBES_SERVICE_URL
    kubectl_bin: str = "kubectl"
    pod_name: str | None = None
    loadgen_base_rate: str | None = None
    extra: dict = field(default_factory=dict)

    @property
    def image_digest(self) -> str:
        if "@sha256:" in self.orchestrator_image:
            return self.orchestrator_image.split("@", 1)[1]
        return ""

    @property
    def results_prefix(self) -> str:
        """s3://<bucket>/campaign/<id>/ for the object store."""
        return f"s3://{self.results_bucket}/{self.results_key_prefix}/"


def environment_from(env: dict[str, str]) -> Environment:
    missing = [k for k in REQUIRED_ENV if not (env.get(k) or "").strip()]
    if missing:
        raise ConfigError(f"required environment variables missing: {missing} (no defaults, protocol 11.11)")
    retired = sorted(k for k in RETIRED_ENV if k in env)
    if retired:
        raise ConfigError("retired environment variables set: " + ", ".join(f"{k} (now {RETIRED_ENV[k]})" for k in retired)
                          + " (rebuild/CONTRACT_CHANGES.md C5)")
    raw_workload = env["WORKLOAD"].strip()
    if raw_workload not in WORKLOAD_ENV:
        raise ConfigError(f"WORKLOAD must be ECOMMERCE or IOT (11.11, CONTRACT_CHANGES C5), got {raw_workload!r}. "
                          "There is no default.")
    workload = WORKLOAD_ENV[raw_workload]
    try:
        index = int(env["CAMPAIGN_INDEX"])
    except ValueError as e:
        raise ConfigError("CAMPAIGN_INDEX must be an integer 0 to 5") from e
    if not 0 <= index <= 5:
        raise ConfigError("CAMPAIGN_INDEX must be an integer 0 to 5")
    cid = env["CAMPAIGN_ID"].strip()
    if ids.campaign_id_for(workload, index) != cid:
        raise ConfigError(f"CAMPAIGN_ID {cid!r} is not campaign {index} of workload {workload} (11.2)")
    sha = env["PLAN_SHA256"].strip()
    if not re.fullmatch(r"[0-9a-f]{64}", sha):
        raise ConfigError("PLAN_SHA256 must be 64 lower-case hex characters")
    bucket = env["RESULTS_BUCKET"].strip()
    if "/" in bucket or bucket.startswith("s3:"):
        raise ConfigError(f"RESULTS_BUCKET must be a bucket name, got {bucket!r}")
    prefix = env["RESULTS_PREFIX"].strip()
    if prefix != f"campaign/{cid}":
        raise ConfigError(f"RESULTS_PREFIX must be campaign/{cid} (11.7, 11.11), got {prefix!r}")
    ns = env.get("NAMESPACE", ids.NAMESPACE)
    if ns != ids.NAMESPACE:
        raise ConfigError(f"NAMESPACE must be {ids.NAMESPACE} (11.11), got {ns!r}")
    jobs = env.get("FLINK_EXPECTED_JOBS")
    if jobs is not None and [j.strip() for j in jobs.split(",")] != [j[0] for j in ids.FLINK_JOBS[workload]]:
        raise ConfigError(f"FLINK_EXPECTED_JOBS {jobs!r} differs from the 11.11 job names of {workload}")
    return Environment(
        workload=workload, campaign_id=cid, campaign_index=index, plan_sha256=sha, results_bucket=bucket,
        results_key_prefix=prefix, manifest_dir=env.get("MANIFEST_DIR", "/manifest"),
        plan_path=env.get("PLAN_PATH") or None,
        thresholds_path=env.get("THRESHOLDS_PATH") or None,
        versions_file=env.get("VERSIONS_FILE") or None,
        images_lock_file=env.get("IMAGES_LOCK_FILE") or None,
        orchestrator_image=env.get("ORCHESTRATOR_IMAGE", ""),
        f6_injector_image=env.get("F6_INJECTOR_IMAGE", ""),
        git_commit=env.get("ORCHESTRATOR_GIT_COMMIT", ""),
        alerts_topic_arn=env.get("SNS_TOPIC_ARN") or None,
        prometheus_url=env.get("PROMETHEUS_URL", ids.PROMETHEUS_URL),
        kafka_bootstrap=env.get("KAFKA_BOOTSTRAP", ids.KAFKA_BOOTSTRAP),
        flink_rest_url=env.get("FLINK_REST_URL", ids.FLINK_REST),
        loadgen_status_url=env.get("LOADGEN_STATUS_URL", ids.LOADGEN_STATUS_URL),
        probes_url=env.get("PROBES_URL", ids.PROBES_SERVICE_URL),
        kubectl_bin=env.get("KUBECTL_BIN", "kubectl"),
        pod_name=env.get("HOSTNAME") or None,
        loadgen_base_rate=env.get("LOADGEN_BASE_RATE") or None,
    )


def real_run_preconditions(env: Environment, cfg: dict) -> list[str]:
    """Everything a run against a real cluster needs on top of the required variables."""
    problems = protocol_fixed_violations(cfg)
    if "@sha256:" not in env.orchestrator_image:
        problems.append("the orchestrator's own image reference (from its pod spec, or ORCHESTRATOR_IMAGE) must be pinned by digest")
    if "@sha256:" not in env.f6_injector_image:
        problems.append("F6_INJECTOR_IMAGE must be an ECR reference pinned by digest")
    if not env.git_commit:
        problems.append("no code identity (git commit or source-tree hash)")
    if cfg["f1_labelled_span"] != "FIXED_300":
        problems.append("f1_labelled_span NOT_READY needs an end_source code that contract 11.5 does not define")
    for attr in MOUNTED_FILES:
        path = getattr(env, attr)
        name = {"plan_path": "PLAN_PATH", "thresholds_path": "THRESHOLDS_PATH", "versions_file": "VERSIONS_FILE",
                "images_lock_file": "IMAGES_LOCK_FILE"}[attr]
        if not path:
            problems.append(f"{name} is not set (ConfigMap campaign-config, mounted at /config)")
        elif not Path(path).is_file():
            problems.append(f"{name} {path} does not exist")
    return problems
