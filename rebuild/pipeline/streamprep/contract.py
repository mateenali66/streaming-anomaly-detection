"""Constants and helpers taken from section 11 of diagnostics/REBUILD_PROTOCOL.md.

Every literal here is quoted from the interface contract. A change to the contract bumps
``schema_version`` there, and this module must change with it.
"""
from __future__ import annotations

import hashlib
import json
import math
import re
from pathlib import Path
from typing import Any

SCHEMA_VERSION = "1"

WORKLOADS = ("ecommerce", "iot")
WORKLOAD_SHORT = {"ecommerce": "ec", "iot": "iot"}
CAMPAIGN_LETTERS = {1: "a", 2: "b", 3: "c", 4: "d", 5: "e"}

RUN_TYPES = ("F0", "F1", "F2", "F4", "F5", "F6", "F7", "F8")
FAULT_RUN_TYPES = ("F1", "F2", "F4", "F5", "F6", "F7", "F8")
CHAOS_RUN_TYPES = ("F1", "F2", "F4", "F5", "F7", "F8")
RECOVER_EVENT_RUN_TYPES = ("F2", "F4", "F7", "F8")  # labelled end from the latest Recover event
FIXED_SPAN_RUN_TYPES = ("F1", "F5")  # labelled end is onset plus 300 s

CAMPAIGN_SEGMENT_TYPES = ("WARMUP", "BASELINE", "TRAILING", "IDLE")
ATTEMPT_SEGMENT_ORDER = ("SETTLE", "PRE_FAULT", "APPLY", "INJECTION", "RECOVERY", "COOLDOWN", "GATE")
ALL_SEGMENT_TYPES = CAMPAIGN_SEGMENT_TYPES + ATTEMPT_SEGMENT_ORDER
# Segment types that carry a window grid (protocol 3.2). Every other type has no windows.
WINDOWED_SEGMENT_TYPES = ("PRE_FAULT", "INJECTION", "RECOVERY", "BASELINE", "TRAILING")

OUTCOMES = ("VALID", "VALID_REMEDIATED", "GATE_FAILED", "TELEMETRY_FAILED", "ABORTED")
VALID_OUTCOMES = ("VALID", "VALID_REMEDIATED")
ATTEMPT_STATUSES = ("RUNNING", "FINISHED")
FLAGS = ("AFTER_REDEPLOY", "AFTER_RESUBMIT", "OVERRUN", "F7_TM_LOSS", "REMEDIATED", "HEALTH_EVENT", "SERIES_GAP",
         "SIGNATURE_FALSE_PASS")
# SIGNATURE_FALSE_PASS is set by the analysis on the windows of a fault whose signature false-passed on two or more shams
# (2.5, decision 22, CONTRACT_CHANGES C23). The orchestrator never writes it into the manifest.
MANIFEST_FLAGS = tuple(f for f in FLAGS if f != "SIGNATURE_FALSE_PASS")
# Flags that add the extra settle of 1.3 item 1 (C28)
EXTRA_SETTLE_FLAGS = ("AFTER_REDEPLOY", "AFTER_RESUBMIT")
ONSET_SOURCES = ("CHAOS_APPLY_EVENT", "FIRST_PRODUCE_ACK", "PLANNED")
END_SOURCES = ("CHAOS_RECOVER_EVENT", "FIXED_SPAN", "LAST_PRODUCE_ACK", "PLANNED")
EXPORT_STATUSES = ("PENDING", "DONE", "FAILED")
CAMPAIGN_STATUSES = ("RUNNING", "COMPLETE", "ABANDONED")
HALT_RESOLUTIONS = ("RESUMED", "ABANDONED")

LABELS = ("NORMAL", "ANOMALOUS", "RECOVERY", "SHAM", "SHAM_POST", "TRAILING", "EXCLUDED")
FOLD_ROLES = ("train", "hpo_val", "cal", "test", "eval_only")

FEATURE_SOURCES = ("broker", "consumer", "flink", "jvm")
VERIFICATION_SOURCES = ("cadvisor", "kube_state", "kafka_controller", "kafka_broker_copy", "scrape")
METRIC_TYPES = ("counter", "gauge", "summary", "histogram", "unknown")
POD_ROLES = ("broker", "jobmanager", "taskmanager", "loadgen", "other")
REMOVED_LABELS = (
    "instance", "pod", "pod_ip", "host", "tm_id", "kubernetes_pod_name", "uid",
    "endpoint", "service", "namespace", "job", "prometheus",
)
PROBE_KINDS = ("tcp", "tcp_burst", "controller", "flink_jobs", "flink_tms", "loadgen", "consumer")
STATS = ("mean", "std", "min", "max", "slope", "iqr")
BROKER_SCOPES = ("b0", "b1", "b2")
TM_SCOPES = ("tm_min", "tm_max", "tm_mean")
DERIVED_NAMES = (
    "lag_velocity", "lag_acceleration", "throughput_ratio", "checkpoint_overhead_ratio",
    "isr_stability", "rebalance_frequency", "backpressure_ratio", "watermark_lag",
)

FLINK_JOBS = {
    "ecommerce": ("Event Enrichment Job", "Windowed Aggregation Job", "Fraud Detection Job"),
    "iot": ("IoT Event Enrichment Job", "IoT Per-Location Aggregation Job", "IoT Threshold Alert Job"),
}

PHASE_KEYS = (
    "settle_max", "pre_fault", "recovery", "cooldown", "gate_hold", "gate_timeout",
    "remediation_timeout", "apply_max", "redeploy_extra_settle",
)

ATTEMPT_KEY_RE = re.compile(r"^(ecommerce|iot)-c[0-5]-b(0[1-9]|10)-p[1-8]-a[1-9]$")
CELL_KEY_RE = re.compile(r"^(ecommerce|iot)-c[0-5]-b(0[1-9]|10)-p[1-8]$")
CAMPAIGN_ID_RE = re.compile(r"^[a-z]{2,5}(-[a-z0-9]{1,3})$")
UUID4_RE = re.compile(r"^[0-9a-f]{8}-[0-9a-f]{4}-4[0-9a-f]{3}-[89ab][0-9a-f]{3}-[0-9a-f]{12}$")
SHA256_RE = re.compile(r"^[0-9a-f]{64}$")
CHUNK_RE = re.compile(r"^chunk-(\d+)\.parquet$")
LABEL_VALUE_SANITIZE_RE = re.compile(r"[^A-Za-z0-9_]")


class ContractError(Exception):
    """An input breaks the interface contract. The build stops."""

    def __init__(self, messages: list[str] | str):
        if isinstance(messages, str):
            messages = [messages]
        self.messages = list(messages)
        super().__init__("\n".join(self.messages))


class BuildError(Exception):
    """A preprocessing rule fails on contract-conformant input. The build stops."""

    def __init__(self, messages: list[str] | str):
        if isinstance(messages, str):
            messages = [messages]
        self.messages = list(messages)
        super().__init__("\n".join(self.messages))


class Errors:
    """Collects messages so one validation pass reports every problem it finds."""

    def __init__(self) -> None:
        self.items: list[str] = []

    def add(self, msg: str) -> None:
        self.items.append(msg)

    def raise_if_any(self, kind: type[Exception] = ContractError) -> None:
        if self.items:
            raise kind(self.items)

    def __bool__(self) -> bool:
        return bool(self.items)


def campaign_id_for(workload: str, campaign_index: int) -> str:
    if workload not in WORKLOADS:
        raise ContractError(f"unknown workload {workload!r}")
    if campaign_index == 0:
        return f"pilot-{WORKLOAD_SHORT[workload]}"
    if campaign_index not in CAMPAIGN_LETTERS:
        raise ContractError(f"campaign_index {campaign_index} outside 0 to 5")
    return f"{WORKLOAD_SHORT[workload]}-{CAMPAIGN_LETTERS[campaign_index]}"


def campaign_index_from_id(campaign_id: str) -> tuple[str, int]:
    """Return (workload, campaign_index) for a campaign_id of 11.2."""
    for workload, short in WORKLOAD_SHORT.items():
        if campaign_id == f"pilot-{short}":
            return workload, 0
        for idx, letter in CAMPAIGN_LETTERS.items():
            if campaign_id == f"{short}-{letter}":
                return workload, idx
    raise ContractError(f"campaign_id {campaign_id!r} does not follow 11.2")


def cluster_name_for(campaign_id: str) -> str:
    return f"streaming-anomaly-detection-{campaign_id}"


def cell_key_for(workload: str, campaign_index: int, block: int, position: int) -> str:
    return f"{workload}-c{campaign_index}-b{block:02d}-p{position}"


def attempt_key_for(cell_key: str, attempt: int) -> str:
    return f"{cell_key}-a{attempt}"


def parse_attempt_key(key: str) -> tuple[str, int, int, int, int]:
    """Return (workload, campaign_index, block, position, attempt)."""
    if not ATTEMPT_KEY_RE.match(key):
        raise ContractError(f"attempt_key {key!r} does not match the 11.2 regex")
    workload, c, b, p, a = key.split("-")
    return workload, int(c[1:]), int(b[1:]), int(p[1:]), int(a[1:])


SEED_RULE_TEMPLATE = "int(sha256('rebuild/<workload>/campaign-<i>').hexdigest()[:16],16)"
GENERATOR_STRING = "numpy.random.Generator(numpy.random.PCG64(seed))"


def seed_for(workload: str, campaign_index: int) -> int:
    text = f"rebuild/{workload}/campaign-{campaign_index}"
    return int(hashlib.sha256(text.encode("utf-8")).hexdigest()[:16], 16)


def seed_rule_strings(workload: str, campaign_index: int) -> tuple[str, str]:
    """The literal template and the substituted form. The contract does not say which is stored."""
    concrete = SEED_RULE_TEMPLATE.replace("<workload>", workload).replace("<i>", str(campaign_index))
    return SEED_RULE_TEMPLATE, concrete


def canonical_json_bytes(obj: Any) -> bytes:
    """11.1.5 canonical JSON."""
    return json.dumps(obj, sort_keys=True, separators=(",", ":"), ensure_ascii=False,
                      allow_nan=False).encode("utf-8")


def _reject_constant(name: str) -> Any:
    raise ContractError(f"JSON constant {name} is not allowed (11.1.5 allow_nan=False)")


def _no_duplicate_keys(pairs: list[tuple[str, Any]]) -> dict:
    out: dict = {}
    for k, v in pairs:
        if k in out:
            raise ContractError(f"duplicate JSON key {k!r}")
        out[k] = v
    return out


def load_json_bytes(data: bytes, what: str) -> Any:
    try:
        text = data.decode("utf-8")
    except UnicodeDecodeError as exc:
        raise ContractError(f"{what}: not UTF-8 ({exc})") from exc
    try:
        return json.loads(text, parse_constant=_reject_constant, object_pairs_hook=_no_duplicate_keys)
    except json.JSONDecodeError as exc:
        raise ContractError(f"{what}: invalid JSON ({exc})") from exc


def sha256_bytes(data: bytes) -> str:
    return hashlib.sha256(data).hexdigest()


def sha256_file(path: Path) -> str:
    h = hashlib.sha256()
    with open(path, "rb") as fh:
        for block in iter(lambda: fh.read(1 << 20), b""):
            h.update(block)
    return h.hexdigest()


def series_id_for(metric: str, pod: str, labels_value: Any) -> str:
    """11.8 series_id. ``labels_value`` is either the labels dict or the canonical labels string."""
    payload = canonical_json_bytes({"pod": pod, "labels": labels_value}).decode("utf-8")
    return hashlib.sha256((metric + "|" + payload).encode("utf-8")).hexdigest()[:16]


def to_ms(value: float, what: str, errs: Errors | None = None) -> int:
    """Instants and durations carry millisecond precision (11.1.1). Returns integer milliseconds."""
    if isinstance(value, bool) or not isinstance(value, (int, float)) or not math.isfinite(value):
        msg = f"{what}: {value!r} is not a finite number"
        if errs is None:
            raise ContractError(msg)
        errs.add(msg)
        return 0
    ms = round(value * 1000.0)
    if abs(value * 1000.0 - ms) > 0.05:
        msg = f"{what}: {value!r} has more than millisecond precision"
        if errs is None:
            raise ContractError(msg)
        errs.add(msg)
    return int(ms)


def floor10(x_ms: int) -> int:
    """Largest multiple of 10 s (in ms) at or before x_ms."""
    return (x_ms // 10000) * 10000


def ceil10(x_ms: int) -> int:
    return -((-x_ms) // 10000) * 10000


def sanitize_label_value(value: str) -> str:
    return LABEL_VALUE_SANITIZE_RE.sub("_", value)


def label_for(segment_type: str, run_type: str | None, outcome: str | None) -> str:
    """Protocol 3.3. EXCLUDED is the default for anything the table does not label."""
    if segment_type == "BASELINE":
        return "NORMAL"
    if segment_type == "TRAILING":
        return "TRAILING"
    if segment_type in ("WARMUP", "IDLE", "SETTLE", "APPLY", "COOLDOWN", "GATE"):
        return "EXCLUDED"
    if segment_type not in ("PRE_FAULT", "INJECTION", "RECOVERY"):
        raise ContractError(f"unknown segment type {segment_type!r}")
    if outcome not in VALID_OUTCOMES:
        return "EXCLUDED"
    if run_type not in RUN_TYPES:
        raise ContractError(f"unknown run type {run_type!r}")
    sham = run_type == "F0"
    if segment_type == "PRE_FAULT":
        return "NORMAL"
    if segment_type == "INJECTION":
        return "SHAM" if sham else "ANOMALOUS"
    return "SHAM_POST" if sham else "RECOVERY"
