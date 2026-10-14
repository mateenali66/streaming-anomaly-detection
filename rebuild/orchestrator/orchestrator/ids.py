"""Identifiers (protocol 11.2) and Kubernetes names (protocol 11.11).

No pod name is hard-coded anywhere in the orchestrator. Pods are always found from the live pod list by
the label selectors below at apply time (protocol 2.7.3). The only fixed names are object names that the
contract fixes: the Kafka cluster, the FlinkDeployment, Deployments, Jobs, chaos resources and services.
"""
from __future__ import annotations

import re
import uuid

from .errors import ConfigError

WORKLOADS = ("ecommerce", "iot")
RUN_TYPES = ("F0", "F1", "F2", "F4", "F5", "F6", "F7", "F8")
FAULTS = ("F1", "F2", "F4", "F5", "F6", "F7", "F8")

ATTEMPT_KEY_RE = re.compile(r"^(ecommerce|iot)-c[0-5]-b(0[1-9]|10)-p[1-8]-a[1-9]$")
CELL_KEY_RE = re.compile(r"^(ecommerce|iot)-c[0-5]-b(0[1-9]|10)-p[1-8]$")
CAMPAIGN_ID_RE = re.compile(r"^[a-z]{2,5}(-[a-z0-9]{1,3})$")  # the Terraform rule quoted in 11.2
UUID4_RE = re.compile(r"^[0-9a-f]{8}-[0-9a-f]{4}-4[0-9a-f]{3}-[89ab][0-9a-f]{3}-[0-9a-f]{12}$")

# ----------------------------------------------------------------------------------------------- 11.11
NAMESPACE = "streaming"
OBSERVABILITY_NAMESPACE = "observability"
CHAOS_NAMESPACE = "chaos-mesh"
KAFKA_CLUSTER = "streaming-kafka"
KAFKA_POOL = "brokers"
# Strimzi labels its node-pool pods strimzi.io/cluster and strimzi.io/pool-name, and names them
# <cluster>-<pool>-<node id> (Strimzi docs, "Configuring node pools" and "Moving nodes between node pools",
# context7 /strimzi/strimzi-kafka-operator). So the node ID is the pod ordinal (11.11).
BROKER_SELECTOR = f"strimzi.io/cluster={KAFKA_CLUSTER},strimzi.io/pool-name={KAFKA_POOL}"
BROKER_POD_PREFIX = f"{KAFKA_CLUSTER}-{KAFKA_POOL}-"
BROKER_CONTAINER = "kafka"
KAFKA_PORT_CONTROLLER = 9090
KAFKA_PORT_REPLICATION = 9091
KAFKA_PORT_PLAIN = 9092
KAFKA_BOOTSTRAP = "streaming-kafka-kafka-bootstrap.streaming.svc.cluster.local:9092"
TOPICS = {"raw-events": 12, "enriched-events": 12, "aggregated-metrics": 6, "fraud-alerts": 3, "dead-letter": 3}

FLINK_DEPLOYMENT = "streaming-flink"
FLINK_REST = "http://streaming-flink-rest.streaming.svc.cluster.local:8081"
FLINK_JM_RPC_PORT = 6123
JM_SELECTOR = "app=streaming-flink,component=jobmanager"
TM_SELECTOR = "app=streaming-flink,component=taskmanager"
FLINK_CONTAINER = "flink-main-container"
EXPECTED_TASKMANAGERS = 3
SLOTS_PER_TASKMANAGER = 4
EXPECTED_BROKERS = 3

FLINK_JOBS = {
    "ecommerce": [
        ("Event Enrichment Job", "com.phonotech.streaming.enrichment.EnrichmentJob", "enrichment.jar"),
        ("Windowed Aggregation Job", "com.phonotech.streaming.aggregation.AggregationJob", "aggregation.jar"),
        ("Fraud Detection Job", "com.phonotech.streaming.fraud.FraudDetectionJob", "fraud.jar"),
    ],
    "iot": [
        ("IoT Event Enrichment Job", "com.phonotech.streaming.iot.enrichment.IoTEnrichmentJob", "iot-enrichment.jar"),
        ("IoT Per-Location Aggregation Job", "com.phonotech.streaming.iot.aggregation.IoTAggregationJob", "iot-aggregation.jar"),
        ("IoT Threshold Alert Job", "com.phonotech.streaming.iot.alert.IoTAlertJob", "iot-alert.jar"),
    ],
}
FLINK_JAR_DIR = "/opt/flink/usrlib"

LOADGEN_SELECTOR = "app=load-generator"
# 11.11 (CONTRACT_CHANGES C7): /status (JSON) and /metrics (Prometheus text) on port 9102 of Service load-generator.
LOADGEN_PORT = 9102
LOADGEN_STATUS_URL = f"http://load-generator.streaming.svc.cluster.local:{LOADGEN_PORT}/status"
CONSUMER_DEPLOYMENT = "verification-consumer"
CONSUMER_SELECTOR = "app=verification-consumer"
# 11.11 (C7): consumer group rebuild-verification, /status and /metrics on port 9103.
CONSUMER_GROUP = "rebuild-verification"
CONSUMER_STATUS_PORT = 9103
# 11.11 (C9): Deployment and Service verification-probes, /status, /samples and /metrics on port 8000, Prometheus job
# rebuild-verification (its PodMonitor). The manifest is rebuild/infra/k8s/base/probes/verification-probes.yaml.
PROBES_DEPLOYMENT = "verification-probes"
PROBES_PORT = 8000
PROBES_SERVICE_URL = f"http://verification-probes.streaming.svc.cluster.local:{PROBES_PORT}"
PROBES_JOB_LABEL = "rebuild-verification"

ORCHESTRATOR_JOB = "campaign-orchestrator"
ORCHESTRATOR_SA = "campaign-orchestrator"
EXPORTER_SA = "metric-exporter"
F6_HEADER = "rebuild-attempt"
SENTINEL_HEADER = "rebuild-sentinel"

LABEL_CAMPAIGN = "rebuild/campaign"
LABEL_ATTEMPT = "rebuild/attempt"
LABEL_RUN_TYPE = "rebuild/run-type"

# The orchestrator Role may list only these three kinds (rebuild/infra/k8s/base/rbac/orchestrator-rbac.yaml); F3 is dropped.
CHAOS_KINDS = ("PodChaos", "NetworkChaos", "StressChaos")

PROMETHEUS_URL = "http://prometheus-kube-prometheus-prometheus.observability.svc.cluster.local:9090"
# REVIEW4 MIN-4 and M12 (C30): infra's EventBridge rule writes the region's aws.health events to this CloudWatch Logs group,
# and the orchestrator role may call logs:FilterLogEvents on it. The orchestrator polls it every 10 minutes.
HEALTH_LOG_GROUP = "/streaming-anomaly-detection/health"
HEALTH_POLL_S = 600
HEALTH_LOOKBACK_S = 86400
REGION = "us-east-2"  # 11.4: the one region of the rebuild


def campaign_id_for(workload: str, index: int) -> str:
    """11.2: pilot-ec / pilot-iot for index 0, otherwise ec-<a..e> / iot-<a..e>."""
    if workload not in WORKLOADS:
        raise ConfigError(f"workload must be one of {WORKLOADS}, got {workload!r}")
    if not isinstance(index, int) or isinstance(index, bool) or not 0 <= index <= 5:
        raise ConfigError(f"campaign_index must be an integer 0 to 5, got {index!r}")
    short = "ec" if workload == "ecommerce" else "iot"
    if index == 0:
        cid = f"pilot-{short}"
    else:
        cid = f"{short}-{'abcde'[index - 1]}"
    if not CAMPAIGN_ID_RE.match(cid):
        raise ConfigError(f"derived campaign_id {cid!r} violates the Terraform rule")
    return cid


def cluster_name(campaign_id: str) -> str:
    return f"streaming-anomaly-detection-{campaign_id}"


def cell_key(workload: str, index: int, block: int, position: int) -> str:
    key = f"{workload}-c{index}-b{block:02d}-p{position}"
    if not CELL_KEY_RE.match(key):
        raise ValueError(f"bad cell key {key}")
    return key


def attempt_key(cell: str, attempt: int) -> str:
    key = f"{cell}-a{attempt}"
    if not ATTEMPT_KEY_RE.match(key):
        raise ValueError(f"bad attempt key {key} (attempt numbers run 1 to 9)")
    return key


def parse_attempt_key(key: str) -> dict:
    m = re.match(r"^(ecommerce|iot)-c([0-5])-b(\d{2})-p([1-8])-a([1-9])$", key)
    if not m or not ATTEMPT_KEY_RE.match(key):
        raise ValueError(f"bad attempt key {key!r}")
    return {"workload": m.group(1), "campaign_index": int(m.group(2)), "block": int(m.group(3)),
            "position": int(m.group(4)), "attempt": int(m.group(5)), "cell_key": key.rsplit("-a", 1)[0]}


def new_run_id() -> str:
    return str(uuid.uuid4())


def k8s_name(prefix: str, akey: str, n: int | None = None) -> str:
    name = f"{prefix}-{akey}" if n is None else f"{prefix}-{akey}-{n}"
    if len(name) > 63:
        raise ValueError(f"Kubernetes name too long: {name}")
    return name


def broker_id_from_pod(pod: str) -> int | None:
    if pod.startswith(BROKER_POD_PREFIX):
        tail = pod[len(BROKER_POD_PREFIX):]
        if tail.isdigit():
            return int(tail)
    return None


def campaign_labels(campaign_id: str, akey: str | None = None, run_type: str | None = None) -> dict:
    labels = {LABEL_CAMPAIGN: campaign_id}
    if akey is not None:
        labels[LABEL_ATTEMPT] = akey
    if run_type is not None:
        labels[LABEL_RUN_TYPE] = run_type
    return labels
