#!/usr/bin/env python3
"""
Fault injection campaign orchestrator for streaming anomaly detection benchmark.

Automates 80 fault injection runs (8 fault types x 10 repetitions) against a
Kafka/Flink streaming pipeline on EKS, using Chaos Mesh for fault injection.

Protocol per run:
    1. PRE-FAULT:  5 min normal operation  (label: NORMAL)
    2. INJECTION:  fault-specific duration  (label: ANOMALOUS)
    3. POST-FAULT: 5 min recovery          (label: RECOVERY)
    4. COOLDOWN:   10 min stabilization    (label: COOLDOWN, excluded from analysis)
    5. VALIDATION: assert recovery criteria for 60 consecutive seconds
    6. LOG:        record to campaign_manifest.json

Usage:
    # Full campaign
    python campaign_orchestrator.py --baseline-stats baseline_stats.json

    # Dry run (print plan, no execution)
    python campaign_orchestrator.py --baseline-stats baseline_stats.json --dry-run

    # Resume from run 35
    python campaign_orchestrator.py --baseline-stats baseline_stats.json --start-from 35

    # Run only F1 (broker crash)
    python campaign_orchestrator.py --baseline-stats baseline_stats.json --fault F1

    # Run F3 and F7 only
    python campaign_orchestrator.py --baseline-stats baseline_stats.json --fault F3 --fault F7
"""
import argparse
import json
import logging
import os
import signal
import subprocess
import sys
import textwrap
import time
import uuid
from datetime import datetime, timezone
from pathlib import Path
from typing import Optional
from urllib.error import HTTPError, URLError
from urllib.parse import quote
from urllib.request import Request, urlopen

# ---------------------------------------------------------------------------
# Constants
# ---------------------------------------------------------------------------

NAMESPACE = "streaming"
KAFKA_CLUSTER = "streaming-kafka"
FLINK_DEPLOYMENT = "streaming-flink"
KAFKA_BOOTSTRAP = "streaming-kafka-kafka-bootstrap.streaming.svc.cluster.local:9092"
_config = {"prometheus_url": "http://localhost:9090"}

PRE_FAULT_SECONDS = 300       # 5 min
POST_FAULT_SECONDS = 300      # 5 min
COOLDOWN_SECONDS = 600        # 10 min
VALIDATION_HOLD_SECONDS = 60  # must pass for 60 consecutive seconds
RECOVERY_TIMEOUT_SECONDS = 900  # 15 min max recovery wait
KUBECTL_TIMEOUT_SECONDS = 60
KUBECTL_MAX_RETRIES = 3

REPETITIONS = 10

# Fault taxonomy
FAULT_DEFINITIONS = {
    "F1": {
        "name": "broker_crash",
        "category": "broker",
        "description": "Kill Kafka broker pod (kafka-0)",
        "duration_seconds": 300,
        "chaos_kind": "PodChaos",
    },
    "F2": {
        "name": "network_partition",
        "category": "broker",
        "description": "Network partition isolating kafka-1 from other brokers",
        "duration_seconds": 180,
        "chaos_kind": "NetworkChaos",
    },
    "F3": {
        "name": "disk_io_saturation",
        "category": "broker",
        "description": "200ms I/O latency on /var/lib/kafka",
        "duration_seconds": 300,
        "chaos_kind": "IOChaos",
    },
    "F4": {
        "name": "consumer_lag_spike",
        "category": "consumer",
        "description": "CPU stress 90% on Flink TaskManagers causing slow processing",
        "duration_seconds": 300,
        "chaos_kind": "StressChaos",
    },
    "F5": {
        "name": "rebalancing_storm",
        "category": "consumer",
        "description": "Scale TaskManagers 3->1->3->1 every 60s for 5 min",
        "duration_seconds": 300,
        "chaos_kind": "custom_scaling",
    },
    "F6": {
        "name": "poison_pill",
        "category": "consumer",
        "description": "Inject malformed Avro records at 5% throughput for 3 min",
        "duration_seconds": 180,
        "chaos_kind": "custom_producer",
    },
    "F7": {
        "name": "checkpoint_failure",
        "category": "flink",
        "description": "50% packet loss on TaskManagers causing checkpoint failures",
        "duration_seconds": 300,
        "chaos_kind": "NetworkChaos",
    },
    "F8": {
        "name": "oom_resource_exhaustion",
        "category": "flink",
        "description": "Memory stress 95% on TaskManagers",
        "duration_seconds": 180,
        "chaos_kind": "StressChaos",
    },
}

# ---------------------------------------------------------------------------
# Logging
# ---------------------------------------------------------------------------

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s",
    datefmt="%Y-%m-%d %H:%M:%S",
    handlers=[
        logging.StreamHandler(sys.stdout),
        logging.FileHandler("campaign.log", mode="a"),
    ],
)
log = logging.getLogger("campaign")

# ---------------------------------------------------------------------------
# Signal handling for graceful shutdown
# ---------------------------------------------------------------------------

_shutdown_requested = False
_active_chaos_cr: Optional[dict] = None  # {"kind": ..., "name": ...}


def _signal_handler(signum, frame):
    global _shutdown_requested
    sig_name = signal.Signals(signum).name
    log.warning("Received %s, initiating graceful shutdown...", sig_name)
    _shutdown_requested = True

    if _active_chaos_cr:
        log.warning("Cleaning up active Chaos Mesh CR: %s/%s",
                     _active_chaos_cr["kind"], _active_chaos_cr["name"])
        _delete_chaos_cr(_active_chaos_cr["kind"], _active_chaos_cr["name"])


signal.signal(signal.SIGINT, _signal_handler)
signal.signal(signal.SIGTERM, _signal_handler)


def check_shutdown():
    """Raise SystemExit if shutdown was requested."""
    if _shutdown_requested:
        raise SystemExit("Shutdown requested by signal")


# ---------------------------------------------------------------------------
# kubectl helpers
# ---------------------------------------------------------------------------

def kubectl(args: list[str], timeout: int = KUBECTL_TIMEOUT_SECONDS,
            retries: int = KUBECTL_MAX_RETRIES, check: bool = True,
            capture_output: bool = True) -> subprocess.CompletedProcess:
    """Run a kubectl command with timeout and retry logic."""
    cmd = ["kubectl"] + args
    last_exc = None

    for attempt in range(1, retries + 1):
        try:
            result = subprocess.run(
                cmd,
                timeout=timeout,
                capture_output=capture_output,
                text=True,
            )
            if result.returncode == 0 or not check:
                return result
            last_exc = RuntimeError(
                f"kubectl failed (rc={result.returncode}): {result.stderr.strip()}"
            )
            log.warning("kubectl attempt %d/%d failed: %s", attempt, retries,
                        result.stderr.strip()[:200])
        except subprocess.TimeoutExpired:
            last_exc = TimeoutError(
                f"kubectl timed out after {timeout}s: {' '.join(cmd)}"
            )
            log.warning("kubectl attempt %d/%d timed out after %ds",
                        attempt, retries, timeout)
        except FileNotFoundError:
            raise RuntimeError("kubectl not found in PATH")

        if attempt < retries:
            time.sleep(2 ** attempt)

    raise last_exc


def kubectl_apply_yaml(yaml_content: str) -> bool:
    """Apply a YAML manifest via kubectl apply -f -. Returns True on success."""
    try:
        result = subprocess.run(
            ["kubectl", "apply", "-f", "-"],
            input=yaml_content,
            timeout=KUBECTL_TIMEOUT_SECONDS,
            capture_output=True,
            text=True,
        )
        if result.returncode == 0:
            log.info("Applied: %s", result.stdout.strip())
            return True
        log.error("kubectl apply failed: %s", result.stderr.strip())
        return False
    except subprocess.TimeoutExpired:
        log.error("kubectl apply timed out after %ds", KUBECTL_TIMEOUT_SECONDS)
        return False


def kubectl_get_json(resource: str, namespace: str = NAMESPACE) -> Optional[dict]:
    """Get a resource as JSON. Returns None on failure."""
    try:
        result = kubectl(
            ["get", resource, "-n", namespace, "-o", "json"],
            check=False, retries=2,
        )
        if result.returncode == 0:
            return json.loads(result.stdout)
        return None
    except (TimeoutError, json.JSONDecodeError):
        return None


# ---------------------------------------------------------------------------
# Chaos Mesh CR generation
# ---------------------------------------------------------------------------

def _chaos_cr_name(fault_id: str, rep: int) -> str:
    return f"campaign-{fault_id.lower()}-rep{rep:02d}"


def generate_chaos_cr(fault_id: str, rep: int) -> Optional[str]:
    """Generate a Chaos Mesh YAML manifest for the given fault. Returns None for custom faults."""
    fault = FAULT_DEFINITIONS[fault_id]
    cr_name = _chaos_cr_name(fault_id, rep)
    duration = f"{fault['duration_seconds']}s"

    if fault_id == "F1":
        # Broker crash: kill kafka-0 pod
        return textwrap.dedent(f"""\
            apiVersion: chaos-mesh.org/v1alpha1
            kind: PodChaos
            metadata:
              name: {cr_name}
              namespace: {NAMESPACE}
            spec:
              action: pod-kill
              mode: one
              selector:
                namespaces:
                  - {NAMESPACE}
                labelSelectors:
                  strimzi.io/cluster: {KAFKA_CLUSTER}
                  strimzi.io/kind: Kafka
                # As run, this name matches no pod. The brokers node pool names its pods
                # streaming-kafka-brokers-N, so this injection did not take effect.
                # Use {KAFKA_CLUSTER}-brokers-0 to reproduce the intended fault.
                fieldSelectors:
                  metadata.name: {KAFKA_CLUSTER}-kafka-0
              duration: "{duration}"
        """)

    elif fault_id == "F2":
        # Network partition: isolate kafka-1 from other brokers
        return textwrap.dedent(f"""\
            apiVersion: chaos-mesh.org/v1alpha1
            kind: NetworkChaos
            metadata:
              name: {cr_name}
              namespace: {NAMESPACE}
            spec:
              action: partition
              mode: one
              selector:
                namespaces:
                  - {NAMESPACE}
                labelSelectors:
                  strimzi.io/cluster: {KAFKA_CLUSTER}
                  strimzi.io/kind: Kafka
                # As run, this name matches no pod. The brokers node pool names its pods
                # streaming-kafka-brokers-N, so this injection did not take effect.
                # Use {KAFKA_CLUSTER}-brokers-1 to reproduce the intended fault.
                fieldSelectors:
                  metadata.name: {KAFKA_CLUSTER}-kafka-1
              direction: both
              target:
                mode: all
                selector:
                  namespaces:
                    - {NAMESPACE}
                  labelSelectors:
                    strimzi.io/cluster: {KAFKA_CLUSTER}
                    strimzi.io/kind: Kafka
              duration: "{duration}"
        """)

    elif fault_id == "F3":
        # Disk I/O saturation on Kafka data dir
        return textwrap.dedent(f"""\
            apiVersion: chaos-mesh.org/v1alpha1
            kind: IOChaos
            metadata:
              name: {cr_name}
              namespace: {NAMESPACE}
            spec:
              action: latency
              mode: all
              selector:
                namespaces:
                  - {NAMESPACE}
                labelSelectors:
                  strimzi.io/cluster: {KAFKA_CLUSTER}
                  strimzi.io/kind: Kafka
              volumePath: /var/lib/kafka
              path: "*"
              delay: "200ms"
              percent: 100
              duration: "{duration}"
        """)

    elif fault_id == "F4":
        # CPU stress on Flink TaskManagers
        return textwrap.dedent(f"""\
            apiVersion: chaos-mesh.org/v1alpha1
            kind: StressChaos
            metadata:
              name: {cr_name}
              namespace: {NAMESPACE}
            spec:
              mode: all
              selector:
                namespaces:
                  - {NAMESPACE}
                labelSelectors:
                  app: {FLINK_DEPLOYMENT}
                  component: taskmanager
              stressors:
                cpu:
                  workers: 4
                  load: 90
              duration: "{duration}"
        """)

    elif fault_id == "F7":
        # 50% packet loss on TaskManagers
        return textwrap.dedent(f"""\
            apiVersion: chaos-mesh.org/v1alpha1
            kind: NetworkChaos
            metadata:
              name: {cr_name}
              namespace: {NAMESPACE}
            spec:
              action: loss
              mode: all
              selector:
                namespaces:
                  - {NAMESPACE}
                labelSelectors:
                  app: {FLINK_DEPLOYMENT}
                  component: taskmanager
              loss:
                loss: "50"
              duration: "{duration}"
        """)

    elif fault_id == "F8":
        # Memory stress on TaskManagers
        return textwrap.dedent(f"""\
            apiVersion: chaos-mesh.org/v1alpha1
            kind: StressChaos
            metadata:
              name: {cr_name}
              namespace: {NAMESPACE}
            spec:
              mode: all
              selector:
                namespaces:
                  - {NAMESPACE}
                labelSelectors:
                  app: {FLINK_DEPLOYMENT}
                  component: taskmanager
              stressors:
                memory:
                  workers: 1
                  size: "95%"
              duration: "{duration}"
        """)

    # F5 and F6 are custom (no Chaos Mesh CR)
    return None


def _chaos_kind_for_fault(fault_id: str) -> Optional[str]:
    """Return the Chaos Mesh CRD kind for deletion purposes."""
    mapping = {
        "F1": "podchaos",
        "F2": "networkchaos",
        "F3": "iochaos",
        "F4": "stresschaos",
        "F7": "networkchaos",
        "F8": "stresschaos",
    }
    return mapping.get(fault_id)


def _delete_chaos_cr(kind: str, name: str):
    """Delete a Chaos Mesh CR, best-effort."""
    try:
        kubectl(
            ["delete", kind, name, "-n", NAMESPACE, "--ignore-not-found"],
            retries=2,
            check=False,
        )
        log.info("Deleted chaos CR %s/%s", kind, name)
    except (TimeoutError, RuntimeError) as e:
        log.error("Failed to delete chaos CR %s/%s: %s", kind, name, e)


# ---------------------------------------------------------------------------
# Fault injection: F5 (rebalancing storm via scaling)
# ---------------------------------------------------------------------------

def inject_f5_rebalancing_storm(fault_id: str, rep: int, duration_seconds: int):
    """Scale Flink TaskManagers 3->1->3->1 every 60s to trigger rebalancing storms."""
    log.info("F5: Starting rebalancing storm (scale cycles for %ds)", duration_seconds)
    end_time = time.time() + duration_seconds
    cycle = 0

    while time.time() < end_time and not _shutdown_requested:
        cycle += 1
        target = 1 if cycle % 2 == 1 else 3
        log.info("F5: Scaling TaskManagers to %d (cycle %d)", target, cycle)

        try:
            # Patch the FlinkDeployment resource to change TM replicas
            patch = json.dumps({
                "spec": {"taskManager": {"replicas": target}}
            })
            kubectl(
                ["patch", "flinkdeployment", FLINK_DEPLOYMENT,
                 "-n", NAMESPACE, "--type=merge", "-p", patch],
                timeout=30,
            )
        except (TimeoutError, RuntimeError) as e:
            log.error("F5: Failed to scale TaskManagers to %d: %s", target, e)

        # Wait 60s before next scale, but check shutdown and end time
        wait_until = min(time.time() + 60, end_time)
        while time.time() < wait_until:
            check_shutdown()
            time.sleep(1)

    # Restore to 3 TaskManagers
    log.info("F5: Restoring TaskManagers to 3")
    try:
        patch = json.dumps({"spec": {"taskManager": {"replicas": 3}}})
        kubectl(
            ["patch", "flinkdeployment", FLINK_DEPLOYMENT,
             "-n", NAMESPACE, "--type=merge", "-p", patch],
            timeout=30,
        )
    except (TimeoutError, RuntimeError) as e:
        log.error("F5: Failed to restore TaskManagers to 3: %s", e)


# ---------------------------------------------------------------------------
# Fault injection: F6 (poison pill via malformed Avro records)
# ---------------------------------------------------------------------------

def inject_f6_poison_pill(fault_id: str, rep: int, duration_seconds: int,
                          baseline_stats: dict):
    """Inject malformed records into raw-events topic at ~5% of baseline throughput."""
    log.info("F6: Starting poison pill injection for %ds", duration_seconds)

    # Calculate injection rate: 5% of baseline throughput
    baseline_throughput = baseline_stats.get("throughput_mean", 5000)
    inject_rate = max(10, int(baseline_throughput * 0.05))
    inject_interval = 1.0 / inject_rate if inject_rate > 0 else 0.1
    log.info("F6: Injecting at ~%d records/sec (5%% of %.0f baseline)",
             inject_rate, baseline_throughput)

    # Run a poison pill producer pod in the cluster
    # This pod sends records with invalid schema IDs (0xDEAD) to raw-events
    producer_script = textwrap.dedent(f"""\
        import struct
        import time
        import socket
        import random
        from confluent_kafka import Producer

        conf = {{
            'bootstrap.servers': '{KAFKA_BOOTSTRAP}',
            'client.id': 'poison-pill-injector',
            'acks': '1',
        }}
        producer = Producer(conf)
        topic = 'raw-events'
        end_time = time.time() + {duration_seconds}
        count = 0
        rate = {inject_rate}
        batch_size = max(1, rate // 10)

        while time.time() < end_time:
            for _ in range(batch_size):
                # Malformed Avro: magic byte 0x00 + invalid schema ID 0xDEAD + garbage
                schema_id = random.choice([0xDEAD, 0xBEEF, 0xCAFE, 0x0000])
                garbage = bytes(random.getrandbits(8) for _ in range(random.randint(10, 200)))
                payload = struct.pack('>bI', 0, schema_id) + garbage
                key = f'poison-{{count}}'.encode()
                producer.produce(topic, value=payload, key=key)
                count += 1
            producer.flush(timeout=2)
            time.sleep(max(0.01, batch_size / rate))

        producer.flush(timeout=10)
        print(f'Injected {{count}} poison pill records')
    """)

    # Create a ConfigMap with the producer script, then run it as a Job
    cm_name = f"poison-pill-script-{fault_id.lower()}-rep{rep:02d}"
    job_name = f"poison-pill-{fault_id.lower()}-rep{rep:02d}"

    configmap_yaml = textwrap.dedent(f"""\
        apiVersion: v1
        kind: ConfigMap
        metadata:
          name: {cm_name}
          namespace: {NAMESPACE}
        data:
          inject.py: |
{textwrap.indent(producer_script, '            ')}
    """)

    job_yaml = textwrap.dedent(f"""\
        apiVersion: batch/v1
        kind: Job
        metadata:
          name: {job_name}
          namespace: {NAMESPACE}
        spec:
          backoffLimit: 0
          activeDeadlineSeconds: {duration_seconds + 60}
          template:
            spec:
              restartPolicy: Never
              tolerations:
                - key: "workload"
                  value: "monitoring"
                  effect: "NoSchedule"
              nodeSelector:
                workload: monitoring
              containers:
                - name: injector
                  image: python:3.11-slim
                  command: ["sh", "-c", "pip install confluent-kafka && python /scripts/inject.py"]
                  volumeMounts:
                    - name: script
                      mountPath: /scripts
              volumes:
                - name: script
                  configMap:
                    name: {cm_name}
    """)

    # Apply ConfigMap and Job
    if not kubectl_apply_yaml(configmap_yaml):
        log.error("F6: Failed to create ConfigMap for poison pill script")
        return
    if not kubectl_apply_yaml(job_yaml):
        log.error("F6: Failed to create poison pill Job")
        _cleanup_f6(cm_name, job_name)
        return

    # Wait for the injection duration
    log.info("F6: Poison pill job running, waiting %ds for completion", duration_seconds)
    deadline = time.time() + duration_seconds + 120  # extra buffer for pod startup
    while time.time() < deadline:
        check_shutdown()
        try:
            result = kubectl(
                ["get", "job", job_name, "-n", NAMESPACE,
                 "-o", "jsonpath={.status.conditions[0].type}"],
                check=False, retries=1,
            )
            status = result.stdout.strip()
            if status == "Complete":
                log.info("F6: Poison pill job completed successfully")
                break
            elif status == "Failed":
                log.warning("F6: Poison pill job failed")
                break
        except (TimeoutError, RuntimeError):
            pass
        time.sleep(10)

    # Cleanup
    _cleanup_f6(cm_name, job_name)


def _cleanup_f6(cm_name: str, job_name: str):
    """Clean up F6 resources."""
    try:
        kubectl(["delete", "job", job_name, "-n", NAMESPACE, "--ignore-not-found"],
                check=False, retries=1)
    except (TimeoutError, RuntimeError):
        pass
    try:
        kubectl(["delete", "configmap", cm_name, "-n", NAMESPACE, "--ignore-not-found"],
                check=False, retries=1)
    except (TimeoutError, RuntimeError):
        pass


# ---------------------------------------------------------------------------
# Prometheus query helpers
# ---------------------------------------------------------------------------

def query_prometheus(promql: str, time_ts: Optional[float] = None) -> Optional[list]:
    """Run an instant Prometheus query. Returns list of results or None on failure."""
    params = f"query={quote(promql, safe='{}=~\"_,()[]|+*')}"
    if time_ts:
        params += f"&time={time_ts}"
    url = f"{_config["prometheus_url"]}/api/v1/query?{params}"

    for attempt in range(3):
        try:
            req = Request(url, headers={"Accept": "application/json"})
            with urlopen(req, timeout=30) as resp:
                data = json.loads(resp.read().decode())
            if data.get("status") == "success":
                return data.get("data", {}).get("result", [])
            log.warning("Prometheus returned status=%s", data.get("status"))
            return None
        except (HTTPError, URLError, TimeoutError, json.JSONDecodeError) as e:
            log.warning("Prometheus query attempt %d/3 failed: %s", attempt + 1, e)
            if attempt < 2:
                time.sleep(2 ** attempt)
    return None


def get_scalar_value(promql: str) -> Optional[float]:
    """Run a PromQL query and return the first scalar result."""
    results = query_prometheus(promql)
    if not results:
        return None
    try:
        return float(results[0]["value"][1])
    except (IndexError, KeyError, ValueError, TypeError):
        return None


# ---------------------------------------------------------------------------
# Recovery validation
# ---------------------------------------------------------------------------

def validate_recovery(baseline_stats: dict) -> tuple[bool, dict]:
    """Check all recovery criteria. Returns (all_passed, details_dict)."""
    checks = {}

    # 1. Consumer lag < baseline_mean + 3*baseline_std
    lag_mean = baseline_stats.get("consumer_lag_mean", 0)
    lag_std = baseline_stats.get("consumer_lag_std", 1)
    lag_threshold = lag_mean + 3 * lag_std

    current_lag = get_scalar_value(
        'max(flink_taskmanager_job_task_operator_KafkaSourceReader_KafkaConsumer_records_lag_max)'
    )
    if current_lag is not None:
        checks["consumer_lag"] = {
            "value": current_lag,
            "threshold": lag_threshold,
            "passed": current_lag < lag_threshold,
        }
    else:
        checks["consumer_lag"] = {"value": None, "threshold": lag_threshold, "passed": False}

    # 2. Flink checkpoints succeeding (no new failures in last 2 min)
    failed_checkpoints = get_scalar_value(
        'sum(increase(flink_jobmanager_job_numberOfFailedCheckpoints[2m]))'
    )
    checks["checkpoints"] = {
        "value": failed_checkpoints,
        "threshold": 0,
        "passed": failed_checkpoints is not None and failed_checkpoints == 0,
    }

    # 3. ISR count = replication factor (3) for all partitions
    under_replicated = get_scalar_value(
        'sum(kafka_server_replicamanager_underreplicatedpartitions)'
    )
    checks["isr_count"] = {
        "value": under_replicated,
        "threshold": 0,
        "passed": under_replicated is not None and under_replicated == 0,
    }

    # 4. Throughput > baseline_mean - 2*baseline_std
    tp_mean = baseline_stats.get("throughput_mean", 0)
    tp_std = baseline_stats.get("throughput_std", 1)
    tp_threshold = tp_mean - 2 * tp_std

    current_tp = get_scalar_value(
        'sum(rate(kafka_server_brokertopicmetrics_messagesinpersec_total[1m]))'
    )
    if current_tp is not None:
        checks["throughput"] = {
            "value": current_tp,
            "threshold": tp_threshold,
            "passed": current_tp > tp_threshold,
        }
    else:
        checks["throughput"] = {"value": None, "threshold": tp_threshold, "passed": False}

    all_passed = all(c["passed"] for c in checks.values())
    return all_passed, checks


def wait_for_recovery(baseline_stats: dict) -> tuple[str, dict]:
    """Wait for recovery criteria to be met for 60 consecutive seconds.

    Returns:
        (status, last_checks) where status is one of:
        "recovered", "extended_recovery", "shutdown"
    """
    log.info("Waiting for recovery validation (60s consecutive pass, 15 min timeout)...")
    deadline = time.time() + RECOVERY_TIMEOUT_SECONDS
    consecutive_pass_start = None

    while time.time() < deadline:
        check_shutdown()

        all_passed, checks = validate_recovery(baseline_stats)

        if all_passed:
            if consecutive_pass_start is None:
                consecutive_pass_start = time.time()
                log.info("Recovery criteria met, starting 60s hold timer")
            elapsed_hold = time.time() - consecutive_pass_start
            if elapsed_hold >= VALIDATION_HOLD_SECONDS:
                log.info("Recovery validated: all criteria met for 60 consecutive seconds")
                return "recovered", checks
        else:
            if consecutive_pass_start is not None:
                log.info("Recovery criteria broken after %.0fs hold, resetting timer",
                         time.time() - consecutive_pass_start)
            consecutive_pass_start = None
            failing = [k for k, v in checks.items() if not v["passed"]]
            log.debug("Failing checks: %s", failing)

        time.sleep(5)

    log.warning("Recovery timeout reached (15 min). Attempting remediation...")
    return "extended_recovery", checks


def restart_flink_deployment() -> bool:
    """Full restart of FlinkDeployment: bump restartNonce, wait for pods, resubmit jobs.

    Returns True if Flink jobs are running after restart.
    """
    log.info("Performing full FlinkDeployment restart...")

    # Step 1: Bump restartNonce to trigger operator-managed restart
    nonce = str(int(time.time()))
    try:
        kubectl(
            ["patch", "flinkdeployment", FLINK_DEPLOYMENT, "-n", NAMESPACE,
             "--type=merge", "-p",
             json.dumps({"spec": {"restartNonce": int(nonce)}})],
            timeout=60,
        )
        log.info("Patched FlinkDeployment restartNonce=%s", nonce)
    except (TimeoutError, RuntimeError) as e:
        log.error("Failed to patch restartNonce: %s", e)
        # Fallback: delete all Flink pods to force operator reconciliation
        try:
            kubectl(
                ["delete", "pod", "-n", NAMESPACE,
                 "-l", f"app={FLINK_DEPLOYMENT}"],
                timeout=120,
            )
            log.info("Fallback: deleted all Flink pods")
        except (TimeoutError, RuntimeError) as e2:
            log.error("Fallback pod deletion also failed: %s", e2)
            return False

    # Step 2: Wait for JobManager to become ready (up to 5 min)
    log.info("Waiting for JobManager to become ready (up to 5 min)...")
    jm_ready = False
    for _ in range(60):
        check_shutdown()
        try:
            result = kubectl(
                ["get", "pods", "-n", NAMESPACE,
                 "-l", f"app={FLINK_DEPLOYMENT},component=jobmanager",
                 "--field-selector=status.phase=Running",
                 "-o", "jsonpath={.items[0].metadata.name}"],
                check=False, retries=1,
            )
            if result.returncode == 0 and result.stdout.strip():
                jm_name = result.stdout.strip()
                # Verify REST API is up
                result2 = kubectl(
                    ["exec", "-n", NAMESPACE, jm_name, "--",
                     "curl", "-sf", "http://localhost:8081/jobs/overview"],
                    check=False, retries=1, timeout=15,
                )
                if result2.returncode == 0:
                    jm_ready = True
                    log.info("JobManager %s is ready with REST API", jm_name)
                    break
        except (TimeoutError, RuntimeError):
            pass
        time.sleep(5)

    if not jm_ready:
        log.error("JobManager did not become ready within 5 min")
        return False

    # Step 3: Wait for TaskManagers (up to 3 min)
    log.info("Waiting for TaskManagers to register (up to 3 min)...")
    tm_ready = False
    for _ in range(36):
        check_shutdown()
        try:
            result = kubectl(
                ["get", "pods", "-n", NAMESPACE,
                 "-l", f"app={FLINK_DEPLOYMENT},component=taskmanager",
                 "--field-selector=status.phase=Running",
                 "--no-headers"],
                check=False, retries=1,
            )
            if result.returncode == 0:
                running = len([l for l in result.stdout.strip().split("\n") if l.strip()])
                if running >= 3:
                    tm_ready = True
                    log.info("%d TaskManagers running", running)
                    break
        except (TimeoutError, RuntimeError):
            pass
        time.sleep(5)

    if not tm_ready:
        log.warning("Not all TaskManagers ready, proceeding with job submission anyway")

    # Step 4: Resubmit Flink jobs if not running
    time.sleep(15)  # Brief pause for TM registration to complete
    jm_name = None
    try:
        result = kubectl(
            ["get", "pods", "-n", NAMESPACE,
             "-l", f"app={FLINK_DEPLOYMENT},component=jobmanager",
             "--field-selector=status.phase=Running",
             "-o", "jsonpath={.items[0].metadata.name}"],
            check=False, retries=1,
        )
        if result.returncode == 0:
            jm_name = result.stdout.strip()
    except (TimeoutError, RuntimeError):
        pass

    if not jm_name:
        log.error("Cannot find JobManager pod for job resubmission")
        return False

    jobs_info = _get_flink_jobs(jm_name)
    workload = os.environ.get("WORKLOAD", "ECOMMERCE").upper()
    if workload == "IOT":
        jobs_needed = {
            "IoT Event Enrichment Job": ("com.phonotech.streaming.iot.enrichment.IoTEnrichmentJob", "iot-enrichment.jar"),
            "IoT Per-Location Aggregation Job": ("com.phonotech.streaming.iot.aggregation.IoTAggregationJob", "iot-aggregation.jar"),
            "IoT Threshold Alert Job": ("com.phonotech.streaming.iot.alert.IoTAlertJob", "iot-alert.jar"),
        }
    else:
        jobs_needed = {
            "Event Enrichment Job": ("com.phonotech.streaming.enrichment.EnrichmentJob", "enrichment.jar"),
            "Windowed Aggregation Job": ("com.phonotech.streaming.aggregation.AggregationJob", "aggregation.jar"),
            "Fraud Detection Job": ("com.phonotech.streaming.fraud.FraudDetectionJob", "fraud.jar"),
        }

    for job_name, (main_class, jar) in jobs_needed.items():
        if job_name not in jobs_info:
            log.info("Resubmitting: %s", job_name)
            try:
                kubectl(
                    ["exec", "-n", NAMESPACE, jm_name, "--",
                     "/opt/flink/bin/flink", "run", "-d",
                     "-c", main_class,
                     f"/opt/flink/usrlib/{jar}"],
                    timeout=60, check=False,
                )
            except (TimeoutError, RuntimeError) as e:
                log.error("Failed to submit %s: %s", job_name, e)

    # Step 5: Verify all 3 jobs are RUNNING (up to 2 min)
    log.info("Verifying all Flink jobs are RUNNING (up to 2 min)...")
    for _ in range(24):
        check_shutdown()
        jobs_info = _get_flink_jobs(jm_name)
        running_count = sum(1 for s in jobs_info.values() if s == "RUNNING")
        if running_count >= 3:
            log.info("All %d Flink jobs confirmed RUNNING", running_count)
            return True
        time.sleep(5)

    log.error("Only %d/3 Flink jobs running after restart", running_count)
    return running_count >= 3


def _get_flink_jobs(jm_pod: str) -> dict:
    """Query Flink REST API for running jobs. Returns {job_name: state}."""
    try:
        result = kubectl(
            ["exec", "-n", NAMESPACE, jm_pod, "--",
             "curl", "-sf", "http://localhost:8081/jobs/overview"],
            check=False, retries=1, timeout=15,
        )
        if result.returncode == 0 and result.stdout.strip():
            data = json.loads(result.stdout)
            return {j["name"]: j["state"] for j in data.get("jobs", [])}
    except (TimeoutError, RuntimeError, json.JSONDecodeError):
        pass
    return {}


def ensure_pipeline_healthy(baseline_stats: dict, max_attempts: int = 3) -> bool:
    """Pre-run health gate: verify pipeline is fully healthy before starting a run.

    Checks Flink jobs are RUNNING, Kafka brokers are ready, and recovery criteria pass.
    If unhealthy, attempts full FlinkDeployment restart.

    Returns True if pipeline is healthy, False if unrecoverable.
    """
    for attempt in range(1, max_attempts + 1):
        log.info("Pre-run health check (attempt %d/%d)...", attempt, max_attempts)

        # Check Flink job status via REST API
        flink_healthy = False
        try:
            jm_result = kubectl(
                ["get", "pods", "-n", NAMESPACE,
                 "-l", f"app={FLINK_DEPLOYMENT},component=jobmanager",
                 "--field-selector=status.phase=Running",
                 "-o", "jsonpath={.items[0].metadata.name}"],
                check=False, retries=1,
            )
            if jm_result.returncode == 0 and jm_result.stdout.strip():
                jm_name = jm_result.stdout.strip()
                jobs_info = _get_flink_jobs(jm_name)
                running_count = sum(1 for s in jobs_info.values() if s == "RUNNING")
                log.info("  Flink jobs: %d/3 RUNNING", running_count)
                flink_healthy = running_count >= 3
            else:
                log.warning("  No Flink JobManager pod found")
        except (TimeoutError, RuntimeError):
            log.warning("  Failed to check Flink status")

        # Check Kafka brokers
        kafka_healthy = False
        try:
            result = kubectl(
                ["get", "pods", "-n", NAMESPACE,
                 "-l", f"strimzi.io/cluster={KAFKA_CLUSTER},strimzi.io/kind=Kafka",
                 "--field-selector=status.phase=Running",
                 "--no-headers"],
                check=False, retries=1,
            )
            if result.returncode == 0:
                broker_count = len([l for l in result.stdout.strip().split("\n") if l.strip()])
                log.info("  Kafka brokers: %d/3 running", broker_count)
                kafka_healthy = broker_count >= 3
        except (TimeoutError, RuntimeError):
            log.warning("  Failed to check Kafka status")

        # Check recovery criteria (throughput, lag, checkpoints, ISR)
        all_passed, checks = validate_recovery(baseline_stats)
        failing = [k for k, v in checks.items() if not v["passed"]]
        if failing:
            log.info("  Recovery criteria failing: %s", failing)
        else:
            log.info("  All recovery criteria passing")

        if flink_healthy and kafka_healthy and all_passed:
            log.info("Pipeline healthy, proceeding with run")
            return True

        if attempt < max_attempts:
            log.warning("Pipeline unhealthy, attempting full restart (attempt %d/%d)...",
                        attempt, max_attempts)
            restart_flink_deployment()
            # Wait for metrics to stabilize after restart
            log.info("Waiting 120s for metrics to stabilize after restart...")
            time.sleep(120)
        else:
            log.error("Pipeline still unhealthy after %d restart attempts", max_attempts)

    return False


def attempt_remediation(baseline_stats: dict) -> tuple[str, dict]:
    """Try to remediate an unrecovered cluster: full Flink restart, wait for Kafka ISR."""
    log.info("Attempting remediation: full Flink deployment restart...")

    # Full FlinkDeployment restart (not just pod deletion)
    restart_success = restart_flink_deployment()
    if not restart_success:
        log.error("FlinkDeployment restart failed")

    # Wait for Flink pods to stabilize after restart
    log.info("Waiting 60s for metrics to stabilize after Flink restart...")
    time.sleep(60)

    # Wait for Kafka broker ISR restoration
    log.info("Checking Kafka ISR status...")
    for _ in range(60):
        check_shutdown()
        under_rep = get_scalar_value(
            'sum(kafka_server_replicamanager_underreplicatedpartitions)'
        )
        if under_rep is not None and under_rep == 0:
            break
        time.sleep(5)

    # Re-check recovery
    log.info("Re-checking recovery criteria after remediation...")
    consecutive_pass_start = None
    remediation_deadline = time.time() + 300  # 5 min extra

    while time.time() < remediation_deadline:
        check_shutdown()
        all_passed, checks = validate_recovery(baseline_stats)
        if all_passed:
            if consecutive_pass_start is None:
                consecutive_pass_start = time.time()
            if time.time() - consecutive_pass_start >= VALIDATION_HOLD_SECONDS:
                log.info("Recovery achieved after remediation")
                return "recovered_after_remediation", checks
        else:
            consecutive_pass_start = None
        time.sleep(5)

    log.error("Recovery still not achieved after remediation")
    _, checks = validate_recovery(baseline_stats)
    return "failed_recovery", checks


# ---------------------------------------------------------------------------
# Cleanup: ensure no leftover Chaos Mesh CRs
# ---------------------------------------------------------------------------

def cleanup_all_chaos_crs():
    """Delete all campaign-related Chaos Mesh CRs from the namespace."""
    for kind in ["podchaos", "networkchaos", "iochaos", "stresschaos"]:
        try:
            result = kubectl(
                ["get", kind, "-n", NAMESPACE, "-o",
                 "jsonpath={range .items[*]}{.metadata.name}{\"\\n\"}{end}"],
                check=False, retries=1,
            )
            if result.returncode == 0 and result.stdout.strip():
                for name in result.stdout.strip().split("\n"):
                    if name.startswith("campaign-"):
                        _delete_chaos_cr(kind, name)
        except (TimeoutError, RuntimeError):
            pass

    # Also clean up any leftover F6 resources
    for resource_type in ["job", "configmap"]:
        try:
            result = kubectl(
                ["get", resource_type, "-n", NAMESPACE, "-o",
                 "jsonpath={range .items[*]}{.metadata.name}{\"\\n\"}{end}"],
                check=False, retries=1,
            )
            if result.returncode == 0 and result.stdout.strip():
                for name in result.stdout.strip().split("\n"):
                    if name.startswith("poison-pill-"):
                        kubectl(["delete", resource_type, name, "-n", NAMESPACE,
                                 "--ignore-not-found"], check=False, retries=1)
        except (TimeoutError, RuntimeError):
            pass


# ---------------------------------------------------------------------------
# Campaign manifest (JSON log of all runs)
# ---------------------------------------------------------------------------

def load_manifest(path: str) -> dict:
    """Load existing manifest or create a new one."""
    if os.path.exists(path):
        with open(path, "r") as f:
            manifest = json.load(f)
        log.info("Loaded existing manifest with %d completed runs", len(manifest.get("runs", [])))
        return manifest
    return {
        "campaign_id": str(uuid.uuid4())[:8],
        "created_at": datetime.now(timezone.utc).isoformat(),
        "total_planned_runs": 80,
        "fault_definitions": {k: v["name"] for k, v in FAULT_DEFINITIONS.items()},
        "protocol": {
            "pre_fault_seconds": PRE_FAULT_SECONDS,
            "post_fault_seconds": POST_FAULT_SECONDS,
            "cooldown_seconds": COOLDOWN_SECONDS,
            "validation_hold_seconds": VALIDATION_HOLD_SECONDS,
            "recovery_timeout_seconds": RECOVERY_TIMEOUT_SECONDS,
        },
        "runs": [],
    }


def save_manifest(manifest: dict, path: str):
    """Save manifest to disk atomically."""
    tmp_path = path + ".tmp"
    with open(tmp_path, "w") as f:
        json.dump(manifest, f, indent=2, default=str)
    os.replace(tmp_path, path)


def find_last_completed_run(manifest: dict) -> int:
    """Find the index of the last completed run (0-based). Returns -1 if none."""
    runs = manifest.get("runs", [])
    for i in range(len(runs) - 1, -1, -1):
        if runs[i].get("status") in ("completed", "completed_extended_recovery",
                                      "completed_failed_recovery",
                                      "completed_recovered_after_remediation"):
            return i
    return -1


# ---------------------------------------------------------------------------
# Build run plan
# ---------------------------------------------------------------------------

def build_run_plan(fault_filter: Optional[list[str]] = None) -> list[dict]:
    """Build the ordered list of 80 runs (or filtered subset)."""
    plan = []
    fault_ids = sorted(FAULT_DEFINITIONS.keys(), key=lambda x: int(x[1:]))

    if fault_filter:
        fault_ids = [f for f in fault_ids if f in fault_filter]

    run_number = 0
    for fault_id in fault_ids:
        for rep in range(1, REPETITIONS + 1):
            run_number += 1
            plan.append({
                "run_number": run_number,
                "fault_id": fault_id,
                "fault_name": FAULT_DEFINITIONS[fault_id]["name"],
                "repetition": rep,
                "duration_seconds": FAULT_DEFINITIONS[fault_id]["duration_seconds"],
            })
    return plan


# ---------------------------------------------------------------------------
# Phase timing helpers
# ---------------------------------------------------------------------------

def iso_now() -> str:
    return datetime.now(timezone.utc).isoformat()


def wait_phase(phase_name: str, duration_seconds: int):
    """Wait for a timed phase, checking for shutdown every second."""
    log.info("Phase [%s]: waiting %d seconds...", phase_name, duration_seconds)
    end_time = time.time() + duration_seconds
    last_log = time.time()

    while time.time() < end_time:
        check_shutdown()
        remaining = end_time - time.time()
        if time.time() - last_log >= 60:
            log.info("Phase [%s]: %.0f seconds remaining", phase_name, remaining)
            last_log = time.time()
        time.sleep(1)


# ---------------------------------------------------------------------------
# Execute a single run
# ---------------------------------------------------------------------------

def execute_run(run: dict, baseline_stats: dict, manifest: dict,
                manifest_path: str) -> dict:
    """Execute a single fault injection run. Returns the run record."""
    global _active_chaos_cr

    fault_id = run["fault_id"]
    rep = run["repetition"]
    run_num = run["run_number"]
    fault = FAULT_DEFINITIONS[fault_id]
    cr_name = _chaos_cr_name(fault_id, rep)
    chaos_kind = _chaos_kind_for_fault(fault_id)

    log.info("=" * 70)
    log.info("RUN %d: %s (%s) rep %d/%d", run_num, fault_id,
             fault["name"], rep, REPETITIONS)
    log.info("=" * 70)

    # --- Pre-run health gate ---
    # Verify pipeline is fully healthy before starting. If Flink died in a
    # previous run, this will restart the FlinkDeployment and resubmit jobs.
    if not ensure_pipeline_healthy(baseline_stats):
        log.error("Pipeline unrecoverable before run %d, skipping", run_num)
        record = {
            "run_number": run_num,
            "run_id": str(uuid.uuid4())[:12],
            "fault_id": fault_id,
            "fault_name": fault["name"],
            "fault_category": fault["category"],
            "repetition": rep,
            "status": "skipped_unhealthy_pipeline",
            "phases": {},
            "recovery_status": "pipeline_not_healthy",
            "recovery_checks": None,
            "errors": ["Pipeline health gate failed after 3 restart attempts"],
            "completed_at": iso_now(),
        }
        manifest["runs"].append(record)
        save_manifest(manifest, manifest_path)
        log.info("Run %d recorded: status=%s", run_num, record["status"])
        return record

    record = {
        "run_number": run_num,
        "run_id": str(uuid.uuid4())[:12],
        "fault_id": fault_id,
        "fault_name": fault["name"],
        "fault_category": fault["category"],
        "repetition": rep,
        "status": "started",
        "phases": {},
        "recovery_status": None,
        "recovery_checks": None,
        "errors": [],
    }

    try:
        # --- Phase 1: PRE-FAULT (NORMAL) ---
        record["phases"]["pre_fault"] = {
            "label": "NORMAL",
            "start": iso_now(),
            "start_epoch": time.time(),
        }
        wait_phase("PRE-FAULT", PRE_FAULT_SECONDS)
        record["phases"]["pre_fault"]["end"] = iso_now()
        record["phases"]["pre_fault"]["end_epoch"] = time.time()

        check_shutdown()

        # --- Phase 2: INJECTION (ANOMALOUS) ---
        record["phases"]["injection"] = {
            "label": "ANOMALOUS",
            "start": iso_now(),
            "start_epoch": time.time(),
            "target_duration_seconds": fault["duration_seconds"],
        }

        injection_success = False

        if fault_id == "F5":
            # Custom: rebalancing storm via scaling
            inject_f5_rebalancing_storm(fault_id, rep, fault["duration_seconds"])
            injection_success = True

        elif fault_id == "F6":
            # Custom: poison pill producer
            inject_f6_poison_pill(fault_id, rep, fault["duration_seconds"],
                                  baseline_stats)
            injection_success = True

        else:
            # Chaos Mesh CR
            cr_yaml = generate_chaos_cr(fault_id, rep)
            if cr_yaml:
                applied = kubectl_apply_yaml(cr_yaml)
                if applied:
                    _active_chaos_cr = {"kind": chaos_kind, "name": cr_name}
                    injection_success = True
                    log.info("Chaos CR applied: %s/%s, waiting %ds",
                             chaos_kind, cr_name, fault["duration_seconds"])
                    wait_phase("INJECTION", fault["duration_seconds"])
                else:
                    record["errors"].append(
                        f"Failed to apply Chaos Mesh CR for {fault_id}")
                    log.error("Failed to apply Chaos Mesh CR, skipping injection phase")

                # Delete the CR after injection period (even if Chaos Mesh auto-expires)
                if chaos_kind and applied:
                    _delete_chaos_cr(chaos_kind, cr_name)
                    _active_chaos_cr = None

        if not injection_success and fault_id not in ("F5", "F6"):
            record["phases"]["injection"]["error"] = "injection_failed"

        record["phases"]["injection"]["end"] = iso_now()
        record["phases"]["injection"]["end_epoch"] = time.time()
        record["phases"]["injection"]["actual_duration_seconds"] = (
            record["phases"]["injection"]["end_epoch"]
            - record["phases"]["injection"]["start_epoch"]
        )

        check_shutdown()

        # --- Phase 3: POST-FAULT (RECOVERY) ---
        record["phases"]["post_fault"] = {
            "label": "RECOVERY",
            "start": iso_now(),
            "start_epoch": time.time(),
        }
        wait_phase("POST-FAULT", POST_FAULT_SECONDS)
        record["phases"]["post_fault"]["end"] = iso_now()
        record["phases"]["post_fault"]["end_epoch"] = time.time()

        check_shutdown()

        # --- Phase 4: COOLDOWN ---
        record["phases"]["cooldown"] = {
            "label": "COOLDOWN",
            "start": iso_now(),
            "start_epoch": time.time(),
        }
        wait_phase("COOLDOWN", COOLDOWN_SECONDS)
        record["phases"]["cooldown"]["end"] = iso_now()
        record["phases"]["cooldown"]["end_epoch"] = time.time()

        check_shutdown()

        # --- Phase 5: VALIDATION ---
        record["phases"]["validation"] = {
            "start": iso_now(),
            "start_epoch": time.time(),
        }

        recovery_status, checks = wait_for_recovery(baseline_stats)

        if recovery_status == "extended_recovery":
            log.warning("Run %d: extended recovery, attempting remediation", run_num)
            recovery_status, checks = attempt_remediation(baseline_stats)

        record["recovery_status"] = recovery_status
        record["recovery_checks"] = checks
        record["phases"]["validation"]["end"] = iso_now()
        record["phases"]["validation"]["end_epoch"] = time.time()

        # Set final status
        if recovery_status in ("recovered", "recovered_after_remediation"):
            record["status"] = "completed"
        elif recovery_status == "extended_recovery":
            record["status"] = "completed_extended_recovery"
        else:
            record["status"] = "completed_failed_recovery"

    except SystemExit:
        record["status"] = "interrupted"
        record["errors"].append("Shutdown requested by signal")
        # Clean up any active chaos CR
        if _active_chaos_cr:
            _delete_chaos_cr(_active_chaos_cr["kind"], _active_chaos_cr["name"])
            _active_chaos_cr = None
        raise

    except Exception as e:
        record["status"] = "error"
        record["errors"].append(str(e))
        log.exception("Unexpected error during run %d", run_num)
        # Clean up any active chaos CR
        if _active_chaos_cr:
            _delete_chaos_cr(_active_chaos_cr["kind"], _active_chaos_cr["name"])
            _active_chaos_cr = None

    finally:
        record["completed_at"] = iso_now()
        # Compute total run duration
        pre_start = record["phases"].get("pre_fault", {}).get("start_epoch")
        if pre_start:
            record["total_duration_seconds"] = time.time() - pre_start

        # Persist to manifest
        manifest["runs"].append(record)
        save_manifest(manifest, manifest_path)
        log.info("Run %d recorded: status=%s, recovery=%s",
                 run_num, record["status"], record.get("recovery_status"))

    return record


# ---------------------------------------------------------------------------
# Dry-run printer
# ---------------------------------------------------------------------------

def print_dry_run(plan: list[dict], start_from: int):
    """Print the campaign plan without executing."""
    total_time_seconds = 0
    print("\n" + "=" * 70)
    print("CAMPAIGN DRY RUN")
    print("=" * 70)

    for run in plan:
        if run["run_number"] < start_from:
            status = "SKIP (already completed)"
        else:
            status = "PENDING"

        fault = FAULT_DEFINITIONS[run["fault_id"]]
        run_time = (
            PRE_FAULT_SECONDS
            + fault["duration_seconds"]
            + POST_FAULT_SECONDS
            + COOLDOWN_SECONDS
            + VALIDATION_HOLD_SECONDS
        )

        if status == "PENDING":
            total_time_seconds += run_time

        print(f"  Run {run['run_number']:3d}: {run['fault_id']} "
              f"({fault['name']}) rep {run['repetition']:2d} "
              f"| ~{run_time // 60} min | {status}")

    total_pending = len([r for r in plan if r["run_number"] >= start_from])
    print(f"\nTotal pending runs: {total_pending}")
    print(f"Estimated time: {total_time_seconds / 3600:.1f} hours "
          f"(excludes recovery delays)")
    print(f"Starting from run: {start_from}")
    print("=" * 70 + "\n")


# ---------------------------------------------------------------------------
# Pre-flight checks
# ---------------------------------------------------------------------------

def preflight_checks(baseline_stats: dict) -> bool:
    """Verify cluster health before starting campaign."""
    log.info("Running pre-flight checks...")
    all_ok = True

    # 1. kubectl connectivity
    try:
        kubectl(["cluster-info"], timeout=15, retries=1)
        log.info("  [OK] kubectl connected to cluster")
    except (TimeoutError, RuntimeError) as e:
        log.error("  [FAIL] kubectl: %s", e)
        return False

    # 2. Kafka pods running
    try:
        result = kubectl(
            ["get", "pods", "-n", NAMESPACE,
             "-l", f"strimzi.io/cluster={KAFKA_CLUSTER},strimzi.io/kind=Kafka",
             "--field-selector=status.phase=Running",
             "-o", "jsonpath={range .items[*]}{.metadata.name}{\"\\n\"}{end}"],
        )
        broker_pods = [p for p in result.stdout.strip().split("\n") if p]
        if len(broker_pods) >= 3:
            log.info("  [OK] %d Kafka broker pods running", len(broker_pods))
        else:
            log.error("  [FAIL] Only %d Kafka broker pods running (need 3)",
                      len(broker_pods))
            all_ok = False
    except (TimeoutError, RuntimeError) as e:
        log.error("  [FAIL] Cannot check Kafka pods: %s", e)
        all_ok = False

    # 3. Flink pods running
    try:
        result = kubectl(
            ["get", "pods", "-n", NAMESPACE,
             "-l", f"app={FLINK_DEPLOYMENT},component=taskmanager",
             "--field-selector=status.phase=Running",
             "-o", "jsonpath={range .items[*]}{.metadata.name}{\"\\n\"}{end}"],
        )
        tm_pods = [p for p in result.stdout.strip().split("\n") if p]
        if len(tm_pods) >= 3:
            log.info("  [OK] %d Flink TaskManager pods running", len(tm_pods))
        else:
            log.warning("  [WARN] Only %d Flink TaskManager pods running (expect 3)",
                        len(tm_pods))
    except (TimeoutError, RuntimeError) as e:
        log.error("  [FAIL] Cannot check Flink pods: %s", e)
        all_ok = False

    # 4. Prometheus connectivity
    try:
        req = Request(f"{_config["prometheus_url"]}/api/v1/status/config",
                      headers={"Accept": "application/json"})
        with urlopen(req, timeout=10) as resp:
            if resp.status == 200:
                log.info("  [OK] Prometheus reachable at %s", _config["prometheus_url"])
    except Exception as e:
        log.error("  [FAIL] Prometheus not reachable at %s: %s", _config["prometheus_url"], e)
        log.error("  Start port-forward: kubectl port-forward -n observability "
                  "svc/prometheus-kube-prometheus-prometheus 9090:9090")
        all_ok = False

    # 5. Chaos Mesh installed
    try:
        result = kubectl(["api-resources", "--api-group=chaos-mesh.org"],
                         timeout=15, retries=1, check=False)
        if "podchaos" in result.stdout.lower():
            log.info("  [OK] Chaos Mesh CRDs installed")
        else:
            log.error("  [FAIL] Chaos Mesh CRDs not found")
            all_ok = False
    except (TimeoutError, RuntimeError) as e:
        log.error("  [FAIL] Cannot check Chaos Mesh CRDs: %s", e)
        all_ok = False

    # 6. Baseline stats sanity
    required_keys = ["consumer_lag_mean", "consumer_lag_std",
                     "throughput_mean", "throughput_std"]
    for key in required_keys:
        if key not in baseline_stats:
            log.error("  [FAIL] Missing baseline stat: %s", key)
            all_ok = False
    if all(k in baseline_stats for k in required_keys):
        log.info("  [OK] Baseline stats loaded (throughput_mean=%.0f, lag_mean=%.0f)",
                 baseline_stats.get("throughput_mean", 0),
                 baseline_stats.get("consumer_lag_mean", 0))

    # 7. Clean up any leftover chaos CRs from previous interrupted runs
    log.info("  Cleaning up any leftover chaos CRs...")
    cleanup_all_chaos_crs()

    return all_ok


# ---------------------------------------------------------------------------
# Main
# ---------------------------------------------------------------------------

def main():
    parser = argparse.ArgumentParser(
        description="Campaign orchestrator for streaming fault injection benchmark",
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog=textwrap.dedent("""\
            Baseline stats JSON format:
            {
                "consumer_lag_mean": 150.0,
                "consumer_lag_std": 50.0,
                "throughput_mean": 5000.0,
                "throughput_std": 500.0
            }
        """),
    )
    parser.add_argument(
        "--baseline-stats", required=True,
        help="Path to baseline statistics JSON (from 24h normal operation)",
    )
    parser.add_argument(
        "--manifest", default="campaign_manifest.json",
        help="Path to campaign manifest JSON (default: campaign_manifest.json)",
    )
    parser.add_argument(
        "--start-from", type=int, default=0,
        help="Resume from this run number (0 = auto-detect from manifest)",
    )
    parser.add_argument(
        "--dry-run", action="store_true",
        help="Print plan without executing",
    )
    parser.add_argument(
        "--fault", action="append", dest="faults",
        choices=list(FAULT_DEFINITIONS.keys()),
        help="Run only specific fault type(s). Can be repeated (e.g., --fault F1 --fault F3)",
    )
    parser.add_argument(
        "--prometheus", default="http://localhost:9090",
        help="Prometheus URL (default: http://localhost:9090)",
    )
    args = parser.parse_args()

    # Override Prometheus URL from CLI args
    _config["prometheus_url"] = args.prometheus

    # Load baseline stats
    if not os.path.exists(args.baseline_stats):
        log.error("Baseline stats file not found: %s", args.baseline_stats)
        sys.exit(1)
    with open(args.baseline_stats, "r") as f:
        baseline_stats = json.load(f)

    # Build run plan
    plan = build_run_plan(fault_filter=args.faults)
    if not plan:
        log.error("No runs in plan. Check --fault filter.")
        sys.exit(1)

    # Load or create manifest
    manifest = load_manifest(args.manifest)

    # Determine start point
    if args.start_from > 0:
        start_from = args.start_from
    else:
        last_completed = find_last_completed_run(manifest)
        if last_completed >= 0:
            # Find the run_number of the last completed run
            last_run_number = manifest["runs"][last_completed]["run_number"]
            start_from = last_run_number + 1
            log.info("Auto-resuming from run %d (last completed: %d)",
                     start_from, last_run_number)
        else:
            start_from = 1

    # Dry run mode
    if args.dry_run:
        print_dry_run(plan, start_from)
        sys.exit(0)

    # Pre-flight checks
    if not preflight_checks(baseline_stats):
        log.error("Pre-flight checks failed. Fix issues and retry.")
        sys.exit(1)

    # Execute campaign
    log.info("=" * 70)
    log.info("STARTING FAULT INJECTION CAMPAIGN")
    log.info("  Campaign ID: %s", manifest["campaign_id"])
    log.info("  Total runs planned: %d", len(plan))
    log.info("  Starting from run: %d", start_from)
    log.info("  Faults: %s", ", ".join(sorted(set(r["fault_id"] for r in plan))))
    log.info("=" * 70)

    completed = 0
    failed = 0
    skipped = 0

    try:
        for run in plan:
            if run["run_number"] < start_from:
                skipped += 1
                continue

            check_shutdown()

            record = execute_run(run, baseline_stats, manifest, args.manifest)

            if record["status"].startswith("completed"):
                completed += 1
            else:
                failed += 1
                log.warning("Run %d ended with status: %s",
                            run["run_number"], record["status"])

    except SystemExit as e:
        log.warning("Campaign interrupted: %s", e)
    except KeyboardInterrupt:
        log.warning("Campaign interrupted by keyboard")
    finally:
        # Final cleanup
        cleanup_all_chaos_crs()

        # Summary
        manifest["last_updated"] = iso_now()
        manifest["summary"] = {
            "completed": completed,
            "failed": failed,
            "skipped": skipped,
            "total_runs_in_manifest": len(manifest["runs"]),
        }
        save_manifest(manifest, args.manifest)

        log.info("=" * 70)
        log.info("CAMPAIGN SUMMARY")
        log.info("  Completed: %d", completed)
        log.info("  Failed: %d", failed)
        log.info("  Skipped: %d", skipped)
        log.info("  Manifest: %s", os.path.abspath(args.manifest))
        log.info("=" * 70)


if __name__ == "__main__":
    main()
