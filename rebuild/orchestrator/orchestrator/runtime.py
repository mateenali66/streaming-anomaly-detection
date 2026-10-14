"""The adapters and settings one orchestrator process works with, plus the in-cluster Job runner."""
from __future__ import annotations

import json
import logging
from dataclasses import dataclass, field
from pathlib import Path
from typing import Callable

from . import ids
from .canonical import sha256_hex
from .clock import Clock
from .config import Environment
from .errors import HaltRequested, KubeError, OrchestratorError
from .flinkrest import FlinkRest
from .httpjson import HttpJson
from .kube import Kube
from .manifest import ManifestStore, Uploader
from .probe_service import ProbeClient
from .prom import Prom
from .store import ObjectStore

log = logging.getLogger("orchestrator")


class HookedClock(Clock):
    """Wraps the process clock so that periodic work (the AWS Health poll, C30) runs inside long sleeps without a thread.

    A hook is called as hook(now, remaining_s) at the start of every sleep slice and returns the instant it is next due (or
    None). Sleeps are absolute: a sleep for s seconds ends at the instant it would have ended without the hook, so a poll
    never moves a planned step. Hooks run only when there is time left (each hook checks its own slack)."""

    def __init__(self, inner: Clock) -> None:
        self.inner = inner
        self.hooks: list = []

    def add_hook(self, fn) -> None:
        self.hooks.append(fn)

    @property
    def shutdown(self):
        return getattr(self.inner, "shutdown", None)

    def now(self) -> float:
        return self.inner.now()

    def check_shutdown(self) -> None:
        self.inner.check_shutdown()

    def sleep(self, seconds: float) -> None:
        if seconds <= 0 or not self.hooks:
            self.inner.sleep(seconds)
            return
        target = self.inner.now() + seconds
        while True:
            now = self.inner.now()
            left = target - now
            if left <= 0:
                return
            dues = [d for d in (h(now, left) for h in self.hooks) if d is not None]
            now = self.inner.now()
            left = target - now
            if left <= 0:
                return
            nxt = min(dues) if dues else None
            step = left if nxt is None or nxt <= now else min(left, nxt - now)
            self.inner.sleep(step)


@dataclass
class Runtime:
    env: Environment
    cfg: dict
    allow: dict
    allow_bytes: bytes
    thresholds: dict
    thresholds_bytes: bytes
    plan: dict
    plan_bytes: bytes
    clock: Clock
    kube: Kube
    prom: Prom
    store: ObjectStore
    flink: FlinkRest
    probes: ProbeClient
    http: HttpJson
    sentinel: Callable[[], None]
    alert: Callable[[str, str], None]
    versions: dict
    images_lock_bytes: bytes
    volume: Path
    manifest_store: ManifestStore = None  # type: ignore[assignment]
    uploader: Uploader = None  # type: ignore[assignment]
    instance_ids_at_start: list[str] = field(default_factory=list)
    log_uploader: Callable[[], object] | None = None
    plan_file_bytes: bytes | None = None         # the mounted plan (PLAN_PATH), compared byte for byte
    loadgen_seed: Callable[[], float | None] | None = None
    health: object | None = None                 # AWS Health events source (health.CloudWatchHealthEvents), C30

    def __post_init__(self) -> None:
        if not isinstance(self.clock, HookedClock):
            self.clock = HookedClock(self.clock)
        self.uploader = Uploader(self.store, self.clock, self.cfg["fixed"]["s3_failure_halt_s"])
        self.manifest_store = ManifestStore(self.volume, self.uploader, self.clock)

    @property
    def thresholds_sha(self) -> str:
        return sha256_hex(self.thresholds_bytes)

    @property
    def plan_sha(self) -> str:
        return sha256_hex(self.plan_bytes)

    @property
    def allow_sha(self) -> str:
        return sha256_hex(self.allow_bytes)

    def recorded_instance_ids(self) -> list[str]:
        return list(self.instance_ids_at_start)

    def clock_skew(self) -> float | None:
        """Orchestrator minus Prometheus time(), measured at the request midpoint (11.4 clock_skew_s)."""
        try:
            t0 = self.clock.now()
            prom_t = self.prom.scalar(self.thresholds["queries"]["time"])
            t1 = self.clock.now()
        except OrchestratorError:
            return None
        return round((t0 + t1) / 2.0 - prom_t, 3)

    def clock_skew_abs(self) -> float | None:
        s = self.clock_skew()
        return None if s is None else abs(s)


class JobRunner:
    """Runs a Kubernetes Job, waits for it, reads its result line and deletes it with NotFound confirmed.

    Used for the exporter Jobs (11.7, 11.11). Retries a failing Job (delete, confirm NotFound, recreate) until it
    succeeds or 15 minutes of failure have passed, then halts the campaign (2.6.5, 6.5.3).
    """

    def __init__(self, rt: Runtime) -> None:
        self.rt = rt

    def export_job(self, name: str, kind: str, key: str, start_at: float, end_at: float, akey: str | None) -> dict:
        rt = self.rt
        labels = ids.campaign_labels(rt.env.campaign_id, akey)
        # The exporter environment of rebuild/infra/NAMES.md section 4 (RESULTS_BUCKET plus RESULTS_PREFIX=campaign/<id>).
        env = [{"name": "WORKLOAD", "value": rt.env.workload.upper()},
               {"name": "CAMPAIGN_ID", "value": rt.env.campaign_id},
               {"name": "RESULTS_BUCKET", "value": rt.env.results_bucket},
               {"name": "RESULTS_PREFIX", "value": rt.env.results_key_prefix},
               {"name": "AWS_REGION", "value": ids.REGION}, {"name": "AWS_DEFAULT_REGION", "value": ids.REGION},
               {"name": "PROMETHEUS_URL", "value": rt.env.prometheus_url},
               {"name": "FEATURE_ALLOWLIST_SHA256", "value": rt.allow_sha},
               {"name": "HOME", "value": "/tmp"}, {"name": "PYTHONUNBUFFERED", "value": "1"},
               {"name": "PYTHONDONTWRITEBYTECODE", "value": "1"}]
        return {
            "apiVersion": "batch/v1", "kind": "Job",
            "metadata": {"name": name, "namespace": ids.NAMESPACE, "labels": labels},
            "spec": {
                "backoffLimit": 0, "activeDeadlineSeconds": 1800,
                "template": {
                    "metadata": {"labels": labels},
                    "spec": {
                        "serviceAccountName": ids.EXPORTER_SA, "automountServiceAccountToken": False,
                        "restartPolicy": "Never",
                        "nodeSelector": {"workload": "monitoring"},
                        "tolerations": [{"key": "workload", "operator": "Exists", "effect": "NoSchedule"}],
                        "securityContext": {"runAsNonRoot": True, "runAsUser": 10001, "runAsGroup": 10001},
                        "containers": [{
                            "name": "exporter", "image": rt.env.orchestrator_image or "orchestrator:dry-run",
                            "args": ["export", "--kind", kind, "--key", key, "--start-at", f"{start_at:.3f}",
                                     "--end-at", f"{end_at:.3f}"],
                            "env": env,
                            "resources": {"requests": {"cpu": "500m", "memory": "1Gi"}, "limits": {"memory": "4Gi"}},
                            "securityContext": {"allowPrivilegeEscalation": False, "readOnlyRootFilesystem": True,
                                                "capabilities": {"drop": ["ALL"]}},
                            "volumeMounts": [{"name": "tmp", "mountPath": "/tmp"}],
                        }],
                        "volumes": [{"name": "tmp", "emptyDir": {"sizeLimit": "1Gi"}}],
                    },
                },
            },
        }

    def delete_confirmed(self, kind: str, name: str, timeout_s: float = 120) -> float:
        rt = self.rt
        rt.kube.delete(kind, name, cascade="foreground")
        deadline = rt.clock.now() + timeout_s
        while rt.kube.get(kind, name) is not None:
            if rt.clock.now() > deadline:
                raise KubeError(f"{kind}/{name} still present {timeout_s} s after delete")
            rt.clock.sleep(1)
        return rt.clock.now()

    def run_once(self, obj: dict, timeout_s: float = 1800) -> dict:
        rt = self.rt
        name = obj["metadata"]["name"]
        if rt.kube.get("Job", name) is not None:
            self.delete_confirmed("Job", name)  # a Job left by an interrupted run is never reused
        rt.kube.create(obj)
        deadline = rt.clock.now() + timeout_s
        while True:
            job = rt.kube.get("Job", name)
            st = (job or {}).get("status") or {}
            if job is None:
                raise KubeError(f"Job {name} disappeared")
            if st.get("succeeded", 0) >= 1 or st.get("failed", 0) >= 1:
                break
            if rt.clock.now() > deadline:
                raise KubeError(f"Job {name} did not finish in {timeout_s} s")
            rt.clock.sleep(5)
        pods = rt.kube.list_pods(f"job-name={name}")
        logs = rt.kube.logs(pods[0]["metadata"]["name"]) if pods else ""
        result = None
        for line in logs.splitlines():
            line = line.strip()
            if line.startswith("{"):
                try:
                    result = json.loads(line)
                except json.JSONDecodeError:
                    continue
        self.delete_confirmed("Job", name)
        if st.get("succeeded", 0) < 1 or not result or not result.get("ok"):
            raise OrchestratorError(f"Job {name} failed: {(result or {}).get('error', 'no result line')}")
        return result

    def run_with_retry(self, obj: dict) -> dict:
        rt = self.rt
        first_failure = None
        while True:
            try:
                return self.run_once(obj)
            except OrchestratorError as e:
                now = rt.clock.now()
                first_failure = first_failure or now
                log.warning("Job %s failed: %s", obj["metadata"]["name"], e)
                if now - first_failure >= rt.cfg["fixed"]["s3_failure_halt_s"]:
                    raise HaltRequested(f"EXPORT_FAILING_15_MIN {obj['metadata']['name']}")
                rt.clock.sleep(30)

    def export(self, kind: str, key: str, start_at: float, end_at: float, akey: str | None = None) -> dict:
        name = f"export-{key}" if kind == "attempt" else f"export-{kind}-{key}"
        result = self.run_with_retry(self.export_job(name, kind, key, start_at, end_at, akey))
        # Never trust the Job's own report alone: download both objects and hash them here.
        for k in ("features", "verification"):
            data = self.rt.store.get(result[f"{k}_key"])
            if sha256_hex(data) != result[f"{k}_sha256"]:
                raise HaltRequested(f"EXPORT_HASH_MISMATCH {result[f'{k}_key']}")
        return result
