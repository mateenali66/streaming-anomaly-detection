"""Kubernetes access through kubectl (the image ships kubectl matching the 1.35 server, protocol 7.3.3).

Docs relied on (context7 /websites/kubectl_kubernetes_io, "Queries and Options > Not Found"): `kubectl get` of a
missing object exits non-zero with a NotFound error unless --ignore-not-found is given. This adapter never uses
--ignore-not-found: NotFound is detected from the error text and returned as None, and every other failure
raises KubeError. Objects are created with `kubectl create` (not apply), so an object left over under the same
name is an error instead of being silently reused.
"""
from __future__ import annotations

import json
import subprocess
from dataclasses import dataclass

from . import ids
from .errors import KubeError

KIND_RESOURCE = {
    "PodChaos": "podchaos.chaos-mesh.org",
    "NetworkChaos": "networkchaos.chaos-mesh.org",
    "StressChaos": "stresschaos.chaos-mesh.org",
    "IOChaos": "iochaos.chaos-mesh.org",
    "Job": "jobs.batch",
    "Pod": "pods",
    "Deployment": "deployments.apps",
    "FlinkDeployment": "flinkdeployments.flink.apache.org",
}


@dataclass
class ExecResult:
    rc: int
    stdout: str
    stderr: str


class Kube:
    """Interface used by the orchestrator and implemented by KubectlKube and fakes.FakeKube."""

    def list_pods(self, selector: str) -> list[dict]: ...
    def get(self, kind: str, name: str) -> dict | None: ...
    def list(self, kind: str, selector: str) -> list[dict]: ...
    def create(self, obj: dict) -> dict: ...
    def delete(self, kind: str, name: str, cascade: str = "background") -> bool: ...
    def patch_merge(self, kind: str, name: str, patch: dict) -> dict: ...
    def exec(self, pod: str, container: str, argv: list[str], timeout_s: float) -> ExecResult: ...
    def logs(self, pod: str, container: str | None = None) -> str: ...
    def list_nodes(self) -> list[dict]: ...
    def server_minor_version(self) -> str: ...


def _not_found(stderr: str) -> bool:
    return "(NotFound)" in stderr or "NotFound" in stderr.split("\n", 1)[0]


class KubectlKube(Kube):
    def __init__(self, namespace: str = ids.NAMESPACE, timeout_s: float = 60.0, kubectl: str = "kubectl") -> None:
        self.ns = namespace
        self.timeout_s = timeout_s
        self.kubectl = kubectl

    def _run(self, args: list[str], stdin: str | None = None, timeout_s: float | None = None) -> subprocess.CompletedProcess:
        cmd = [self.kubectl] + args
        try:
            return subprocess.run(cmd, input=stdin, capture_output=True, text=True, timeout=timeout_s or self.timeout_s)
        except subprocess.TimeoutExpired as e:
            raise KubeError(f"kubectl timed out: {' '.join(args[:4])}") from e
        except FileNotFoundError as e:
            raise KubeError("kubectl not found in PATH") from e

    def _json(self, args: list[str]) -> dict:
        r = self._run(args + ["-o", "json"])
        if r.returncode != 0:
            raise KubeError(f"kubectl {' '.join(args[:3])} failed: {r.stderr.strip()[:400]}")
        try:
            return json.loads(r.stdout)
        except json.JSONDecodeError as e:
            raise KubeError(f"kubectl returned invalid JSON for {' '.join(args[:3])}") from e

    def list_pods(self, selector: str) -> list[dict]:
        return self._json(["get", "pods", "-n", self.ns, "-l", selector])["items"]

    def get(self, kind: str, name: str) -> dict | None:
        r = self._run(["get", KIND_RESOURCE[kind], name, "-n", self.ns, "-o", "json"])
        if r.returncode != 0:
            if _not_found(r.stderr):
                return None
            raise KubeError(f"kubectl get {kind}/{name} failed: {r.stderr.strip()[:400]}")
        return json.loads(r.stdout)

    def list(self, kind: str, selector: str) -> list[dict]:
        return self._json(["get", KIND_RESOURCE[kind], "-n", self.ns, "-l", selector])["items"]

    def create(self, obj: dict) -> dict:
        r = self._run(["create", "-n", self.ns, "-f", "-", "-o", "json"], stdin=json.dumps(obj))
        if r.returncode != 0:
            raise KubeError(f"kubectl create {obj.get('kind')}/{obj.get('metadata', {}).get('name')} failed: "
                            f"{r.stderr.strip()[:400]}")
        return json.loads(r.stdout)

    def delete(self, kind: str, name: str, cascade: str = "background") -> bool:
        # --wait=false: the caller polls for NotFound itself and records the time (2.4 timing rule).
        r = self._run(["delete", KIND_RESOURCE[kind], name, "-n", self.ns, "--wait=false", f"--cascade={cascade}"])
        if r.returncode != 0:
            if _not_found(r.stderr):
                return False
            raise KubeError(f"kubectl delete {kind}/{name} failed: {r.stderr.strip()[:400]}")
        return True

    def patch_merge(self, kind: str, name: str, patch: dict) -> dict:
        return self._json(["patch", KIND_RESOURCE[kind], name, "-n", self.ns, "--type=merge", "-p", json.dumps(patch)])

    def exec(self, pod: str, container: str, argv: list[str], timeout_s: float) -> ExecResult:
        r = self._run(["exec", "-n", self.ns, pod, "-c", container, "--"] + argv, timeout_s=timeout_s)
        return ExecResult(r.returncode, r.stdout, r.stderr)

    def logs(self, pod: str, container: str | None = None) -> str:
        args = ["logs", "-n", self.ns, pod]
        if container:
            args += ["-c", container]
        r = self._run(args)
        if r.returncode != 0:
            raise KubeError(f"kubectl logs {pod} failed: {r.stderr.strip()[:400]}")
        return r.stdout

    def list_nodes(self) -> list[dict]:
        # Needs the read-only ClusterRole campaign-orchestrator-nodes (k8s/orchestrator.yaml).
        return self._json(["get", "nodes"])["items"]

    def server_minor_version(self) -> str:
        r = self._run(["version", "-o", "json"])
        if r.returncode != 0:
            raise KubeError(f"kubectl version failed: {r.stderr.strip()[:400]}")
        sv = json.loads(r.stdout).get("serverVersion") or {}
        major, minor = sv.get("major", ""), sv.get("minor", "").rstrip("+")
        if not major or not minor:
            raise KubeError("kubectl version returned no serverVersion")
        return f"{major}.{minor}"


# ------------------------------------------------------------------------------------------- pod helpers
def pod_ready(pod: dict) -> bool:
    if (pod.get("metadata") or {}).get("deletionTimestamp"):
        return False
    for c in (pod.get("status") or {}).get("conditions") or []:
        if c.get("type") == "Ready":
            return c.get("status") == "True"
    return False


def pod_summary(pod: dict, container: str | None = None) -> dict:
    """The pods_before / pods_after element of 11.6."""
    md, st = pod.get("metadata") or {}, pod.get("status") or {}
    restarts, reason = 0, None
    for cs in st.get("containerStatuses") or []:
        if container is None or cs.get("name") == container:
            restarts += int(cs.get("restartCount", 0))
            term = (cs.get("lastState") or {}).get("terminated")
            if term and term.get("reason"):
                reason = term["reason"]
    from .timeutil import parse_rfc3339
    started = st.get("startTime") or md.get("creationTimestamp")
    return {
        "pod": md["name"], "uid": md["uid"], "node": (pod.get("spec") or {}).get("nodeName") or "",
        "ready": pod_ready(pod), "restart_count": restarts, "last_termination_reason": reason,
        "started_at": parse_rfc3339(started) if started else 0.0,
    }


def pod_ip(pod: dict) -> str | None:
    return (pod.get("status") or {}).get("podIP") or None
