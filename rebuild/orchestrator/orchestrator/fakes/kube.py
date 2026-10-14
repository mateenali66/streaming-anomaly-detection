"""Fake Kubernetes API over the simulated world: the subset of kubectl the orchestrator uses."""
from __future__ import annotations

import json

from .. import ids
from ..errors import KubeError
from ..kube import ExecResult, Kube
from ..probe_service import QUORUM_CMD
from ..recovery import Remediation
from ..timeutil import to_rfc3339
from .clock import ProcessDead
from .world import FakePod, FakeWorld


def _match(labels: dict, selector: str) -> bool:
    for part in [p for p in selector.split(",") if p]:
        if "=" in part:
            k, v = part.split("=", 1)
            if labels.get(k) != v:
                return False
        elif part not in labels:
            return False
    return True


class FakeKube(Kube):
    def __init__(self, world: FakeWorld) -> None:
        self.w = world
        self.calls: list[tuple] = []

    def _t(self) -> float:
        if self.w.dead:
            raise ProcessDead("process is dead")
        self.w.advance(self.w.clock.t)
        return self.w.clock.t

    # ------------------------------------------------------------------------------------------ pods
    def pod_json(self, p: FakePod, t: float) -> dict:
        ready = p.ready(t)
        conds = [{"type": "Ready", "status": "True" if ready else "False",
                  "lastTransitionTime": to_rfc3339(p.ready_at if ready and p.ready_at else p.created_at)}]
        state = {"running": {}}
        if p.terminated_at is not None and t >= p.terminated_at:
            state = {"terminated": {"exitCode": p.exit_code, "reason": "Completed" if p.exit_code == 0 else "Error"}}
        cs = {"name": p.container, "restartCount": p.restart_count, "state": state, "ready": ready}
        if p.last_reason:
            cs["lastState"] = {"terminated": {"reason": p.last_reason}}
        return {"metadata": {"name": p.name, "uid": p.uid, "labels": dict(p.labels), "creationTimestamp": to_rfc3339(p.created_at)},
                "spec": {"nodeName": p.node, "containers": [{"name": p.container}]},
                "status": {"phase": "Running", "podIP": p.ip, "startTime": to_rfc3339(p.created_at), "conditions": conds,
                           "containerStatuses": [cs]}}

    def list_pods(self, selector: str) -> list[dict]:
        t = self._t()
        self.calls.append(("list_pods", selector))
        return [self.pod_json(p, t) for p in self.w.pods_at(t) if _match(p.labels, selector)]

    # ------------------------------------------------------------------------------------------ objects
    def get(self, kind: str, name: str) -> dict | None:
        t = self._t()
        if kind in ids.CHAOS_KINDS:
            c = self.w.chaos_by_name(kind, name, t)
            return None if c is None else self.w.chaos_obj(c, t)
        if kind == "Job":
            j = self.w.jobs_k8s.get(name)
            if j is None or (j.get("gone_at") is not None and t >= j["gone_at"]):
                return None
            st = {}
            if j.get("done_at") is not None and t >= j["done_at"]:
                st = {"succeeded": 1} if j["ok"] else {"failed": 1}
            md = {"name": name, "labels": j["labels"]}
            if j.get("delete_at") is not None:
                md["deletionTimestamp"] = to_rfc3339(j["delete_at"])
            return {"apiVersion": "batch/v1", "kind": "Job", "metadata": md, "status": st}
        if kind == "FlinkDeployment":
            return {"metadata": {"name": name}, "spec": {}}
        raise KubeError(f"fake get of {kind} unsupported")

    def list(self, kind: str, selector: str) -> list[dict]:
        t = self._t()
        if kind in ids.CHAOS_KINDS:
            return [self.w.chaos_obj(c, t) for c in self.w.chaos
                    if c.kind == kind and (c.gone_at is None or t < c.gone_at) and _match(c.labels, selector)]
        if kind == "Job":
            out = []
            for name, j in self.w.jobs_k8s.items():
                if (j.get("gone_at") is None or t < j["gone_at"]) and _match(j["labels"], selector):
                    out.append(self.get("Job", name))
            return out
        raise KubeError(f"fake list of {kind} unsupported")

    def create(self, obj: dict) -> dict:
        t = self._t()
        kind, name = obj["kind"], obj["metadata"]["name"]
        self.calls.append(("create", kind, name))
        if kind in ids.CHAOS_KINDS:
            if self.w.chaos_by_name(kind, name, t) is not None:
                raise KubeError(f"{kind} {name} AlreadyExists")
            self.w.create_chaos(obj, t)
            return obj
        if kind == "Job":
            existing = self.w.jobs_k8s.get(name)
            if existing is not None and (existing.get("gone_at") is None or t < existing["gone_at"]):
                raise KubeError(f"Job {name} AlreadyExists")
            if name.startswith("export-"):
                self._export_job(obj, t)
            elif name.startswith("poison-"):
                self._injector_job(obj, t)
            else:
                raise KubeError(f"unexpected Job {name}")
            return obj
        raise KubeError(f"fake create of {kind} unsupported")

    def _export_job(self, obj: dict, t: float) -> None:
        name = obj["metadata"]["name"]
        args = obj["spec"]["template"]["spec"]["containers"][0]["args"]
        a = dict(zip(args[1::2], args[2::2]))
        fail_until = self.w.global_knobs["export_fail_until"]
        ok = not (fail_until is not None and t < fail_until)
        log = ""
        if ok:
            result = self.w.exporter_hook(a["--kind"], a["--key"], float(a["--start-at"]), float(a["--end-at"]))
            log = json.dumps({"ok": True, **result}) + "\n"
        else:
            log = json.dumps({"ok": False, "error": "simulated Prometheus outage"}) + "\n"
        pod = self.w.add_pod(f"{name}-x7k2q"[:63], "job", {"job-name": name, **obj["metadata"]["labels"]}, t, 1, "exporter")
        pod.log = log
        pod.exit_code = 0 if ok else 1
        pod.terminated_at = t + 3
        self.w.jobs_k8s[name] = {"labels": dict(obj["metadata"]["labels"]), "done_at": t + 3, "ok": ok, "pods": [pod]}

    def _injector_job(self, obj: dict, t: float) -> None:
        """Runs the real f6_injector.run() on a private virtual timeline with a fake producer."""
        from .. import f6_injector
        name = obj["metadata"]["name"]
        c = obj["spec"]["template"]["spec"]["containers"][0]
        env = {e["name"]: e["value"] for e in c["env"]}
        gk = self.w.global_knobs
        assert "readinessProbe" not in c  # the injector Job has no readiness probe: Ready means running
        pod = self.w.add_pod(f"{name}-p8m3d"[:63], "injector", {"job-name": name, **obj["metadata"]["labels"]}, t,
                             4.0 if gk["f6_ready"] else None, "injector")
        start_at = float(env["START_AT_UNIX"])
        planned = int(round(float(env["INJECT_RATE"]) * float(env["INJECT_DURATION"])))
        send = max(1, int(planned * gk["f6_send_frac"]))
        clock = {"t": t + 3.0}
        lines: list[str] = []

        class Producer:
            def __init__(self):
                self.pending = []

            def produce(self, topic, value=None, key=None, headers=None, callback=None):
                assert topic == "raw-events" and headers and headers[0][0] == ids.F6_HEADER
                assert not value.decode().rstrip().endswith("}")
                self.pending.append((clock["t"] + gk["f6_ack_delay_s"], callback))

            def poll(self, timeout):
                keep = []
                for due, cb in self.pending:
                    if due <= clock["t"]:
                        cb(None, None)
                    else:
                        keep.append((due, cb))
                self.pending = keep

            def flush(self, timeout):
                if self.pending:
                    clock["t"] = max(clock["t"], max(d for d, _ in self.pending))
                self.poll(0)
                return 0

        def emit(event, **kw):
            lines.append(json.dumps({"event": event, **kw}, sort_keys=True))

        def make_producer():
            # decision 28 (REVIEW4 MIN-5): the injector opens its Kafka connection at START_AT_UNIX, never before
            assert clock["t"] >= start_at, "the F6 injector connected before its start time"
            self.w.f6_connects.append((env["ATTEMPT_KEY"], clock["t"]))
            return Producer()

        if gk["f6_ready"]:
            summ = f6_injector.run(make_producer, env["ATTEMPT_KEY"], start_at, float(env["INJECT_DURATION"]), send,
                                   int(env["INJECT_SEED"]), now=lambda: clock["t"],
                                   sleep=lambda s: clock.__setitem__("t", clock["t"] + max(s, 1e-4)),
                                   ready_file=None, emit=emit, print_line=lines.append)
            if send != planned:  # the injector sent fewer than planned: report the planned count it was given
                lines = [ln.replace(f'"planned_count": {send}', f'"planned_count": {planned}')
                         .replace(f" planned={send} ", f" planned={planned} ") for ln in lines]
            self.w.created_by_type["F6"] = self.w.created_by_type.get("F6", 0) + 1
            lost = self.w.created_by_type["F6"] <= self.w.knob("F6", "fail_first_n")
            if not self.w.knob("F6", "wrong_target") and not lost:
                # wrong_target (and the first fail_first_n injections): the injector reports success but the records
                # never reach raw-events, so the verification consumer counts nothing for the attempt
                self.w.f6_runs.append({"akey": env["ATTEMPT_KEY"], "first": summ["first_produce_at"],
                                       "last": summ["last_produce_at"], "count": summ["acked_count"]})
            done = clock["t"] + 0.5
            pod.exit_code = gk["f6_exit_code"]
        else:
            done = t + 120
            pod.exit_code = 2
            lines.append(json.dumps({"event": "error", "message": "could not connect"}))
        pod.log = "\n".join(lines) + "\n"
        pod.terminated_at = done
        self.w.jobs_k8s[name] = {"labels": dict(obj["metadata"]["labels"]), "done_at": done, "ok": pod.exit_code == 0,
                                 "pods": [pod]}

    def delete(self, kind: str, name: str, cascade: str = "background") -> bool:
        t = self._t()
        self.calls.append(("delete", kind, name))
        if kind in ids.CHAOS_KINDS:
            c = self.w.chaos_by_name(kind, name, t)
            if c is None:
                return False
            self.w.delete_chaos(c, t)
            return True
        if kind == "Job":
            j = self.w.jobs_k8s.get(name)
            if j is None or (j.get("gone_at") is not None and t >= j["gone_at"]):
                return False
            if j.get("delete_at") is None:
                j["delete_at"] = t
                j["gone_at"] = t + 1.0
                for p in j["pods"]:
                    p.deleted_at = t + 0.5
                    if p.terminated_at is None or p.terminated_at > t:
                        p.terminated_at = t
                if name.startswith("poison-"):
                    for r in self.w.f6_runs:
                        if r["akey"] == name[len("poison-"):] and r["last"] > t:
                            r["last"] = max(r["first"], t)
            return True
        raise KubeError(f"fake delete of {kind} unsupported")

    def patch_merge(self, kind: str, name: str, patch: dict) -> dict:
        t = self._t()
        self.calls.append(("patch", kind, name))
        if kind == "FlinkDeployment" and "restartNonce" in (patch.get("spec") or {}):
            self.w.redeploy(t)
            return {"metadata": {"name": name}}
        raise KubeError("fake patch unsupported")

    def exec(self, pod: str, container: str, argv: list[str], timeout_s: float) -> ExecResult:
        t = self._t()
        self.calls.append(("exec", pod, argv[0]))
        p = self.w.pod_by_name(pod, t)
        if p is None or not p.ready(t):
            return ExecResult(1, "", f"error: pod {pod} not ready")
        if argv == QUORUM_CMD:
            if any(a <= t < b for a, b in self.w.global_knobs["quorum_fail"]):
                return ExecResult(1, "", "org.apache.kafka.common.errors.TimeoutException: Timed out waiting for a node")
            return ExecResult(0, f"ClusterId: abc\nLeaderId: {self.w.controller()}\nLeaderEpoch: 4\n", "")
        if argv == Remediation.LEADER_ELECTION:
            self.w.leader_elections.append(t)
            self.w.imbalance_until = t
            return ExecResult(0, "Successfully completed leader election", "")
        if argv and argv[0] == Remediation.RUN_JOB:
            # run-job.sh <main-class> <jar> [<checkpoint path>] (REVIEW4 L1): it passes -p 4 and the checkpoint settings
            if not 3 <= len(argv) <= 4 or not argv[2].startswith(ids.FLINK_JAR_DIR + "/"):
                return ExecResult(2, "", "usage: run-job.sh <main-class> <jar> [<checkpoint path>]")
            cls, restored = argv[1], (argv[3] if len(argv) == 4 else None)
            if restored is not None and not restored.startswith("s3://"):
                return ExecResult(1, "", f"Could not restore from {restored}: not a checkpoint URI")
            name = next(n for n, c, _ in ids.FLINK_JOBS[self.w.workload] if c == cls)
            self.w.submit_job(name, t + 2, restored)
            return ExecResult(0, "Job has been submitted", "")
        if argv[:2] == ["/opt/flink/bin/flink", "run"]:
            # The plain CLI builds the JobGraph from the distribution config: parallelism 1, no checkpoints (REVIEW4 L1).
            # The orchestrator must never submit this way.
            return ExecResult(126, "", "refused by the simulated cluster: submit with /opt/flink/bin/run-job.sh (L1)")
        return ExecResult(127, "", f"fake exec does not know {argv[0]}")

    def logs(self, pod: str, container: str | None = None) -> str:
        self._t()
        for p in reversed(self.w.pods):
            if p.name == pod:
                return p.log
        raise KubeError(f"no pod {pod}")

    def list_nodes(self) -> list[dict]:
        t = self._t()
        out = []
        rep = self.w.global_knobs["node_replaced_at"]
        for i, n in enumerate(self.w.nodes):
            iid = n["iid"]
            if rep is not None and t >= rep and i == 4:
                iid = "i-0ffffffffffffffff"
            out.append({"metadata": {"name": n["name"], "labels": {"topology.kubernetes.io/zone": "us-east-2b"}},
                        "spec": {"providerID": f"aws:///us-east-2b/{iid}"}})
        return out

    def server_minor_version(self) -> str:
        self._t()
        return "1.35"

