#!/usr/bin/env python3
"""Alert-only watchdog of the campaign orchestrator (protocol 6.5 item 4; REVIEW4_OPERATIONS C6).

Runs every 10 minutes as the CronJob campaign-watchdog on the monitoring node, with the orchestrator image (python, boto3,
kubectl) and the orchestrator's service account, so it uses the orchestrator's Pod Identity role (S3 read of campaign/<id>/,
sns:Publish on the alert topic) and its Role (jobs and pods get and list, plus get, update and patch on ConfigMap
campaign-watchdog-state only). It mounts this file from ConfigMap campaign-watchdog.

It NEVER restarts, resubmits, heals or deletes anything (REVIEW1 A1: R1's healing CronJob acted behind the orchestrator).
It publishes to the alert topic when:
  1. the Job campaign-orchestrator has failed (a halt, a crash, an OOM kill at the 2 Gi limit, a deleted pod after a node loss);
  2. the orchestrator container has ended with a non-zero code while the pod still runs (its sidecar still syncing);
  3. the campaign manifest still says RUNNING and has not changed in S3 for WATCHDOG_STALE_S seconds, while the Job runs or is
     gone. The default 7,200 s is twice the longest expected attempt (about 3,600 s with a full remediation; protocol 1.6,
     1.9). The manifest changes at every attempt and every hourly baseline chunk.
A Job that succeeded (exit 0) raises nothing. Each alert is sent once and repeated every WATCHDOG_REMIND_S seconds while the
condition holds; the send times live in ConfigMap campaign-watchdog-state. Nothing is written to S3.

Environment: CAMPAIGN_ID, RESULTS_BUCKET, RESULTS_PREFIX (campaign/<id>), SNS_TOPIC_ARN, AWS_REGION, NAMESPACE (streaming),
KUBECTL_BIN, WATCHDOG_STALE_S (7200), WATCHDOG_REMIND_S (7200). Exit 0 when the checks ran, 1 when a read failed (that failure
is itself alerted).
"""
from __future__ import annotations

import datetime as dt
import json
import os
import subprocess
import sys
import time
from typing import Callable

JOB = "campaign-orchestrator"
STATE = "campaign-watchdog-state"
KEEP_STATE_S = 7 * 86400


class ReadError(Exception):
    pass


# ------------------------------------------------------------------ Kubernetes (kubectl with the in-cluster service account)
def kubectl(args: list[str], stdin: str | None = None) -> tuple[int, str, str]:
    p = subprocess.run([os.environ.get("KUBECTL_BIN", "kubectl"), "-n", os.environ.get("NAMESPACE", "streaming"), *args],
                       input=stdin, capture_output=True, text=True, timeout=60)
    return p.returncode, p.stdout, p.stderr


def kube_get(run: Callable = kubectl) -> Callable[[str, str], dict | None]:
    def get(kind: str, name: str) -> dict | None:
        rc, out, err = run(["get", kind, name, "-o", "json"])
        if rc == 0:
            return json.loads(out)
        if "NotFound" in err or "not found" in err:
            return None
        raise ReadError(f"kubectl get {kind} {name}: {err.strip()[:200]}")
    return get


def kube_pods(run: Callable = kubectl) -> Callable[[str], list[dict]]:
    def pods(selector: str) -> list[dict]:
        rc, out, err = run(["get", "pods", "-l", selector, "-o", "json"])
        if rc != 0:
            raise ReadError(f"kubectl get pods -l {selector}: {err.strip()[:200]}")
        return json.loads(out).get("items", [])
    return pods


def load_state(get: Callable[[str, str], dict | None]) -> dict[str, float]:
    cm = get("configmap", STATE)
    if not cm:
        return {}
    try:
        return {k: float(v) for k, v in json.loads(cm.get("data", {}).get("sent", "{}")).items()}
    except (ValueError, TypeError):
        return {}


def save_state(sent: dict[str, float], run: Callable = kubectl) -> None:
    doc = {"apiVersion": "v1", "kind": "ConfigMap", "metadata": {"name": STATE, "labels": {"app": "campaign-watchdog"}},
           "data": {"sent": json.dumps(sent, sort_keys=True)}}
    rc, _, err = run(["apply", "-f", "-"], stdin=json.dumps(doc))
    if rc != 0:
        raise ReadError(f"kubectl apply {STATE}: {err.strip()[:200]}")


# ------------------------------------------------------------------ what is wrong
def job_findings(job: dict | None, pods: list[dict]) -> list[tuple[str, str]]:
    """(alert key, message) for a failed Job or a failed orchestrator container."""
    if job is None:
        return []
    st = job.get("status", {})
    uid = job["metadata"].get("uid", "?")
    failed = st.get("failed", 0) >= 1 or any(c.get("type") == "Failed" and c.get("status") == "True" for c in st.get("conditions", []))
    exits = []
    for p in pods:
        for cs in p.get("status", {}).get("containerStatuses", []) or []:
            if cs.get("name") != "orchestrator":
                continue
            term = (cs.get("state") or {}).get("terminated") or (cs.get("lastState") or {}).get("terminated")
            if term and term.get("exitCode", 0) != 0:
                exits.append((p["metadata"].get("uid", "?"), term.get("exitCode"), term.get("reason", "")))
    detail = "; ".join(f"exit code {c} {r}".strip() for _, c, r in exits) or "no container status left (pod gone, for example after a node loss)"
    if failed:
        reason = next((c.get("reason", "") for c in st.get("conditions", []) if c.get("type") == "Failed"), "")
        return [(f"job-failed:{uid}", f"The orchestrator Job has FAILED ({reason or 'no reason given'}): {detail}.")]
    if st.get("succeeded", 0) >= 1:
        return []
    return [(f"exit:{u}:{c}", f"The orchestrator container ended with exit code {c} ({r or 'no reason given'}). The pod is still shutting down.")
            for u, c, r in exits]


def manifest_findings(job: dict | None, manifest: dict | None, now: float, stale_s: float) -> list[tuple[str, str]]:
    """(alert key, message) for a RUNNING campaign whose manifest has stopped changing."""
    if manifest is None:
        return []
    if job is not None and job.get("status", {}).get("succeeded", 0) >= 1:
        return []
    if manifest.get("status") != "RUNNING":
        return []
    age = now - manifest["last_modified"]
    if age < stale_s:
        return []
    where = "and there is no orchestrator Job" if job is None else "while the orchestrator Job is still running (stuck?)"
    stamp = dt.datetime.fromtimestamp(manifest["last_modified"], dt.timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ")
    return [(f"stale:{int(manifest['last_modified'])}",
             f"The campaign manifest says RUNNING but has not changed for {age / 3600:.1f} h (last change {stamp}) {where}.")]


# ------------------------------------------------------------------ S3 and SNS (boto3, Pod Identity)
def read_manifest(s3, bucket: str, prefix: str) -> dict | None:
    key = f"{prefix}/manifest/manifest.json"
    try:
        listing = s3.list_objects_v2(Bucket=bucket, Prefix=key, MaxKeys=1)  # existence by listing (403 for HEAD of a missing key)
        items = [o for o in listing.get("Contents", []) if o["Key"] == key]
        if not items:
            return None
        body = json.loads(s3.get_object(Bucket=bucket, Key=key)["Body"].read())
    except Exception as e:  # noqa: BLE001  any read failure is reported as such
        raise ReadError(f"s3 read of {key}: {type(e).__name__}") from e
    return {"status": body.get("status"), "last_modified": items[0]["LastModified"].timestamp()}


def decide(findings: list[tuple[str, str]], sent: dict[str, float], now: float, remind_s: float) -> list[tuple[str, str]]:
    return [(k, m) for k, m in findings if now - sent.get(k, -1e18) >= remind_s]


def run_once(get, pods, s3, sns, env: dict, now: float, save: Callable[[dict], None]) -> int:
    cid, topic = env["CAMPAIGN_ID"], env["SNS_TOPIC_ARN"]
    stale_s, remind_s = float(env.get("WATCHDOG_STALE_S", 7200)), float(env.get("WATCHDOG_REMIND_S", 7200))
    findings: list[tuple[str, str]] = []
    sent: dict[str, float] = {}
    rc = 0
    try:
        sent = load_state(get)
        job = get("job", JOB)
        findings += job_findings(job, pods(f"job-name={JOB}") if job is not None else [])
        findings += manifest_findings(job, read_manifest(s3, env["RESULTS_BUCKET"], env["RESULTS_PREFIX"]), now, stale_s)
    except ReadError as e:
        findings.append(("watchdog-read-error", f"The watchdog could not read its inputs: {e}."))
        rc = 1
    due = decide(findings, sent, now, remind_s)
    for key, msg in due:
        body = (f"Campaign {cid}: {msg}\n\nThe orchestrator does not restart itself. A transient stop may be resumed on the same "
                f"cluster within two hours (protocol 2.6 item 6); otherwise follow the abandon and teardown steps of the runbook. "
                f"Nothing was changed by this alert.")
        sns.publish(TopicArn=topic, Subject=f"[watchdog] {cid}: {msg}"[:99], Message=body)
        sent[key] = now
        print(f"ALERT {key}: {msg}")
    if not due:
        print(f"ok: {len(findings)} open finding(s), none due" if findings else "ok: nothing to report")
    if due:
        save({k: v for k, v in sent.items() if now - v < KEEP_STATE_S})
    return rc


def main() -> int:
    import boto3  # in the orchestrator image
    env = dict(os.environ)
    for k in ("CAMPAIGN_ID", "RESULTS_BUCKET", "RESULTS_PREFIX", "SNS_TOPIC_ARN"):
        if not env.get(k):
            print(f"watchdog: {k} is required", file=sys.stderr)
            return 2
    region = env.get("AWS_REGION", "us-east-2")
    return run_once(kube_get(), kube_pods(), boto3.client("s3", region_name=region), boto3.client("sns", region_name=region),
                    env, time.time(), save_state)


if __name__ == "__main__":
    sys.exit(main())
