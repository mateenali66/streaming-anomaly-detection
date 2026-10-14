#!/usr/bin/env python3
"""Check that the Flink jobs run with the campaign's job settings (REVIEW4_LIVE_CONTRACT L1). Read only. Needs kubectl.

Reads the Flink REST API through the Kubernetes API server's service proxy (no port-forward, nothing installed in the cluster):
  /jobs/overview                  exactly one RUNNING job per expected name, and no other live job
  /jobs/<id>/config               execution-config.job-parallelism
  /jobs/<id>/checkpoints/config   mode, interval, timeout, min_pause, externalization, state backend, checkpoint storage
The expected names come from images/flink-jobs/submit-jobs.sh and the expected settings from images/flink-jobs/run-job.sh, so
the check and the submission cannot drift apart. scripts/deploy_workload.sh runs it after phase `submit`; it exits non-zero
on any mismatch, before the orchestrator starts.

  check_flink_jobs.py --context <kube context> --workload ecommerce|iot
"""
from __future__ import annotations

import argparse
import json
import re
import shlex
import subprocess
import sys
from pathlib import Path
from typing import Callable

INFRA = Path(__file__).resolve().parent.parent
SUBMIT = INFRA / "images/flink-jobs/submit-jobs.sh"
RUN_JOB = INFRA / "images/flink-jobs/run-job.sh"
PROXY = "/api/v1/namespaces/streaming/services/streaming-flink-rest:rest/proxy"
LIVE = {"CREATED", "RUNNING", "FAILING", "CANCELLING", "RESTARTING", "SUSPENDED", "RECONCILING", "INITIALIZING"}


def expected_names(workload: str, submit_text: str) -> list[str]:
    block = re.search(rf"^\s*{workload.upper()}\)\s*\n\s*NAMES=\((.*?)\)\s*$", submit_text, re.M)
    if not block:
        raise SystemExit(f"no NAMES for {workload} in {SUBMIT}")
    return re.findall(r'"([^"]+)"', block.group(1))


def expected_settings(run_job_text: str) -> dict:
    block = re.search(r"^SETTINGS=\((.*?)^\)", run_job_text, re.S | re.M)
    if not block:
        raise SystemExit(f"no SETTINGS in {RUN_JOB}")
    words = shlex.split(block.group(1), comments=True)
    out: dict[str, str] = {}
    i = 0
    while i < len(words):
        w = words[i]
        if w == "-p":
            out["parallelism"] = words[i + 1]
            i += 2
            continue
        if w.startswith("-D") and "=" in w:
            k, v = w[2:].split("=", 1)
            out[k] = v
        i += 1
    return out


def check_jobs(get: Callable[[str], dict], names: list[str], want: dict) -> list[str]:
    problems: list[str] = []
    jobs = get("/jobs/overview").get("jobs", [])
    live = [j for j in jobs if j.get("state") in LIVE]
    for n in names:
        mine = [j for j in live if j.get("name") == n]
        if len(mine) != 1 or mine[0].get("state") != "RUNNING":
            problems.append(f"{n}: expected one RUNNING job, found {[j.get('state') for j in mine]}")
    for j in live:
        if j.get("name") not in names:
            problems.append(f"unexpected live job {j.get('name')!r} ({j.get('state')})")
    for j in [j for j in live if j.get("name") in names and j.get("state") == "RUNNING"]:
        jid, name = j["jid"], j["name"]
        par = get(f"/jobs/{jid}/config").get("execution-config", {}).get("job-parallelism")
        if str(par) != want["parallelism"]:
            problems.append(f"{name}: job-parallelism {par}, expected {want['parallelism']}")
        cp = get(f"/jobs/{jid}/checkpoints/config")
        checks = {
            "mode": (cp.get("mode"), want["execution.checkpointing.mode"].lower()),
            "interval": (cp.get("interval"), int(want["execution.checkpointing.interval"])),
            "timeout": (cp.get("timeout"), int(want["execution.checkpointing.timeout"])),
            "min_pause": (cp.get("min_pause"), int(want["execution.checkpointing.min-pause"])),
            "externalization.enabled": ((cp.get("externalization") or {}).get("enabled"), True),
            "externalization.delete_on_cancellation": ((cp.get("externalization") or {}).get("delete_on_cancellation"),
                                                       want["execution.checkpointing.externalized-checkpoint-retention"] != "RETAIN_ON_CANCELLATION"),
        }
        for k, (got, exp) in checks.items():
            if got != exp:
                problems.append(f"{name}: checkpoints {k} = {got!r}, expected {exp!r}")
        backend = str(cp.get("state_backend", ""))
        if want["state.backend.type"] == "rocksdb" and "rocksdb" not in backend.lower():
            problems.append(f"{name}: state backend {backend!r}, expected RocksDB")
        storage = str(cp.get("checkpoint_storage", ""))
        if "filesystem" not in storage.lower():
            problems.append(f"{name}: checkpoint storage {storage!r}, expected the file system storage on S3")
    return problems


def kubectl_get(context: str) -> Callable[[str], dict]:
    def get(path: str) -> dict:
        out = subprocess.run(["kubectl", "--context", context, "get", "--raw", PROXY + path], capture_output=True, text=True, timeout=60)
        if out.returncode != 0:
            raise SystemExit(f"kubectl get --raw {PROXY + path} failed: {out.stderr.strip()[:300]}")
        return json.loads(out.stdout)
    return get


def main() -> int:
    ap = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    ap.add_argument("--context", required=True)
    ap.add_argument("--workload", required=True, choices=["ecommerce", "iot"])
    a = ap.parse_args()
    names = expected_names(a.workload, SUBMIT.read_text())
    want = expected_settings(RUN_JOB.read_text())
    problems = check_jobs(kubectl_get(a.context), names, want)
    if problems:
        print("FLINK JOB SETTINGS WRONG:")
        for p in problems:
            print("  " + p)
        return 1
    print(f"ok: {len(names)} jobs RUNNING with parallelism {want['parallelism']}, checkpoints every "
          f"{want['execution.checkpointing.interval']} ms {want['execution.checkpointing.mode']}, RocksDB, retained on cancel")
    return 0


if __name__ == "__main__":
    sys.exit(main())
