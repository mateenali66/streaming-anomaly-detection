#!/usr/bin/env python3
"""Offline test of scripts/check_flink_jobs.py (REVIEW4_LIVE_CONTRACT L1) with fake Flink REST answers in the JSON shapes of
Flink 1.19.3 (JobConfigInfo: execution-config.job-parallelism; CheckpointConfigInfo: mode in lower case, interval, timeout,
min_pause, externalization.enabled and delete_on_cancellation, state_backend, checkpoint_storage). Also checks that the
session cluster config and run-job.sh carry the same job settings."""
import copy
import sys
from pathlib import Path

import yaml

ROOT = Path(__file__).resolve().parent.parent
sys.path.insert(0, str(ROOT / "scripts"))
import check_flink_jobs as cfj  # noqa: E402

fails = 0


def check(name, cond, extra=""):
    global fails
    print(("ok   " if cond else "FAIL ") + name + ("" if cond else f"  {extra}"))
    fails += 0 if cond else 1


names = cfj.expected_names("ecommerce", cfj.SUBMIT.read_text())
check("job names come from submit-jobs.sh", names == ["Event Enrichment Job", "Windowed Aggregation Job", "Fraud Detection Job"], names)
check("IoT names too", cfj.expected_names("iot", cfj.SUBMIT.read_text())[0] == "IoT Event Enrichment Job")
want = cfj.expected_settings(cfj.RUN_JOB.read_text())
check("settings come from run-job.sh", want["parallelism"] == "4" and want["execution.checkpointing.interval"] == "60000"
      and want["restart-strategy.failure-rate.failure-rate-interval"] == "10 min", want)

GOOD_CP = {"mode": "exactly_once", "interval": 60000, "timeout": 120000, "min_pause": 30000, "max_concurrent": 1,
           "externalization": {"enabled": True, "delete_on_cancellation": False}, "state_backend": "EmbeddedRocksDBStateBackend",
           "checkpoint_storage": "FileSystemCheckpointStorage", "unaligned_checkpoints": False}


def rest(jobs, par=4, cp=None):
    cp = cp or GOOD_CP
    def get(path):
        if path == "/jobs/overview":
            return {"jobs": jobs}
        if path.endswith("/checkpoints/config"):
            return cp
        if path.endswith("/config"):
            return {"jid": path.split("/")[2], "execution-config": {"job-parallelism": par, "restart-strategy": "Cluster level default restart strategy"}}
        raise KeyError(path)
    return get


RUNNING = [{"jid": f"{i:032x}", "name": n, "state": "RUNNING"} for i, n in enumerate(names)]
check("three RUNNING jobs with the R1 settings pass", cfj.check_jobs(rest(RUNNING), names, want) == [])
p = cfj.check_jobs(rest(RUNNING, par=1), names, want)
check("parallelism 1 (the distribution default, L1) fails for every job", len(p) == 3 and "job-parallelism 1" in p[0], p)
cp = copy.deepcopy(GOOD_CP); cp["interval"] = 9223372036854775807; cp["externalization"] = {"enabled": False, "delete_on_cancellation": True}
p = cfj.check_jobs(rest(RUNNING, cp=cp), names, want)
check("checkpointing off and no retention fail", any("interval" in x for x in p) and any("externalization.enabled" in x for x in p), p)
cp = copy.deepcopy(GOOD_CP); cp["state_backend"] = "HashMapStateBackend"
check("a HashMap state backend fails", any("state backend" in x for x in cfj.check_jobs(rest(RUNNING, cp=cp), names, want)))
cp = copy.deepcopy(GOOD_CP); cp["mode"] = "at_least_once"
check("at-least-once fails", any("mode" in x for x in cfj.check_jobs(rest(RUNNING, cp=cp), names, want)))
p = cfj.check_jobs(rest(RUNNING[:2]), names, want)
check("a missing job fails", any("Fraud Detection Job" in x for x in p), p)
p = cfj.check_jobs(rest(RUNNING + [dict(RUNNING[0], jid="f" * 32)]), names, want)
check("a duplicate job fails", any("Event Enrichment Job" in x for x in p), p)
p = cfj.check_jobs(rest(RUNNING + [{"jid": "e" * 32, "name": "IoT Event Enrichment Job", "state": "RUNNING"}]), names, want)
check("a job of the other workload fails", any("unexpected live job" in x for x in p), p)
check("a FINISHED or CANCELED old job is ignored", cfj.check_jobs(rest(RUNNING + [{"jid": "d" * 32, "name": "Fraud Detection Job", "state": "CANCELED"}]), names, want) == [])

# the session default agrees with run-job.sh, so a job that somehow misses the flags still gets the same values (L1)
fd = yaml.safe_load((ROOT / "k8s/base/flink/flink-session-cluster.yaml").read_text().replace("${", "{"))
fc = fd["spec"]["flinkConfiguration"]
same = {k: v for k, v in want.items() if k != "parallelism"}
diff = {k: (fc.get(k), v) for k, v in same.items() if str(fc.get(k)) != v}
check("session config and run-job.sh carry the same job settings", not diff and fc["parallelism.default"] == want["parallelism"], diff)
check("three TaskManagers stay up without jobs: slotmanager.number-of-slots.min 12 (L4)",
      fc.get("slotmanager.number-of-slots.min") == "12" and int(fc["taskmanager.numberOfTaskSlots"]) * 3 == 12)
sys.exit(1 if fails else 0)
