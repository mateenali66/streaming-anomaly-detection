#!/usr/bin/env python3
"""Offline test of the alert-only watchdog k8s/base/watchdog/watchdog.py (REVIEW4 C6) with fake kubectl, S3 and SNS."""
import datetime as dt
import io
import json
import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parent.parent / "k8s" / "base" / "watchdog"))
import watchdog as wd  # noqa: E402

fails = 0


def check(name, cond, extra=""):
    global fails
    print(("ok   " if cond else "FAIL ") + name + ("" if cond else f"  {extra}"))
    fails += 0 if cond else 1


NOW = 1_800_000_000.0
ENV = {"CAMPAIGN_ID": "ec-a", "RESULTS_BUCKET": "b", "RESULTS_PREFIX": "campaign/ec-a", "SNS_TOPIC_ARN": "arn:aws:sns:us-east-2:000000000000:t",
       "WATCHDOG_STALE_S": "7200", "WATCHDOG_REMIND_S": "7200"}


class SNS:
    def __init__(self): self.sent = []
    def publish(self, **kw): self.sent.append(kw)


class S3:
    def __init__(self, status="RUNNING", age=60.0, missing=False, broken=False):
        self.status, self.age, self.missing, self.broken = status, age, missing, broken
    def list_objects_v2(self, Bucket, Prefix, MaxKeys):
        if self.broken:
            raise RuntimeError("AccessDenied")
        if self.missing:
            return {}
        return {"Contents": [{"Key": Prefix, "LastModified": dt.datetime.fromtimestamp(NOW - self.age, dt.timezone.utc)}]}
    def get_object(self, Bucket, Key): return {"Body": io.BytesIO(json.dumps({"status": self.status}).encode())}


def job(active=0, failed=0, succeeded=0, reason=None):
    st = {"active": active, "failed": failed, "succeeded": succeeded, "conditions": []}
    if reason:
        st["conditions"].append({"type": "Failed", "status": "True", "reason": reason})
    return {"metadata": {"name": "campaign-orchestrator", "uid": "job-uid-1"}, "status": st}


def pod(exit_code=None, reason="", running=False):
    cs = {"name": "orchestrator", "state": {"running": {}} if running else {"terminated": {"exitCode": exit_code, "reason": reason}}}
    return {"metadata": {"uid": "pod-uid-1"}, "status": {"containerStatuses": [cs, {"name": "s3-sync", "state": {"running": {}}}]}}


def scenario(j, pods, s3, state=None, now=NOW):
    store = {"cm": {"data": {"sent": json.dumps(state or {})}} if state is not None else None}
    def get(kind, name):
        if kind == "configmap":
            return store["cm"]
        return j
    saved = {}
    sns = SNS()
    rc = wd.run_once(get, lambda sel: pods, s3, sns, ENV, now, lambda sent: saved.update(sent=sent))
    return rc, sns.sent, saved.get("sent")


rc, sent, saved = scenario(job(active=1), [pod(running=True)], S3())
check("a healthy running campaign sends nothing", rc == 0 and sent == [] and saved is None)
rc, sent, saved = scenario(job(failed=1, reason="BackoffLimitExceeded"), [pod(137, "OOMKilled")], S3())
check("an OOM-killed orchestrator (Job failed) alerts with the exit code", rc == 0 and len(sent) == 1 and "FAILED" in sent[0]["Message"]
      and "137 OOMKilled" in sent[0]["Message"] and sent[0]["TopicArn"] == ENV["SNS_TOPIC_ARN"], sent)
check("the subject fits the SNS limit and names the campaign", len(sent[0]["Subject"]) < 100 and "ec-a" in sent[0]["Subject"])
check("the send time is stored for deduplication", saved == {"job-failed:job-uid-1": NOW}, saved)
rc, sent, _ = scenario(job(failed=1, reason="BackoffLimitExceeded"), [], S3(), state={"job-failed:job-uid-1": NOW - 600})
check("the same failure is not sent again within the reminder time", sent == [], sent)
rc, sent, _ = scenario(job(failed=1, reason="BackoffLimitExceeded"), [], S3(), state={"job-failed:job-uid-1": NOW - 7300})
check("a reminder after 2 h; a node loss (no pod left) is named", len(sent) == 1 and "no container status left" in sent[0]["Message"], sent)
rc, sent, _ = scenario(job(active=1), [pod(4, "Error")], S3())
check("a non-zero exit while the pod still shuts down alerts", len(sent) == 1 and "exit code 4" in sent[0]["Message"], sent)
rc, sent, _ = scenario(job(active=1), [pod(running=True)], S3(age=7300))
check("a RUNNING manifest unchanged for 2 h alerts (stuck orchestrator)", len(sent) == 1 and "has not changed" in sent[0]["Message"], sent)
rc, sent, _ = scenario(None, [], S3(age=7300))
check("no Job and a stale RUNNING manifest alerts", len(sent) == 1 and "no orchestrator Job" in sent[0]["Message"], sent)
rc, sent, _ = scenario(None, [], S3(age=600))
check("no Job and a fresh manifest (between a stop and a resume) sends nothing", sent == [], sent)
rc, sent, _ = scenario(job(succeeded=1), [pod(0, "Completed")], S3(status="RUNNING", age=99999))
check("a Job that succeeded (exit 0, including a pilot subset) sends nothing", sent == [], sent)
rc, sent, _ = scenario(job(active=1), [pod(running=True)], S3(status="COMPLETE", age=99999))
check("a COMPLETE manifest is never stale", sent == [], sent)
rc, sent, _ = scenario(job(active=1), [pod(running=True)], S3(missing=True))
check("no manifest yet (start-up) sends nothing", rc == 0 and sent == [], sent)
rc, sent, _ = scenario(job(active=1), [pod(running=True)], S3(broken=True))
check("a read failure is alerted and the run exits 1", rc == 1 and len(sent) == 1 and "could not read" in sent[0]["Message"], sent)

# kubectl wrappers: NotFound is None, other errors raise
get = wd.kube_get(lambda args, stdin=None: (1, "", 'Error from server (NotFound): jobs.batch "campaign-orchestrator" not found'))
check("kubectl NotFound reads as no Job", get("job", "campaign-orchestrator") is None)
get = wd.kube_get(lambda args, stdin=None: (1, "", "Error from server (Forbidden): forbidden"))
try:
    get("job", "x"); check("a Forbidden kubectl error is never read as no Job", False)
except wd.ReadError:
    check("a Forbidden kubectl error is never read as no Job", True)
calls = []
wd.save_state({"k": 1.0}, run=lambda args, stdin=None: (calls.append((args, json.loads(stdin))), (0, "", ""))[1])
check("the state is applied as ConfigMap campaign-watchdog-state", calls and calls[0][0] == ["apply", "-f", "-"]
      and calls[0][1]["metadata"]["name"] == "campaign-watchdog-state", calls)

src = Path(wd.__file__).read_text()
mutating = [w for w in ('"delete"', '"patch"', '"scale"', '"create"', '"exec"', "put_object", "delete_object", "flink") if w in src.split('"""', 2)[2]]
check("the watchdog never deletes, patches, scales, creates, execs or writes to S3, and never touches Flink", mutating == [], mutating)
sys.exit(1 if fails else 0)
