#!/usr/bin/env python3
"""Offline test of scripts/campaign_inputs.py and the plan plumbing of scripts/deploy_workload.sh --dry-run
(rebuild/CONTRACT_CHANGES.md C5, C8, C12). Needs kubectl (client-side kustomize only) and PyYAML. No cluster, no AWS.

Checks: plan facts come from the plan (hash, index, the load generator seed), a plan of another campaign, a re-serialised
plan and a wrong plan.json.sha256 are refused; versions.json comes from the captured tfvars and the chart pins; the
ConfigMap round-trips every input byte for byte; the dry run puts PLAN_SHA256, CAMPAIGN_INDEX and the plan's
load_generator_seed (as strings) into the orchestrator Job and the load generator; the retired seed options and a load or
run phase without a plan are refused."""
import hashlib
import json
import os
import subprocess
import sys
import tempfile
from pathlib import Path

import yaml

ROOT = Path(__file__).resolve().parent.parent
TOOL = ROOT / "scripts" / "campaign_inputs.py"
DEPLOY = ROOT / "scripts" / "deploy_workload.sh"
EXAMPLE_TFVARS = ROOT / "terraform" / "cluster" / "addon-versions.auto.tfvars.example"
bad = 0


def check(name, cond, extra=""):
    global bad
    print(("ok   " if cond else "FAIL ") + name, extra if not cond else "")
    bad += 0 if cond else 1


def canonical(obj) -> bytes:
    return json.dumps(obj, sort_keys=True, separators=(",", ":"), ensure_ascii=False, allow_nan=False).encode()


def run(*args, **kw):
    return subprocess.run([sys.executable, str(TOOL), *args], capture_output=True, text=True, **kw)


d = Path(tempfile.mkdtemp())
plan = {"schema_version": "1", "workload": "ecommerce", "campaign_index": 1, "campaign_id": "ec-a", "seed": "1234567890123",
        "load_generator_seed": 987654321, "f6_payload_seed": 5, "blocks": []}
pp = d / "plan.json"
pp.write_bytes(canonical(plan))
sha = hashlib.sha256(pp.read_bytes()).hexdigest()
(d / "plan.json.sha256").write_text(sha + "\n")
th = d / "thresholds.json"
th.write_bytes(canonical({"schema_version": "1", "gate": {"F6": {"apply_max_s": 5}}}))

r = run("plan-facts", "--plan", str(pp), "--campaign-id", "ec-a", "--workload", "ecommerce")
facts = dict(line.split("=", 1) for line in r.stdout.split())
check("plan facts from the plan", r.returncode == 0 and facts == {"PLAN_SHA256": sha, "CAMPAIGN_INDEX": "1",
                                                                  "LOADGEN_SEED": "987654321", "PLAN_SEED": "1234567890123"},
      r.stdout + r.stderr)
r = run("plan-facts", "--plan", str(pp), "--campaign-id", "ec-b", "--workload", "ecommerce")
check("plan of another campaign refused", r.returncode != 0 and "not ecommerce ec-b" in r.stderr, r.stderr)
other = d / "pretty" / "plan.json"
other.parent.mkdir()
other.write_text(json.dumps(plan, indent=1))
r = run("plan-facts", "--plan", str(other), "--campaign-id", "ec-a", "--workload", "ecommerce")
check("re-serialised plan refused", r.returncode != 0 and "canonical" in r.stderr, r.stderr)
wrong = d / "wrong" / "plan.json"
wrong.parent.mkdir()
wrong.write_bytes(pp.read_bytes())
(wrong.parent / "plan.json.sha256").write_text("0" * 64 + "\n")
r = run("plan-facts", "--plan", str(wrong), "--campaign-id", "ec-a", "--workload", "ecommerce")
check("plan.json.sha256 that does not match refused", r.returncode != 0 and "does not hold the hash" in r.stderr, r.stderr)
idx = d / "idx" / "plan.json"
idx.parent.mkdir()
idx.write_bytes(canonical(dict(plan, campaign_index=2)))
r = run("plan-facts", "--plan", str(idx), "--campaign-id", "ec-a", "--workload", "ecommerce")
check("campaign_index that does not belong to the id refused", r.returncode != 0, r.stderr)

r = run("versions", "--tfvars", str(EXAMPLE_TFVARS))
v = json.loads(r.stdout)
check("versions.json from the captured tfvars and the chart pins",
      r.returncode == 0 and v["kubernetes_version"] == "1.35" and v["node_ami_release"] == "1.35.0-20260101"
      and set(v["addon_versions"]) == {"coredns", "kube-proxy", "vpc-cni", "eks-pod-identity-agent", "aws-ebs-csi-driver"}
      and v["chart_versions"]["strimzi"] == "0.45.2" and r.stdout.strip().encode() == canonical(v), r.stdout + r.stderr)

r = run("configmap", "--plan", str(pp), "--thresholds", str(th), "--tfvars", str(EXAMPLE_TFVARS))
cm = yaml.safe_load(r.stdout)
lock = (ROOT / "images" / "images.lock.yaml").read_text()
check("ConfigMap campaign-config round-trips every input byte for byte",
      r.returncode == 0 and cm["metadata"]["name"] == "campaign-config" and cm["data"]["plan.json"] == pp.read_text()
      and cm["data"]["thresholds.json"] == th.read_text() and cm["data"]["images.lock.yaml"] == lock
      and json.loads(cm["data"]["versions.json"])["node_ami_release"] == "1.35.0-20260101", r.stderr)

env = dict(os.environ)
base = ["bash", str(DEPLOY), "--workload", "ecommerce", "--campaign-id", "ec-a", "--results-bucket", "bucket",
        "--sns-topic-arn", "arn:x", "--ecr-registry", "registry.example", "--dry-run"]
r = subprocess.run(base + ["--phase", "run", "--plan-file", str(pp), "--thresholds-file", str(th),
                           "--versions-tfvars", str(EXAMPLE_TFVARS)], capture_output=True, text=True, env=env)
docs = [x for x in yaml.safe_load_all(r.stdout) if x] if r.returncode == 0 else []
job = next((x for x in docs if x["kind"] == "Job"), None)
jenv = {e["name"]: e.get("value") for e in job["spec"]["template"]["spec"]["containers"][0]["env"]} if job else {}
check("dry run, phase run: PLAN_SHA256 and CAMPAIGN_INDEX from the plan, as strings",
      jenv.get("PLAN_SHA256") == sha and jenv.get("CAMPAIGN_INDEX") == "1" and any(x["kind"] == "ConfigMap" and
      x["metadata"]["name"] == "campaign-config" for x in docs), r.stderr[-500:])
r = subprocess.run(base + ["--phase", "load", "--plan-file", str(pp)], capture_output=True, text=True, env=env)
docs = [x for x in yaml.safe_load_all(r.stdout) if x] if r.returncode == 0 else []
lg = next((x for x in docs if x["kind"] == "Deployment" and x["metadata"]["name"] == "load-generator"), None)
lenv = {e["name"]: e.get("value") for e in lg["spec"]["template"]["spec"]["containers"][0]["env"]} if lg else {}
check("dry run, phase load: LOADGEN_SEED is the plan's load_generator_seed", lenv.get("LOADGEN_SEED") == "987654321"
      and any(x["kind"] == "Deployment" and x["metadata"]["name"] == "verification-probes" for x in docs), r.stderr[-500:])
for flag in ("--campaign-seed", "--loadgen-seed", "--schedule-file"):
    r = subprocess.run(base + ["--phase", "rbac", flag, "1"], capture_output=True, text=True, env=env)
    check(f"retired option {flag} refused", r.returncode == 2 and "retired" in r.stderr, r.stderr)
r = subprocess.run(base + ["--phase", "load"], capture_output=True, text=True, env=env)
check("phase load without a plan refused", r.returncode == 2 and "--plan-file" in r.stderr, r.stderr)
r = subprocess.run(base + ["--phase", "run", "--plan-file", str(pp)], capture_output=True, text=True, env=env)
check("phase run without thresholds refused", r.returncode == 2 and "--thresholds-file" in r.stderr, r.stderr)

# ---- REVIEW4 B6: --resume, --fault and --block reach the orchestrator Job's arguments
def job_args(out: str):
    docs = [x for x in yaml.safe_load_all(out) if x]
    jobs = [x for x in docs if x["kind"] == "Job" and x["metadata"]["name"] == "campaign-orchestrator"]
    if len(jobs) != 1:
        return None
    return next(c for c in jobs[0]["spec"]["template"]["spec"]["containers"] if c["name"] == "orchestrator")["args"]


def non_job_docs(out: str) -> list:
    return [x for x in yaml.safe_load_all(out) if x and not (x["kind"] == "Job" and x["metadata"]["name"] == "campaign-orchestrator")]


runargs = ["--phase", "run", "--plan-file", str(pp), "--thresholds-file", str(th), "--versions-tfvars", str(EXAMPLE_TFVARS)]
plain = subprocess.run(base + runargs, capture_output=True, text=True, env=env)
check("phase run without options: args are [run]", job_args(plain.stdout) == ["run"], plain.stderr[-300:])
r = subprocess.run(base + runargs + ["--resume"], capture_output=True, text=True, env=env)
check("--resume reaches the Job: run --resume", r.returncode == 0 and job_args(r.stdout) == ["run", "--resume"], r.stderr[-300:])
check("--resume changes nothing but the Job", non_job_docs(r.stdout) == non_job_docs(plain.stdout))
check("phase run also applies the alert-only watchdog CronJob",
      any(x["kind"] == "CronJob" and x["metadata"]["name"] == "campaign-watchdog" for x in non_job_docs(plain.stdout)))
pilot = dict(plan, campaign_index=0, campaign_id="pilot-ec")
pdir = d / "pilot"
pdir.mkdir()
ppp = pdir / "plan.json"
ppp.write_bytes(canonical(pilot))
pbase = [x if x != "ec-a" else "pilot-ec" for x in base]
prun = ["--phase", "run", "--plan-file", str(ppp), "--thresholds-file", str(th), "--versions-tfvars", str(EXAMPLE_TFVARS)]
r = subprocess.run(pbase + prun + ["--resume", "--fault", "F5", "--block", "1"], capture_output=True, text=True, env=env)
check("pilot: --resume --fault F5 --block 1 reach the Job", r.returncode == 0 and job_args(r.stdout) == ["run", "--resume", "--fault", "F5", "--block", "1"], r.stderr[-300:])
r = subprocess.run(pbase + prun + ["--fault", "F3"], capture_output=True, text=True, env=env)
check("--fault F3 (dropped) refused", r.returncode != 0 and "--fault must be one of" in r.stderr, r.stderr[-300:])
r = subprocess.run(base + runargs + ["--fault", "F5"], capture_output=True, text=True, env=env)
check("--fault outside the pilot refused", r.returncode != 0 and "pilot only" in r.stderr, r.stderr[-300:])
r = subprocess.run(pbase + prun + ["--block", "0"], capture_output=True, text=True, env=env)
check("--block 0 refused", r.returncode == 2, r.stderr[-300:])
r = subprocess.run(pbase + ["--phase", "all", "--plan-file", str(ppp), "--resume"], capture_output=True, text=True, env=env)
check("--resume outside phase run refused", r.returncode == 2 and "need --phase run" in r.stderr, r.stderr[-300:])
sys.exit(1 if bad else 0)
