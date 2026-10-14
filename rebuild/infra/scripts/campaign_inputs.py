#!/usr/bin/env python3
"""The orchestrator's mounted inputs and the values read from the plan (protocol 11.3, 11.7, 11.11; rebuild/CONTRACT_CHANGES.md
C5, C8 and C12). Standard library only; no cluster and no AWS.

  campaign_inputs.py plan-facts --plan plan.json --campaign-id ec-a --workload ecommerce
      Checks the committed plan and prints shell assignments for scripts/deploy_workload.sh:
        PLAN_SHA256      SHA-256 of the plan bytes (also checked against plan.json.sha256 next to the plan, if present)
        CAMPAIGN_INDEX   the plan's campaign_index (it must match the campaign id, 11.2)
        LOADGEN_SEED     the plan's load_generator_seed (11.3 step 6). The load generator never gets the campaign seed.
        PLAN_SEED        the plan's campaign seed, printed for the operator only; no component reads it, because the
                         orchestrator regenerates the plan from the 1.4.1 rule

  campaign_inputs.py versions --tfvars terraform/cluster/addon-versions.auto.tfvars [--variables terraform/cluster/variables.tf]
                              [--kubernetes-version 1.35]
      Writes run/versions.json content (canonical JSON) from the captured versions file (scripts/capture_versions.py,
      node AMI release from SSM as applied) and the chart pins of variables.tf.

  campaign_inputs.py configmap --plan plan.json --thresholds thresholds.json --tfvars FILE [--images-lock images/images.lock.yaml]
      Prints the ConfigMap campaign-config (keys plan.json, thresholds.json, versions.json, images.lock.yaml) that the
      orchestrator Job mounts at /config as PLAN_PATH, THRESHOLDS_PATH, VERSIONS_FILE and IMAGES_LOCK_FILE.
"""
from __future__ import annotations

import argparse
import hashlib
import json
import re
import sys
from pathlib import Path

HERE = Path(__file__).resolve().parent
INFRA = HERE.parent
CAMPAIGN_RE = re.compile(r"^[a-z]{2,5}-[a-z0-9]{1,3}$")


def campaign_id_for(workload: str, index: int) -> str:
    short = {"ecommerce": "ec", "iot": "iot"}[workload]
    return f"pilot-{short}" if index == 0 else f"{short}-{'abcde'[index - 1]}"


def canonical(obj) -> bytes:
    return json.dumps(obj, sort_keys=True, separators=(",", ":"), ensure_ascii=False, allow_nan=False).encode("utf-8")


def plan_facts(plan_path: Path, campaign_id: str, workload: str) -> dict:
    raw = plan_path.read_bytes()
    try:
        plan = json.loads(raw)
    except json.JSONDecodeError as e:
        raise SystemExit(f"{plan_path}: not JSON ({e})")
    if canonical(plan) != raw:
        raise SystemExit(f"{plan_path}: not the canonical bytes of 11.1.5 (was it edited or re-serialised?)")
    for k in ("workload", "campaign_index", "campaign_id", "seed", "load_generator_seed"):
        if k not in plan:
            raise SystemExit(f"{plan_path}: no {k}")
    if plan["workload"] != workload or plan["campaign_id"] != campaign_id:
        raise SystemExit(f"{plan_path} is the plan of {plan['workload']} {plan['campaign_id']}, not {workload} {campaign_id}")
    if campaign_id_for(workload, int(plan["campaign_index"])) != campaign_id:
        raise SystemExit(f"{plan_path}: campaign_index {plan['campaign_index']} does not belong to {campaign_id} (11.2)")
    sha = hashlib.sha256(raw).hexdigest()
    side = plan_path.with_name(plan_path.name + ".sha256")
    if side.exists() and side.read_text() != sha + "\n":
        raise SystemExit(f"{side} does not hold the hash of {plan_path}")
    lg = plan["load_generator_seed"]
    if not isinstance(lg, int) or isinstance(lg, bool) or not 0 <= lg <= 2147483646:
        raise SystemExit(f"{plan_path}: load_generator_seed {lg!r} out of range")
    return {"PLAN_SHA256": sha, "CAMPAIGN_INDEX": str(plan["campaign_index"]), "LOADGEN_SEED": str(lg),
            "PLAN_SEED": str(plan["seed"])}


def parse_tfvars(path: Path) -> dict:
    text = path.read_text()
    m = re.search(r'^\s*node_ami_release_version\s*=\s*"([^"]+)"', text, re.M)
    if not m:
        raise SystemExit(f"{path}: no node_ami_release_version")
    block = re.search(r"addon_versions\s*=\s*\{(.*?)\}", text, re.S)
    if not block:
        raise SystemExit(f"{path}: no addon_versions map")
    addons = dict(re.findall(r'"([a-z0-9-]+)"\s*=\s*"([^"]+)"', block.group(1)))
    return {"node_ami_release": m.group(1), "addon_versions": addons}


def chart_versions(variables_tf: Path) -> dict:
    text = variables_tf.read_text()
    block = re.search(r'variable\s+"chart_versions"\s*\{(.*?)\n\}', text, re.S)
    if not block:
        raise SystemExit(f"{variables_tf}: no chart_versions variable")
    default = re.search(r"default\s*=\s*\{(.*?)\}", block.group(1), re.S)
    if not default:
        raise SystemExit(f"{variables_tf}: chart_versions has no default")
    return dict(re.findall(r'([a-z_]+)\s*=\s*"([0-9][^"]*)"', default.group(1)))


def versions(tfvars: Path, variables_tf: Path, kubernetes_version: str) -> dict:
    tv = parse_tfvars(tfvars)
    out = {"kubernetes_version": kubernetes_version, "node_ami_release": tv["node_ami_release"],
           "addon_versions": tv["addon_versions"], "chart_versions": chart_versions(variables_tf),
           "source": f"{tfvars.name} (scripts/capture_versions.py: EKS add-on API and the SSM AMI release, as applied)"}
    if not re.match(r"^[0-9]+\.[0-9]+\.[0-9]+-[0-9]{8}$", out["node_ami_release"]):
        raise SystemExit(f"node_ami_release {out['node_ami_release']!r} is not an EKS AMI release version")
    return out


def configmap(plan: Path, thresholds: Path, vers: dict, images_lock: Path) -> str:
    def block(text: str) -> str:
        return "\n".join("    " + line if line else "" for line in text.splitlines())
    th = thresholds.read_bytes()
    try:
        json.loads(th)
    except json.JSONDecodeError as e:
        raise SystemExit(f"{thresholds}: not JSON ({e})")
    if canonical(json.loads(th)) != th:
        raise SystemExit(f"{thresholds}: not canonical JSON (write it with `orchestrator thresholds`)")
    data = {"plan.json": plan.read_text(), "thresholds.json": th.decode("utf-8"),
            "versions.json": canonical(vers).decode("utf-8"), "images.lock.yaml": images_lock.read_text()}
    lines = ["apiVersion: v1", "kind: ConfigMap", "metadata:", "  name: campaign-config", "  namespace: streaming",
             "  labels:", "    infra.phase: run", "data:"]
    for k, v in data.items():
        # Literal block with an explicit indentation indicator, so the mounted file holds exactly these bytes: "|2-" adds
        # no newline to the canonical JSON files, "|2+" keeps a text file's own trailing newlines.
        chomp = "|2+" if v.endswith("\n") else "|2-"
        lines.append(f"  {k}: {chomp}")
        lines.append(block(v))
    return "\n".join(lines) + "\n"


def main() -> int:
    ap = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    sub = ap.add_subparsers(dest="cmd", required=True)
    p = sub.add_parser("plan-facts")
    p.add_argument("--plan", type=Path, required=True)
    p.add_argument("--campaign-id", required=True)
    p.add_argument("--workload", required=True, choices=["ecommerce", "iot"])
    for name in ("versions", "configmap"):
        p = sub.add_parser(name)
        p.add_argument("--tfvars", type=Path, required=True)
        p.add_argument("--variables", type=Path, default=INFRA / "terraform/cluster/variables.tf")
        p.add_argument("--kubernetes-version", default="1.35")
        if name == "configmap":
            p.add_argument("--plan", type=Path, required=True)
            p.add_argument("--thresholds", type=Path, required=True)
            p.add_argument("--images-lock", type=Path, default=INFRA / "images/images.lock.yaml")
    a = ap.parse_args()
    if a.cmd == "plan-facts":
        if not CAMPAIGN_RE.match(a.campaign_id):
            raise SystemExit(f"bad campaign id {a.campaign_id!r}")
        for k, v in plan_facts(a.plan, a.campaign_id, a.workload).items():
            print(f"{k}={v}")
        return 0
    vers = versions(a.tfvars, a.variables, a.kubernetes_version)
    if a.cmd == "versions":
        sys.stdout.write(canonical(vers).decode("utf-8") + "\n")
        return 0
    sys.stdout.write(configmap(a.plan, a.thresholds, vers, a.images_lock))
    return 0


if __name__ == "__main__":
    sys.exit(main())
