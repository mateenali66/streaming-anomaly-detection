#!/usr/bin/env python3
"""Set the arguments of the orchestrator container in rendered manifests (REVIEW4_OPERATIONS B6). Needs PyYAML.

The orchestrator Job runs `run`. After a halt or a crash it refuses to continue without `--resume` (protocol 2.6 item 6), and the
pilot reruns one fault with `--fault <F>` or one block with `--block <n>` (protocol 1.8 item 6, 2.7 item 7). This filter writes
those options into the Job, so scripts/deploy_workload.sh can start every supported form of `run`:

  render ... | orchestrator_args.py --campaign-id pilot-ec [--resume] [--fault F5] [--block 1]  > manifests.yaml

Only the Job `campaign-orchestrator` changes. Every other document passes through byte for byte. The values are checked here
and again by the orchestrator, which refuses --fault and --block outside the pilot (campaign_index 0).
"""
from __future__ import annotations

import argparse
import re
import sys

import yaml

RUN_TYPES = ("F0", "F1", "F2", "F4", "F5", "F6", "F7", "F8")  # rebuild/orchestrator/orchestrator/ids.py RUN_TYPES
JOB = "campaign-orchestrator"
SEP = re.compile(r"(?m)^---[ \t]*\n")


def build_args(resume: bool, fault: str | None, block: int | None) -> list[str]:
    args = ["run"]
    if resume:
        args.append("--resume")
    if fault is not None:
        args += ["--fault", fault]
    if block is not None:
        args += ["--block", str(block)]
    return args


def check(campaign_id: str, fault: str | None, block: int | None) -> None:
    if (fault is not None or block is not None) and not campaign_id.startswith("pilot-"):
        raise SystemExit("--fault and --block are for the pilot only (protocol 1.8); campaign " + campaign_id)
    if fault is not None and fault not in RUN_TYPES:
        raise SystemExit(f"--fault must be one of {' '.join(RUN_TYPES)}, not {fault!r}")
    if block is not None and block < 1:
        raise SystemExit("--block counts from 1")


def apply(text: str, args: list[str]) -> str:
    chunks = SEP.split(text)
    hits = 0
    out = []
    for chunk in chunks:
        doc = yaml.safe_load(chunk) if chunk.strip() else None
        if isinstance(doc, dict) and doc.get("kind") == "Job" and doc.get("metadata", {}).get("name") == JOB:
            containers = doc["spec"]["template"]["spec"]["containers"]
            main = [c for c in containers if c.get("name") == "orchestrator"]
            if len(main) != 1:
                raise SystemExit("the Job has no single container named orchestrator")
            main[0]["args"] = args
            chunk = yaml.safe_dump(doc, sort_keys=False, default_flow_style=False)
            hits += 1
        out.append(chunk)
    if hits != 1:
        raise SystemExit(f"expected exactly one Job {JOB} in the input, found {hits}")
    return "---\n".join(out)


def main() -> int:
    ap = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    ap.add_argument("--campaign-id", required=True)
    ap.add_argument("--resume", action="store_true")
    ap.add_argument("--fault", default=None)
    ap.add_argument("--block", type=int, default=None)
    a = ap.parse_args()
    check(a.campaign_id, a.fault, a.block)
    sys.stdout.write(apply(sys.stdin.read(), build_args(a.resume, a.fault, a.block)))
    return 0


if __name__ == "__main__":
    sys.exit(main())
