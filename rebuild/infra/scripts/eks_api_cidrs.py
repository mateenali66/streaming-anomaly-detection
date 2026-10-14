#!/usr/bin/env python3
"""Change the CIDRs allowed to reach ONE project cluster's public API endpoint, and wait for EKS to apply it (REVIEW4 C3). boto3.

A campaign runs 69 to 81 hours. If the laptop's public address changes, kubectl, the resume command and pre_destroy.sh can no
longer reach the cluster, and `terraform apply` cannot fix it either, because its refresh goes through the same blocked
endpoint. This script calls eks.update_cluster_config directly, so it needs only the AWS API.

  eks_api_cidrs.py --cluster-name streaming-anomaly-detection-pilot-ec --campaign pilot-ec --cidr 203.0.113.9/32 [--cidr ...]
  eks_api_cidrs.py --cluster-name ... --campaign ... --wait-update <update id>     (keep waiting for an earlier update)

It refuses a cluster without the tags Project=<project> and Campaign=<id>, 0.0.0.0/0, and any range wider than /16 (the same
rules as terraform/cluster/variables.tf). The endpoint's public and private access flags are passed unchanged. It waits at
most --timeout seconds (default 540, inside a 10-minute command limit); if EKS is still working it prints the update id and
exits 3, and `--wait-update <id>` waits again. AFTERWARDS put the same list into api_allowed_cidrs of terraform/cluster/<id>.tfvars,
or the next plan would change it back. Exit 0 applied (or nothing to change), 1 refused or failed, 2 bad usage, 3 still running.
"""
from __future__ import annotations

import argparse
import ipaddress
import sys
import time
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parent))
from eks_kubeconfig import check_tags  # noqa: E402


def check_cidrs(cidrs: list[str]) -> list[str]:
    out = []
    for c in cidrs:
        try:
            net = ipaddress.ip_network(c, strict=True)
        except ValueError as e:
            raise SystemExit(f"bad CIDR {c!r}: {e}")
        if net.version != 4:
            raise SystemExit(f"{c}: IPv4 only")
        if net.prefixlen < 16:
            raise SystemExit(f"{c}: 0.0.0.0/0 and ranges wider than /16 are not allowed in a shared account")
        out.append(str(net))
    if not out:
        raise SystemExit("give at least one --cidr")
    return sorted(set(out))


def wait_update(eks, name: str, update_id: str, timeout_s: float, poll_s: float = 15, sleep=time.sleep, now=time.time) -> str:
    deadline = now() + timeout_s
    while True:
        u = eks.describe_update(name=name, updateId=update_id)["update"]
        status = u["status"]
        if status in ("Successful", "Failed", "Cancelled"):
            if status != "Successful":
                print(f"update {update_id} {status}: {u.get('errors')}")
            return status
        if now() >= deadline:
            return status
        sleep(poll_s)


def run(eks, a) -> int:
    cluster = eks.describe_cluster(name=a.cluster_name)["cluster"]
    check_tags(cluster, a.project, a.campaign)
    if a.wait_update:
        st = wait_update(eks, a.cluster_name, a.wait_update, a.timeout)
        print(f"update {a.wait_update}: {st}")
        return 0 if st == "Successful" else (3 if st == "InProgress" else 1)
    want = check_cidrs(a.cidr)
    vpc = cluster["resourcesVpcConfig"]
    have = sorted(vpc.get("publicAccessCidrs", []))
    print(f"{a.cluster_name}: public endpoint {vpc.get('endpointPublicAccess')}, private endpoint {vpc.get('endpointPrivateAccess')}")
    print(f"  now allowed: {', '.join(have) or 'none'}")
    print(f"  wanted:      {', '.join(want)}")
    if have == want:
        print("nothing to change")
        return 0
    if a.dry_run:
        print("dry run: nothing changed")
        return 0
    upd = eks.update_cluster_config(name=a.cluster_name, resourcesVpcConfig={
        "endpointPublicAccess": bool(vpc.get("endpointPublicAccess", True)),
        "endpointPrivateAccess": bool(vpc.get("endpointPrivateAccess", True)),
        "publicAccessCidrs": want})["update"]
    print(f"update {upd['id']} started ({upd['status']})")
    st = wait_update(eks, a.cluster_name, upd["id"], a.timeout)
    if st == "InProgress":
        print(f"still InProgress after {a.timeout:.0f} s. Wait again with: --wait-update {upd['id']}")
        return 3
    if st != "Successful":
        return 1
    print("applied. Now set the same list in terraform/cluster/<id>.tfvars:")
    print("  api_allowed_cidrs = [" + ", ".join(f'"{c}"' for c in want) + "]")
    return 0


def main() -> int:
    ap = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    ap.add_argument("--cluster-name", required=True)
    ap.add_argument("--campaign", required=True)
    ap.add_argument("--cidr", action="append", default=[])
    ap.add_argument("--wait-update", default=None)
    ap.add_argument("--timeout", type=float, default=540)
    ap.add_argument("--dry-run", action="store_true")
    ap.add_argument("--region", default="us-east-2")
    ap.add_argument("--profile", default=None)
    ap.add_argument("--project", default="streaming-anomaly-detection")
    a = ap.parse_args()
    if bool(a.cidr) == bool(a.wait_update):
        print("give --cidr (one or more) or --wait-update, not both", file=sys.stderr)
        return 2
    import boto3
    return run(boto3.Session(profile_name=a.profile, region_name=a.region).client("eks"), a)


if __name__ == "__main__":
    sys.exit(main())
