#!/usr/bin/env python3
"""Destroy guard of the network root (REVIEW4 M5). boto3, read only. terraform/network runs it before it removes anything.

  network_guard.py --vpc-id vpc-... [--region us-east-2] [--project streaming-anomaly-detection]

Refuses (exit 1) when
  1. an EKS cluster named "<project>-..." still uses the VPC: destroying the network first would cut the NAT and S3 endpoint
     routes of a live campaign before the subnet deletes fail;
  2. a VPC endpoint in the VPC is not tagged Project=<project>: for example the GuardDutyManaged endpoint that GuardDuty
     Runtime Monitoring adds and removes only when Runtime Monitoring is disabled;
  3. a network interface in the VPC belongs to neither the NAT gateway nor this project's endpoints: for example ENIs the VPC
     CNI left behind, which carry no Campaign tag. Each one would make the subnet and VPC deletes hang.
Every call is scoped: EKS clusters are described only when named for the project, and EC2 is filtered on this VPC.
"""
from __future__ import annotations

import argparse
import sys


def tag_map(tags) -> dict[str, str]:
    return {t["Key"]: t["Value"] for t in (tags or [])}


def paged(client, op: str, key: str, **kw):
    for page in client.get_paginator(op).paginate(**kw):
        yield from page.get(key, [])


def problems(eks, ec2, vpc_id: str, project: str) -> list[str]:
    out = []
    for name in paged(eks, "list_clusters", "clusters"):
        if not name.startswith(f"{project}-"):
            continue
        c = eks.describe_cluster(name=name)["cluster"]
        if c.get("resourcesVpcConfig", {}).get("vpcId") == vpc_id:
            out.append(f"EKS cluster {name} ({c.get('status')}) still uses {vpc_id}: destroy that cluster first")
    flt = [{"Name": "vpc-id", "Values": [vpc_id]}]
    ours: set[str] = set()
    for ep in paged(ec2, "describe_vpc_endpoints", "VpcEndpoints", Filters=flt):
        t = tag_map(ep.get("Tags"))
        if t.get("Project") == project:
            ours.update(ep.get("NetworkInterfaceIds", []))
            continue
        why = " (GuardDuty Runtime Monitoring; removed only when Runtime Monitoring is disabled)" if "GuardDutyManaged" in t else ""
        out.append(f"VPC endpoint {ep['VpcEndpointId']} {ep.get('ServiceName', '')} is not this project's{why}")
    for eni in paged(ec2, "describe_network_interfaces", "NetworkInterfaces", Filters=flt):
        if eni.get("InterfaceType") == "nat_gateway" or eni["NetworkInterfaceId"] in ours:
            continue
        out.append(f"network interface {eni['NetworkInterfaceId']} ({eni.get('InterfaceType')}, {eni.get('Status')}, "
                   f"'{eni.get('Description', '')}') would block the subnet delete; check sweep.py, then delete it by hand")
    return out


def main() -> int:
    ap = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    ap.add_argument("--vpc-id", required=True)
    ap.add_argument("--region", default="us-east-2")
    ap.add_argument("--project", default="streaming-anomaly-detection")
    a = ap.parse_args()
    import boto3
    s = boto3.Session(region_name=a.region)
    found = problems(s.client("eks"), s.client("ec2"), a.vpc_id, a.project)
    if found:
        print("REFUSING TO DESTROY THE NETWORK:")
        for f in found:
            print("  " + f)
        return 1
    print(f"ok: no project cluster, foreign endpoint or stray network interface in {a.vpc_id}")
    return 0


if __name__ == "__main__":
    sys.exit(main())
