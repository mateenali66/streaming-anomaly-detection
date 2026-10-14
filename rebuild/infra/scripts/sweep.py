#!/usr/bin/env python3
"""Read-only sweep: list anything of THIS project that carries a `Campaign` tag and is still there after teardown.

  python3 scripts/sweep.py --campaign ec-a [--region us-east-2] [--project streaming-anomaly-detection]

Exit 0 when nothing is found, 2 when something is. It only calls Describe/List/Get APIs, never a mutating one, and it
only reports; deleting is a person's decision. boto3 (the AWS CLI is not used).

Scope (REVIEW4 M2): a resource counts only with BOTH tags Project=<project> and Campaign (=<id> with --campaign). EC2, Auto
Scaling and the tagging API are filtered on tag:Project server side, EKS clusters are described only when their name starts
with "<project>-", IAM tags are read only for roles and policies named "<project>-...", and load balancers come from the
tagging API, so no other team's resource is described or reported. Without --campaign every campaign of the project is listed.

What it looks at, because the Resource Groups Tagging API misses several of these types:
  EC2 instances, volumes, snapshots, network interfaces, security groups, NAT gateways, addresses, subnets, VPC endpoints,
  internet gateways, VPCs; network interfaces the VPC CNI left behind (no Campaign tag, but cluster.k8s.amazonaws.com/name =
  <project>-<id>: they make the VPC delete hang, REVIEW4 M5); Auto Scaling groups; EKS clusters and node groups; IAM roles and
  policies named like the project; CloudWatch log groups named for the project's clusters; and the tagging API as a catch-all
  (load balancers among others). No IAM OIDC provider is ever created (Pod Identity, enable_irsa = false), so none is read.
Use `--include-shared` to also list resources tagged Scope=shared or Scope=network (the results bucket, ECR, the VPC).
"""
from __future__ import annotations

import argparse
import sys
from dataclasses import dataclass

import boto3
from botocore.exceptions import ClientError


@dataclass(frozen=True)
class Finding:
    kind: str
    ident: str
    campaign: str
    detail: str = ""


def tag_map(tags) -> dict[str, str]:
    return {t["Key"]: t["Value"] for t in (tags or [])}


def keep(tags: dict[str, str], project: str, campaign: str | None, include_shared: bool) -> bool:
    if tags.get("Project") != project or "Campaign" not in tags:
        return False
    if campaign and tags["Campaign"] != campaign:
        return False
    if not include_shared and tags.get("Scope") in ("shared", "network"):
        return False
    return True


def paged(client, op: str, key: str, **kw):
    for page in client.get_paginator(op).paginate(**kw):
        yield from page.get(key, [])


def ec2_findings(ec2, project, campaign, include_shared) -> list[Finding]:
    f: list[Finding] = []
    flt = [{"Name": "tag:Project", "Values": [project]}] + (
        [{"Name": "tag:Campaign", "Values": [campaign]}] if campaign else [{"Name": "tag-key", "Values": ["Campaign"]}])
    def add(kind, items, idkey, tagkey="Tags", extra=lambda i: ""):
        for i in items:
            t = tag_map(i.get(tagkey))
            if keep(t, project, campaign, include_shared):
                f.append(Finding(kind, i[idkey], t["Campaign"], extra(i)))
    add("ec2:instance", [i for r in paged(ec2, "describe_instances", "Reservations", Filters=flt) for i in r["Instances"] if i["State"]["Name"] != "terminated"], "InstanceId", extra=lambda i: i["State"]["Name"])
    add("ec2:volume", paged(ec2, "describe_volumes", "Volumes", Filters=flt), "VolumeId", extra=lambda i: f"{i['Size']} GiB {i['State']}")
    add("ec2:snapshot", paged(ec2, "describe_snapshots", "Snapshots", OwnerIds=["self"], Filters=flt), "SnapshotId", extra=lambda i: f"{i['VolumeSize']} GiB")
    add("ec2:network-interface", paged(ec2, "describe_network_interfaces", "NetworkInterfaces", Filters=flt), "NetworkInterfaceId", "TagSet", lambda i: i.get("Status", ""))
    add("ec2:security-group", paged(ec2, "describe_security_groups", "SecurityGroups", Filters=flt), "GroupId", extra=lambda i: i["GroupName"])
    add("ec2:nat-gateway", [n for n in paged(ec2, "describe_nat_gateways", "NatGateways", Filter=flt) if n["State"] != "deleted"], "NatGatewayId", extra=lambda i: i["State"])
    add("ec2:address", ec2.describe_addresses(Filters=flt)["Addresses"], "AllocationId")
    add("ec2:subnet", paged(ec2, "describe_subnets", "Subnets", Filters=flt), "SubnetId")
    add("ec2:vpc-endpoint", paged(ec2, "describe_vpc_endpoints", "VpcEndpoints", Filters=flt), "VpcEndpointId")
    add("ec2:internet-gateway", paged(ec2, "describe_internet_gateways", "InternetGateways", Filters=flt), "InternetGatewayId")
    add("ec2:vpc", paged(ec2, "describe_vpcs", "Vpcs", Filters=flt), "VpcId")
    # ENIs of the VPC CNI carry no Campaign tag, only the cluster name (REVIEW4 M5)
    cni = [{"Name": "tag:cluster.k8s.amazonaws.com/name", "Values": [f"{project}-{campaign}" if campaign else f"{project}-*"]}]
    for i in paged(ec2, "describe_network_interfaces", "NetworkInterfaces", Filters=cni):
        cname = tag_map(i.get("TagSet")).get("cluster.k8s.amazonaws.com/name", "")
        f.append(Finding("ec2:network-interface(cni)", i["NetworkInterfaceId"], cname[len(project) + 1:], i.get("Status", "")))
    return f


def asg_findings(asg, project, campaign, include_shared) -> list[Finding]:
    out = []
    for g in paged(asg, "describe_auto_scaling_groups", "AutoScalingGroups", Filters=[{"Name": "tag:Project", "Values": [project]}]):
        t = {x["Key"]: x["Value"] for x in g.get("Tags", [])}
        if keep(t, project, campaign, include_shared):
            out.append(Finding("autoscaling:group", g["AutoScalingGroupName"], t["Campaign"], f"{len(g.get('Instances', []))} instances"))
    return out


def eks_findings(eks, project, campaign, include_shared) -> list[Finding]:
    out = []
    for name in paged(eks, "list_clusters", "clusters"):
        if not name.startswith(f"{project}-"):
            continue  # never describe another team's cluster
        try:
            c = eks.describe_cluster(name=name)["cluster"]
        except ClientError:
            continue
        t = c.get("tags", {})
        if keep(t, project, campaign, include_shared):
            out.append(Finding("eks:cluster", name, t["Campaign"], c.get("status", "")))
            for ng in paged(eks, "list_nodegroups", "nodegroups", clusterName=name):
                out.append(Finding("eks:nodegroup", f"{name}/{ng}", t["Campaign"]))
    return out


def iam_findings(iam, project, campaign, include_shared) -> list[Finding]:
    out = []
    for r in paged(iam, "list_roles", "Roles"):
        if not r["RoleName"].startswith(project):
            continue
        t = tag_map(iam.list_role_tags(RoleName=r["RoleName"]).get("Tags"))
        if keep(t, project, campaign, include_shared):
            out.append(Finding("iam:role", r["RoleName"], t["Campaign"]))
    for p in paged(iam, "list_policies", "Policies", Scope="Local"):
        if not p["PolicyName"].startswith(project):
            continue
        t = tag_map(iam.list_policy_tags(PolicyArn=p["Arn"]).get("Tags"))
        if keep(t, project, campaign, include_shared):
            out.append(Finding("iam:policy", p["PolicyName"], t["Campaign"]))
    return out


def log_findings(logs, project, campaign) -> list[Finding]:
    out = []
    for g in paged(logs, "describe_log_groups", "logGroups", logGroupNamePrefix=f"/aws/eks/{project}"):
        out.append(Finding("logs:log-group", g["logGroupName"], campaign or "?", "named for a project cluster"))
    return out


def tagging_findings(rgt, project, campaign, include_shared) -> list[Finding]:
    out = []
    flt = [{"Key": "Project", "Values": [project]}, {"Key": "Campaign", **({"Values": [campaign]} if campaign else {})}]
    for page in rgt.get_paginator("get_resources").paginate(TagFilters=flt):
        for r in page["ResourceTagMappingList"]:
            t = tag_map(r.get("Tags"))
            if keep(t, project, campaign, include_shared):
                out.append(Finding("tagging-api", r["ResourceARN"], t["Campaign"]))
    return out


def sweep(session, project: str, campaign: str | None, include_shared: bool) -> list[Finding]:
    c = session.client
    found = []
    found += ec2_findings(c("ec2"), project, campaign, include_shared)
    found += asg_findings(c("autoscaling"), project, campaign, include_shared)
    found += eks_findings(c("eks"), project, campaign, include_shared)
    found += iam_findings(c("iam"), project, campaign, include_shared)
    found += log_findings(c("logs"), project, campaign)
    found += tagging_findings(c("resourcegroupstaggingapi"), project, campaign, include_shared)
    return sorted(set(found), key=lambda x: (x.campaign, x.kind, x.ident))


def main() -> int:
    ap = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    ap.add_argument("--campaign", default=None, help="only this campaign id")
    ap.add_argument("--region", default="us-east-2")
    ap.add_argument("--project", default="streaming-anomaly-detection")
    ap.add_argument("--profile", default=None, help="optional; prefer exporting AWS_PROFILE")
    ap.add_argument("--include-shared", action="store_true")
    a = ap.parse_args()
    found = sweep(boto3.Session(profile_name=a.profile, region_name=a.region), a.project, a.campaign, a.include_shared)
    if not found:
        print(f"clean: nothing tagged Project={a.project} and Campaign" + (f"={a.campaign}" if a.campaign else "") + f" in {a.region}")
        return 0
    print(f"{len(found)} resource(s) of {a.project} still tagged Campaign" + (f"={a.campaign}" if a.campaign else "") + f" in {a.region}:")
    for x in found:
        print(f"  {x.campaign:<10} {x.kind:<24} {x.ident}  {x.detail}")
    print("Nothing was changed. Delete by hand after checking each one belongs to a finished campaign.")
    return 2


if __name__ == "__main__":
    sys.exit(main())
