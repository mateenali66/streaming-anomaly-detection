#!/usr/bin/env python3
"""Read-only discovery before the first apply (REVIEW3 section I, step 2; REVIEW4 C5 and M6). boto3. Changes nothing.

  python3 scripts/discover.py [--vpc-cidr 10.123.0.0/16] [--need-vcpu 72] [--clusters 2] [--region us-east-2]

Prints, and checks where it can:
  - VPC CIDRs and the VPC count against the quota (so a /16 can be chosen that overlaps nothing). This reads other teams' VPC
    CIDRs only, because there is no other way to avoid an overlap; Terraform itself never reads them.
  - the zones that offer BOTH r6g.xlarge and r6g.2xlarge, Elastic IP use against the quota;
  - on-demand standard vCPU: the quota (applied value, else the AWS default) and the CURRENT USE from the CloudWatch metric
    AWS/Usage ResourceCount (Service EC2, Resource vCPU, Class Standard/OnDemand), which counts every team's instances
    without listing them. BLOCK when quota minus use is below --need-vcpu (two pilots 72, six campaigns 216, with a reserve 252);
  - the default EBS encryption key. BLOCK when it is a customer-managed key: node launches would need a key grant for the Auto
    Scaling service-linked role and the CSI role kms:CreateGrant, and the code has neither;
  - GuardDuty Runtime Monitoring with automated EKS agent management. DECIDE: it adds an agent DaemonSet to every node, which
    shares CPU with the system under test, and a VPC endpoint that blocks the network destroy (network_guard.py reports it);
  - the gp3 storage quota (TiB) against about 1.6 TiB per cluster (BLOCK when below);
  - whether this session may create IAM roles without a permissions boundary (iam simulate_principal_policy). BLOCK when not;
  - any existing resource named like the project (EKS clusters, ECR repositories).
Each check reads only account-level settings, aggregate usage metrics or this project's names. Exit 0 no BLOCK, 1 a BLOCK,
2 bad usage. DECIDE lines need the author's decision before the pilot.
"""
from __future__ import annotations

import argparse
import datetime as dt
import ipaddress
import sys

INSTANCE_TYPES = ["r6g.xlarge", "r6g.2xlarge"]
VCPU_PER_CLUSTER = 3 * 4 + 3 * 8          # 3 r6g.xlarge + 3 r6g.2xlarge
TIB_PER_CLUSTER = 1.6                     # 6 x 100 GB roots + 3 x 300 Gi Kafka + 100 Gi Prometheus + 10 Gi manifest


def zones_offering_all(ec2, types) -> list[str]:
    zones: dict[str, set[str]] = {}
    for page in ec2.get_paginator("describe_instance_type_offerings").paginate(
            LocationType="availability-zone", Filters=[{"Name": "instance-type", "Values": types}]):
        for o in page["InstanceTypeOfferings"]:
            zones.setdefault(o["Location"], set()).add(o["InstanceType"])
    return sorted(z for z, s in zones.items() if s >= set(types))


def overlaps(candidate: str, existing: list[str]) -> list[str]:
    c = ipaddress.ip_network(candidate)
    return [e for e in existing if c.overlaps(ipaddress.ip_network(e))]


def quota(sq, service, code) -> tuple[float | None, str]:
    """The applied quota, else the AWS default (get_service_quota can fail for a quota that was never changed)."""
    try:
        return sq.get_service_quota(ServiceCode=service, QuotaCode=code)["Quota"]["Value"], "applied"
    except Exception:  # noqa: BLE001
        pass
    try:
        return sq.get_aws_default_service_quota(ServiceCode=service, QuotaCode=code)["Quota"]["Value"], "AWS default"
    except Exception:  # noqa: BLE001
        return None, "unreadable"


def vcpu_in_use(cw, now: dt.datetime | None = None) -> float | None:
    now = now or dt.datetime.now(dt.timezone.utc)
    r = cw.get_metric_statistics(
        Namespace="AWS/Usage", MetricName="ResourceCount",
        Dimensions=[{"Name": "Service", "Value": "EC2"}, {"Name": "Resource", "Value": "vCPU"},
                    {"Name": "Type", "Value": "Resource"}, {"Name": "Class", "Value": "Standard/OnDemand"}],
        StartTime=now - dt.timedelta(hours=1), EndTime=now, Period=300, Statistics=["Maximum"])
    points = r.get("Datapoints", [])
    if not points:
        return None
    return max(points, key=lambda p: p["Timestamp"])["Maximum"]


def vcpu_check(quota_v: float | None, used: float | None, need: int) -> tuple[str, str]:
    if quota_v is None:
        return "BLOCK", f"on-demand standard vCPU quota unreadable; need {need}"
    if used is None:
        return "DECIDE", f"vCPU quota {quota_v:.0f}, current use unknown (no AWS/Usage datapoint in the last hour); need {need}"
    free = quota_v - used
    level = "ok" if free >= need else "BLOCK"
    return level, f"vCPU quota {quota_v:.0f}, in use {used:.0f}, free {free:.0f}, need {need}"


def ebs_key_check(ec2, kms) -> tuple[str, str]:
    enc = ec2.get_ebs_encryption_by_default().get("EbsEncryptionByDefault", False)
    key = ec2.get_ebs_default_kms_key_id().get("KmsKeyId", "")
    if key in ("", "alias/aws/ebs") or key.endswith(":alias/aws/ebs"):
        return "ok", f"default EBS key is the AWS managed key (encryption by default {enc})"
    try:
        manager = kms.describe_key(KeyId=key)["KeyMetadata"].get("KeyManager")
    except Exception:  # noqa: BLE001
        return "DECIDE", "default EBS key is not alias/aws/ebs and its metadata is unreadable: check whether it is customer managed"
    if manager == "AWS":
        return "ok", "default EBS key is AWS managed"
    return "BLOCK", ("default EBS key is CUSTOMER managed: node launches need a key grant for the Auto Scaling service-linked role "
                     "and the EBS CSI role needs kms:CreateGrant. Neither is in this code")


def guardduty_check(gd) -> tuple[str, str]:
    ids = gd.list_detectors().get("DetectorIds", [])
    for d in ids:
        det = gd.get_detector(DetectorId=d)
        for f in det.get("Features", []):
            if f.get("Name") in ("RUNTIME_MONITORING", "EKS_RUNTIME_MONITORING") and f.get("Status") == "ENABLED":
                auto = [c for c in f.get("AdditionalConfiguration", []) if c.get("Name") == "EKS_ADDON_MANAGEMENT" and c.get("Status") == "ENABLED"]
                if auto:
                    return "DECIDE", ("GuardDuty Runtime Monitoring with automated EKS agent management is ON: an agent DaemonSet on every "
                                      "node (shares CPU with the system under test) and a GuardDutyManaged VPC endpoint in the new VPC, "
                                      "which only disabling Runtime Monitoring removes")
                return "DECIDE", "GuardDuty Runtime Monitoring is ON without automated EKS agents: check that no agent is installed"
    return "ok", f"GuardDuty Runtime Monitoring off ({len(ids)} detector(s))"


def storage_check(sq, clusters: int) -> tuple[str, str]:
    q, how = quota(sq, "ebs", "L-7A658B76")
    need = clusters * TIB_PER_CLUSTER
    if q is None:
        return "DECIDE", f"gp3 storage quota unreadable; need about {need:.1f} TiB"
    return ("ok" if q >= need else "BLOCK"), f"gp3 storage quota {q:.0f} TiB ({how}); this needs about {need:.1f} TiB besides other teams' volumes"


def role_arn_of(caller_arn: str, iam) -> str:
    """The IAM principal to simulate: a user ARN as it is, an assumed-role session mapped to its role (with its path)."""
    if ":assumed-role/" in caller_arn:
        role = caller_arn.split(":assumed-role/")[1].split("/")[0]
        return iam.get_role(RoleName=role)["Role"]["Arn"]
    return caller_arn


def iam_check(sts, iam) -> tuple[str, str]:
    try:
        src = role_arn_of(sts.get_caller_identity()["Arn"], iam)
        res = iam.simulate_principal_policy(
            PolicySourceArn=src, ActionNames=["iam:CreateRole", "iam:PutRolePolicy", "iam:TagRole", "eks:CreateCluster"],
            ContextEntries=[{"ContextKeyName": "aws:MultiFactorAuthPresent", "ContextKeyValues": ["true"], "ContextKeyType": "boolean"}])
    except Exception as e:  # noqa: BLE001
        return "DECIDE", f"IAM simulation not possible ({type(e).__name__}); check by hand that roles can be created without a boundary"
    denied = sorted({r["EvalActionName"] for r in res.get("EvaluationResults", []) if r.get("EvalDecision") != "allowed"})
    if denied:
        return "BLOCK", f"this session may not {', '.join(denied)} without conditions (a permissions boundary may be required)"
    return "ok", "this session may create roles, put role policies, tag roles and create EKS clusters"


def main() -> int:
    ap = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    ap.add_argument("--region", default="us-east-2")
    ap.add_argument("--profile", default=None)
    ap.add_argument("--vpc-cidr", default=None)
    ap.add_argument("--project", default="streaming-anomaly-detection")
    ap.add_argument("--clusters", type=int, default=2, help="clusters that will run at the same time (2 pilots, 6 or 7 campaigns)")
    ap.add_argument("--need-vcpu", type=int, default=None, help="default: --clusters x 36")
    a = ap.parse_args()
    need = a.need_vcpu if a.need_vcpu is not None else a.clusters * VCPU_PER_CLUSTER
    import boto3
    s = boto3.Session(profile_name=a.profile, region_name=a.region)
    c = s.client
    ec2, sq = c("ec2"), c("service-quotas")
    results: list[tuple[str, str]] = []

    vpcs = ec2.describe_vpcs()["Vpcs"]
    cidrs = sorted({b["CidrBlock"] for v in vpcs for b in v.get("CidrBlockAssociationSet", [{"CidrBlock": v["CidrBlock"]}])})
    vq, _ = quota(sq, "vpc", "L-F678F1CE")
    results.append(("ok" if vq is None or len(vpcs) < vq else "BLOCK", f"VPCs: {len(vpcs)} (quota {vq}). Existing CIDRs: {', '.join(cidrs)}"))
    if a.vpc_cidr:
        bad = overlaps(a.vpc_cidr, cidrs)
        results.append(("BLOCK" if bad else "ok", f"{a.vpc_cidr}: " + ("OVERLAPS " + ", ".join(bad) if bad else "overlaps nothing")))
    results.append(("ok", f"zones offering {' and '.join(INSTANCE_TYPES)}: {', '.join(zones_offering_all(ec2, INSTANCE_TYPES))}"))
    eips = ec2.describe_addresses()["Addresses"]
    eq, _ = quota(sq, "ec2", "L-0263D0A3")
    results.append(("ok" if eq is None or len(eips) < eq else "BLOCK", f"Elastic IPs in use: {len(eips)} (quota {eq}); the network needs 1"))
    qv, how = quota(sq, "ec2", "L-1216C47A")
    lvl, msg = vcpu_check(qv, vcpu_in_use(c("cloudwatch")), need)
    results.append((lvl, msg + f" ({how} quota; {a.clusters} cluster(s) x {VCPU_PER_CLUSTER})"))
    results.append(ebs_key_check(ec2, c("kms")))
    try:
        results.append(guardduty_check(c("guardduty")))
    except Exception as e:  # noqa: BLE001
        results.append(("DECIDE", f"GuardDuty settings unreadable ({type(e).__name__})"))
    results.append(storage_check(sq, a.clusters))
    results.append(iam_check(c("sts"), c("iam")))
    clusters = [x for x in c("eks").get_paginator("list_clusters").paginate().build_full_result()["clusters"] if x.startswith(a.project)]
    results.append(("ok", f"EKS clusters named {a.project}*: {clusters or 'none'}"))
    repos = [r["repositoryName"] for r in c("ecr").get_paginator("describe_repositories").paginate().build_full_result()["repositories"]
             if r["repositoryName"].startswith(a.project)]
    results.append(("ok", f"ECR repositories named {a.project}/*: {len(repos)}"))
    for lvl, msg in results:
        print(f"{lvl:<6} {msg}")
    blocks = [m for lv, m in results if lv == "BLOCK"]
    decide = [m for lv, m in results if lv == "DECIDE"]
    print(f"\n{len(blocks)} BLOCK, {len(decide)} DECIDE. Nothing was changed.")
    return 1 if blocks else 0


if __name__ == "__main__":
    sys.exit(main())
