#!/usr/bin/env python3
"""Delete the PV volumes a campaign left behind after its cluster is gone (REVIEW4 M4 item 1). boto3.

  delete_orphan_volumes.py --campaign ID [--region us-east-2] [--project streaming-anomaly-detection]          (list only)
  delete_orphan_volumes.py --campaign ID --yes                                                                 (delete)

When a first apply fails after a PVC has bound, no teardown guard is in state, and a destroy removes the cluster before the
volume. The volume then bills until someone deletes it. This script deletes ONLY volumes that
  - carry Project=<project>, Campaign=<ID> and Scope=pv (the tags of extraVolumeTags; node root volumes have Scope=cluster),
  - are `available` (attached to nothing),
  - belong to a campaign whose EKS cluster <project>-<ID> no longer exists (while it exists, pre_destroy.sh and the CSI
    driver delete PV volumes, never this script).
No snapshot is taken: these volumes hold Kafka, Prometheus or the manifest volume, never campaign data (that is in S3).
Without --yes it lists and changes nothing. Exit 0 done or nothing to do, 1 refused, 2 bad usage.
"""
from __future__ import annotations

import argparse
import re
import sys

from botocore.exceptions import ClientError


def orphans(ec2, project: str, campaign: str) -> list[dict]:
    flt = [{"Name": "tag:Project", "Values": [project]}, {"Name": "tag:Campaign", "Values": [campaign]},
           {"Name": "tag:Scope", "Values": ["pv"]}, {"Name": "status", "Values": ["available"]}]
    return [v for p in ec2.get_paginator("describe_volumes").paginate(Filters=flt) for v in p["Volumes"]]


def cluster_exists(eks, name: str) -> bool:
    try:
        eks.describe_cluster(name=name)
        return True
    except ClientError as e:
        if e.response.get("Error", {}).get("Code") == "ResourceNotFoundException":
            return False
        raise


def run(ec2, eks, project: str, campaign: str, yes: bool) -> int:
    name = f"{project}-{campaign}"
    if cluster_exists(eks, name):
        print(f"refusing: cluster {name} still exists. Its PV volumes are deleted by pre_destroy.sh while the CSI driver runs")
        return 1
    vols = orphans(ec2, project, campaign)
    if not vols:
        print(f"nothing to do: no available Scope=pv volume of {campaign}")
        return 0
    for v in vols:
        print(f"{'deleting' if yes else 'would delete'} {v['VolumeId']} {v['Size']} GiB created {v.get('CreateTime')}")
        if yes:
            ec2.delete_volume(VolumeId=v["VolumeId"])
    if not yes:
        print("nothing changed; add --yes to delete these volumes")
    return 0


def main() -> int:
    ap = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    ap.add_argument("--campaign", required=True)
    ap.add_argument("--yes", action="store_true")
    ap.add_argument("--region", default="us-east-2")
    ap.add_argument("--profile", default=None)
    ap.add_argument("--project", default="streaming-anomaly-detection")
    a = ap.parse_args()
    if not re.fullmatch(r"[a-z]{2,5}-[a-z0-9]{1,3}", a.campaign):
        print(f"bad campaign id {a.campaign!r}", file=sys.stderr)
        return 2
    import boto3
    s = boto3.Session(profile_name=a.profile, region_name=a.region)
    return run(s.client("ec2"), s.client("eks"), a.project, a.campaign, a.yes)


if __name__ == "__main__":
    sys.exit(main())
