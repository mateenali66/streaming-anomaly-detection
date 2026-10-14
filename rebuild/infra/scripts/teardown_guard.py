#!/usr/bin/env python3
"""The two AWS checks of scripts/pre_destroy.sh. boto3, read-only.

  teardown_guard.py complete --bucket B --campaign ID      exit 0 only if campaign/<ID>/COMPLETE exists, is a JSON object that
                                                           names this campaign, has an accepted status, and is NEWER than every
                                                           other object under campaign/<ID>/
  teardown_guard.py volumes  --campaign ID [--wait 600]    exit 0 only when no EBS volume tagged Campaign=<ID> remains
                                                           (only Scope=pv volumes; node root volumes go with their instances)
There is no override. A marker that is older than a result object means data arrived after the completeness check.

Accepted markers (protocol 11.10, orchestrator complete.py; REVIEW4 M4):
  - the orchestrator's: status COMPLETE, or ABANDONED (written by `orchestrator abandon --confirm`, the author's confirmation),
    with at least one result object under the prefix;
  - a manual marker of scripts/mark_complete.py: kind manual-marker, status ABANDONED, author_confirmed true. A pilot that
    failed before the orchestrator wrote anything has no other object, so an empty prefix is accepted for this kind only.
Anything else is refused: no JSON, another campaign, another status, a manual marker without the author's confirmation.
"""
from __future__ import annotations

import argparse
import json
import sys
import time

import boto3
from botocore.exceptions import ClientError


def check_complete(s3, bucket: str, campaign: str) -> tuple[bool, str]:
    prefix = f"campaign/{campaign}/"
    marker_key = prefix + "COMPLETE"
    newest = None
    marker = None
    count = 0
    for page in s3.get_paginator("list_objects_v2").paginate(Bucket=bucket, Prefix=prefix):
        for o in page.get("Contents", []):
            if o["Key"] == marker_key:
                marker = o
                continue
            count += 1
            if newest is None or o["LastModified"] > newest["LastModified"]:
                newest = o
    if marker is None:
        return False, f"REFUSING TO DESTROY: s3://{bucket}/{marker_key} does not exist"
    try:
        doc = json.loads(s3.get_object(Bucket=bucket, Key=marker_key)["Body"].read())
    except (ClientError, ValueError):
        return False, "REFUSING: the COMPLETE marker is unreadable or not JSON"
    if not isinstance(doc, dict):
        return False, "REFUSING: the COMPLETE marker is not a JSON object"
    if doc.get("campaign_id") != campaign:
        return False, f"REFUSING: COMPLETE names campaign {doc.get('campaign_id')!r}, not {campaign!r}"
    manual = doc.get("kind") == "manual-marker"
    if manual:
        if doc.get("status") != "ABANDONED" or doc.get("author_confirmed") is not True:
            return False, "REFUSING: a manual marker needs status ABANDONED and author_confirmed true (scripts/mark_complete.py)"
    elif doc.get("status") not in ("COMPLETE", "ABANDONED"):
        return False, f"REFUSING: COMPLETE has status {doc.get('status')!r}, not COMPLETE or ABANDONED"
    if count == 0:
        if manual:
            return True, "ok: manual marker on an empty prefix (a pilot stopped before the orchestrator wrote anything)"
        return False, f"REFUSING: the marker exists but there is no result object under s3://{bucket}/{prefix}"
    if marker["LastModified"] <= newest["LastModified"]:
        return False, (f"REFUSING: COMPLETE ({marker['LastModified']}) is not newer than {newest['Key']} ({newest['LastModified']}). "
                       f"Was the orchestrator still running? Stop it (deploy_workload.sh --phase stop) and write the marker again")
    kind = "manual marker" if manual else f"orchestrator marker, status {doc['status']}"
    return True, f"ok: COMPLETE ({kind}) is newer than all {count} result objects"


def volumes_left(ec2, campaign: str) -> list[dict]:
    # Scope=pv: volumes the CSI driver made for PVCs. Node root volumes carry Campaign too (Scope=cluster) but exist until the
    # nodes are deleted, which happens after this guard, so they must not be counted here.
    flt = [{"Name": "tag:Campaign", "Values": [campaign]}, {"Name": "tag:Scope", "Values": ["pv"]}]
    return [v for p in ec2.get_paginator("describe_volumes").paginate(Filters=flt) for v in p["Volumes"] if v["State"] != "deleting"]


def main() -> int:
    ap = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    ap.add_argument("--region", default="us-east-2")
    sub = ap.add_subparsers(dest="cmd", required=True)
    c = sub.add_parser("complete")
    c.add_argument("--bucket", required=True)
    c.add_argument("--campaign", required=True)
    v = sub.add_parser("volumes")
    v.add_argument("--campaign", required=True)
    v.add_argument("--wait", type=int, default=600, help="seconds to wait for volumes to disappear")
    a = ap.parse_args()
    sess = boto3.Session(region_name=a.region)
    if a.cmd == "complete":
        ok, msg = check_complete(sess.client("s3"), a.bucket, a.campaign)
        print(msg)
        return 0 if ok else 1
    ec2 = sess.client("ec2")
    deadline = time.time() + a.wait
    while True:
        left = volumes_left(ec2, a.campaign)
        if not left:
            print(f"ok: no PV volume tagged Campaign={a.campaign} Scope=pv")
            return 0
        if time.time() >= deadline:
            for x in left:
                print(f"STILL THERE: {x['VolumeId']} {x['Size']} GiB {x['State']} tags={ {t['Key']: t['Value'] for t in x.get('Tags', [])} }")
            return 1
        time.sleep(15)


if __name__ == "__main__":
    sys.exit(main())
