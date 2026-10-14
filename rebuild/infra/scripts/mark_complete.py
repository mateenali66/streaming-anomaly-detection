#!/usr/bin/env python3
"""Write campaign/<id>/COMPLETE by hand. ONLY for a pilot or an abandoned campaign whose data you have checked yourself.

A real campaign gets its marker from the orchestrator's verify_complete step, which checks run counts and SHA-256 sums. This script
writes a JSON marker that says it was written by the author, why, and when: kind manual-marker, status ABANDONED,
author_confirmed true, the only manual form scripts/teardown_guard.py accepts. It refuses without --i-verified-the-data,
and it refuses when COMPLETE already exists, so an orchestrator's marker is never replaced by an ABANDONED one (pilot, 14
October 2026: both pilot orchestrators wrote their own COMPLETE).
No user name or address goes into the marker: COMPLETE is part of the deposited prefix.
STOP THE ORCHESTRATOR FIRST (scripts/deploy_workload.sh --phase stop) and let its last manifest sync finish. A manifest upload
after the marker makes the marker older than a result object, and the teardown guard then refuses (REVIEW4 M4).
Needs s3:PutObject on the key (an administrator identity; the cluster roles cannot do this for the exporter, only the orchestrator).
"""
import argparse
import datetime as dt
import json
import sys

import boto3
from botocore.exceptions import ClientError


def write_marker(s3, bucket: str, campaign: str, reason: str) -> int:
    key = f"campaign/{campaign}/COMPLETE"
    try:
        s3.head_object(Bucket=bucket, Key=key)
    except ClientError as e:
        if e.response.get("Error", {}).get("Code") not in ("404", "NoSuchKey", "NotFound"):
            raise
    else:
        print(f"refusing: s3://{bucket}/{key} already exists. Keep it, and fetch the campaign as it is", file=sys.stderr)
        return 3
    body = {"campaign_id": campaign, "kind": "manual-marker", "status": "ABANDONED", "author_confirmed": True,
            "reason": reason, "written_by": "author",
            "written_at": dt.datetime.now(dt.timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ")}
    s3.put_object(Bucket=bucket, Key=key, Body=json.dumps(body).encode(), ContentType="application/json")
    print(f"wrote s3://{bucket}/{key}")
    return 0


def main() -> int:
    ap = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    ap.add_argument("--bucket", required=True)
    ap.add_argument("--campaign", required=True)
    ap.add_argument("--reason", required=True)
    ap.add_argument("--region", default="us-east-2")
    ap.add_argument("--i-verified-the-data", action="store_true", dest="verified")
    a = ap.parse_args()
    if not a.verified:
        print("refusing: pass --i-verified-the-data after you have compared the S3 prefix with the cluster", file=sys.stderr)
        return 2
    return write_marker(boto3.Session(region_name=a.region).client("s3"), a.bucket, a.campaign, a.reason)


if __name__ == "__main__":
    sys.exit(main())
