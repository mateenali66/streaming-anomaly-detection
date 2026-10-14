#!/usr/bin/env python3
"""kubectl/Helm/Terraform exec credential plugin for EKS, written with boto3 (replaces `aws eks get-token`).

Prints a client.authentication.k8s.io ExecCredential with a presigned STS GetCallerIdentity token, the same token the AWS CLI makes:
"k8s-aws-v1." + urlsafe base64 of a presigned URL that carries the header x-k8s-aws-id=<cluster>. Signing is local; this script
makes no AWS call. Credentials come from the usual boto3 chain (AWS_PROFILE, environment, SSO cache).
"""
from __future__ import annotations

import argparse
import base64
import datetime as dt
import json
import sys

import boto3
from botocore.signers import RequestSigner

TOKEN_LIFETIME_SEC = 60        # lifetime of the presigned URL
EXPIRES_AFTER_MIN = 14         # the token is accepted for 15 minutes; refresh a minute early


def make_token(cluster_name: str, region: str, profile: str | None = None, session: boto3.Session | None = None) -> str:
    session = session or boto3.Session(profile_name=profile, region_name=region)
    creds = session.get_credentials()
    if creds is None:
        raise SystemExit("no AWS credentials found")
    sts = session.client("sts", region_name=region)
    signer = RequestSigner(sts.meta.service_model.service_id, region, "sts", "v4", creds, session.events)
    params = {
        "method": "GET",
        "url": f"https://sts.{region}.amazonaws.com/?Action=GetCallerIdentity&Version=2011-06-15",
        "body": {},
        "headers": {"x-k8s-aws-id": cluster_name},
        "context": {},
    }
    url = signer.generate_presigned_url(params, region_name=region, expires_in=TOKEN_LIFETIME_SEC, operation_name="")
    return "k8s-aws-v1." + base64.urlsafe_b64encode(url.encode()).decode().rstrip("=")


def exec_credential(token: str, now: dt.datetime | None = None) -> dict:
    now = now or dt.datetime.now(dt.timezone.utc)
    return {
        "apiVersion": "client.authentication.k8s.io/v1beta1",
        "kind": "ExecCredential",
        "spec": {},
        "status": {
            "expirationTimestamp": (now + dt.timedelta(minutes=EXPIRES_AFTER_MIN)).strftime("%Y-%m-%dT%H:%M:%SZ"),
            "token": token,
        },
    }


def main() -> int:
    ap = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    ap.add_argument("--cluster-name", required=True)
    ap.add_argument("--region", required=True)
    ap.add_argument("--profile", default=None)
    a = ap.parse_args()
    json.dump(exec_credential(make_token(a.cluster_name, a.region, a.profile)), sys.stdout)
    return 0


if __name__ == "__main__":
    sys.exit(main())
