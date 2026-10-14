#!/usr/bin/env python3
"""Print an ECR docker password (boto3; the AWS CLI is not used) or the registry host.

  python3 scripts/ecr_login.py | docker login --username AWS --password-stdin "$(python3 scripts/ecr_login.py --registry)"
Credentials come from the environment (AWS_PROFILE etc.), nothing is stored. Read-only apart from the token request.
"""
import argparse
import base64
import sys

import boto3


def main() -> int:
    ap = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    ap.add_argument("--region", default="us-east-2")
    ap.add_argument("--registry", action="store_true", help="print the registry host instead of the password")
    a = ap.parse_args()
    s = boto3.Session(region_name=a.region)
    if a.registry:
        print(f"{s.client('sts').get_caller_identity()['Account']}.dkr.ecr.{a.region}.amazonaws.com")
        return 0
    token = s.client("ecr").get_authorization_token()["authorizationData"][0]["authorizationToken"]
    user, pw = base64.b64decode(token).decode().split(":", 1)
    assert user == "AWS"
    sys.stdout.write(pw)
    return 0


if __name__ == "__main__":
    sys.exit(main())
