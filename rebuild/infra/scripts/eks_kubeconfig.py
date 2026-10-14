#!/usr/bin/env python3
"""Write a kubeconfig for ONE of this project's EKS clusters (boto3; replaces `aws eks update-kubeconfig`).

Refuses a cluster that does not carry the tags Project=<project> (and Campaign=<id> when --campaign is given), so a mistyped
cluster name can never point kubectl, and therefore pre_destroy.sh, at another team's cluster. The kubeconfig authenticates with
scripts/eks_token.py, so it needs the same Python environment (boto3) when kubectl runs.
Reads one cluster description from AWS; writes only the output file (mode 0600).
"""
from __future__ import annotations

import argparse
import os
import sys
from pathlib import Path

import boto3
import yaml


def build_kubeconfig(cluster: dict, region: str, token_script: Path, python: str, profile: str | None) -> dict:
    name = cluster["name"]
    args = [str(token_script), "--cluster-name", name, "--region", region] + (["--profile", profile] if profile else [])
    return {
        "apiVersion": "v1",
        "kind": "Config",
        "clusters": [{"name": name, "cluster": {"server": cluster["endpoint"], "certificate-authority-data": cluster["certificateAuthority"]["data"]}}],
        "contexts": [{"name": name, "context": {"cluster": name, "user": name}}],
        "current-context": name,
        "users": [{"name": name, "user": {"exec": {"apiVersion": "client.authentication.k8s.io/v1beta1", "command": python, "args": args, "interactiveMode": "Never"}}}],
    }


def check_tags(cluster: dict, project: str, campaign: str | None) -> None:
    tags = cluster.get("tags", {})
    if tags.get("Project") != project:
        raise SystemExit(f"refusing: cluster {cluster['name']} is not tagged Project={project}")
    if campaign and tags.get("Campaign") != campaign:
        raise SystemExit(f"refusing: cluster {cluster['name']} is not tagged Campaign={campaign}")


def main() -> int:
    ap = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    ap.add_argument("--cluster-name", required=True)
    ap.add_argument("--region", default="us-east-2")
    ap.add_argument("--profile", default=None)
    ap.add_argument("--project", default="streaming-anomaly-detection")
    ap.add_argument("--campaign", default=None)
    ap.add_argument("--output", required=True, type=Path)
    a = ap.parse_args()
    eks = boto3.Session(profile_name=a.profile, region_name=a.region).client("eks")
    cluster = eks.describe_cluster(name=a.cluster_name)["cluster"]
    check_tags(cluster, a.project, a.campaign)
    cfg = build_kubeconfig(cluster, a.region, Path(__file__).resolve().parent / "eks_token.py", sys.executable, a.profile)
    fd = os.open(a.output, os.O_WRONLY | os.O_CREAT | os.O_TRUNC, 0o600)
    with os.fdopen(fd, "w") as f:
        yaml.safe_dump(cfg, f, sort_keys=False)
    print(f"wrote {a.output} for {a.cluster_name}")
    return 0


if __name__ == "__main__":
    sys.exit(main())
