#!/usr/bin/env python3
"""Mirror every public image the clusters run into the project's ECR, by digest (REVIEW3 A9 and G).

  --resolve        Read-only, no AWS. For every `mirrored` and `build_bases` entry, ask the upstream registry for the digest of
                   source:tag, check that the image has a linux/arm64 entry, and write the digest into images.lock.yaml.
                   If the lock already holds a digest and the tag now points elsewhere, stop (the tag moved) unless
                   --accept-moved is given. Needs only the network.
  --copy           Copy each mirrored image into ECR with the SAME digest (multi-arch index preserved), using `crane`
                   (go-containerregistry). ECR login comes from boto3 (the AWS CLI is not used). The repositories must already
                   exist (terraform/shared). Skips images whose digest is already in ECR. Add --dry-run to print the plan.
  --verify         Check with boto3 that every locked digest exists in its ECR repository. Read-only.

The copy keeps the upstream index digest, so the digest in the lock is both the upstream and the ECR digest. Nodes are arm64 and
containerd picks the linux/arm64 entry of the index.

Standard library, PyYAML, and boto3 for --copy and --verify. Registry access uses plain HTTPS (anonymous bearer tokens).
"""
from __future__ import annotations

import argparse
import base64
import hashlib
import json
import os
import re
import shutil
import subprocess
import sys
import tempfile
import urllib.error
import urllib.parse
import urllib.request
from pathlib import Path

import yaml

HERE = Path(__file__).resolve().parent
DEFAULT_LOCK = HERE.parent / "images" / "images.lock.yaml"

ACCEPT = ", ".join([
    "application/vnd.oci.image.index.v1+json",
    "application/vnd.docker.distribution.manifest.list.v2+json",
    "application/vnd.oci.image.manifest.v1+json",
    "application/vnd.docker.distribution.manifest.v2+json",
])
INDEX_TYPES = {"application/vnd.oci.image.index.v1+json", "application/vnd.docker.distribution.manifest.list.v2+json"}
MANIFEST_TYPES = {"application/vnd.oci.image.manifest.v1+json", "application/vnd.docker.distribution.manifest.v2+json"}
DIGEST_RE = re.compile(r"^sha256:[0-9a-f]{64}$")


# ----------------------------------------------------------------------------------------- reference parsing
def split_ref(source: str) -> tuple[str, str]:
    """'docker.io/library/python' -> ('registry-1.docker.io', 'library/python')."""
    parts = source.split("/", 1)
    if len(parts) == 2 and ("." in parts[0] or ":" in parts[0] or parts[0] == "localhost"):
        host, path = parts
    else:
        host, path = "docker.io", source
    if host == "docker.io":
        host = "registry-1.docker.io"
        if "/" not in path:
            path = f"library/{path}"
    return host, path


# ----------------------------------------------------------------------------------------- registry client
class Registry:
    def __init__(self) -> None:
        self._tokens: dict[tuple[str, str], str] = {}

    def _open(self, url: str, host: str, path: str, accept: str | None, method: str = "GET"):
        def req(token: str | None):
            r = urllib.request.Request(url, method=method)
            if accept:
                r.add_header("Accept", accept)
            if token:
                r.add_header("Authorization", f"Bearer {token}")
            return urllib.request.urlopen(r, timeout=60)

        token = self._tokens.get((host, path))
        try:
            return req(token)
        except urllib.error.HTTPError as e:
            if e.code != 401:
                raise
            challenge = e.headers.get("WWW-Authenticate", "")
            m = dict(re.findall(r'(\w+)="([^"]*)"', challenge))
            if "realm" not in m:
                raise
            q = {"service": m.get("service", ""), "scope": m.get("scope") or f"repository:{path}:pull"}
            tok_url = m["realm"] + "?" + urllib.parse.urlencode({k: v for k, v in q.items() if v})
            with urllib.request.urlopen(tok_url, timeout=60) as resp:
                body = json.load(resp)
            token = body.get("token") or body.get("access_token")
            self._tokens[(host, path)] = token
            return req(token)

    def manifest(self, host: str, path: str, ref: str) -> tuple[dict, str, str]:
        """Returns (parsed manifest, media type, digest computed from the bytes)."""
        url = f"https://{host}/v2/{path}/manifests/{ref}"
        with self._open(url, host, path, ACCEPT) as resp:
            body = resp.read()
            ctype = resp.headers.get("Content-Type", "").split(";")[0].strip()
            header_digest = resp.headers.get("Docker-Content-Digest")
        digest = "sha256:" + hashlib.sha256(body).hexdigest()
        if header_digest and header_digest != digest:
            raise RuntimeError(f"{host}/{path}@{ref}: registry header digest {header_digest} != computed {digest}")
        doc = json.loads(body)
        return doc, doc.get("mediaType") or ctype, digest

    def blob_json(self, host: str, path: str, digest: str) -> dict:
        url = f"https://{host}/v2/{path}/blobs/{digest}"
        with self._open(url, host, path, None) as resp:
            return json.load(resp)


def resolve(reg: Registry, source: str, tag: str) -> dict:
    """Digest of source:tag plus proof of a linux/arm64 entry."""
    host, path = split_ref(source)
    doc, mtype, digest = reg.manifest(host, path, tag)
    if mtype in INDEX_TYPES or "manifests" in doc:
        arm = [m for m in doc["manifests"]
               if m.get("platform", {}).get("os") == "linux" and m.get("platform", {}).get("architecture") == "arm64"]
        if not arm:
            have = sorted({f"{m.get('platform', {}).get('os')}/{m.get('platform', {}).get('architecture')}" for m in doc["manifests"]})
            raise RuntimeError(f"{source}:{tag} has no linux/arm64 entry (has {have})")
        child = arm[0]["digest"]
        reg.manifest(host, path, child)  # the arm64 child must really be fetchable
        return {"digest": digest, "kind": "index", "arm64_child": child}
    if mtype in MANIFEST_TYPES or "config" in doc:
        cfg = reg.blob_json(host, path, doc["config"]["digest"])
        if cfg.get("architecture") != "arm64" or cfg.get("os") != "linux":
            raise RuntimeError(f"{source}:{tag} is a single-platform image for {cfg.get('os')}/{cfg.get('architecture')}, not linux/arm64")
        return {"digest": digest, "kind": "single", "arm64_child": digest}
    raise RuntimeError(f"{source}:{tag}: unknown manifest type {mtype}")


# ----------------------------------------------------------------------------------------- lock file editing
def _set_digest_text(text: str, section: str, key: str, digest: str) -> str:
    sys.path.insert(0, str(HERE))
    import lock_env  # noqa: PLC0415

    return lock_env.set_field(text, section, key, "digest", digest)


def entries(lock: dict) -> list[tuple[str, str, dict]]:
    out = [("mirrored", m["key"], m) for m in lock["mirrored"]]
    out += [("build_bases", k, v) for k, v in lock["build_bases"].items()]
    return out


def do_resolve(lock_path: Path, accept_moved: bool) -> int:
    lock = yaml.safe_load(lock_path.read_text())
    text = lock_path.read_text()
    reg = Registry()
    bad = 0
    for section, key, e in entries(lock):
        try:
            r = resolve(reg, e["source"], e["tag"])
        except Exception as ex:  # noqa: BLE001
            print(f"FAIL  {section}.{key}: {ex}")
            bad += 1
            continue
        old = e.get("digest") or ""
        if old and old != r["digest"] and not accept_moved:
            print(f"MOVED {section}.{key}: lock has {old}, {e['source']}:{e['tag']} now points to {r['digest']}. Refusing. Use --accept-moved after reading the upstream change.")
            bad += 1
            continue
        if old != r["digest"]:
            text = _set_digest_text(text, section, key, r["digest"])
        print(f"OK    {section}.{key}: {e['source']}:{e['tag']} -> {r['digest']} ({r['kind']}, arm64 {r['arm64_child'][:19]}...)")
    lock_path.write_text(text)
    return 1 if bad else 0


# ----------------------------------------------------------------------------------------- ECR (boto3)
def ecr_client(region: str, profile: str | None):
    import boto3  # noqa: PLC0415

    return boto3.Session(profile_name=profile, region_name=region).client("ecr")


def ecr_has(ecr, repo: str, digest: str) -> bool:
    try:
        ecr.describe_images(repositoryName=repo, imageIds=[{"imageDigest": digest}])
        return True
    except ecr.exceptions.ImageNotFoundException:
        return False


def do_verify(lock_path: Path, project: str, region: str, profile: str | None) -> int:
    lock = yaml.safe_load(lock_path.read_text())
    ecr = ecr_client(region, profile)
    bad = 0
    items = [("mirrored", m["key"], m["repo"], m.get("digest")) for m in lock["mirrored"]]
    items += [("built", k, v["repo"], v.get("digest")) for k, v in lock["built"].items()]
    for section, key, repo, digest in items:
        name = f"{project}/{repo}"
        if not digest or not DIGEST_RE.match(digest):
            print(f"EMPTY   {section}.{key}: no digest in the lock")
            bad += 1
        elif ecr_has(ecr, name, digest):
            print(f"PRESENT {section}.{key}: {name}@{digest[:19]}...")
        else:
            print(f"MISSING {section}.{key}: {name}@{digest}")
            bad += 1
    return 1 if bad else 0


def do_copy(lock_path: Path, project: str, region: str, profile: str | None, dry_run: bool) -> int:
    lock = yaml.safe_load(lock_path.read_text())
    reg = Registry()
    ecr = None if dry_run else ecr_client(region, profile)
    crane = shutil.which("crane")
    if not crane and not dry_run:
        raise SystemExit("crane not found. Install go-containerregistry's crane (for example `brew install crane`) and retry.")
    registry_host = None
    workdir = None
    env = os.environ.copy()
    if not dry_run:
        import boto3  # noqa: PLC0415

        sess = boto3.Session(profile_name=profile, region_name=region)
        account = sess.client("sts").get_caller_identity()["Account"]
        registry_host = f"{account}.dkr.ecr.{region}.amazonaws.com"
        auth = ecr.get_authorization_token()["authorizationData"][0]
        user, pw = base64.b64decode(auth["authorizationToken"]).decode().split(":", 1)
        workdir = tempfile.mkdtemp(prefix="mirror-docker-config-")
        cfg = {"auths": {registry_host: {"auth": base64.b64encode(f"{user}:{pw}".encode()).decode()}}}
        Path(workdir, "config.json").write_text(json.dumps(cfg))
        env["DOCKER_CONFIG"] = workdir
    bad = 0
    try:
        for m in lock["mirrored"]:
            digest = m.get("digest") or ""
            if not DIGEST_RE.match(digest):
                print(f"SKIP  {m['key']}: no digest in the lock. Run --resolve first.")
                bad += 1
                continue
            # The tag must still point at the locked digest, or the lock is stale.
            r = resolve(reg, m["source"], m["tag"])
            if r["digest"] != digest:
                print(f"MOVED {m['key']}: upstream {m['source']}:{m['tag']} is now {r['digest']}, lock has {digest}")
                bad += 1
                continue
            host, path = split_ref(m["source"])
            src = f"{m['source']}@{digest}"
            repo = f"{project}/{m['repo']}"
            dst = f"{registry_host or 'ACCOUNT.dkr.ecr.REGION.amazonaws.com'}/{repo}:{m['tag']}"
            if dry_run:
                print(f"PLAN  crane copy {src} {dst}   (expect digest {digest})")
                continue
            if ecr_has(ecr, repo, digest):
                print(f"HAVE  {m['key']}: {repo}@{digest[:19]}... already in ECR")
                continue
            proc = subprocess.run([crane, "copy", src, dst], env=env, capture_output=True, text=True)
            if proc.returncode != 0:
                print(f"FAIL  {m['key']}: {proc.stderr.strip()}")
                bad += 1
                continue
            if not ecr_has(ecr, repo, digest):
                print(f"FAIL  {m['key']}: copied, but {digest} is not in {repo} (the copy changed the digest)")
                bad += 1
                continue
            print(f"DONE  {m['key']}: {repo}:{m['tag']} @ {digest}")
    finally:
        if workdir:
            shutil.rmtree(workdir, ignore_errors=True)
    return 1 if bad else 0


def main() -> int:
    ap = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    ap.add_argument("--lock", type=Path, default=DEFAULT_LOCK)
    ap.add_argument("--project", default="streaming-anomaly-detection")
    ap.add_argument("--region", default="us-east-2")
    ap.add_argument("--profile", default=None, help="optional; prefer exporting AWS_PROFILE")
    g = ap.add_mutually_exclusive_group(required=True)
    g.add_argument("--resolve", action="store_true")
    g.add_argument("--copy", action="store_true")
    g.add_argument("--verify", action="store_true")
    ap.add_argument("--dry-run", action="store_true", help="with --copy: print the plan, touch nothing")
    ap.add_argument("--accept-moved", action="store_true", help="with --resolve: accept a tag that now points to a new digest")
    a = ap.parse_args()
    if a.resolve:
        return do_resolve(a.lock, a.accept_moved)
    if a.verify:
        return do_verify(a.lock, a.project, a.region, a.profile)
    return do_copy(a.lock, a.project, a.region, a.profile, a.dry_run)


if __name__ == "__main__":
    sys.exit(main())
