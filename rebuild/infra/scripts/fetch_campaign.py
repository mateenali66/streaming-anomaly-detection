#!/usr/bin/env python3
"""Copy one campaign's S3 prefix to the laptop and verify it (REVIEW4_OPERATIONS B7; protocol 7.4, 11.10). boto3, read only.

  fetch_campaign.py --bucket B --campaign ID --out DIR [--region us-east-2] [--profile P]

1. Lists every current object under campaign/<ID>/ and downloads it to DIR/<key relative to the prefix>. DIR is what
   `streamprep --campaign-dir` and `orchestrator gate-offline --store` read. DIR must be empty or absent.
2. Checks each file's size against the listing.
3. If checksums/SHA256SUMS exists: every listed file must exist with that SHA-256, and every downloaded object except
   COMPLETE and SHA256SUMS itself must be listed (11.10, C20).
4. If COMPLETE exists and was written by the orchestrator: campaign_id, status COMPLETE or ABANDONED, checksums_sha256 (the
   hash of SHA256SUMS), manifest_sha256 (manifest/manifest.json), plan_sha256 (plan/plan.json), object_count and total_bytes.
5. A pilot, or a campaign that has not finished, has no COMPLETE (or only a manual marker from mark_complete.py). Then the
   copy cannot be checked against the orchestrator's record: a WARNING says so and the run still exits 0.
Every run writes DIR.LOCAL_SHA256SUMS NEXT TO the directory (never inside it, so the copy holds exactly the S3 layout), with
the hash of every downloaded file, so a later copy can be compared with this one.
Exit 0 verified (or a pilot with the warning), 1 a check failed, 2 bad usage. Nothing in S3 is changed.
"""
from __future__ import annotations

import argparse
import hashlib
import json
import sys
from pathlib import Path, PurePosixPath

CHUNK = 1 << 20


def sha256_file(p: Path) -> str:
    h = hashlib.sha256()
    with p.open("rb") as f:
        for b in iter(lambda: f.read(CHUNK), b""):
            h.update(b)
    return h.hexdigest()


def list_objects(s3, bucket: str, prefix: str) -> list[dict]:
    out = []
    for page in s3.get_paginator("list_objects_v2").paginate(Bucket=bucket, Prefix=prefix):
        out += page.get("Contents", [])
    return out


def safe_relative(key: str, prefix: str) -> str:
    rel = key[len(prefix):]
    parts = PurePosixPath(rel).parts
    if not rel or rel.startswith("/") or any(p in ("..", ".") for p in parts):
        raise SystemExit(f"refusing an object key that would leave the output directory: {key!r}")
    return rel


def download(s3, bucket: str, key: str, dest: Path) -> None:
    dest.parent.mkdir(parents=True, exist_ok=True)
    body = s3.get_object(Bucket=bucket, Key=key)["Body"]
    tmp = dest.with_name(dest.name + ".part")
    with tmp.open("wb") as f:
        for b in iter(lambda: body.read(CHUNK), b""):
            f.write(b)
    tmp.replace(dest)


def local_sums(out: Path) -> Path:
    return out.with_name(out.name + ".LOCAL_SHA256SUMS")


def parse_sums(text: str) -> dict[str, str]:
    sums = {}
    for n, line in enumerate(text.splitlines(), 1):
        if not line.strip():
            continue
        h, sep, rel = line.partition("  ")
        if not sep or len(h) != 64 or any(c not in "0123456789abcdef" for c in h):
            raise ValueError(f"line {n} is not '<sha256>  <key>'")
        sums[rel] = h
    return sums


def verify(out: Path, campaign: str, objects: dict[str, int]) -> tuple[list[str], list[str]]:
    """Returns (failures, warnings). objects: relative key -> size from the listing."""
    fails: list[str] = []
    warns: list[str] = []
    for rel, size in sorted(objects.items()):
        got = (out / rel).stat().st_size
        if got != size:
            fails.append(f"{rel}: {got} bytes on disk, {size} in the listing")
    sums_path = out / "checksums" / "SHA256SUMS"
    sums: dict[str, str] | None = None
    if sums_path.exists():
        try:
            sums = parse_sums(sums_path.read_text())
        except ValueError as e:
            fails.append(f"checksums/SHA256SUMS: {e}")
    if sums is not None:
        for rel, h in sorted(sums.items()):
            if rel not in objects:
                fails.append(f"listed in SHA256SUMS but not in S3: {rel}")
            elif sha256_file(out / rel) != h:
                fails.append(f"SHA-256 differs: {rel}")
        extra = sorted(set(objects) - set(sums) - {"COMPLETE", "checksums/SHA256SUMS"})
        if extra:
            fails.append(f"{len(extra)} object(s) in S3 but not in SHA256SUMS, for example {extra[:3]}")
    complete = out / "COMPLETE"
    marker = None
    if complete.exists():
        try:
            marker = json.loads(complete.read_bytes())
        except ValueError:
            fails.append("COMPLETE is not JSON")
    if marker is None or marker.get("kind") == "manual-marker":
        what = "no COMPLETE marker" if marker is None else "only a manual marker (scripts/mark_complete.py)"
        warns.append(f"{what}: this copy is NOT verified against the orchestrator's record. That is expected for a pilot or a "
                     f"campaign that has not finished. <DIR>.LOCAL_SHA256SUMS lists what was downloaded.")
        if sums is None:
            warns.append("no checksums/SHA256SUMS either: only sizes were checked")
        return fails, warns
    if marker.get("campaign_id") != campaign:
        fails.append(f"COMPLETE names campaign {marker.get('campaign_id')!r}, not {campaign!r}")
    if marker.get("status") not in ("COMPLETE", "ABANDONED"):
        fails.append(f"COMPLETE status {marker.get('status')!r}")
    if sums is None:
        fails.append("COMPLETE exists but checksums/SHA256SUMS does not")
        return fails, warns
    pairs = (("checksums_sha256", "checksums/SHA256SUMS"), ("manifest_sha256", "manifest/manifest.json"), ("plan_sha256", "plan/plan.json"))
    for field, rel in pairs:
        if field not in marker:
            fails.append(f"COMPLETE has no {field}")
        elif rel not in objects:
            fails.append(f"COMPLETE names {field} but {rel} is not in S3")
        elif sha256_file(out / rel) != marker[field]:
            fails.append(f"COMPLETE {field} does not match {rel}")
    if marker.get("object_count") != len(sums):
        fails.append(f"COMPLETE object_count {marker.get('object_count')} but SHA256SUMS lists {len(sums)}")
    total = sum(objects.get(rel, 0) for rel in sums)
    if marker.get("total_bytes") != total:
        fails.append(f"COMPLETE total_bytes {marker.get('total_bytes')} but the listed objects hold {total}")
    if marker.get("status") == "ABANDONED":
        warns.append("COMPLETE says ABANDONED: deposit flagged, never analysed (protocol 2.6 item 8)")
    return fails, warns


def main() -> int:
    ap = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    ap.add_argument("--bucket", required=True)
    ap.add_argument("--campaign", required=True)
    ap.add_argument("--out", required=True, type=Path)
    ap.add_argument("--region", default="us-east-2")
    ap.add_argument("--profile", default=None, help="optional; prefer exporting AWS_PROFILE")
    a = ap.parse_args()
    import re
    if not re.fullmatch(r"[a-z]{2,5}-[a-z0-9]{1,3}", a.campaign):
        print(f"bad campaign id {a.campaign!r}", file=sys.stderr)
        return 2
    if a.out.exists() and any(a.out.iterdir()):
        print(f"{a.out} is not empty; give a new directory", file=sys.stderr)
        return 2
    if local_sums(a.out).exists():
        print(f"{local_sums(a.out)} exists; give a new directory name", file=sys.stderr)
        return 2
    import boto3
    s3 = boto3.Session(profile_name=a.profile, region_name=a.region).client("s3")
    return fetch(s3, a.bucket, a.campaign, a.out)


def fetch(s3, bucket: str, campaign: str, out: Path) -> int:
    prefix = f"campaign/{campaign}/"
    listing = list_objects(s3, bucket, prefix)
    if not listing:
        print(f"nothing under s3://{bucket}/{prefix}")
        return 1
    objects: dict[str, int] = {}
    for o in listing:
        rel = safe_relative(o["Key"], prefix)
        if rel.endswith("/"):
            continue  # a folder placeholder, not data
        download(s3, bucket, o["Key"], out / rel)
        objects[rel] = o["Size"]
    local_sums(out).write_text("".join(f"{sha256_file(out / r)}  {r}\n" for r in sorted(objects)))
    fails, warns = verify(out, campaign, objects)
    print(f"downloaded {len(objects)} objects, {sum(objects.values())} bytes, to {out}")
    for w in warns:
        print("WARNING " + w)
    for f in fails:
        print("FAIL " + f)
    if fails:
        return 1
    print("ok: every check that applies passed" + (" (with the warnings above)" if warns else ""))
    return 0


if __name__ == "__main__":
    sys.exit(main())
