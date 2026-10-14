#!/usr/bin/env python3
"""Turn images/images.lock.yaml into template variables, and update the lock file in place.

Variables (K is the image key upper-cased with - replaced by _):
  ECR_REGISTRY ECR_PREFIX CAMPAIGN_ID WORKLOAD AWS_REGION RESULTS_BUCKET  (from the command line)
  TAG_K DIGEST_K (sha256:<hex>) SHA_K (<hex>) PATH_K (<project>/<repo>) REPO_K (<registry>/<project>/<repo>)
  REF_K (<registry>/<project>/<repo>:<tag>@<digest>)

The same names are produced for Terraform templatefile() in terraform/cluster/platform.tf, so one ${NAME} placeholder works in
a Helm values file (Terraform) and in a manifest (envsubst via render.sh). tests/check_template_vars.py guards that.

Usage:
  lock_env.py env     --ecr-registry R --campaign-id ID --workload ecommerce [--fill-missing]   # export lines
  lock_env.py names   [...]                                                                      # '$A $B' list for envsubst
  lock_env.py set-built KEY TAG DIGEST                                                           # edit the lock in place
  lock_env.py set-digest SECTION KEY DIGEST                                                      # mirrored | build_bases
  lock_env.py base KEY                                                                           # build_bases entry as source:tag@digest
Standard library plus PyYAML.
"""
from __future__ import annotations

import argparse
import re
import shlex
import sys
from pathlib import Path

import yaml

DEFAULT_LOCK = Path(__file__).resolve().parent.parent / "images" / "images.lock.yaml"
DIGEST_RE = re.compile(r"^sha256:[0-9a-f]{64}$")
DUMMY_DIGEST = "sha256:" + "0" * 64


def var_key(key: str) -> str:
    return key.upper().replace("-", "_")


def load(path: Path) -> dict:
    return yaml.safe_load(path.read_text())


def all_images(lock: dict) -> dict[str, dict]:
    out: dict[str, dict] = {}
    for k, v in lock["built"].items():
        out[k] = dict(v)
    for m in lock["mirrored"]:
        if m["key"] in out:
            raise SystemExit(f"duplicate image key {m['key']}")
        out[m["key"]] = dict(m)
    return out


def build_vars(lock: dict, *, ecr_registry: str, project: str, extra: dict[str, str], fill_missing: bool) -> dict[str, str]:
    v: dict[str, str] = {"ECR_REGISTRY": ecr_registry, "ECR_PREFIX": project}
    v.update(extra)
    for key, img in all_images(lock).items():
        k = var_key(key)
        tag = img.get("tag") or ("dummy" if fill_missing else "")
        digest = img.get("digest") or (DUMMY_DIGEST if fill_missing else "")
        if digest and not DIGEST_RE.match(digest):
            raise SystemExit(f"{key}: malformed digest {digest!r}")
        path = f"{project}/{img['repo']}"
        v[f"TAG_{k}"] = tag
        v[f"DIGEST_{k}"] = digest
        v[f"SHA_{k}"] = digest.removeprefix("sha256:")
        v[f"PATH_{k}"] = path
        v[f"REPO_{k}"] = f"{ecr_registry}/{path}"
        v[f"REF_{k}"] = f"{ecr_registry}/{path}:{tag}@{digest}"
    return v


def _entry_span(lines: list[str], section: str, key: str) -> tuple[int, int]:
    """Line range [start, end) of one entry inside a top-level section."""
    sec = next((i for i, l in enumerate(lines) if re.match(rf"^{section}:\s*$", l)), None)
    if sec is None:
        raise SystemExit(f"section {section} not found")
    pat = re.compile(rf"^  - key: {re.escape(key)}\s*$") if section == "mirrored" else re.compile(rf"^  {re.escape(key)}:\s*$")
    start = next((i for i in range(sec + 1, len(lines)) if pat.match(lines[i])), None)
    if start is None:
        raise SystemExit(f"{section}.{key} not found")
    end = len(lines)
    for i in range(start + 1, len(lines)):
        l = lines[i]
        if l and not l.startswith(" ") and not l.startswith("#"):
            end = i
            break
        if section == "mirrored" and l.startswith("  - key:"):
            end = i
            break
        if section != "mirrored" and re.match(r"^  \S", l):
            end = i
            break
    return start, end


def set_field(text: str, section: str, key: str, field: str, value: str) -> str:
    lines = text.split("\n")
    start, end = _entry_span(lines, section, key)
    for i in range(start, end):
        m = re.match(rf'^(\s+(?:- )?){field}:\s*(?:"[^"]*"|\S*)(\s*#.*)?$', lines[i])
        if m:
            lines[i] = f'{m.group(1)}{field}: "{value}"{m.group(2) or ""}'
            return "\n".join(lines)
    raise SystemExit(f"{section}.{key}.{field} not found")


def main() -> int:
    ap = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    ap.add_argument("--lock", type=Path, default=DEFAULT_LOCK)
    sub = ap.add_subparsers(dest="cmd", required=True)
    for name in ("env", "names"):
        p = sub.add_parser(name)
        p.add_argument("--ecr-registry", default="ACCOUNT.dkr.ecr.REGION.amazonaws.com")
        p.add_argument("--project", default="streaming-anomaly-detection")
        p.add_argument("--campaign-id", default="")
        p.add_argument("--workload", default="")
        p.add_argument("--aws-region", default="us-east-2")
        p.add_argument("--results-bucket", default="")
        p.add_argument("--fill-missing", action="store_true", help="dummy tag and digest for empty entries (validation only)")
    p = sub.add_parser("base")
    p.add_argument("key")
    p = sub.add_parser("set-built")
    p.add_argument("key"); p.add_argument("tag"); p.add_argument("digest")
    p = sub.add_parser("set-digest")
    p.add_argument("section", choices=["mirrored", "build_bases"]); p.add_argument("key"); p.add_argument("digest")
    a = ap.parse_args()

    if a.cmd in ("env", "names"):
        lock = load(a.lock)
        extra = {"CAMPAIGN_ID": a.campaign_id, "WORKLOAD": a.workload, "AWS_REGION": a.aws_region, "RESULTS_BUCKET": a.results_bucket}
        v = build_vars(lock, ecr_registry=a.ecr_registry, project=a.project, extra=extra, fill_missing=a.fill_missing)
        if a.cmd == "names":
            print(" ".join(f"${{{k}}}" for k in v))
        else:
            for k, val in v.items():
                print(f"export {k}={shlex.quote(val)}")
        return 0

    if a.cmd == "base":
        b = load(a.lock)["build_bases"][a.key]
        if not DIGEST_RE.match(b.get("digest") or ""):
            raise SystemExit(f"build_bases.{a.key} has no digest. Run scripts/mirror_images.py --resolve")
        print(f"{b['source']}:{b['tag']}@{b['digest']}")
        return 0

    if not DIGEST_RE.match(a.digest):
        raise SystemExit(f"digest must look like sha256:<64 hex>, got {a.digest!r}")
    text = a.lock.read_text()
    if a.cmd == "set-built":
        text = set_field(text, "built", a.key, "tag", a.tag)
        text = set_field(text, "built", a.key, "digest", a.digest)
    else:
        text = set_field(text, a.section, a.key, "digest", a.digest)
    a.lock.write_text(text)
    yaml.safe_load(text)  # still valid
    return 0


if __name__ == "__main__":
    sys.exit(main())
