#!/usr/bin/env python3
"""Fill ${NAME} placeholders from images.lock.yaml and the command line. Fails on any unknown or empty name.

Replaces envsubst (REVIEW3 H asked for an explicit variable list, and a shell ${VAR:-x} must never be touched). Only
${UPPER_CASE_NAMES} are replaced. A name that is not defined, or defined empty, is an error, so a half-rendered manifest can
never reach kubectl. Names are the ones in lock_env.py plus the --set options.

  render.py --campaign-id ec-a --workload ECOMMERCE --results-bucket B --region us-east-2 \
            --set CAMPAIGN_INDEX=1 --set PLAN_SHA256=<hash> --set LOADGEN_SEED=<plan seed> FILE [FILE ...]   (stdout)
  kubectl kustomize k8s/ecommerce | render.py ... -                                       (stdin)
  --validate   use dummy tags and digests for empty lock entries (offline checks only)
"""
from __future__ import annotations

import argparse
import json
import re
import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parent))
import lock_env  # noqa: E402

PLACEHOLDER = re.compile(r"\$\{([A-Z][A-Z0-9_]*)\}")
# A placeholder that is a whole plain YAML scalar ("key: ${NAME}" or "- ${NAME}", optionally followed by a comment). kustomize
# drops the quotes of `value: "${LOADGEN_SEED}"`, so a plain substitution of 4242 would become a YAML integer, which the
# API server rejects for an env value. Such a placeholder is written as a double-quoted YAML string instead.
WHOLE = re.compile(r"^(?P<head>\s*(?:- )?(?:[A-Za-z0-9_.\-/]+:\s+)?)\$\{(?P<name>[A-Z][A-Z0-9_]*)\}(?P<tail>\s*(?:#.*)?)$")


def main() -> int:
    ap = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    ap.add_argument("--lock", type=Path, default=lock_env.DEFAULT_LOCK)
    ap.add_argument("--ecr-registry", default="")
    ap.add_argument("--project", default="streaming-anomaly-detection")
    ap.add_argument("--campaign-id", default="")
    ap.add_argument("--workload", default="", help="ECOMMERCE or IOT as the pods see it (Terraform uses lower case)")
    ap.add_argument("--region", default="us-east-2")
    ap.add_argument("--results-bucket", default="")
    ap.add_argument("--set", action="append", default=[], metavar="NAME=VALUE")
    ap.add_argument("--validate", action="store_true")
    ap.add_argument("files", nargs="+", help="files, or - for stdin")
    a = ap.parse_args()

    lock = lock_env.load(a.lock)
    registry = a.ecr_registry or ("ACCOUNT.dkr.ecr.REGION.amazonaws.com" if a.validate else "")
    extra = {"CAMPAIGN_ID": a.campaign_id, "WORKLOAD": a.workload, "AWS_REGION": a.region, "RESULTS_BUCKET": a.results_bucket}
    values = lock_env.build_vars(lock, ecr_registry=registry, project=a.project, extra=extra, fill_missing=a.validate)
    for kv in a.set:
        k, _, v = kv.partition("=")
        values[k] = v

    status = 0
    for f in a.files:
        text = sys.stdin.read() if f == "-" else Path(f).read_text()
        missing = sorted({n for n in PLACEHOLDER.findall(text) if not values.get(n)})
        if missing:
            print(f"render.py: {f}: undefined or empty: {', '.join(missing)}", file=sys.stderr)
            status = 1
            continue
        out = []
        for line in text.splitlines(keepends=True):
            body = line.rstrip("\n")
            m = WHOLE.match(body)
            if m and not body.lstrip().startswith("#"):
                out.append(f"{m.group('head')}{json.dumps(values[m.group('name')])}{m.group('tail')}" + line[len(body):])
            else:
                out.append(PLACEHOLDER.sub(lambda mm: values[mm.group(1)], line))
        sys.stdout.write("".join(out))
        if not text.endswith("\n"):
            sys.stdout.write("\n")
        if len(a.files) > 1 and f != a.files[-1]:
            sys.stdout.write("---\n")
    return status


if __name__ == "__main__":
    sys.exit(main())
