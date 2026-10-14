#!/usr/bin/env bash
# The one supported way to make a plan and the thresholds for a pilot or a campaign (rebuild/CONTRACT_CHANGES.md C14).
#
#   rebuild/integration/plan_tool.sh plan --workload ecommerce --index 0 --out plans/pilot-ec
#   rebuild/integration/plan_tool.sh thresholds --out plans/thresholds.json
#   rebuild/integration/plan_tool.sh verify-plan --workload ecommerce --index 0 --plan plans/pilot-ec/plan.json --sha <hash>
#
# Why: the plan records numpy_version, and the orchestrator regenerates the plan in its image and compares bytes at every
# start (protocol 1.4.4, 11.3). A plan drawn with any other NumPy is refused, so the plan must be drawn with the image's
# exact NumPy. This script builds (once) a virtual environment with CPython 3.12 and exactly the image's NumPy and PyYAML,
# installed from rebuild/integration/plan-tool.lock with --require-hashes, checks that those pins are still the image's
# (rebuild/infra/images/orchestrator/requirements.in), and runs `python -m orchestrator <command>` from rebuild/orchestrator.
# `orchestrator plan` itself refuses to write a plan with any other NumPy (orchestrator/plan.py IMAGE_NUMPY_VERSION).
#
# Environment: PLAN_TOOL_PYTHON (default python3.12), PLAN_TOOL_VENV (default rebuild/integration/.plan-env). Network is used
# once, to download the two wheels from PyPI. No AWS, no cluster, no Docker.
set -euo pipefail
HERE="$(cd "$(dirname "$0")" && pwd)"
REBUILD="$(cd "$HERE/.." && pwd)"
LOCK="$HERE/plan-tool.lock"
IMAGE_REQ="$REBUILD/infra/images/orchestrator/requirements.in"
PY="${PLAN_TOOL_PYTHON:-python3.12}"
VENV="${PLAN_TOOL_VENV:-$HERE/.plan-env}"

[ $# -ge 1 ] || { sed -n '2,8p' "$0"; exit 2; }
case "$1" in plan|thresholds|verify-plan) ;; *) echo "plan_tool.sh: command must be plan, thresholds or verify-plan" >&2; exit 2;; esac

# The lock must pin exactly the image's NumPy and PyYAML.
pin() { grep -E "^$1==" "$2" | head -1 | cut -d= -f3 | cut -d' ' -f1; }
for pkg in numpy PyYAML; do
  img="$(pin "$pkg" "$IMAGE_REQ")"; lck="$(pin "$pkg" "$LOCK")"
  [ -n "$img" ] && [ "$img" = "$lck" ] || { echo "plan_tool.sh: $pkg is $img in the image but $lck in $LOCK; regenerate the lock" >&2; exit 1; }
done
NUMPY="$(pin numpy "$LOCK")"

"$PY" -c 'import sys; sys.exit(0 if sys.version_info[:2] == (3, 12) else 1)' || { echo "plan_tool.sh: $PY is not CPython 3.12 (the image's Python)" >&2; exit 1; }
if [ ! -x "$VENV/bin/python" ] || ! "$VENV/bin/python" -c "import numpy, yaml, sys; sys.exit(0 if numpy.__version__ == '$NUMPY' else 1)" 2>/dev/null; then
  rm -rf "$VENV"
  "$PY" -m venv "$VENV"
  "$VENV/bin/python" -m pip install --quiet --disable-pip-version-check --no-cache-dir --require-hashes --only-binary=:all: -r "$LOCK"
fi
"$VENV/bin/python" -c "import numpy, yaml; print(f'plan_tool.sh: numpy {numpy.__version__}, PyYAML {yaml.__version__}', flush=True)" >&2
# REVIEW4 M10: --out and --plan name paths relative to the caller's working directory, so they are made absolute before the
# cd into rebuild/orchestrator (both the "--out DIR" and the "--out=DIR" forms).
ARGS=()
while [ $# -gt 0 ]; do
  case "$1" in
    --out|--plan|--config)
      [ $# -ge 2 ] || { echo "plan_tool.sh: $1 needs a value" >&2; exit 2; }
      v="$2"; case "$v" in /*) ;; *) v="$PWD/$v";; esac
      ARGS+=("$1" "$v"); shift 2;;
    --out=*|--plan=*|--config=*)
      k="${1%%=*}"; v="${1#*=}"; case "$v" in /*) ;; *) v="$PWD/$v";; esac
      ARGS+=("$k=$v"); shift;;
    *) ARGS+=("$1"); shift;;
  esac
done
cd "$REBUILD/orchestrator"
exec "$VENV/bin/python" -m orchestrator "${ARGS[@]}"
