#!/usr/bin/env bash
# Build the four images this repository owns, for linux/arm64, push them to ECR by immutable tag, and write tag + digest into
# images/images.lock.yaml. Needs Docker with buildx (the daemon must be running) and boto3 for the ECR login.
#
#   scripts/build_images.sh --tag r2-001 [--only orchestrator,f6-injector,flink-jobs,load-generator]
#                           [--orchestrator-src DIR] [--no-push]
#
#   --tag                immutable tag. ECR repositories are IMMUTABLE, so a tag can be used once.
#   --orchestrator-src   directory that CONTAINS the python package `orchestrator/` (default: rebuild/orchestrator, a sibling
#                        of this infra directory). Both the orchestrator image and the f6-injector image are built from it.
#   --no-push            build and load into the local Docker (arm64) for a smoke test; the lock file is not touched
# Base images come from the lock file (digest-pinned): `python scripts/mirror_images.py --resolve` first.
set -euo pipefail
HERE="$(cd "$(dirname "$0")" && pwd)"
INFRA="$(cd "$HERE/.." && pwd)"
REPO_ROOT="$(cd "$INFRA/../.." && pwd)"     # paper directory: contains streaming-app/ and rebuild/
PROJECT="${PROJECT:-streaming-anomaly-detection}"
REGION="${AWS_REGION:-us-east-2}"
PYTHON_BIN="${PYTHON_BIN:-python3}"
TAG=""; ONLY="orchestrator,f6-injector,flink-jobs,load-generator"; ORCH_SRC="$REPO_ROOT/rebuild/orchestrator"; PUSH=1

while [ $# -gt 0 ]; do
  case "$1" in
    --tag) TAG="$2"; shift 2;;
    --only) ONLY="$2"; shift 2;;
    --orchestrator-src) ORCH_SRC="$2"; shift 2;;
    --no-push) PUSH=0; shift;;
    -h|--help) sed -n '2,15p' "$0"; exit 0;;
    *) echo "unknown argument $1" >&2; exit 2;;
  esac
done
[ -n "$TAG" ] || { echo "--tag is required" >&2; exit 2; }
[[ "$TAG" =~ ^[A-Za-z0-9._-]{1,128}$ ]] || { echo "bad tag $TAG" >&2; exit 2; }
command -v docker >/dev/null || { echo "docker not found" >&2; exit 1; }
docker buildx version >/dev/null 2>&1 || { echo "docker buildx not available" >&2; exit 1; }
docker info >/dev/null 2>&1 || { echo "the Docker daemon is not running" >&2; exit 1; }

PYTHON_BASE="$("$PYTHON_BIN" "$HERE/lock_env.py" base python)"
FLINK_BASE="$("$PYTHON_BIN" "$HERE/lock_env.py" base flink)"
GRADLE_BASE="$("$PYTHON_BIN" "$HERE/lock_env.py" base gradle)"

if [ "$PUSH" = 1 ]; then
  ECR_REGISTRY="${ECR_REGISTRY:-$("$PYTHON_BIN" "$HERE/ecr_login.py" --region "$REGION" --registry)}"
  "$PYTHON_BIN" "$HERE/ecr_login.py" --region "$REGION" | docker login --username AWS --password-stdin "$ECR_REGISTRY" >/dev/null
fi

META="$(mktemp -d)"; trap 'rm -rf "$META"' EXIT
COMMON=(--platform linux/arm64 --provenance=false --sbom=false)

build_one() {
  local key="$1" repo="$1" ctx dockerfile
  local -a extra
  case "$key" in
    orchestrator)
      [ -f "$ORCH_SRC/orchestrator/__main__.py" ] || { echo "orchestrator source not found: $ORCH_SRC/orchestrator/__main__.py (use --orchestrator-src)" >&2; return 1; }
      ctx="$INFRA/images/orchestrator"; dockerfile="$ctx/Dockerfile"
      extra=(--build-arg "PYTHON_BASE=$PYTHON_BASE" --build-context "orchestrator_src=$ORCH_SRC");;
    f6-injector)
      # the injector is orchestrator/f6_injector.py from the orchestrator source (rebuild/CONTRACT_CHANGES.md C6)
      [ -f "$ORCH_SRC/orchestrator/f6_injector.py" ] || { echo "injector source not found: $ORCH_SRC/orchestrator/f6_injector.py (use --orchestrator-src)" >&2; return 1; }
      ctx="$INFRA/images/f6-injector"; dockerfile="$ctx/Dockerfile"
      extra=(--build-arg "PYTHON_BASE=$PYTHON_BASE" --build-context "orchestrator_src=$ORCH_SRC");;
    load-generator)
      ctx="$INFRA/images/load-generator"; dockerfile="$ctx/Dockerfile"
      extra=(--build-arg "PYTHON_BASE=$PYTHON_BASE" --build-context "app=$REPO_ROOT/streaming-app");;
    flink-jobs)
      ctx="$INFRA/images/flink-jobs"; dockerfile="$ctx/Dockerfile"
      extra=(--build-arg "GRADLE_BASE=$GRADLE_BASE" --build-arg "FLINK_BASE=$FLINK_BASE" --build-context "flink_src=$REPO_ROOT/streaming-app/flink-jobs");;
    *) echo "unknown image $key" >&2; return 2;;
  esac
  if [ "$PUSH" = 1 ]; then
    local ref="$ECR_REGISTRY/$PROJECT/$repo:$TAG"
    echo "== building and pushing $ref"
    docker buildx build "${COMMON[@]}" "${extra[@]}" -f "$dockerfile" -t "$ref" --push --metadata-file "$META/$key.json" "$ctx"
    local digest
    digest="$(jq -r '."containerimage.digest"' "$META/$key.json")"
    [[ "$digest" =~ ^sha256:[0-9a-f]{64}$ ]] || { echo "no digest in build metadata for $key" >&2; return 1; }
    "$PYTHON_BIN" "$HERE/lock_env.py" set-built "$key" "$TAG" "$digest"
    echo "   $key $TAG $digest"
  else
    echo "== building $key locally (not pushed)"
    docker buildx build "${COMMON[@]}" "${extra[@]}" -f "$dockerfile" -t "local/$key:$TAG" --load "$ctx"
  fi
}

IFS=',' read -r -a WANT <<< "$ONLY"
for k in "${WANT[@]}"; do build_one "$k"; done
if [ "$PUSH" = 1 ]; then
  "$PYTHON_BIN" "$HERE/mirror_images.py" --verify --region "$REGION" --project "$PROJECT" || true
  echo "Commit images/images.lock.yaml now. It is the record of what the clusters run."
fi
