#!/usr/bin/env bash
# Build the training image, check it, and run training inside it on one multi-GPU machine (protocol 7.3.1). Added on
# 17 October 2026, after registration. Every training process runs in the container. The host only starts it, copies the
# results folder to S3 and sends alerts, because the image holds no AWS tools and the container has no network.
#
#   run_in_image.sh build  --out DIR
#       Builds sad-training:<tag> for linux/amd64 and writes DIR/image.json (image ID, base digest, lock and Dockerfile
#       hashes, the versions recorded in the image, the Docker and driver versions).
#   run_in_image.sh check  --image ID --code DIR --out DIR
#       Runs the pipeline tests and a CUDA check in the image and writes DIR/image-check.log.
#   run_in_image.sh run    --image ID --code DIR --derived DIR --out DIR [--s3-uri URI] [--sns-topic ARN] -- ARGS...
#       Starts training/run_matrix.py ARGS in the image, detached, then syncs DIR to URI every 5 minutes, alerts on a job
#       that failed twice, and on exit alerts with the final counts. Run it under setsid nohup.
#
# --code is a `git archive` of one commit (it holds rebuild/pipeline), mounted read-only. --derived is mounted read-only.
set -euo pipefail
HERE="$(cd "$(dirname "$0")" && pwd)"
TAG="r2-001"; IMAGE=""; CODE=""; DERIVED=""; OUT=""; S3_URI=""; TOPIC=""; REGION="us-east-2"; NAME="sad-training"
cmd="${1:-}"; shift || true
while [ $# -gt 0 ]; do
  case "$1" in
    --tag) TAG="$2"; shift 2;;
    --image) IMAGE="$2"; shift 2;;
    --code) CODE="$2"; shift 2;;
    --derived) DERIVED="$2"; shift 2;;
    --out) OUT="$2"; shift 2;;
    --s3-uri) S3_URI="$2"; shift 2;;
    --sns-topic) TOPIC="$2"; shift 2;;
    --name) NAME="$2"; shift 2;;
    --) shift; break;;
    *) echo "unknown argument $1" >&2; exit 2;;
  esac
done
[ -n "$OUT" ] || { echo "--out is required" >&2; exit 2; }
mkdir -p "$OUT"

notify() {
  [ -n "$TOPIC" ] || return 0
  aws sns publish --region "$REGION" --topic-arn "$TOPIC" --subject "streaming-anomaly-detection training" \
    --message "$1" >/dev/null 2>&1 || true
}

sync_out() {
  [ -n "$S3_URI" ] || return 0
  aws s3 sync --quiet --region "$REGION" "$OUT" "$S3_URI" || echo "$(date -u +%FT%TZ) s3 sync failed" >&2
}

case "$cmd" in
  build)
    docker buildx build --platform linux/amd64 --provenance=false --sbom=false --load -t "sad-training:$TAG" "$HERE"
    id="$(docker image inspect -f '{{.Id}}' "sad-training:$TAG")"
    python3 - "$id" "$TAG" "$HERE" "$OUT/image.json" <<'PY'
import hashlib, json, re, subprocess, sys
image_id, tag, here, out = sys.argv[1:]
sha = lambda p: hashlib.sha256(open(p, "rb").read()).hexdigest()
dockerfile = open(f"{here}/Dockerfile").read()
env = subprocess.run(["docker", "run", "--rm", "--network", "none", image_id, "cat", "/etc/training-env.txt"],
                     capture_output=True, text=True, check=True).stdout
rec = {"image_id": image_id, "tag": f"sad-training:{tag}", "platform": "linux/amd64",
       "base": re.search(r"^FROM (\S+)", dockerfile, re.M).group(1),
       "dockerfile_sha256": sha(f"{here}/Dockerfile"), "requirements_lock_sha256": sha(f"{here}/requirements.lock"),
       "training_env": env.strip().splitlines(),
       "docker": subprocess.run(["docker", "version", "-f", "{{.Server.Version}}"], capture_output=True, text=True).stdout.strip(),
       "nvidia_driver": subprocess.run(["nvidia-smi", "--query-gpu=driver_version", "--format=csv,noheader"],
                                       capture_output=True, text=True).stdout.split()[:1]}
open(out, "w").write(json.dumps(rec, indent=1) + "\n")
print(json.dumps(rec, indent=1))
PY
    ;;
  check)
    [ -n "$IMAGE" ] && [ -d "$CODE/rebuild/pipeline" ] || { echo "--image and --code are required" >&2; exit 2; }
    {
      echo "# $(date -u +%FT%TZ) image $IMAGE"
      docker run --rm --network none --gpus all -v "$CODE:/code:ro" -w /code/rebuild/pipeline "$IMAGE" \
        python -m pytest -q -p no:cacheprovider
      docker run --rm --network none --gpus all "$IMAGE" python - <<'PY'
import torch
print("cuda available", torch.cuda.is_available(), "devices", torch.cuda.device_count(),
      [torch.cuda.get_device_name(i) for i in range(torch.cuda.device_count())])
x = torch.randn(64, 8, device="cuda"); s = x.T @ x / 64 + 1e-3 * torch.eye(8, device="cuda")
print("cuda cholesky finite", bool(torch.isfinite(torch.linalg.cholesky(s)).all()))
PY
    } 2>&1 | tee "$OUT/image-check.log"
    ;;
  run)
    [ -n "$IMAGE" ] && [ -d "$CODE/rebuild/pipeline" ] && [ -d "$DERIVED" ] || { echo "--image, --code and --derived are required" >&2; exit 2; }
    chown 10001:10001 "$OUT"
    docker run -d --name "$NAME" --network none --gpus all --shm-size 16g --user 10001:10001 \
      -v "$CODE:/code:ro" -v "$DERIVED:/data:ro" -v "$OUT:/results" -w /code/rebuild/pipeline "$IMAGE" \
      python -u /code/rebuild/pipeline/training/run_matrix.py --derived /data --out /results "$@" >"$OUT/container.id"
    echo "$(date -u +%FT%TZ) started container $(cut -c1-12 "$OUT/container.id") from image $IMAGE" | tee -a "$OUT/host.log"
    notified=0
    while :; do
      sleep 300
      state="$(docker inspect -f '{{.State.Status}}' "$NAME")"
      failed="$(python3 -c "import json;print(len(json.load(open('$OUT/status.json'))['failed_final']))" 2>/dev/null || echo 0)"
      if [ "$failed" -gt "$notified" ]; then
        notify "$failed training job(s) failed twice, see status.json in $S3_URI"; notified="$failed"
      fi
      if [ "$state" != "running" ]; then
        rc="$(docker inspect -f '{{.State.ExitCode}}' "$NAME")"
        docker logs "$NAME" >"$OUT/container.log" 2>&1 || true
        if [ -f "$OUT/DONE.json" ]; then
          msg="$(python3 -c "import json;s=json.load(open('$OUT/DONE.json'));print(f\"training finished: {s['done']} of {s['total']} jobs done, {len(s['failed_final'])} failed, {s['elapsed_h']} h\")")"
        else
          msg="training container exited with code $rc and no DONE.json"
        fi
        echo "$(date -u +%FT%TZ) $msg" | tee -a "$OUT/host.log"
        sync_out; notify "$msg"; break
      fi
      sync_out
    done
    ;;
  *) sed -n '2,17p' "$0"; exit 2;;
esac
