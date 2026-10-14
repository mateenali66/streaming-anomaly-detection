#!/usr/bin/env bash
# Offline test of the cluster side of scripts/deploy_workload.sh phases run and stop (REVIEW4 B6, M4 item 4, C6) with a stub
# kubectl. `kubectl kustomize` is passed to the real kubectl (client only); every other call is answered by the stub and logged.
set -u
HERE="$(cd "$(dirname "$0")" && pwd)"
SCRIPT="$HERE/../scripts/deploy_workload.sh"
REAL_KUBECTL="$(command -v kubectl)"
T="$(mktemp -d)"; trap 'rm -rf "$T"' EXIT
fail=0
check() { if [ "$2" = "$3" ]; then echo "ok   $1"; else echo "FAIL $1: got '$2' expected '$3'"; fail=1; fi; }
mkdir -p "$T/bin"
cat > "$T/bin/kubectl" <<STUB
#!/usr/bin/env bash
[ "\$1" = kustomize ] && exec "$REAL_KUBECTL" "\$@"
echo "kubectl \$*" >> "\$STUB_LOG"
case "\$*" in
  *"version --client -o json"*) echo "{\"clientVersion\": {\"major\": \"1\", \"minor\": \"\${STUB_MINOR:-35}\"}}";;
  *"config current-context"*) echo "streaming-anomaly-detection-pilot-ec";;
  *"get job campaign-orchestrator -o jsonpath"*) printf '%s' "\${STUB_ACTIVE:-}";;
  *"get job campaign-orchestrator"*) [ "\${STUB_JOB:-0}" = 1 ] || exit 1;;
  *"apply -f -"*) cat > /dev/null;;
esac
exit 0
STUB
chmod +x "$T/bin/kubectl"
export PATH="$T/bin:$PATH" STUB_LOG="$T/log" PYTHON_BIN="${PYTHON_BIN:-python3}"
canon() { "$PYTHON_BIN" -c 'import json,sys; sys.stdout.write(json.dumps(json.loads(sys.argv[1]),sort_keys=True,separators=(",",":")))' "$1"; }
canon '{"schema_version":"1","workload":"ecommerce","campaign_index":0,"campaign_id":"pilot-ec","seed":"1","load_generator_seed":7,"f6_payload_seed":5,"blocks":[]}' > "$T/plan.json"
canon '{"schema_version":"1"}' > "$T/th.json"
TFV="$HERE/../terraform/cluster/addon-versions.auto.tfvars.example"
RUN=(bash "$SCRIPT" --workload ecommerce --campaign-id pilot-ec --results-bucket b --sns-topic-arn arn:x --ecr-registry r.example
     --plan-file "$T/plan.json" --thresholds-file "$T/th.json" --versions-tfvars "$TFV" --phase run)

# The run phase renders with images.lock.yaml, whose built digests are empty until the images are built. Give the render a
# lock with dummy digests through a copy of the infra tree, so only the phase logic is under test.
rsync -a --exclude .terraform --exclude __pycache__ --exclude '*.tfvars' --exclude backend.hcl "$HERE/../" "$T/infra/"
"$PYTHON_BIN" - "$T/infra/images/images.lock.yaml" <<'PY'
import sys, yaml
p = sys.argv[1]; d = yaml.safe_load(open(p))
for v in d["built"].values():
    v["tag"], v["digest"] = "t", "sha256:" + "b" * 64
open(p, "w").write(yaml.safe_dump(d, sort_keys=False))
PY
RUN[1]="$T/infra/scripts/deploy_workload.sh"

: > "$T/log"; STUB_JOB=1 STUB_ACTIVE=1 "${RUN[@]}" --recreate-job > "$T/out" 2>&1; rc=$?
check "--recreate-job refuses while the Job is active" "$rc" "1"
check "an active Job is never deleted" "$(grep -c 'delete job' "$T/log")" "0"
: > "$T/log"; STUB_JOB=1 STUB_ACTIVE='' "${RUN[@]}" --recreate-job --resume > "$T/out" 2>&1; rc=$?
check "an ended Job is recreated: rc" "$rc" "0"
check "the ended Job is deleted with foreground deletion and a wait" "$(grep -c 'delete job campaign-orchestrator --cascade=foreground --wait=true' "$T/log")" "1"
check "the new Job runs run --resume" "$(grep -c 'orchestrator started with: run --resume' "$T/out")" "1"
check "the watchdog is unsuspended" "$(grep -c 'patch cronjob campaign-watchdog .*"suspend":false' "$T/log")" "1"
: > "$T/log"; STUB_JOB=1 "${RUN[@]}" --resume > "$T/out" 2>&1; rc=$?
check "--resume with an existing Job but no --recreate-job refuses" "$rc" "1"
: > "$T/log"; STUB_JOB=0 "${RUN[@]}" --fault F5 --block 1 > "$T/out" 2>&1; rc=$?
check "first pilot start with --fault F5 --block 1: rc" "$rc" "0"
check "first pilot start: args" "$(grep -c 'orchestrator started with: run --fault F5 --block 1' "$T/out")" "1"
: > "$T/log"; STUB_MINOR=37 "${RUN[@]}" > "$T/out" 2>&1; rc=$?
check "kubectl 1.37 against EKS 1.35 is refused" "$rc" "2"
: > "$T/log"; bash "$T/infra/scripts/deploy_workload.sh" --workload ecommerce --campaign-id pilot-ec --phase stop > "$T/out" 2>&1; rc=$?
check "stop: rc (no bucket, topic or registry needed)" "$rc" "0"
p_line="$(grep -n 'patch cronjob campaign-watchdog' "$T/log" | head -1 | cut -d: -f1)"; d_line="$(grep -n 'delete job campaign-orchestrator --cascade=foreground --wait=true' "$T/log" | head -1 | cut -d: -f1)"
check "stop: watchdog suspended first, then a foreground delete with a wait" "$([ -n "$p_line" ] && [ -n "$d_line" ] && [ "$p_line" -lt "$d_line" ] && echo yes || echo no)" "yes"
exit $fail
