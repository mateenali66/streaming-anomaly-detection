#!/usr/bin/env bash
# Apply the Kubernetes objects of one workload to one campaign cluster, in phases, waiting for each to be healthy.
# (Terraform has already installed the operators, Prometheus and Chaos Mesh: terraform/cluster.)
#
#   scripts/deploy_workload.sh --workload ecommerce --campaign-id ec-a \
#       --results-bucket <bucket> --sns-topic-arn <arn> --ecr-registry <registry> --plan-file plan.json [--phase all]
#
# Phases (order): rbac  kafka  flink  submit  load  workload  run   (and `stop`, on its own)
#   all       = rbac kafka flink submit load workload      (everything except the orchestrator)
#   submit    = the flink-submit Job, then scripts/check_flink_jobs.py: every job RUNNING at parallelism 4 with checkpoints
#               every 60 s exactly once, RocksDB, retained on cancel (REVIEW4_LIVE_CONTRACT L1). A mismatch stops here.
#   load      = load generator, verification consumer, verification probes. Needs --plan-file: the load generator's seed is
#               the plan's load_generator_seed (protocol 11.3 step 6), read from the plan, never typed by hand.
#   run       = ConfigMap campaign-config (plan.json, thresholds.json, versions.json, images.lock.yaml) and the orchestrator Job.
#               Needs --plan-file and --thresholds-file. versions.json is made from --versions-tfvars (default
#               terraform/cluster/addon-versions.auto.tfvars, written by scripts/capture_versions.py).
#               It also applies the alert-only watchdog CronJob campaign-watchdog (REVIEW4 C6).
#               --recreate-job deletes an ENDED orchestrator Job first and waits until its pod is gone. It refuses while the Job
#               is still active: stop it first with --phase stop.
#               --resume        start the orchestrator with `run --resume` (after a transient halt, protocol 2.6 item 6)
#               --fault F       pilot only: `run --fault F` (one run type: F0 F1 F2 F4 F5 F6 F7 F8; protocol 1.8 item 6)
#               --block N       pilot only: `run --block N` (one block, counted from 1)
#               These three need --phase run. A Job's arguments cannot change in place, so with an existing Job they also
#               need --recreate-job (REVIEW4 B6).
#   stop      = delete the orchestrator Job with foreground deletion and wait: the orchestrator gets SIGTERM, writes its
#               manifest, and the s3-sync sidecar syncs it one last time. Suspends the watchdog. Use it before writing a
#               manual COMPLETE marker (REVIEW4 M4) and before a deliberate rerun.
# The plan is the committed plan.json of this campaign (rebuild/integration/plan_tool.sh writes it with the image's NumPy).
# Its hash becomes PLAN_SHA256 and is checked against plan.json.sha256 next to it; its campaign_index becomes CAMPAIGN_INDEX
# (rebuild/CONTRACT_CHANGES.md C5 and C12). There is no separate seed option, so a seed cannot disagree with the plan.
# --dry-run renders and prints the YAML (and the ConfigMap) and contacts no cluster.
# The kubeconfig must be the one from scripts/eks_kubeconfig.py for THIS campaign; the script refuses any other context.
set -euo pipefail
HERE="$(cd "$(dirname "$0")" && pwd)"
INFRA="$(cd "$HERE/.." && pwd)"
PYTHON_BIN="${PYTHON_BIN:-python3}"
PROJECT="${PROJECT:-streaming-anomaly-detection}"

WORKLOAD=""; CAMPAIGN=""; BUCKET=""; SNS=""; REGISTRY=""; REGION="${AWS_REGION:-us-east-2}"; PHASE="all"
PLAN=""; THRESHOLDS=""; TFVARS="$INFRA/terraform/cluster/addon-versions.auto.tfvars"; DRY=0; RECREATE=0
RESUME=0; FAULT=""; BLOCK=""
while [ $# -gt 0 ]; do
  case "$1" in
    --workload) WORKLOAD="$2"; shift 2;;
    --campaign-id) CAMPAIGN="$2"; shift 2;;
    --results-bucket) BUCKET="$2"; shift 2;;
    --sns-topic-arn) SNS="$2"; shift 2;;
    --ecr-registry) REGISTRY="$2"; shift 2;;
    --region) REGION="$2"; shift 2;;
    --phase) PHASE="$2"; shift 2;;
    --plan-file) PLAN="$2"; shift 2;;
    --thresholds-file) THRESHOLDS="$2"; shift 2;;
    --versions-tfvars) TFVARS="$2"; shift 2;;
    --recreate-job) RECREATE=1; shift;;
    --resume) RESUME=1; shift;;
    --fault) FAULT="$2"; shift 2;;
    --block) BLOCK="$2"; shift 2;;
    --dry-run) DRY=1; shift;;
    --campaign-seed|--loadgen-seed|--schedule-file)
      echo "$1 is retired: the seeds and the plan come from --plan-file (rebuild/CONTRACT_CHANGES.md C12)" >&2; exit 2;;
    -h|--help) sed -n '2,36p' "$0"; exit 0;;
    *) echo "unknown argument $1" >&2; exit 2;;
  esac
done

case "$WORKLOAD" in ecommerce) WL=ECOMMERCE;; iot) WL=IOT;; *) echo "--workload must be ecommerce or iot (no default)" >&2; exit 2;; esac
[[ "$CAMPAIGN" =~ ^[a-z]{2,5}-[a-z0-9]{1,3}$ ]] || { echo "--campaign-id like ec-a, iot-b, pilot-ec" >&2; exit 2; }
case "$WORKLOAD:$CAMPAIGN" in ecommerce:ec-*|ecommerce:pilot-ec|iot:iot-*|iot:pilot-iot) ;; *) echo "campaign $CAMPAIGN does not belong to workload $WORKLOAD" >&2; exit 2;; esac
if [ "$PHASE" != stop ]; then
  [ -n "$BUCKET" ] && [ -n "$SNS" ] && [ -n "$REGISTRY" ] || { echo "--results-bucket, --sns-topic-arn and --ecr-registry are required" >&2; exit 2; }
fi

PHASES_ALL="rbac kafka flink submit load workload"
if [ "$PHASE" = "all" ]; then PHASE_LIST="$PHASES_ALL"; else PHASE_LIST="$PHASE"; fi
for p in $PHASE_LIST; do case "$p" in rbac|kafka|flink|submit|load|workload|run|stop) ;; *) echo "unknown phase $p" >&2; exit 2;; esac; done
if [ "$RESUME" = 1 ] || [ -n "$FAULT" ] || [ -n "$BLOCK" ]; then
  [ "$PHASE_LIST" = run ] || { echo "--resume, --fault and --block need --phase run" >&2; exit 2; }
fi
if [ -n "$BLOCK" ] && ! [[ "$BLOCK" =~ ^[1-9][0-9]*$ ]]; then echo "--block must be a whole number from 1" >&2; exit 2; fi
ORCH_ARGS=(--campaign-id "$CAMPAIGN")
[ "$RESUME" = 1 ] && ORCH_ARGS+=(--resume)
[ -n "$FAULT" ] && ORCH_ARGS+=(--fault "$FAULT")
[ -n "$BLOCK" ] && ORCH_ARGS+=(--block "$BLOCK")
# kubectl must be within one minor version of the 1.35 server (kubernetes.io/releases/version-skew-policy; REVIEW4 M8)
if [ "$DRY" = 0 ]; then
  KMINOR="$(kubectl version --client -o json 2>/dev/null | "$PYTHON_BIN" -c 'import json,sys; print(int(json.load(sys.stdin)["clientVersion"]["minor"].rstrip("+")))' 2>/dev/null || echo 0)"
  if [ "$KMINOR" -lt 34 ] || [ "$KMINOR" -gt 36 ]; then
    echo "kubectl client minor version is '$KMINOR'; use kubectl 1.34 to 1.36 against EKS 1.35 (PILOT_RUNBOOK.md, tools)" >&2; exit 2
  fi
fi

needs() { [[ " $PHASE_LIST " == *" $1 "* ]]; }
if needs load || needs run; then
  [ -n "$PLAN" ] && [ -f "$PLAN" ] || { echo "phases load and run need --plan-file (the committed plan.json of $CAMPAIGN)" >&2; exit 2; }
fi
if needs run; then
  [ -n "$THRESHOLDS" ] && [ -f "$THRESHOLDS" ] || { echo "phase run needs --thresholds-file" >&2; exit 2; }
  [ -f "$TFVARS" ] || { echo "versions file $TFVARS not found (scripts/capture_versions.py, or --versions-tfvars)" >&2; exit 2; }
fi

# Values from the plan (checked: canonical bytes, campaign, index, hash file). Without a plan only phases that need none run.
PLAN_SHA256="not-needed"; LOADGEN_SEED="none"
case "$CAMPAIGN" in pilot-*) CAMPAIGN_INDEX=0;; *-a) CAMPAIGN_INDEX=1;; *-b) CAMPAIGN_INDEX=2;; *-c) CAMPAIGN_INDEX=3;; *-d) CAMPAIGN_INDEX=4;; *-e) CAMPAIGN_INDEX=5;; *) echo "no campaign index for $CAMPAIGN" >&2; exit 2;; esac
if [ -n "$PLAN" ]; then
  FACTS="$("$PYTHON_BIN" "$HERE/campaign_inputs.py" plan-facts --plan "$PLAN" --campaign-id "$CAMPAIGN" --workload "$WORKLOAD")"
  PLAN_SHA256="$(printf '%s\n' "$FACTS" | sed -n 's/^PLAN_SHA256=//p')"
  CAMPAIGN_INDEX="$(printf '%s\n' "$FACTS" | sed -n 's/^CAMPAIGN_INDEX=//p')"
  LOADGEN_SEED="$(printf '%s\n' "$FACTS" | sed -n 's/^LOADGEN_SEED=//p')"
  echo "plan $PLAN: PLAN_SHA256=$PLAN_SHA256 CAMPAIGN_INDEX=$CAMPAIGN_INDEX LOADGEN_SEED=$LOADGEN_SEED" >&2
fi

render() {
  local extra=()
  [ "$DRY" = 1 ] && extra=(--validate) # dummy tags and digests for an empty lock: a dry run is for looking, never for applying
  kubectl kustomize "$INFRA/k8s/$WORKLOAD" | "$PYTHON_BIN" "$HERE/render.py" "${extra[@]+"${extra[@]}"}" \
    --ecr-registry "$REGISTRY" --campaign-id "$CAMPAIGN" --workload "$WL" --region "$REGION" --results-bucket "$BUCKET" \
    --set "SNS_TOPIC_ARN=$SNS" --set "CAMPAIGN_INDEX=$CAMPAIGN_INDEX" --set "PLAN_SHA256=$PLAN_SHA256" \
    --set "LOADGEN_SEED=$LOADGEN_SEED" - \
    | "$PYTHON_BIN" "$HERE/orchestrator_args.py" "${ORCH_ARGS[@]}"
}
config_map() {
  "$PYTHON_BIN" "$HERE/campaign_inputs.py" configmap --plan "$PLAN" --thresholds "$THRESHOLDS" --tfvars "$TFVARS" \
    --images-lock "$INFRA/images/images.lock.yaml"
}

if [ "$DRY" = 1 ]; then
  RENDERED="$(render)"
  FILTER='import sys, yaml
phase = sys.argv[1]
docs = [d for d in yaml.safe_load_all(sys.stdin) if d and d.get("metadata", {}).get("labels", {}).get("infra.phase") == phase]
print(yaml.safe_dump_all(docs, sort_keys=False), end="")'
  for p in $PHASE_LIST; do
    echo "# ---- phase $p"
    if [ "$p" = run ]; then config_map; echo "---"; fi
    printf '%s\n' "$RENDERED" | "$PYTHON_BIN" -c "$FILTER" "$p"
  done
  exit 0
fi

FULL="${PROJECT}-${CAMPAIGN}"
CTX="$(kubectl config current-context)"
[ "$CTX" = "$FULL" ] || { echo "kubectl context is '$CTX', expected '$FULL'. Use scripts/eks_kubeconfig.py for this campaign." >&2; exit 1; }
K="kubectl --context $FULL -n streaming"
RENDERED=""
[ "$PHASE_LIST" = stop ] || RENDERED="$(render)"
apply_phase() { printf '%s\n' "$RENDERED" | kubectl --context "$FULL" apply -f - -l "infra.phase=$1"; }

for p in $PHASE_LIST; do
  echo "== phase $p"
  case "$p" in
    rbac)
      apply_phase rbac;;
    kafka)
      apply_phase kafka
      $K wait kafka/streaming-kafka --for=condition=Ready --timeout=1200s
      $K wait kafkatopic --all --for=condition=Ready --timeout=300s;;
    flink)
      apply_phase flink
      $K wait flinkdeployment/streaming-flink --for=jsonpath='{.status.jobManagerDeploymentStatus}'=READY --timeout=900s;;
    submit)
      $K delete job flink-submit --ignore-not-found
      apply_phase submit
      $K wait job/flink-submit --for=condition=complete --timeout=900s
      "$PYTHON_BIN" "$HERE/check_flink_jobs.py" --context "$FULL" --workload "$WORKLOAD";;
    load)
      # The load cycle is anchored at an epoch that survives pod restarts. Written once; never changed by a re-run.
      if ! $K get configmap loadgen-epoch >/dev/null 2>&1; then
        $K create configmap loadgen-epoch --from-literal=LOADGEN_EPOCH_UNIX="$(date +%s)"
      fi
      apply_phase load
      $K rollout status deployment/load-generator --timeout=300s
      $K rollout status deployment/verification-consumer --timeout=300s
      $K rollout status deployment/verification-probes --timeout=300s;;
    workload)
      apply_phase workload;;
    run)
      config_map | kubectl --context "$FULL" apply -f -
      if $K get job campaign-orchestrator >/dev/null 2>&1; then
        ACTIVE="$($K get job campaign-orchestrator -o jsonpath='{.status.active}')"
        if [ "$RECREATE" = 1 ]; then
          if [ -n "$ACTIVE" ] && [ "$ACTIVE" != 0 ]; then
            echo "the orchestrator Job is still running; refusing to delete it. Stop it on purpose with --phase stop first." >&2; exit 1
          fi
          $K delete job campaign-orchestrator --cascade=foreground --wait=true --timeout=300s
        elif [ "$RESUME" = 1 ] || [ -n "$FAULT" ] || [ -n "$BLOCK" ]; then
          echo "a Job campaign-orchestrator exists and its arguments cannot change in place; add --recreate-job" >&2; exit 1
        fi
      fi
      apply_phase run
      $K patch cronjob campaign-watchdog --type merge -p '{"spec":{"suspend":false}}' >/dev/null   # undo an earlier --phase stop
      echo "orchestrator started with: $(printf '%s\n' "$RENDERED" | "$PYTHON_BIN" -c 'import sys, yaml
for d in yaml.safe_load_all(sys.stdin):
    if d and d.get("kind") == "Job" and d["metadata"]["name"] == "campaign-orchestrator":
        print(" ".join(next(c for c in d["spec"]["template"]["spec"]["containers"] if c["name"] == "orchestrator")["args"]))')"
      echo "logs: $K logs -f job/campaign-orchestrator -c orchestrator";;
    stop)
      $K patch cronjob campaign-watchdog --type merge -p '{"spec":{"suspend":true}}' 2>/dev/null || true
      $K delete job campaign-orchestrator --cascade=foreground --wait=true --timeout=300s --ignore-not-found
      echo "orchestrator Job gone; its final manifest sync ran on SIGTERM. The watchdog is suspended until the next --phase run.";;
  esac
done
echo "done: phases $PHASE_LIST on $FULL"
