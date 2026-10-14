#!/usr/bin/env bash
# Teardown guard, run by Terraform BEFORE it destroys anything (terraform/cluster/guard.tf, destroy-time provisioner), or by hand:
#
#   scripts/pre_destroy.sh <results-bucket> <campaign-id> <region> <cluster-name>
#
# 1. Completeness gate: campaign/<id>/COMPLETE must exist and be newer than every result object (scripts/teardown_guard.py).
#    There is NO override flag.
# 2. While the CSI driver and the operators still run: delete chaos resources, Flink, Kafka (so Strimzi removes its volumes),
#    LoadBalancer services, then every PVC, and wait until no PV exists.
# 3. Prove it from the AWS side: no PV volume (Campaign=<id>, Scope=pv) remains.
# Any failure stops the destroy. All AWS reads are boto3; the AWS CLI is not used (it does not run on the author's machine).
# Env: PYTHON_BIN (python with boto3), GUARD_PROFILE (optional; otherwise the exported AWS_PROFILE is used).
set -euo pipefail

if [ "$#" -ne 4 ]; then echo "usage: $0 <bucket> <campaign-id> <region> <cluster-name>" >&2; exit 2; fi
BUCKET="$1"; CAMPAIGN="$2"; REGION="$3"; CLUSTER="$4"
PYTHON_BIN="${PYTHON_BIN:-python3}"
[ -z "${GUARD_PROFILE:-}" ] || export AWS_PROFILE="$GUARD_PROFILE"
export AWS_DEFAULT_REGION="$REGION"
HERE="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"

[[ "$CAMPAIGN" =~ ^[a-z]{2,5}-[a-z0-9]{1,3}$ ]] || { echo "bad campaign id '$CAMPAIGN'" >&2; exit 2; }
[[ "$CLUSTER" == *"-$CAMPAIGN" ]] || { echo "cluster '$CLUSTER' does not end with -$CAMPAIGN; refusing" >&2; exit 2; }
command -v kubectl >/dev/null || { echo "kubectl not found" >&2; exit 1; }

echo "== 1/3 completeness gate"
"$PYTHON_BIN" "$HERE/teardown_guard.py" --region "$REGION" complete --bucket "$BUCKET" --campaign "$CAMPAIGN"

echo "== 2/3 kubernetes cleanup on $CLUSTER"
KC="$(mktemp)"; trap 'rm -f "$KC"' EXIT
"$PYTHON_BIN" "$HERE/eks_kubeconfig.py" --cluster-name "$CLUSTER" --region "$REGION" --campaign "$CAMPAIGN" --output "$KC" >/dev/null
export KUBECONFIG="$KC"
CTX="$(kubectl config current-context)"
[ "$CTX" = "$CLUSTER" ] || { echo "kubectl context '$CTX' is not '$CLUSTER'" >&2; exit 1; }
# Fails fast if the API is unreachable. A changed laptop address is the usual cause: allow the new one with
# scripts/eks_api_cidrs.py (REVIEW4 C3), then run the destroy again.
kubectl get ns streaming >/dev/null || { echo "the cluster API is unreachable from here; see scripts/eks_api_cidrs.py" >&2; exit 1; }

kctl() { kubectl --context "$CLUSTER" "$@"; }
# kubectl should be within one minor version of the 1.35 server (REVIEW4 M8). A warning only: a teardown must not stop on it.
KMINOR="$(kctl version --client -o json 2>/dev/null | "$PYTHON_BIN" -c 'import json,sys; print(json.load(sys.stdin)["clientVersion"]["minor"])' 2>/dev/null || echo "?")"
case "$KMINOR" in 34*|35*|36*) ;; *) echo "WARNING: kubectl client minor version $KMINOR is outside 1.34 to 1.36 for an EKS 1.35 server" >&2;; esac

# chaos resources first: nothing may be injecting while we tear down
for kind in podchaos networkchaos stresschaos iochaos; do
  kctl delete "$kind" --all -n streaming --wait=true --timeout=120s --ignore-not-found || true
done
# workloads that own volumes or pods
kctl delete job --all -n streaming --wait=true --timeout=300s --ignore-not-found || true
kctl delete deployment --all -n streaming --wait=true --timeout=300s --ignore-not-found || true
kctl delete flinkdeployment --all -n streaming --wait=true --timeout=600s --ignore-not-found || true
kctl delete kafkatopic --all -n streaming --wait=true --timeout=300s --ignore-not-found || true
kctl delete kafka,kafkanodepool --all -n streaming --wait=true --timeout=900s --ignore-not-found || true
# load balancers (none expected)
kctl get svc -A -o json | "$PYTHON_BIN" -c '
import json, sys
for i in json.load(sys.stdin)["items"]:
    if i["spec"].get("type") == "LoadBalancer":
        print(i["metadata"]["namespace"], i["metadata"]["name"])
' | while read -r ns n; do kctl delete svc -n "$ns" "$n" --wait=true --timeout=300s; done
# every remaining PVC (Prometheus, the manifest volume, the export scratch)
kctl delete prometheus --all -n observability --wait=true --timeout=300s --ignore-not-found || true
kctl delete pvc --all -A --wait=true --timeout=600s || true
for _ in $(seq 1 "${PV_WAIT_TRIES:-60}"); do
  [ "$(kctl get pv --no-headers 2>/dev/null | wc -l | tr -d ' ')" = "0" ] && break
  sleep "${PV_WAIT_SLEEP:-10}"
done
LEFT="$(kctl get pv --no-headers 2>/dev/null | wc -l | tr -d ' ')"
[ "$LEFT" = "0" ] || { echo "$LEFT persistent volumes remain" >&2; kctl get pv >&2; exit 1; }

echo "== 3/3 PV volumes tagged Campaign=$CAMPAIGN Scope=pv"
"$PYTHON_BIN" "$HERE/teardown_guard.py" --region "$REGION" volumes --campaign "$CAMPAIGN" --wait 600
echo "pre-destroy checks passed for $CAMPAIGN"
