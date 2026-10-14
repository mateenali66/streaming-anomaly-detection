#!/usr/bin/env bash
# Everything that can be checked on a laptop with no AWS account and no cluster. Works on COPIES in a temp directory; nothing under
# the repository is changed. Network is used only to download providers, modules and Helm charts.
#
#   scripts/validate_local.sh                      uses `terraform` from PATH
#   TF_BINS="/path/tf-1.10.5 /path/tf-1.16.5" scripts/validate_local.sh
# The committed code needs Terraform >= 1.10 (S3 use_lockfile, cross-variable validation); give only such binaries in TF_BINS.
# Step 1b runs a FULL offline plan of every root with mock providers (`terraform test`, tests/test_tf_mock_plans.sh) and the
# destroy-order check of both guards, so plan-time errors such as REVIEW4 B1 and B2 are caught here from now on.
# TF_PLUGIN_CACHE_DIR is honoured; without it one cache in the temp directory serves every root and version of this run.
# Needs: terraform, helm, kubectl (client only), python3 with PyYAML and boto3 (scripts/requirements.txt), shellcheck (optional).
# shellcheck disable=SC2015
set -u
HERE="$(cd "$(dirname "$0")" && pwd)"
INFRA="$(cd "$HERE/.." && pwd)"
WORK="$(mktemp -d)"; trap 'rm -rf "$WORK"' EXIT
export TF_PLUGIN_CACHE_DIR="${TF_PLUGIN_CACHE_DIR:-$WORK/plugin-cache}"; mkdir -p "$TF_PLUGIN_CACHE_DIR"
PYTHON_BIN="${PYTHON_BIN:-python3}"
TF_BINS="${TF_BINS:-terraform}"
fail=0
step() { printf '\n== %s\n' "$*"; }
ok()   { echo "ok   $*"; }
bad()  { echo "FAIL $*"; fail=1; }

# --- 1. Terraform: fmt on the originals, init -backend=false and validate on copies
for TF in $TF_BINS; do
  ver="$("$TF" version | head -1)"
  step "terraform ($ver)"
  for r in shared network cluster; do
    if (cd "$INFRA/terraform/$r" && "$TF" fmt -check -diff >/dev/null 2>&1); then ok "fmt -check $r"; else bad "fmt -check $r"; fi
    d="$WORK/tf-$(basename "$TF")-$r"; mkdir -p "$d"
    rsync -a --exclude .terraform --exclude '*.tfstate*' "$INFRA/" "$d/infra/"
    case "$ver" in *v1.[0-9].*) find "$d/infra/terraform" -name versions.tf -exec sed -i.bak 's/required_version = ">= 1.10.0"/required_version = ">= 1.5.7"/' {} + ;; esac
    ( cd "$d/infra/terraform/$r" \
      && "$TF" init -backend=false -input=false -no-color >"$d/init.log" 2>&1 \
      && "$TF" validate -no-color >"$d/validate.log" 2>&1 ) && ok "init -backend=false + validate $r" || { bad "init/validate $r"; tail -20 "$d/init.log" "$d/validate.log" 2>/dev/null; }
    if (cd "$d/infra/terraform/$r" && diff -q .terraform.lock.hcl "$INFRA/terraform/$r/.terraform.lock.hcl" >/dev/null); then ok "lock file unchanged by init ($r)"; else bad "lock file differs after init ($r)"; fi
  done
  # --- 1b. full offline plans with mock providers, and the destroy order of both guards
  step "terraform mock plans ($ver)"
  TF="$TF" PYTHON_BIN="$PYTHON_BIN" bash "$INFRA/tests/test_tf_mock_plans.sh" || fail=1
done

# --- 2. Helm: each chart with its values file, plus structural checks on the output
step "helm template"
export HELM_CONFIG_HOME="$WORK/helm/config" HELM_CACHE_HOME="$WORK/helm/cache" HELM_DATA_HOME="$WORK/helm/data"
CH="$WORK/charts"; mkdir -p "$CH" "$WORK/h"
ver() { grep -E "^[[:space:]]+$1[[:space:]]*= \"[0-9.]+\"" "$INFRA/terraform/cluster/variables.tf" | head -1 | grep -o '"[0-9.]*"' | tr -d '"'; }
V_STRIMZI="$(ver strimzi)"; V_FLINK="$(ver flink_operator)"; V_PROM="$(ver kube_prometheus_stack)"; V_CHAOS="$(ver chaos_mesh)"
echo "chart versions from variables.tf: strimzi $V_STRIMZI, flink-operator $V_FLINK, kube-prometheus-stack $V_PROM, chaos-mesh $V_CHAOS"
(cd "$CH" \
  && helm pull oci://quay.io/strimzi-helm/strimzi-kafka-operator --version "$V_STRIMZI" >/dev/null 2>&1 \
  && helm pull "https://archive.apache.org/dist/flink/flink-kubernetes-operator-$V_FLINK/flink-kubernetes-operator-$V_FLINK-helm.tgz" >/dev/null 2>&1 \
  && helm repo add pc https://prometheus-community.github.io/helm-charts >/dev/null 2>&1 && helm pull pc/kube-prometheus-stack --version "$V_PROM" >/dev/null 2>&1 \
  && helm repo add cm https://charts.chaos-mesh.org >/dev/null 2>&1 && helm pull cm/chaos-mesh --version "$V_CHAOS" >/dev/null 2>&1 ) && ok "charts downloaded" || bad "chart download"
RV="$INFRA/k8s/base/helm-values"
rv() { "$PYTHON_BIN" "$HERE/render.py" --validate --campaign-id ec-a --workload ECOMMERCE "$RV/$1-values.yaml" > "$WORK/h/$1-values.yaml"; }
for n in strimzi flink-operator prometheus chaos-mesh; do rv "$n" || bad "render values $n"; done
helm template strimzi "$CH"/strimzi-kafka-operator-*.tgz -n streaming -f "$WORK/h/strimzi-values.yaml" > "$WORK/h/strimzi.yaml" 2>"$WORK/h/e1" && ok "helm template strimzi" || { bad "helm template strimzi"; cat "$WORK/h/e1"; }
helm template flink-operator "$CH"/flink-kubernetes-operator-*.tgz -n streaming -f "$WORK/h/flink-operator-values.yaml" > "$WORK/h/flink.yaml" 2>"$WORK/h/e2" && ok "helm template flink-operator" || { bad "helm template flink-operator"; cat "$WORK/h/e2"; }
helm template prometheus "$CH"/kube-prometheus-stack-*.tgz -n observability -f "$WORK/h/prometheus-values.yaml" > "$WORK/h/prometheus.yaml" 2>"$WORK/h/e3" && ok "helm template kube-prometheus-stack" || { bad "helm template kube-prometheus-stack"; cat "$WORK/h/e3"; }
helm template chaos-mesh "$CH"/chaos-mesh-*.tgz -n chaos-mesh -f "$WORK/h/chaos-mesh-values.yaml" > "$WORK/h/chaos-mesh.yaml" 2>"$WORK/h/e4" && ok "helm template chaos-mesh" || { bad "helm template chaos-mesh"; cat "$WORK/h/e4"; }
"$PYTHON_BIN" "$INFRA/tests/check_helm_output.py" "$WORK/h" || fail=1

# --- 3. kustomize, rendering, YAML parse, policy checks
step "manifests"
"$PYTHON_BIN" "$INFRA/tests/check_manifests.py" || fail=1

# --- 4. unit tests of the helpers
step "unit tests"
"$PYTHON_BIN" "$INFRA/tests/test_jmx_rules.py" || fail=1
for TF in $TF_BINS; do TF="$TF" bash "$INFRA/tests/test_tf_variable_validation.sh" || fail=1; done
bash "$INFRA/tests/test_submit_jobs.sh" || fail=1
bash "$INFRA/tests/test_pre_destroy.sh" || fail=1
PYTHON_BIN="$PYTHON_BIN" bash "$INFRA/tests/test_deploy_run_phase.sh" || fail=1
"$PYTHON_BIN" "$INFRA/tests/test_f6_injector.py" || fail=1
"$PYTHON_BIN" "$INFRA/tests/test_campaign_inputs.py" || fail=1
"$PYTHON_BIN" "$INFRA/tests/test_loadgen_entry.py" || fail=1
"$PYTHON_BIN" "$INFRA/tests/test_aws_helpers.py" || fail=1
"$PYTHON_BIN" "$INFRA/tests/test_mfa_session.py" || fail=1
"$PYTHON_BIN" "$INFRA/tests/test_fetch_campaign.py" || fail=1
"$PYTHON_BIN" "$INFRA/tests/test_watchdog.py" || fail=1
"$PYTHON_BIN" "$INFRA/tests/test_check_flink_jobs.py" || fail=1

# --- 5. shell scripts and Python syntax
step "lint"
if command -v shellcheck >/dev/null; then
  shellcheck "$INFRA"/scripts/*.sh "$INFRA"/tests/*.sh "$INFRA"/images/flink-jobs/submit-jobs.sh "$INFRA"/images/flink-jobs/run-job.sh && ok shellcheck || bad shellcheck
fi
"$PYTHON_BIN" -m py_compile "$INFRA"/scripts/*.py "$INFRA"/images/*/*.py "$INFRA"/tests/*.py "$INFRA"/k8s/base/watchdog/*.py && ok "python compiles" || bad "python compile"
find "$INFRA" -name __pycache__ -prune -exec rm -rf {} + 2>/dev/null

echo; if [ "$fail" = 0 ]; then echo "ALL LOCAL CHECKS PASSED"; else echo "SOME CHECKS FAILED"; fi
exit "$fail"
