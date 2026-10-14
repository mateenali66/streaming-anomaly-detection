#!/usr/bin/env bash
# Offline test of scripts/pre_destroy.sh with stub kubectl and a stub Python that stands in for the two boto3 helpers.
# Proves the ORDER and the refusals; it cannot prove that real chaos, Strimzi or CSI behave (the pilot dry-run does that).
set -u
HERE="$(cd "$(dirname "$0")" && pwd)"
SCRIPT="$HERE/../scripts/pre_destroy.sh"
T="$(mktemp -d)"; trap 'rm -rf "$T"' EXIT
fail=0
check() { if [ "$2" = "$3" ]; then echo "ok   $1"; else echo "FAIL $1: got '$2' expected '$3'"; fail=1; fi; }

cat > "$T/pystub" <<'STUB'
#!/usr/bin/env bash
case "$1" in
  *teardown_guard.py)
    echo "guard $*" >> "$STUB_LOG"
    case "$*" in *" complete "*) exit "${STUB_COMPLETE_RC:-0}";; *) exit "${STUB_VOLUMES_RC:-0}";; esac;;
  *eks_kubeconfig.py)
    echo "kubeconfig $*" >> "$STUB_LOG"
    while [ $# -gt 0 ]; do [ "$1" = "--output" ] && echo "stub" > "$2"; shift; done; exit 0;;
  *) exec python3 "$@";;
esac
STUB
mkdir -p "$T/bin"
cat > "$T/bin/kubectl" <<'STUB'
#!/usr/bin/env bash
echo "kubectl $*" >> "$STUB_LOG"
case "$*" in
  *"config current-context"*) echo "${STUB_CTX:-streaming-anomaly-detection-ec-a}";;
  *"get pv --no-headers"*) [ "${STUB_PV_LEFT:-0}" = 1 ] && echo "pv-1 300Gi";;
  *"get svc -A -o json"*) echo '{"items": []}';;
esac
exit 0
STUB
chmod +x "$T/pystub" "$T/bin/kubectl"
export PATH="$T/bin:$PATH" PYTHON_BIN="$T/pystub" STUB_LOG="$T/log" PV_WAIT_TRIES=2 PV_WAIT_SLEEP=0
C=streaming-anomaly-detection-ec-a

: > "$T/log"; STUB_COMPLETE_RC=1 bash "$SCRIPT" bucket ec-a us-east-2 "$C" > /dev/null 2>&1; rc=$?
check "no COMPLETE: refuses" "$rc" "1"
check "no COMPLETE: nothing touched in Kubernetes" "$(grep -c '^kubectl' "$T/log")" "0"

: > "$T/log"; bash "$SCRIPT" bucket ec-a us-east-2 streaming-anomaly-detection-iot-a > /dev/null 2>&1; rc=$?
check "cluster name not ending in the campaign id: refuses" "$rc" "2"

: > "$T/log"; bash "$SCRIPT" bucket 'ec a' us-east-2 "$C" > /dev/null 2>&1; rc=$?
check "bad campaign id: refuses" "$rc" "2"

: > "$T/log"; STUB_CTX=other-cluster bash "$SCRIPT" bucket ec-a us-east-2 "$C" > /dev/null 2>&1; rc=$?
check "wrong kubectl context: refuses" "$rc" "1"
check "wrong context: nothing deleted" "$(grep -c 'kubectl.* delete ' "$T/log")" "0"

: > "$T/log"; STUB_PV_LEFT=1 bash "$SCRIPT" bucket ec-a us-east-2 "$C" > /dev/null 2>&1; rc=$?
check "persistent volumes remain: refuses" "$rc" "1"
check "volumes check not reached when PVs remain" "$(grep -c 'guard .* volumes' "$T/log")" "0"

: > "$T/log"; STUB_VOLUMES_RC=1 bash "$SCRIPT" bucket ec-a us-east-2 "$C" > /dev/null 2>&1; rc=$?
check "tagged EBS volume remains: refuses" "$rc" "1"

: > "$T/log"; bash "$SCRIPT" bucket ec-a us-east-2 "$C" > "$T/out" 2>&1; rc=$?
check "all clear: exit 0" "$rc" "0"
order="$(grep -n 'delete' "$T/log" | sed -E 's/.*delete ([a-z,]+).*/\1/' | tr '\n' ' ')"
echo "     delete order: $order"
first_guard="$(grep -n 'guard .* complete' "$T/log" | head -1 | cut -d: -f1)"; first_kube="$(grep -n '^kubectl' "$T/log" | head -1 | cut -d: -f1)"
check "completeness gate runs before any kubectl call" "$([ "$first_guard" -lt "$first_kube" ] && echo yes || echo no)" "yes"
check "chaos resources deleted before Flink" "$([ "$(grep -n 'delete podchaos' "$T/log" | head -1 | cut -d: -f1)" -lt "$(grep -n 'delete flinkdeployment' "$T/log" | head -1 | cut -d: -f1)" ] && echo yes || echo no)" "yes"
check "Kafka deleted before the PVC sweep" "$([ "$(grep -n 'delete kafka,kafkanodepool' "$T/log" | head -1 | cut -d: -f1)" -lt "$(grep -n 'delete pvc' "$T/log" | head -1 | cut -d: -f1)" ] && echo yes || echo no)" "yes"
check "volume check is the last step" "$(tail -1 "$T/log" | grep -c 'guard .* volumes')" "1"
check "every kubectl call is pinned to the cluster context" "$(grep '^kubectl' "$T/log" | grep -v -e 'config current-context' -e 'get ns streaming' | grep -vc -e '--context streaming-anomaly-detection-ec-a')" "0"
exit $fail
