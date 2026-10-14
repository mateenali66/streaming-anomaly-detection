#!/usr/bin/env bash
# Submit the Flink jobs of one workload to the session cluster. Idempotent: a job whose name is already RUNNING or RESTARTING is
# left alone. Baked into the flink-jobs image at /opt/flink/bin/submit-jobs.sh, so the same file serves
#   - the `flink-submit` Job (k8s/<workload>/flink-submit-job.yaml), which talks to the REST service, and
#   - the orchestrator, which may run it again after a redeploy:  kubectl exec <jobmanager pod> -- /opt/flink/bin/submit-jobs.sh
# Every job is submitted by run-job.sh (next to this file), which carries the job settings: parallelism 4, checkpoints every
# 60 s exactly once, RocksDB incremental state (REVIEW4_LIVE_CONTRACT L1). Never call `flink run` directly.
#
# Usage: submit-jobs.sh ECOMMERCE|IOT          (WORKLOAD env is used when no argument is given; there is NO default)
# Env:   FLINK_REST            host:port of the REST endpoint (default streaming-flink-rest.streaming.svc.cluster.local:8081)
#        FLINK_BIN             flink CLI (default /opt/flink/bin/flink; tests point it at a stub)
#        SUBMIT_TIMEOUT_SEC    how long to wait for every job to be RUNNING (default 300)
#        SUBMIT_POLL_SEC       poll interval (default 5)
#        RUN_JOB               the one-job submitter (default: run-job.sh next to this script)
# Exit:  0 all expected jobs RUNNING; 1 a submit failed or the wait timed out; 2 bad usage.
#
# The jobs start from the latest Kafka offsets (OffsetsInitializer.latest(), unchanged from R1) and without a savepoint.
set -u
FLINK_BIN="${FLINK_BIN:-/opt/flink/bin/flink}"
FLINK_REST="${FLINK_REST:-streaming-flink-rest.streaming.svc.cluster.local:8081}"
SUBMIT_TIMEOUT_SEC="${SUBMIT_TIMEOUT_SEC:-300}"
SUBMIT_POLL_SEC="${SUBMIT_POLL_SEC:-5}"
USRLIB="${USRLIB:-/opt/flink/usrlib}"
RUN_JOB="${RUN_JOB:-$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)/run-job.sh}"
export FLINK_BIN FLINK_REST USRLIB

WORKLOAD_ARG="${1:-${WORKLOAD:-}}"
case "$WORKLOAD_ARG" in
  ECOMMERCE)
    NAMES=("Event Enrichment Job" "Windowed Aggregation Job" "Fraud Detection Job")
    CLASSES=("com.phonotech.streaming.enrichment.EnrichmentJob" "com.phonotech.streaming.aggregation.AggregationJob" "com.phonotech.streaming.fraud.FraudDetectionJob")
    JARS=("enrichment.jar" "aggregation.jar" "fraud.jar")
    ;;
  IOT)
    NAMES=("IoT Event Enrichment Job" "IoT Per-Location Aggregation Job" "IoT Threshold Alert Job")
    CLASSES=("com.phonotech.streaming.iot.enrichment.IoTEnrichmentJob" "com.phonotech.streaming.iot.aggregation.IoTAggregationJob" "com.phonotech.streaming.iot.alert.IoTAlertJob")
    JARS=("iot-enrichment.jar" "iot-aggregation.jar" "iot-alert.jar")
    ;;
  *)
    echo "submit-jobs.sh: WORKLOAD must be ECOMMERCE or IOT (got '${WORKLOAD_ARG}'). There is no default." >&2
    exit 2
    ;;
esac

# Prints "name|STATE" for every job the cluster knows about that is not finished.
list_jobs() {
  "$FLINK_BIN" list -m "$FLINK_REST" -a 2>/dev/null \
    | sed -n 's/^.* : [0-9a-f]\{32\} : \(.*\) (\([A-Z_]*\))$/\1|\2/p'
}

state_of() { # name -> RUNNING if any entry with that name runs, else RESTARTING, else CREATED, else the last state seen, or empty
  list_jobs | awk -F'|' -v n="$1" '
    $1 == n { seen = $2; if ($2 == "RUNNING") r = 1; else if ($2 == "RESTARTING") t = 1; else if ($2 == "CREATED") c = 1 }
    END { print r ? "RUNNING" : (t ? "RESTARTING" : (c ? "CREATED" : seen)) }'
}

for i in "${!NAMES[@]}"; do
  name="${NAMES[$i]}"
  st="$(state_of "$name")"
  if [ "$st" = "RUNNING" ] || [ "$st" = "RESTARTING" ] || [ "$st" = "CREATED" ]; then
    echo "skip   : $name is $st"
    continue
  fi
  echo "submit : $name (${CLASSES[$i]})"
  if ! bash "$RUN_JOB" "${CLASSES[$i]}" "${JARS[$i]}"; then
    echo "FAILED to submit: $name" >&2
    exit 1
  fi
done

deadline=$(( $(date +%s) + SUBMIT_TIMEOUT_SEC ))
while :; do
  missing=()
  for name in "${NAMES[@]}"; do
    [ "$(state_of "$name")" = "RUNNING" ] || missing+=("$name")
  done
  if [ "${#missing[@]}" -eq 0 ]; then
    echo "JOBS_RUNNING=${#NAMES[@]} workload=$WORKLOAD_ARG"
    exit 0
  fi
  if [ "$(date +%s)" -ge "$deadline" ]; then
    echo "TIMEOUT waiting for RUNNING: ${missing[*]}" >&2
    exit 1
  fi
  sleep "$SUBMIT_POLL_SEC"
done
