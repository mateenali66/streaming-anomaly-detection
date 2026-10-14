#!/usr/bin/env bash
# Offline test of images/flink-jobs/submit-jobs.sh with a stub `flink` CLI. No cluster, no network.
set -u
HERE="$(cd "$(dirname "$0")" && pwd)"
SCRIPT="$HERE/../images/flink-jobs/submit-jobs.sh"
T="$(mktemp -d)"; trap 'rm -rf "$T"' EXIT
fail=0
check() { if [ "$2" = "$3" ]; then echo "ok   $1"; else echo "FAIL $1: got '$2' expected '$3'"; fail=1; fi; }

cat > "$T/flink" <<'STUB'
#!/usr/bin/env bash
# state file: lines "name|STATE"; a submit marks the job RUNNING unless STUB_FAIL_SUBMIT is set
S="$STUB_STATE"
case "$1" in
  list)
    echo "------------------ Running/Restarting Jobs -------------------"
    while IFS='|' read -r n st; do
      [ -n "$n" ] && echo "13.10.2026 17:24:01 : $(printf '%032d' "${#n}") : $n ($st)"
    done < "$S"
    echo "--------------------------------------------------------------"
    echo "No scheduled jobs."
    ;;
  run)
    echo "RUN $*" >> "$STUB_LOG"
    [ -n "${STUB_FAIL_SUBMIT:-}" ] && exit 1
    shift
    # map class to job name the way the jobs name themselves
    case "$*" in
      *EnrichmentJob*IoT*|*iot.enrichment*) echo "IoT Event Enrichment Job|RUNNING" >> "$S";;
      *iot.aggregation*) echo "IoT Per-Location Aggregation Job|RUNNING" >> "$S";;
      *iot.alert*) echo "IoT Threshold Alert Job|RUNNING" >> "$S";;
      *streaming.enrichment*) echo "Event Enrichment Job|RUNNING" >> "$S";;
      *streaming.aggregation*) echo "Windowed Aggregation Job|RUNNING" >> "$S";;
      *streaming.fraud*) echo "Fraud Detection Job|RUNNING" >> "$S";;
    esac
    ;;
esac
STUB
chmod +x "$T/flink"
export FLINK_BIN="$T/flink" STUB_STATE="$T/state" STUB_LOG="$T/log" SUBMIT_TIMEOUT_SEC=3 SUBMIT_POLL_SEC=1

# 1. no workload: refused
: > "$T/state"; : > "$T/log"
env -u WORKLOAD bash "$SCRIPT" > /dev/null 2>&1; rc=$?
check "no default workload (rc)" "$rc" "2"

# 2. empty cluster, ECOMMERCE: three submits, rc 0
: > "$T/state"; : > "$T/log"
bash "$SCRIPT" ECOMMERCE > "$T/out" 2>&1; rc=$?
check "ecommerce fresh rc" "$rc" "0"
check "ecommerce fresh submits" "$(grep -c '^RUN' "$T/log")" "3"
check "ecommerce marker" "$(grep -c 'JOBS_RUNNING=3 workload=ECOMMERCE' "$T/out")" "1"
# REVIEW4_LIVE_CONTRACT L1: every submit carries the job settings, so the client config of the pod does not decide them
for want in ' -p 4 ' ' -Dexecution.checkpointing.interval=60000 ' ' -Dexecution.checkpointing.mode=EXACTLY_ONCE ' \
            ' -Dexecution.checkpointing.timeout=120000 ' ' -Dexecution.checkpointing.min-pause=30000 ' \
            ' -Dexecution.checkpointing.externalized-checkpoint-retention=RETAIN_ON_CANCELLATION ' \
            ' -Dstate.backend.type=rocksdb ' ' -Dstate.backend.incremental=true ' ' -Drestart-strategy.type=failure-rate ' \
            ' -Drestart-strategy.failure-rate.max-failures-per-interval=40 '; do
  check "every submit has${want}" "$(grep '^RUN' "$T/log" | grep -c -F -- "$want")" "3"
done
check "submits target the REST service" "$(grep '^RUN' "$T/log" | grep -c -- '-m streaming-flink-rest.streaming.svc.cluster.local:8081')" "3"
check "no restore path on a fresh submit" "$(grep '^RUN' "$T/log" | grep -c -- ' -s ')" "0"

# 3. rerun: idempotent, no new submits
: > "$T/log"
bash "$SCRIPT" ECOMMERCE > "$T/out" 2>&1; rc=$?
check "ecommerce rerun rc" "$rc" "0"
check "ecommerce rerun submits" "$(grep -c '^RUN' "$T/log")" "0"

# 4. one job missing (killed) and one FAILED: only those are resubmitted
printf 'Event Enrichment Job|RUNNING\nWindowed Aggregation Job|FAILED\n' > "$T/state"; : > "$T/log"
bash "$SCRIPT" ECOMMERCE > "$T/out" 2>&1; rc=$?
check "partial rc" "$rc" "0"
check "partial submits" "$(grep -c '^RUN' "$T/log")" "2"

# 5. IoT names
: > "$T/state"; : > "$T/log"
bash "$SCRIPT" IOT > "$T/out" 2>&1; rc=$?
check "iot rc" "$rc" "0"
check "iot jobs" "$(grep -c 'IoT' "$T/state")" "3"
check "iot never submits ecommerce classes" "$(grep -c 'streaming.fraud\|streaming.enrichment\|streaming.aggregation' "$T/log")" "0"

# 6. submit failure: rc 1
: > "$T/state"; : > "$T/log"
STUB_FAIL_SUBMIT=1 bash "$SCRIPT" ECOMMERCE > "$T/out" 2>&1; rc=$?
check "submit failure rc" "$rc" "1"

# 7. job never reaches RUNNING: timeout rc 1
printf 'Event Enrichment Job|RUNNING\nWindowed Aggregation Job|RUNNING\nFraud Detection Job|RESTARTING\n' > "$T/state"; : > "$T/log"
bash "$SCRIPT" ECOMMERCE > "$T/out" 2>&1; rc=$?
check "restarting counts as present, not running (rc)" "$rc" "1"
check "restarting is not resubmitted" "$(grep -c '^RUN' "$T/log")" "0"

# 8. run-job.sh on its own, as the orchestrator's remediation runs it in the JobManager: restore path, absolute jar, usage
: > "$T/state"; : > "$T/log"
bash "$HERE/../images/flink-jobs/run-job.sh" com.phonotech.streaming.fraud.FraudDetectionJob fraud.jar s3://b/scratch/ec-a/flink-checkpoints/abc/chk-7 > "$T/out" 2>&1; rc=$?
check "run-job with a checkpoint: rc" "$rc" "0"
check "run-job with a checkpoint: -s before -c, jar under usrlib" "$(grep -c -- '-s s3://b/scratch/ec-a/flink-checkpoints/abc/chk-7 -c com.phonotech.streaming.fraud.FraudDetectionJob /opt/flink/usrlib/fraud.jar$' "$T/log")" "1"
check "run-job with a checkpoint: settings present" "$(grep -c -- ' -p 4 .*-Dstate.backend.type=rocksdb' "$T/log")" "1"
: > "$T/log"
bash "$HERE/../images/flink-jobs/run-job.sh" com.phonotech.streaming.fraud.FraudDetectionJob /x/fraud.jar > /dev/null 2>&1
check "run-job keeps an absolute jar path" "$(grep -c -- '-c com.phonotech.streaming.fraud.FraudDetectionJob /x/fraud.jar$' "$T/log")" "1"
bash "$HERE/../images/flink-jobs/run-job.sh" onlyone > /dev/null 2>&1; rc=$?
check "run-job bad usage rc" "$rc" "2"
# the time values with a space reach the CLI as one argument each
cat > "$T/argstub" <<'STUB'
#!/usr/bin/env bash
for a in "$@"; do echo "ARG<$a>"; done >> "$STUB_LOG"
STUB
chmod +x "$T/argstub"
: > "$T/log"; FLINK_BIN="$T/argstub" bash "$HERE/../images/flink-jobs/run-job.sh" C j.jar > /dev/null 2>&1
check "run-job passes '10 min' as one argument" "$(grep -c '^ARG<-Drestart-strategy.failure-rate.failure-rate-interval=10 min>$' "$T/log")" "1"

exit $fail
